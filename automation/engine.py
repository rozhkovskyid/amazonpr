import asyncio
import logging
from datetime import datetime

from ingestion.alibaba_client import fetch_and_normalize
from ingestion.amazon_client import build_market_snapshot
from analysis.claude_analyst import analyse_opportunity
from database.db import (
    upsert_product, log_search, upsert_market_snapshot,
    get_market_snapshot, upsert_opportunity, get_opportunity, get_pool,
)
from automation.events import emit

logger = logging.getLogger(__name__)

MARKET_ANALYSIS_LIMIT = 15
AI_ANALYSIS_LIMIT = 5


def _is_viable(product: dict, snapshot: dict) -> bool:
    price_low = product.get("price_low") or 0
    avg_price = snapshot.get("avg_price") or 0
    competition = snapshot.get("competition_level") or "high"

    if price_low <= 0 or avg_price <= 0:
        return False
    if avg_price < 8:
        return False

    markup = avg_price / price_low

    if competition == "high" and markup < 4.0:
        return False
    if competition != "high" and markup < 2.5:
        return False

    return True


async def _log_run(category, products_found, markets_analyzed, ai_analyzed, started_at, status="completed"):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO automation_runs
               (category, products_found, markets_analyzed, ai_analyzed, status, started_at, completed_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7)""",
            category, products_found, markets_analyzed, ai_analyzed,
            status, started_at, datetime.utcnow().isoformat()
        )


async def run_category_scan(category: str) -> dict:
    logger.info(f"[Automation] Starting scan: {category}")
    started_at = datetime.utcnow().isoformat()
    products_found = markets_analyzed = ai_analyzed = 0

    await emit("scan_started", category=category)

    try:
        # ── Step 1: Alibaba ─────────────────────────────────────────────
        await emit("stage", stage="alibaba", status="running", category=category,
                   msg=f"Searching Alibaba for \"{category}\"…")

        products = await fetch_and_normalize(category, page=1, fetch_details=False)
        products_found = len(products)
        for p in products:
            await upsert_product(p)
        await log_search(category, products_found)

        await emit("stage", stage="alibaba", status="done", category=category,
                   count=products_found, msg=f"Found {products_found} suppliers for \"{category}\"")

        # ── Step 2: Amazon market analysis ──────────────────────────────
        await emit("stage", stage="amazon", status="running", category=category,
                   msg=f"Checking Amazon market for {min(MARKET_ANALYSIS_LIMIT, products_found)} products…")

        for p in products[:MARKET_ANALYSIS_LIMIT]:
            try:
                if await get_market_snapshot(p.product_id):
                    markets_analyzed += 1
                    await emit("market_done", product_id=p.product_id,
                               title=p.title[:60], cached=True,
                               markets_analyzed=markets_analyzed)
                    continue

                await emit("market_checking", title=p.title[:60],
                           msg=f"Amazon search: {p.title[:50]}…")

                snapshot = await build_market_snapshot(
                    supplier_product_id=p.product_id,
                    product_title=p.title,
                )
                await upsert_market_snapshot(snapshot)
                markets_analyzed += 1

                await emit("market_done", product_id=p.product_id,
                           title=p.title[:60], cached=False,
                           competition=snapshot.competition_level,
                           avg_price=round(snapshot.avg_price or 0, 2),
                           markets_analyzed=markets_analyzed,
                           msg=f"Amazon: {snapshot.competition_level} competition · avg ${snapshot.avg_price:.2f}" if snapshot.avg_price else "Amazon data fetched")

                await asyncio.sleep(1.5)
            except Exception as e:
                await emit("market_error", title=p.title[:60], error=str(e)[:80])
                logger.warning(f"[Automation] Market analysis failed {p.product_id}: {e}")

        await emit("stage", stage="amazon", status="done", category=category,
                   count=markets_analyzed, msg=f"Market data collected for {markets_analyzed} products")

        # ── Step 3: AI analysis ─────────────────────────────────────────
        await emit("stage", stage="ai", status="running", category=category,
                   msg=f"Running AI viability filter…")

        from database.db import get_products
        db_products = {r["product_id"]: r for r in await get_products(query=category, limit=MARKET_ANALYSIS_LIMIT)}

        viable_analyzed = 0
        for p in products[:MARKET_ANALYSIS_LIMIT]:
            if viable_analyzed >= AI_ANALYSIS_LIMIT:
                break
            try:
                if await get_opportunity(p.product_id):
                    continue

                snapshot = await get_market_snapshot(p.product_id)
                if not snapshot:
                    continue

                db_row = db_products.get(p.product_id, {})
                product_dict = {
                    "product_id": p.product_id,
                    "title": p.title,
                    "price_range": db_row.get("price_range") or p.pricing.range_formatted,
                    "price_low": db_row.get("price_low") or p.pricing.lowest_unit_price,
                    "moq": db_row.get("moq") or p.pricing.minimum_order_qty,
                    "moq_unit": db_row.get("moq_unit") or p.pricing.minimum_order_unit,
                    "supplier_name": p.supplier.name,
                    "supplier_country": p.supplier.country,
                    "is_gold_supplier": int(p.supplier.is_gold_supplier),
                    "has_trade_assurance": int(p.supplier.has_trade_assurance),
                    "supplier_quality_score": p.supplier_quality_score,
                    "employee_count": p.supplier.employee_count,
                    "years_active": p.seller.years_active,
                    "spec_summary": p.specifications.summary,
                }

                if not _is_viable(product_dict, snapshot):
                    await emit("ai_skipped", title=p.title[:60],
                               reason=f"Filtered: {snapshot.get('competition_level')} competition, {(snapshot.get('avg_price') or 0) / max(db_row.get('price_low') or 1, 0.01):.1f}x markup")
                    continue

                await emit("ai_running", title=p.title[:60],
                           msg=f"Claude analysing: {p.title[:55]}…")

                analysis = await analyse_opportunity(product_dict, snapshot)
                await upsert_opportunity(analysis)
                ai_analyzed += 1
                viable_analyzed += 1

                is_opportunity = analysis.score in ("strong", "average")
                await emit("ai_done",
                           product_id=p.product_id,
                           title=p.title[:60],
                           score=analysis.score,
                           is_opportunity=is_opportunity,
                           summary=analysis.summary[:120] if analysis.summary else "",
                           ai_analyzed=ai_analyzed,
                           msg=f"AI scored {analysis.score.upper()}: {p.title[:45]}")

                if is_opportunity:
                    await emit("opportunity_found",
                               product_id=p.product_id,
                               title=p.title[:80],
                               score=analysis.score,
                               summary=analysis.summary[:150] if analysis.summary else "",
                               price_low=db_row.get("price_low"),
                               avg_price=snapshot.get("avg_price"),
                               category=category)

                await asyncio.sleep(2)

            except Exception as e:
                await emit("ai_error", title=p.title[:60], error=str(e)[:80])
                logger.warning(f"[Automation] AI analysis failed {p.product_id}: {e}")

        await emit("stage", stage="ai", status="done", category=category,
                   count=ai_analyzed, msg=f"AI analysis complete: {ai_analyzed} products scored")

        await emit("scan_completed",
                   category=category,
                   products_found=products_found,
                   markets_analyzed=markets_analyzed,
                   ai_analyzed=ai_analyzed,
                   msg=f"Scan complete — {products_found} products · {markets_analyzed} markets · {ai_analyzed} AI")

        logger.info(f"[Automation] [{category}] Complete — P:{products_found} M:{markets_analyzed} AI:{ai_analyzed}")

    except Exception as e:
        logger.error(f"[Automation] Category scan failed [{category}]: {e}")
        await emit("scan_error", category=category, error=str(e)[:100])
        await _log_run(category, products_found, markets_analyzed, ai_analyzed, started_at, status="failed")
        return {"category": category, "status": "failed", "error": str(e)}

    await _log_run(category, products_found, markets_analyzed, ai_analyzed, started_at)
    return {
        "category": category,
        "products_found": products_found,
        "markets_analyzed": markets_analyzed,
        "ai_analyzed": ai_analyzed,
        "started_at": started_at,
        "completed_at": datetime.utcnow().isoformat(),
        "status": "completed",
    }
