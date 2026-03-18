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

logger = logging.getLogger(__name__)

# Max products to run Amazon analysis on per scan cycle
MARKET_ANALYSIS_LIMIT = 15
# Max AI analyses per scan cycle (controls Claude API spend)
AI_ANALYSIS_LIMIT = 5


def _is_viable(product: dict, snapshot: dict) -> bool:
    """Filter products before spending Claude API credits."""
    price_low = product.get("price_low") or 0
    avg_price = snapshot.get("avg_price") or 0
    competition = snapshot.get("competition_level") or "high"

    # Need real price data
    if price_low <= 0 or avg_price <= 0:
        return False

    # Skip ultra-cheap Amazon items (margins never work after FBA fees)
    if avg_price < 8:
        return False

    markup = avg_price / price_low

    # High competition: only worth Claude's time if markup is exceptional (4x+)
    if competition == "high" and markup < 4.0:
        return False

    # Medium/low competition: standard 2.5x minimum
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
    """Full pipeline for one category: Alibaba → Amazon → AI."""
    logger.info(f"[Automation] Starting scan: {category}")
    started_at = datetime.utcnow().isoformat()
    products_found = markets_analyzed = ai_analyzed = 0

    try:
        # ── Step 1: Alibaba search ──────────────────────────────────────
        products = await fetch_and_normalize(category, page=1, fetch_details=False)
        products_found = len(products)
        for p in products:
            await upsert_product(p)
        await log_search(category, products_found)
        logger.info(f"[Automation] [{category}] {products_found} products fetched")

        # ── Step 2: Amazon market analysis ─────────────────────────────
        for p in products[:MARKET_ANALYSIS_LIMIT]:
            try:
                if await get_market_snapshot(p.product_id):
                    markets_analyzed += 1
                    continue
                snapshot = await build_market_snapshot(
                    supplier_product_id=p.product_id,
                    product_title=p.title,
                )
                await upsert_market_snapshot(snapshot)
                markets_analyzed += 1
                await asyncio.sleep(1.5)
            except Exception as e:
                logger.warning(f"[Automation] Market analysis failed {p.product_id}: {e}")

        logger.info(f"[Automation] [{category}] {markets_analyzed} market snapshots done")

        # ── Step 3: AI opportunity analysis (viable products only) ──────
        # Fetch DB rows — they have price_low reliably stored (Pydantic objects may lack it without fetch_details)
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

                # Use DB row for reliable price data
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
                    continue

                analysis = await analyse_opportunity(product_dict, snapshot)
                await upsert_opportunity(analysis)
                ai_analyzed += 1
                viable_analyzed += 1
                logger.info(f"[Automation] [{category}] AI score for {p.product_id}: {analysis.score}")
                await asyncio.sleep(2)

            except Exception as e:
                logger.warning(f"[Automation] AI analysis failed {p.product_id}: {e}")

        logger.info(f"[Automation] [{category}] Complete — P:{products_found} M:{markets_analyzed} AI:{ai_analyzed}")

    except Exception as e:
        logger.error(f"[Automation] Category scan failed [{category}]: {e}")
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
