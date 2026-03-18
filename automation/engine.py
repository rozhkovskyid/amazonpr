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

# ── Tuning constants ──────────────────────────────────────────────────────────
PRODUCTS_PER_CATEGORY = 30   # how many Alibaba products to pull per category
AMAZON_CONCURRENCY    = 6    # parallel Amazon search calls (semaphore)
AI_SCORE_THRESHOLD    = 58   # math score required to trigger AI analysis
WATCH_THRESHOLD       = 40   # minimum score worth storing/logging


# ── Pure math opportunity scoring ────────────────────────────────────────────
def score_opportunity(product: dict, snapshot: dict) -> tuple[int, str]:
    """
    Score a product 0-100 using only numbers — no AI involved.
    Returns (score, reason_string).
    """
    price_low        = product.get("price_low") or 0
    avg_price        = snapshot.get("avg_price") or 0
    avg_reviews      = snapshot.get("avg_reviews") or 0
    avg_rating       = snapshot.get("avg_rating") or 0
    competition      = snapshot.get("competition_level") or "high"
    under_100        = snapshot.get("listings_under_100_reviews") or 0
    over_1000        = snapshot.get("listings_over_1000_reviews") or 0
    n_listings       = max(snapshot.get("listings_analyzed") or 1, 1)

    # ── Hard cutoffs (disqualify immediately) ────────────────────────────────
    if price_low <= 0 or avg_price < 10:
        return 0, f"avg price ${avg_price:.0f} < $10 minimum"

    markup = avg_price / price_low
    if markup < 2.5:
        return 0, f"markup {markup:.1f}x below 2.5x minimum"

    score = 0

    # 1. Markup ratio (0-35 pts) — most important
    if   markup >= 10: score += 35
    elif markup >= 7:  score += 30
    elif markup >= 5:  score += 24
    elif markup >= 4:  score += 18
    elif markup >= 3:  score += 10
    else:              score += 4

    # 2. Competition gap (0-30 pts)
    weak_ratio     = under_100 / n_listings
    dominant_ratio = over_1000 / n_listings
    if competition == "low":
        score += 30
    elif competition == "medium":
        score += 25 if dominant_ratio < 0.2 else 15
    elif competition == "high":
        if   weak_ratio > 0.4:  score += 12   # lots of weak players = can enter
        elif weak_ratio > 0.2:  score += 6

    # 3. Demand signal via reviews (0-20 pts)
    if   avg_reviews >= 500: score += 20
    elif avg_reviews >= 200: score += 15
    elif avg_reviews >= 100: score += 10
    elif avg_reviews >= 50:  score += 6
    elif avg_reviews >= 20:  score += 3

    # 4. FBA-friendly price band $15-$80 (0-10 pts)
    if   20 <= avg_price <= 60:   score += 10
    elif 15 <= avg_price <= 80:   score += 7
    elif 10 <= avg_price <= 120:  score += 4

    # 5. Rating gap = unhappy buyers = differentiation opportunity (0-5 pts)
    if   avg_rating and avg_rating < 3.8: score += 5
    elif avg_rating and avg_rating < 4.2: score += 3

    reason = (
        f"math={score} markup={markup:.1f}x "
        f"comp={competition} reviews={int(avg_reviews)} "
        f"weak={int(weak_ratio*100)}%"
    )
    return score, reason


# ── Amazon snapshot helper (returns DB dict always) ──────────────────────────
async def _fetch_snapshot(p, sem, markets_ref: list):
    """Fetch or retrieve Amazon snapshot for one product. Returns DB dict or None."""
    async with sem:
        try:
            cached = await get_market_snapshot(p.product_id)
            if cached:
                markets_ref[0] += 1
                await emit("market_done", product_id=p.product_id,
                           title=p.title[:60], cached=True,
                           markets_analyzed=markets_ref[0])
                return cached

            await emit("market_checking", title=p.title[:60],
                       msg=f"Amazon: {p.title[:50]}…")

            snapshot = await build_market_snapshot(
                supplier_product_id=p.product_id,
                product_title=p.title,
            )
            await upsert_market_snapshot(snapshot)
            markets_ref[0] += 1

            snap_dict = await get_market_snapshot(p.product_id)
            comp  = snapshot.competition_level
            avg_p = round(snapshot.avg_price or 0, 2)
            await emit("market_done", product_id=p.product_id,
                       title=p.title[:60], cached=False,
                       competition=comp, avg_price=avg_p,
                       markets_analyzed=markets_ref[0],
                       msg=f"{comp} · avg ${avg_p}" if avg_p else "fetched")
            return snap_dict

        except Exception as e:
            await emit("market_error", title=p.title[:60], error=str(e)[:80])
            logger.warning(f"[Engine] Amazon failed {p.product_id}: {e}")
            return None


# ── Main scan ─────────────────────────────────────────────────────────────────
async def run_category_scan(category: str) -> dict:
    logger.info(f"[Engine] Scan: {category}")
    started_at     = datetime.utcnow().isoformat()
    products_found = markets_analyzed = ai_analyzed = opps_found = 0

    await emit("scan_started", category=category)

    try:
        # ── 1. Alibaba ────────────────────────────────────────────────────────
        await emit("stage", stage="alibaba", status="running", category=category,
                   msg=f'Searching Alibaba: "{category}"…')

        products = await fetch_and_normalize(category, page=1, fetch_details=False)
        products_found = len(products)
        for p in products:
            await upsert_product(p)
        await log_search(category, products_found)

        await emit("stage", stage="alibaba", status="done", category=category,
                   count=products_found,
                   msg=f"Found {products_found} products for \"{category}\"")

        if not products:
            return _build_result(category, 0, 0, 0, started_at)

        # ── 2. Parallel Amazon lookups ────────────────────────────────────────
        batch = products[:PRODUCTS_PER_CATEGORY]
        await emit("stage", stage="amazon", status="running", category=category,
                   msg=f"Amazon: scanning {len(batch)} products ({AMAZON_CONCURRENCY} parallel)…")

        sem        = asyncio.Semaphore(AMAZON_CONCURRENCY)
        mref       = [0]
        snap_dicts = await asyncio.gather(*[_fetch_snapshot(p, sem, mref) for p in batch])
        markets_analyzed = mref[0]

        await emit("stage", stage="amazon", status="done", category=category,
                   count=markets_analyzed,
                   msg=f"Amazon done: {markets_analyzed}/{len(batch)} fetched")

        # ── 3. Math scoring + selective AI ───────────────────────────────────
        await emit("stage", stage="ai", status="running", category=category,
                   msg=f"Scoring {markets_analyzed} products mathematically…")

        from database.db import get_products
        db_rows = {r["product_id"]: r
                   for r in await get_products(query=category, limit=100)}

        for p, snap in zip(batch, snap_dicts):
            if not snap:
                continue

            db_row = db_rows.get(p.product_id, {})
            product_dict = {
                "product_id":           p.product_id,
                "title":                p.title,
                "price_range":          db_row.get("price_range") or p.pricing.range_formatted,
                "price_low":            db_row.get("price_low")   or p.pricing.lowest_unit_price,
                "moq":                  db_row.get("moq")         or p.pricing.minimum_order_qty,
                "moq_unit":             db_row.get("moq_unit")    or p.pricing.minimum_order_unit,
                "supplier_name":        p.supplier.name,
                "supplier_country":     p.supplier.country,
                "is_gold_supplier":     int(p.supplier.is_gold_supplier),
                "has_trade_assurance":  int(p.supplier.has_trade_assurance),
                "supplier_quality_score": p.supplier_quality_score,
            }

            math_score, reason = score_opportunity(product_dict, snap)

            if math_score < WATCH_THRESHOLD:
                await emit("ai_skipped", title=p.title[:60], reason=reason)
                continue

            if await get_opportunity(p.product_id):
                continue  # already analysed

            if math_score >= AI_SCORE_THRESHOLD:
                await emit("ai_running", title=p.title[:60],
                           msg=f"AI (math={math_score}): {p.title[:50]}…")
                try:
                    analysis = await analyse_opportunity(product_dict, snap)
                    await upsert_opportunity(analysis)
                    ai_analyzed += 1

                    is_opp = analysis.score in ("strong", "average")
                    if is_opp:
                        opps_found += 1

                    await emit("ai_done",
                               product_id=p.product_id,
                               title=p.title[:60],
                               score=analysis.score,
                               math_score=math_score,
                               is_opportunity=is_opp,
                               summary=(analysis.summary or "")[:120],
                               ai_analyzed=ai_analyzed,
                               msg=f"AI: {analysis.score.upper()} (math {math_score}) · {p.title[:45]}")

                    if is_opp:
                        await emit("opportunity_found",
                                   product_id=p.product_id,
                                   title=p.title[:80],
                                   score=analysis.score,
                                   math_score=math_score,
                                   summary=(analysis.summary or "")[:150],
                                   price_low=product_dict.get("price_low"),
                                   avg_price=snap.get("avg_price"),
                                   category=category)
                except Exception as e:
                    await emit("ai_error", title=p.title[:60], error=str(e)[:80])
                    logger.warning(f"[Engine] AI failed {p.product_id}: {e}")
            else:
                await emit("ai_skipped", title=p.title[:60],
                           reason=f"watch (math={math_score}) — below AI threshold {AI_SCORE_THRESHOLD}")

        await emit("stage", stage="ai", status="done", category=category,
                   count=ai_analyzed,
                   msg=f"Done: {ai_analyzed} AI · {opps_found} opportunities")

        await emit("scan_completed",
                   category=category,
                   products_found=products_found,
                   markets_analyzed=markets_analyzed,
                   ai_analyzed=ai_analyzed,
                   opportunities_found=opps_found,
                   msg=f"✓ {category} — {products_found}p · {markets_analyzed}m · {ai_analyzed}AI · {opps_found}opps")

    except Exception as e:
        logger.error(f"[Engine] Scan failed [{category}]: {e}")
        await emit("scan_error", category=category, error=str(e)[:100])
        await _log_run(category, products_found, markets_analyzed, ai_analyzed, started_at, "failed")
        return {"category": category, "status": "failed", "error": str(e)}

    await _log_run(category, products_found, markets_analyzed, ai_analyzed, started_at)
    return _build_result(category, products_found, markets_analyzed, ai_analyzed, started_at)


def _build_result(category, p, m, a, started_at, status="completed"):
    return {
        "category":        category,
        "products_found":  p,
        "markets_analyzed": m,
        "ai_analyzed":     a,
        "started_at":      started_at,
        "completed_at":    datetime.utcnow().isoformat(),
        "status":          status,
    }


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
