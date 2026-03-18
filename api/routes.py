from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from ingestion.alibaba_client import fetch_and_normalize
from ingestion.amazon_client import build_market_snapshot
from database.db import (
    upsert_product, log_search, get_products, get_recent_searches,
    upsert_market_snapshot, get_market_snapshot, get_products_with_snapshots,
    upsert_opportunity, get_opportunity, get_products_with_analyses
)

router = APIRouter()


class SearchRequest(BaseModel):
    query: str
    page: int = 1
    fetch_details: bool = False


class MarketAnalyzeRequest(BaseModel):
    product_id: str
    product_title: str
    custom_query: str = None  # optional override for the Amazon search query


@router.post("/api/search")
async def run_search(req: SearchRequest):
    """Fetch Alibaba products for a query, normalize, and store."""
    try:
        products = await fetch_and_normalize(req.query, req.page, req.fetch_details)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Alibaba API error: {str(e)}")

    for p in products:
        await upsert_product(p)

    await log_search(req.query, len(products))

    return {
        "query": req.query,
        "products_found": len(products),
        "message": f"Fetched and stored {len(products)} products for '{req.query}'",
    }


@router.post("/api/analyze-market")
async def analyze_market(req: MarketAnalyzeRequest):
    """
    Run Amazon market analysis for a single supplier product.
    Searches Amazon, builds a MarketSnapshot, and stores it.
    """
    try:
        snapshot = await build_market_snapshot(
            supplier_product_id=req.product_id,
            product_title=req.product_title,
            custom_query=req.custom_query,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Amazon API error: {str(e)}")

    await upsert_market_snapshot(snapshot)

    return {
        "product_id": req.product_id,
        "amazon_search_query": snapshot.search_query,
        "competition_level": snapshot.competition_level,
        "avg_price": snapshot.avg_price,
        "avg_reviews": snapshot.avg_reviews,
        "listings_analyzed": snapshot.listings_analyzed,
        "message": f"Market snapshot saved for product {req.product_id}",
    }


@router.post("/api/analyze-market-batch")
async def analyze_market_batch(background_tasks: BackgroundTasks, query: str):
    """
    Kick off Amazon market analysis for all stored products from a given search query.
    Runs in the background so the response is immediate.
    """
    products = await get_products(query=query, limit=100)
    if not products:
        raise HTTPException(status_code=404, detail=f"No products found for query '{query}'")

    async def _run_batch():
        for p in products:
            try:
                snapshot = await build_market_snapshot(
                    supplier_product_id=p["product_id"],
                    product_title=p["title"],
                )
                await upsert_market_snapshot(snapshot)
            except Exception:
                pass  # Don't let one failure abort the batch

    background_tasks.add_task(_run_batch)

    return {
        "message": f"Market analysis started for {len(products)} products from '{query}'",
        "products_queued": len(products),
    }


@router.post("/api/analyse-opportunity/{product_id}")
async def analyse_opportunity(product_id: str):
    """
    Run Claude AI analysis on a product that already has both
    Alibaba data and an Amazon market snapshot.
    """
    from analysis.claude_analyst import analyse_opportunity as run_analysis

    from database.db import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        p_row = await conn.fetchrow("SELECT * FROM products WHERE product_id = $1", product_id)
        s_row = await conn.fetchrow("SELECT * FROM market_snapshots WHERE supplier_product_id = $1", product_id)

    if not p_row:
        raise HTTPException(status_code=404, detail="Product not found")
    if not s_row:
        raise HTTPException(status_code=400, detail="Run Amazon market analysis first before AI analysis")

    product = dict(p_row)
    snapshot = dict(s_row)

    try:
        analysis = await run_analysis(product, snapshot)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"AI analysis failed: {str(e)}")

    await upsert_opportunity(analysis)
    return analysis.model_dump()


@router.get("/api/opportunity/{product_id}")
async def get_opportunity_route(product_id: str):
    """Return stored AI analysis for a product."""
    result = await get_opportunity(product_id)
    if not result:
        raise HTTPException(status_code=404, detail="No AI analysis found for this product")
    return result


@router.get("/api/products")
async def list_products(query: str = None, limit: int = 50, offset: int = 0):
    """Return stored products joined with market snapshots and AI analyses."""
    products = await get_products_with_analyses(query=query, limit=limit, offset=offset)
    return {"products": products, "count": len(products)}


@router.get("/api/market/{product_id}")
async def get_market(product_id: str):
    """Return the Amazon market snapshot for a specific product."""
    snapshot = await get_market_snapshot(product_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="No market snapshot found for this product")
    return snapshot


@router.get("/api/amazon/product/{asin}")
async def get_amazon_product(asin: str):
    """Fetch full Amazon product detail for a given ASIN."""
    from ingestion.amazon_client import get_amazon_detail
    try:
        detail = await get_amazon_detail(asin)
        return detail.model_dump()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/api/searches")
async def list_searches():
    searches = await get_recent_searches()
    return {"searches": searches}


@router.get("/api/automation/status")
async def automation_status():
    from automation.scheduler import get_status
    return get_status()


@router.get("/api/automation/stream")
async def automation_stream():
    """SSE stream of real-time automation events."""
    import asyncio
    from automation.events import subscribe, unsubscribe

    q = subscribe()

    async def generator():
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=20)
                    yield f"data: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield "data: {\"type\":\"ping\"}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            unsubscribe(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/api/automation/start")
async def automation_start():
    from automation.scheduler import start_scheduler
    start_scheduler(interval_minutes=30)
    return {"message": "Automation started"}


@router.post("/api/automation/pause")
async def automation_pause():
    from automation.scheduler import stop_scheduler
    stop_scheduler()
    return {"message": "Automation paused"}


@router.post("/api/automation/trigger")
async def automation_trigger(background_tasks: BackgroundTasks, category: str = None):
    from automation.scheduler import trigger_scan_now
    background_tasks.add_task(trigger_scan_now, category)
    return {"message": f"Scan triggered for: {category or 'auto-selected category'}"}


@router.get("/api/opportunities")
async def list_opportunities(limit: int = 50, offset: int = 0):
    import json
    from database.db import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*,
                ms.avg_price AS amz_avg_price, ms.min_price AS amz_min_price,
                ms.max_price AS amz_max_price, ms.avg_reviews AS amz_avg_reviews,
                ms.competition_level AS amz_competition_level,
                ms.avg_rating AS amz_avg_rating, ms.search_query AS amz_search_query,
                oa.score AS ai_score, oa.summary AS ai_summary,
                oa.margin_assessment AS ai_margin, oa.competition_analysis AS ai_competition,
                oa.differentiation_ideas AS ai_differentiation, oa.risk_flags AS ai_risks,
                oa.final_recommendation AS ai_recommendation, oa.analysed_at AS ai_analysed_at
            FROM products p
            JOIN market_snapshots ms ON p.product_id = ms.supplier_product_id
            JOIN opportunity_analyses oa ON p.product_id = oa.product_id
            WHERE oa.score IN ('strong', 'average')
            ORDER BY
                CASE oa.score WHEN 'strong' THEN 1 WHEN 'average' THEN 2 ELSE 3 END,
                ms.avg_price DESC
            LIMIT $1 OFFSET $2
        """, limit, offset)
    results = []
    for row in rows:
        d = dict(row)
        for field in ("ai_differentiation", "ai_risks"):
            if d.get(field):
                try:
                    d[field] = json.loads(d[field])
                except Exception:
                    d[field] = []
        results.append(d)
    return {"opportunities": results, "count": len(results)}
