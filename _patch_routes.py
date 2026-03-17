addition = r"""

@router.get("/api/automation/status")
async def automation_status():
    """Return current automation scheduler status."""
    from automation.scheduler import get_status
    return get_status()


@router.post("/api/automation/trigger")
async def automation_trigger(category: str = None):
    """Manually trigger a scan cycle, optionally for a specific category."""
    import asyncio
    from automation.scheduler import trigger_scan_now
    asyncio.create_task(trigger_scan_now(category))
    return {"message": f"Scan triggered for: {category or 'auto-selected category'}"}


@router.get("/api/opportunities")
async def list_opportunities(limit: int = 50, offset: int = 0):
    """Return only AI-analysed products scored strong or average, best first."""
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
"""

# Fix docstrings (the raw string above has plain quotes, need to keep them as-is)
content = open(r'c:\amazonpr\api\routes.py').read()
content += addition
open(r'c:\amazonpr\api\routes.py', 'w').write(content)
print('done')
