import asyncpg
import json
import os
from datetime import datetime
from models.supplier_product import SupplierProduct
from models.amazon_listing import MarketSnapshot
from models.opportunity import OpportunityAnalysis

DATABASE_URL = os.environ.get("DATABASE_URL")

_pool: asyncpg.Pool = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10, ssl="require")
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")


async def upsert_product(product: SupplierProduct):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO products (
                product_id, search_query, title, url, thumbnail, category_id,
                is_available, price_range, price_low, moq, moq_unit,
                supplier_name, supplier_country, is_gold_supplier,
                has_trade_assurance, is_assessed, is_verified,
                employee_count, years_active, supplier_quality_score,
                spec_summary, raw_json, fetched_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
            ON CONFLICT (product_id) DO UPDATE SET
                search_query = EXCLUDED.search_query, title = EXCLUDED.title,
                url = EXCLUDED.url, thumbnail = EXCLUDED.thumbnail,
                category_id = EXCLUDED.category_id, is_available = EXCLUDED.is_available,
                price_range = EXCLUDED.price_range, price_low = EXCLUDED.price_low,
                moq = EXCLUDED.moq, moq_unit = EXCLUDED.moq_unit,
                supplier_name = EXCLUDED.supplier_name, supplier_country = EXCLUDED.supplier_country,
                is_gold_supplier = EXCLUDED.is_gold_supplier,
                has_trade_assurance = EXCLUDED.has_trade_assurance,
                is_assessed = EXCLUDED.is_assessed, is_verified = EXCLUDED.is_verified,
                employee_count = EXCLUDED.employee_count, years_active = EXCLUDED.years_active,
                supplier_quality_score = EXCLUDED.supplier_quality_score,
                spec_summary = EXCLUDED.spec_summary, raw_json = EXCLUDED.raw_json,
                fetched_at = EXCLUDED.fetched_at
        """,
            product.product_id, product.search_query, product.title, product.url,
            product.thumbnail, product.category_id, int(product.is_available),
            product.pricing.range_formatted, product.pricing.lowest_unit_price,
            product.pricing.minimum_order_qty, product.pricing.minimum_order_unit,
            product.supplier.name, product.supplier.country,
            int(product.supplier.is_gold_supplier), int(product.supplier.has_trade_assurance),
            int(product.supplier.is_assessed), int(product.supplier.is_verified),
            product.supplier.employee_count, product.seller.years_active,
            product.supplier_quality_score, product.specifications.summary,
            product.model_dump_json(), product.fetched_at.isoformat(),
        )


async def log_search(query: str, result_count: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO searches (query, result_count, searched_at) VALUES ($1, $2, $3)",
            query, result_count, datetime.utcnow().isoformat()
        )


async def get_products(query: str = None, limit: int = 50, offset: int = 0) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if query:
            rows = await conn.fetch(
                "SELECT * FROM products WHERE search_query = $1 ORDER BY supplier_quality_score DESC, price_low ASC LIMIT $2 OFFSET $3",
                query, limit, offset
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM products ORDER BY fetched_at DESC LIMIT $1 OFFSET $2",
                limit, offset
            )
        return [dict(r) for r in rows]


async def upsert_market_snapshot(snapshot: MarketSnapshot):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM market_snapshots WHERE supplier_product_id = $1",
            snapshot.supplier_product_id
        )
        await conn.execute("""
            INSERT INTO market_snapshots (
                supplier_product_id, search_query, total_results, listings_analyzed,
                avg_price, min_price, max_price, price_spread,
                avg_reviews, median_reviews, max_reviews,
                listings_under_100_reviews, listings_over_1000_reviews,
                best_seller_count, amazon_choice_count, prime_listing_count,
                avg_rating, listings_with_sales_volume, competition_level,
                raw_json, fetched_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
        """,
            snapshot.supplier_product_id, snapshot.search_query, snapshot.total_results,
            snapshot.listings_analyzed, snapshot.avg_price, snapshot.min_price,
            snapshot.max_price, snapshot.price_spread, snapshot.avg_reviews,
            snapshot.median_reviews, snapshot.max_reviews,
            snapshot.listings_under_100_reviews, snapshot.listings_over_1000_reviews,
            snapshot.best_seller_count, snapshot.amazon_choice_count,
            snapshot.prime_listing_count, snapshot.avg_rating,
            snapshot.listings_with_sales_volume, snapshot.competition_level,
            snapshot.model_dump_json(), snapshot.fetched_at.isoformat(),
        )


async def get_market_snapshot(supplier_product_id: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM market_snapshots WHERE supplier_product_id = $1",
            supplier_product_id
        )
        return dict(row) if row else None


async def upsert_opportunity(analysis: OpportunityAnalysis):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO opportunity_analyses (
                product_id, score, summary, margin_assessment,
                competition_analysis, differentiation_ideas, risk_flags,
                final_recommendation, model_used, analysed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (product_id) DO UPDATE SET
                score = EXCLUDED.score, summary = EXCLUDED.summary,
                margin_assessment = EXCLUDED.margin_assessment,
                competition_analysis = EXCLUDED.competition_analysis,
                differentiation_ideas = EXCLUDED.differentiation_ideas,
                risk_flags = EXCLUDED.risk_flags,
                final_recommendation = EXCLUDED.final_recommendation,
                model_used = EXCLUDED.model_used, analysed_at = EXCLUDED.analysed_at
        """,
            analysis.product_id, analysis.score, analysis.summary,
            analysis.margin_assessment, analysis.competition_analysis,
            json.dumps(analysis.differentiation_ideas), json.dumps(analysis.risk_flags),
            analysis.final_recommendation, analysis.model_used,
            analysis.analysed_at.isoformat(),
        )


async def get_opportunity(product_id: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM opportunity_analyses WHERE product_id = $1",
            product_id
        )
        if not row:
            return None
        d = dict(row)
        d["differentiation_ideas"] = json.loads(d.get("differentiation_ideas") or "[]")
        d["risk_flags"] = json.loads(d.get("risk_flags") or "[]")
        return d


async def get_products_with_snapshots(query: str = None, limit: int = 50, offset: int = 0) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        base = """SELECT p.*,
            ms.avg_price AS amz_avg_price, ms.min_price AS amz_min_price,
            ms.max_price AS amz_max_price, ms.price_spread AS amz_price_spread,
            ms.avg_reviews AS amz_avg_reviews, ms.max_reviews AS amz_max_reviews,
            ms.listings_under_100_reviews AS amz_weak_competitors,
            ms.listings_over_1000_reviews AS amz_strong_competitors,
            ms.competition_level AS amz_competition_level, ms.avg_rating AS amz_avg_rating,
            ms.best_seller_count AS amz_best_seller_count,
            ms.listings_with_sales_volume AS amz_listings_with_sales,
            ms.search_query AS amz_search_query, ms.raw_json AS amz_raw_json
            FROM products p
            LEFT JOIN market_snapshots ms ON p.product_id = ms.supplier_product_id"""
        if query:
            rows = await conn.fetch(
                base + " WHERE p.search_query = $1 ORDER BY p.supplier_quality_score DESC, p.price_low ASC LIMIT $2 OFFSET $3",
                query, limit, offset)
        else:
            rows = await conn.fetch(
                base + " ORDER BY p.supplier_quality_score DESC, p.price_low ASC LIMIT $1 OFFSET $2",
                limit, offset)
        return [dict(r) for r in rows]


async def get_products_with_analyses(query: str = None, limit: int = 50, offset: int = 0) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        base = """SELECT p.*,
            ms.avg_price AS amz_avg_price, ms.min_price AS amz_min_price,
            ms.max_price AS amz_max_price, ms.price_spread AS amz_price_spread,
            ms.avg_reviews AS amz_avg_reviews, ms.max_reviews AS amz_max_reviews,
            ms.listings_under_100_reviews AS amz_weak_competitors,
            ms.listings_over_1000_reviews AS amz_strong_competitors,
            ms.competition_level AS amz_competition_level, ms.avg_rating AS amz_avg_rating,
            ms.best_seller_count AS amz_best_seller_count,
            ms.listings_with_sales_volume AS amz_listings_with_sales,
            ms.search_query AS amz_search_query, ms.raw_json AS amz_raw_json,
            oa.score AS ai_score, oa.summary AS ai_summary,
            oa.margin_assessment AS ai_margin, oa.competition_analysis AS ai_competition,
            oa.differentiation_ideas AS ai_differentiation, oa.risk_flags AS ai_risks,
            oa.final_recommendation AS ai_recommendation, oa.analysed_at AS ai_analysed_at
            FROM products p
            LEFT JOIN market_snapshots ms ON p.product_id = ms.supplier_product_id
            LEFT JOIN opportunity_analyses oa ON p.product_id = oa.product_id"""
        order = """ ORDER BY CASE oa.score WHEN 'strong' THEN 1 WHEN 'average' THEN 2
            WHEN 'weak' THEN 3 WHEN 'avoid' THEN 4 ELSE 5 END,
            p.supplier_quality_score DESC"""
        if query:
            rows = await conn.fetch(
                base + " WHERE p.search_query = $1" + order + " LIMIT $2 OFFSET $3",
                query, limit, offset)
        else:
            rows = await conn.fetch(base + order + " LIMIT $1 OFFSET $2", limit, offset)
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
        return results


async def get_recent_searches(limit: int = 20) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT query, MAX(searched_at) as last_searched, SUM(result_count) as total FROM searches GROUP BY query ORDER BY last_searched DESC LIMIT $1",
            limit
        )
        return [dict(r) for r in rows]
