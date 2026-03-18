import httpx
import os
import re
import statistics
from models.amazon_listing import AmazonListing, AmazonListingDetail, MarketSnapshot

BASE_URL = "https://amazon-scraper-api.omkar.cloud"


def _get_api_key() -> str:
    key = os.getenv("ALIBABA_SCRAPER_API_KEY")
    if not key:
        raise RuntimeError("ALIBABA_SCRAPER_API_KEY not set in environment")
    return key


def _clean_search_query(title: str) -> str:
    """
    Convert a verbose Alibaba product title into a clean Amazon search query.
    Alibaba titles are keyword-stuffed. We strip filler and keep the core product.
    e.g. "High Quality OEM Custom Logo Stainless Steel Kitchen Spatula Cooking Turner"
      -> "Stainless Steel Kitchen Spatula"
    """
    # Remove common Alibaba filler phrases
    filler = [
        r'\bOEM\b', r'\bODM\b', r'\bcustom\b', r'\bcustomize\b', r'\bcustomized\b',
        r'\blogo\b', r'\bhigh quality\b', r'\bhot sale\b', r'\bhot selling\b',
        r'\bbest price\b', r'\bfactory\b', r'\bwholesale\b', r'\bmanufacturer\b',
        r'\bsupplier\b', r'\bwith logo\b', r'\bprice\b', r'\bcheap\b',
        r'\bfree sample\b', r'\bsample\b', r'\bin stock\b', r'\bfast delivery\b',
    ]
    query = title
    for pattern in filler:
        query = re.sub(pattern, '', query, flags=re.IGNORECASE)

    # Remove extra whitespace
    query = ' '.join(query.split())

    # Truncate to ~60 chars at a word boundary to keep it focused
    if len(query) > 60:
        query = query[:60].rsplit(' ', 1)[0]

    return query.strip()


def _parse_listing(item: dict) -> AmazonListing:
    return AmazonListing(
        asin=item.get("asin", ""),
        title=item.get("title", ""),
        price=item.get("price"),
        original_price=item.get("original_price"),
        currency=item.get("currency", "USD"),
        rating=item.get("rating"),
        reviews=item.get("reviews"),
        link=item.get("link"),
        image_url=item.get("image_url"),
        is_best_seller=bool(item.get("is_best_seller", False)),
        is_amazon_choice=bool(item.get("is_amazon_choice", False)),
        is_prime=bool(item.get("is_prime", False)),
        sales_volume=item.get("sales_volume"),
        number_of_offers=item.get("number_of_offers"),
        lowest_offer_price=item.get("lowest_offer_price"),
        has_variations=bool(item.get("has_variations", False)),
        delivery_info=item.get("delivery_info"),
    )


async def search_amazon(query: str, page: int = 1, sort_by: str = "relevance") -> list[AmazonListing]:
    """Search Amazon using direct HTML scraper."""
    from ingestion.amazon_scraper import scrape_amazon_search
    return await scrape_amazon_search(query, page)


async def get_amazon_detail(asin: str) -> AmazonListingDetail | None:
    """Fetch full product details for a single ASIN."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{BASE_URL}/amazon/product-details",
            params={"asin": asin, "country_code": "US"},
            headers={"API-Key": _get_api_key()},
        )
        resp.raise_for_status()
        d = resp.json()

        # API may return an error payload instead of product data
        if not d.get("asin") and not d.get("product_name"):
            raise ValueError(f"Amazon API returned empty product for ASIN {asin}: {d}")

        return AmazonListingDetail(
            asin=d.get("asin", asin),
            title=d.get("product_name", ""),
            link=d.get("link"),
            brand=d.get("brand_info"),
            current_price=d.get("current_price"),
            original_price=d.get("original_price"),
            currency=d.get("currency", "USD"),
            availability=d.get("availability"),
            number_of_offers=d.get("number_of_offers"),
            rating=d.get("rating"),
            reviews=d.get("reviews"),
            detailed_rating=d.get("detailed_rating"),
            is_bestseller=bool(d.get("is_bestseller", False)),
            is_amazon_choice=bool(d.get("is_amazon_choice", False)),
            is_prime=bool(d.get("is_prime", False)),
            sales_volume=d.get("sales_volume"),
            main_image_url=d.get("main_image_url"),
            key_features=d.get("key_features", []),
            technical_details=d.get("technical_details", {}),
            product_details=d.get("product_details", {}),
            category_hierarchy=d.get("category_hierarchy", []),
            has_aplus_content=bool(d.get("has_aplus_content", False)),
        )


async def build_market_snapshot(
    supplier_product_id: str,
    product_title: str,
    custom_query: str = None,
) -> MarketSnapshot:
    """
    Core Stage 2 function.
    Given a supplier product, search Amazon and build a MarketSnapshot.
    """
    query = custom_query or _clean_search_query(product_title)

    listings = await search_amazon(query, page=1)

    if not listings:
        return MarketSnapshot(
            search_query=query,
            supplier_product_id=supplier_product_id,
            total_results=0,
            listings_analyzed=0,
            competition_level="unknown",
        )

    # Filter out listings without price (irrelevant/ad slots)
    priced = [l for l in listings if l.price is not None]

    prices = [l.price for l in priced]
    reviews = [l.reviews for l in listings if l.reviews is not None]

    avg_price = round(sum(prices) / len(prices), 2) if prices else None
    min_price = round(min(prices), 2) if prices else None
    max_price = round(max(prices), 2) if prices else None
    price_spread = round(max_price - min_price, 2) if (max_price and min_price) else None

    avg_reviews = round(sum(reviews) / len(reviews), 1) if reviews else None
    median_reviews = round(statistics.median(reviews), 1) if reviews else None
    max_reviews = max(reviews) if reviews else None

    snapshot = MarketSnapshot(
        search_query=query,
        supplier_product_id=supplier_product_id,
        total_results=len(listings),
        listings_analyzed=len(listings),
        avg_price=avg_price,
        min_price=min_price,
        max_price=max_price,
        price_spread=price_spread,
        avg_reviews=avg_reviews,
        median_reviews=median_reviews,
        max_reviews=max_reviews,
        listings_under_100_reviews=sum(1 for r in reviews if r < 100),
        listings_over_1000_reviews=sum(1 for r in reviews if r >= 1000),
        best_seller_count=sum(1 for l in listings if l.is_best_seller),
        amazon_choice_count=sum(1 for l in listings if l.is_amazon_choice),
        prime_listing_count=sum(1 for l in listings if l.is_prime),
        avg_rating=round(sum(l.rating for l in listings if l.rating) / max(sum(1 for l in listings if l.rating), 1), 2),
        listings_with_sales_volume=sum(1 for l in listings if l.sales_volume),
        top_listings=listings[:10],
    )

    snapshot.competition_level = snapshot.compute_competition_level()
    return snapshot
