import httpx
import os
from typing import Optional
from models.supplier_product import (
    SupplierProduct, Pricing, PricingTier, Seller, SellerRating,
    Supplier, Specifications, SpecAttribute, Variants, VariantGroup, VariantOption
)

BASE_URL = "https://alibaba-scraper.omkar.cloud"


def _get_api_key() -> str:
    key = os.getenv("ALIBABA_SCRAPER_API_KEY")
    if not key:
        raise RuntimeError("ALIBABA_SCRAPER_API_KEY not set in environment")
    return key


def _parse_pricing(raw: dict) -> Pricing:
    tiers = []
    for t in raw.get("tiers", []):
        price_str = t.get("unit_price") or t.get("formatted_price")
        unit_price = None
        if price_str:
            try:
                unit_price = float(str(price_str).replace("$", "").split("-")[0])
            except ValueError:
                pass
        tiers.append(PricingTier(
            unit_price=unit_price,
            formatted_price=t.get("formatted_price") or t.get("unit_price"),
            min_units=t.get("min_units"),
            max_units=t.get("max_units"),
            unit_label=t.get("unit_label") or t.get("quantity_label"),
        ))

    moq = raw.get("minimum_order_qty")
    if moq is not None:
        try:
            moq = int(moq)
        except (ValueError, TypeError):
            moq = None

    return Pricing(
        range=raw.get("range"),
        range_formatted=raw.get("range_formatted"),
        currency_symbol=raw.get("currency_symbol", "$"),
        tiers=tiers,
        minimum_order_qty=moq,
        minimum_order_unit=raw.get("minimum_order_unit"),
        minimum_order_label=raw.get("minimum_order_label"),
    )


def _parse_seller(raw: dict) -> Seller:
    ratings = []
    for r in raw.get("ratings", []):
        try:
            score = float(r.get("score", 0))
        except (ValueError, TypeError):
            score = 0.0
        ratings.append(SellerRating(label=r.get("label", ""), score=score))
    return Seller(
        shop_url=raw.get("shop_url"),
        years_active=raw.get("years_active"),
        ratings=ratings,
    )


def _parse_supplier(raw: dict) -> Supplier:
    return Supplier(
        name=raw.get("name"),
        id=raw.get("id"),
        country=raw.get("country"),
        country_code=raw.get("country_code"),
        business_type=raw.get("business_type"),
        is_gold_supplier=bool(raw.get("is_gold_supplier", False)),
        is_assessed=bool(raw.get("is_assessed", False)),
        is_verified=bool(raw.get("is_verified", False)),
        has_trade_assurance=bool(raw.get("has_trade_assurance", False)),
        facility_size=raw.get("facility_size"),
        employee_count=raw.get("employee_count"),
        transaction_volume=raw.get("transaction_volume"),
    )


def _parse_specifications(raw: dict) -> Specifications:
    attributes = [
        SpecAttribute(label=a["label"], value=a["value"])
        for a in raw.get("attributes", [])
        if a.get("label") and a.get("value")
    ]
    return Specifications(summary=raw.get("summary"), attributes=attributes)


def _parse_variants(raw: dict) -> Variants:
    groups = []
    for g in raw.get("groups", []):
        options = [
            VariantOption(
                option_id=o.get("option_id"),
                label=o.get("label"),
                swatch_image=o.get("swatch_image"),
            )
            for o in g.get("options", [])
        ]
        groups.append(VariantGroup(attribute_name=g.get("attribute_name", ""), options=options))
    combinations = raw.get("combinations", [])
    return Variants(groups=groups, combination_count=len(combinations))


def _merge_search_and_detail(search_item: dict, detail: dict, query: str) -> SupplierProduct:
    """Combine search result (lightweight) with detail response (full) into one model."""
    base = detail if detail else search_item

    return SupplierProduct(
        product_id=base.get("product_id", search_item.get("product_id")),
        title=base.get("title", ""),
        url=base.get("url"),
        thumbnail=search_item.get("thumbnail") or (base.get("gallery_images") or [None])[0],
        gallery_images=base.get("gallery_images", []),
        category_id=base.get("category_id"),
        is_available=base.get("is_available", True),
        video_url=base.get("video"),
        pricing=_parse_pricing(base.get("pricing", {})),
        seller=_parse_seller(search_item.get("seller", {})),
        supplier=_parse_supplier(base.get("supplier", {})),
        specifications=_parse_specifications(base.get("specifications", {})),
        variants=_parse_variants(base.get("variants", {})),
        search_query=query,
    )


async def search_products(query: str, page: int = 1) -> dict:
    """Fetch one page of search results."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{BASE_URL}/alibaba/products/search",
            params={"search_query": query, "page": page},
            headers={"API-Key": _get_api_key()},
        )
        resp.raise_for_status()
        return resp.json()


async def get_product_detail(product_id: str) -> Optional[dict]:
    """Fetch full product detail. Returns None on failure."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{BASE_URL}/alibaba/products/details",
                params={"product_id": product_id},
                headers={"API-Key": _get_api_key()},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None


async def fetch_and_normalize(query: str, page: int = 1, fetch_details: bool = True) -> list[SupplierProduct]:
    """
    Search Alibaba for a query, optionally enrich each result with full detail,
    and return a list of normalized SupplierProduct models.
    """
    search_data = await search_products(query, page)
    items = search_data.get("products", [])

    products = []
    for item in items:
        detail = {}
        if fetch_details:
            detail = await get_product_detail(item["product_id"]) or {}
        products.append(_merge_search_and_detail(item, detail, query))

    return products
