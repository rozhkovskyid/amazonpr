from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class PricingTier(BaseModel):
    unit_price: Optional[float] = None
    formatted_price: Optional[str] = None
    min_units: Optional[int] = None
    max_units: Optional[int] = None
    unit_label: Optional[str] = None


class Pricing(BaseModel):
    range: Optional[str] = None
    range_formatted: Optional[str] = None
    currency_symbol: Optional[str] = "$"
    tiers: list[PricingTier] = []
    minimum_order_qty: Optional[int] = None
    minimum_order_unit: Optional[str] = None
    minimum_order_label: Optional[str] = None

    @property
    def lowest_unit_price(self) -> Optional[float]:
        """Extract the lowest numeric price from the range string e.g. '1.5-3.0' -> 1.5"""
        if not self.range:
            return None
        try:
            return float(self.range.split("-")[0])
        except (ValueError, IndexError):
            return None


class SellerRating(BaseModel):
    label: str
    score: float = 0.0


class Seller(BaseModel):
    shop_url: Optional[str] = None
    years_active: Optional[str] = None
    ratings: list[SellerRating] = []

    @property
    def product_rating(self) -> Optional[float]:
        for r in self.ratings:
            if "described" in r.label.lower():
                return r.score
        return None


class Supplier(BaseModel):
    name: Optional[str] = None
    id: Optional[int] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    business_type: Optional[str] = None
    is_gold_supplier: bool = False
    is_assessed: bool = False
    is_verified: bool = False
    has_trade_assurance: bool = False
    facility_size: Optional[str] = None
    employee_count: Optional[str] = None
    transaction_volume: Optional[str] = None


class SpecAttribute(BaseModel):
    label: str
    value: str


class Specifications(BaseModel):
    summary: Optional[str] = None
    attributes: list[SpecAttribute] = []


class VariantOption(BaseModel):
    option_id: Optional[str] = None
    label: Optional[str] = None
    swatch_image: Optional[str] = None


class VariantGroup(BaseModel):
    attribute_name: str
    options: list[VariantOption] = []


class Variants(BaseModel):
    groups: list[VariantGroup] = []
    combination_count: int = 0


class SupplierProduct(BaseModel):
    """
    Normalized supplier product from Alibaba.
    Combines data from both the search endpoint and the detail endpoint.
    """
    product_id: str
    title: str
    url: Optional[str] = None
    thumbnail: Optional[str] = None
    gallery_images: list[str] = []
    category_id: Optional[int] = None
    is_available: bool = True
    video_url: Optional[str] = None

    pricing: Pricing = Field(default_factory=Pricing)
    seller: Seller = Field(default_factory=Seller)
    supplier: Supplier = Field(default_factory=Supplier)
    specifications: Specifications = Field(default_factory=Specifications)
    variants: Variants = Field(default_factory=Variants)

    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    search_query: Optional[str] = None

    @property
    def supplier_quality_score(self) -> int:
        """
        Simple 0-4 score based on supplier trust signals.
        Used later for filtering weak suppliers.
        """
        score = 0
        if self.supplier.is_gold_supplier:
            score += 1
        if self.supplier.has_trade_assurance:
            score += 1
        if self.supplier.is_assessed:
            score += 1
        if self.supplier.is_verified:
            score += 1
        return score

    @property
    def has_volume_pricing(self) -> bool:
        return len(self.pricing.tiers) > 1
