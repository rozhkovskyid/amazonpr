from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class AmazonListing(BaseModel):
    """A single Amazon product listing from search results."""
    asin: str = ""
    title: str = ""
    price: Optional[float] = None
    original_price: Optional[float] = None
    currency: Optional[str] = "USD"
    rating: Optional[float] = None
    reviews: Optional[int] = None
    link: Optional[str] = None
    image_url: Optional[str] = None
    is_best_seller: bool = False
    is_amazon_choice: bool = False
    is_prime: bool = False
    sales_volume: Optional[str] = None
    number_of_offers: Optional[int] = None
    lowest_offer_price: Optional[float] = None
    has_variations: bool = False
    delivery_info: Optional[str] = None


class AmazonListingDetail(BaseModel):
    """Full product detail from the product-details endpoint."""
    asin: str
    title: str
    link: Optional[str] = None
    brand: Optional[str] = None
    current_price: Optional[float] = None
    original_price: Optional[float] = None
    currency: Optional[str] = "USD"
    availability: Optional[str] = None
    number_of_offers: Optional[int] = None
    rating: Optional[float] = None
    reviews: Optional[int] = None
    detailed_rating: Optional[dict] = None  # {"1": 3, "2": 0, "3": 3, "4": 5, "5": 89}
    is_bestseller: bool = False
    is_amazon_choice: bool = False
    is_prime: bool = False
    sales_volume: Optional[str] = None
    main_image_url: Optional[str] = None
    key_features: list[str] = []
    technical_details: dict = {}
    product_details: dict = {}
    category_hierarchy: list = []
    has_aplus_content: bool = False


class MarketSnapshot(BaseModel):
    """
    Summarized Amazon market analysis for a given search query.
    This is the output of Stage 2 — what the market looks like for a product.
    """
    search_query: str
    supplier_product_id: str
    fetched_at: datetime = Field(default_factory=datetime.utcnow)

    # Volume
    total_results: int = 0
    listings_analyzed: int = 0

    # Pricing
    avg_price: Optional[float] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    price_spread: Optional[float] = None  # max - min, indicates room for positioning

    # Competition signals
    avg_reviews: Optional[float] = None
    median_reviews: Optional[float] = None
    max_reviews: Optional[int] = None
    listings_under_100_reviews: int = 0   # weak competitors
    listings_over_1000_reviews: int = 0   # strong competitors
    best_seller_count: int = 0
    amazon_choice_count: int = 0
    prime_listing_count: int = 0

    # Demand signals
    avg_rating: Optional[float] = None
    listings_with_sales_volume: int = 0   # count that show "X bought in past month"

    # Competition level: "low" / "medium" / "high" — computed
    competition_level: str = "unknown"

    # Raw listings for reference
    top_listings: list[AmazonListing] = []

    def compute_competition_level(self) -> str:
        """
        Heuristic competition scoring.
        Low:    few listings, low reviews, no dominant brands
        Medium: some strong listings but gaps exist
        High:   saturated, high review counts, brand-heavy
        """
        if self.listings_analyzed == 0:
            return "unknown"

        strong = self.listings_over_1000_reviews
        weak = self.listings_under_100_reviews
        total = self.listings_analyzed

        strong_ratio = strong / total if total else 0
        avg_rev = self.avg_reviews or 0

        if avg_rev < 100 and strong_ratio < 0.2:
            return "low"
        elif avg_rev < 500 and strong_ratio < 0.5:
            return "medium"
        else:
            return "high"

    @property
    def margin_possible(self) -> Optional[bool]:
        """True if there appears to be room for a new listing based on price spread."""
        if self.price_spread is None:
            return None
        return self.price_spread > 10  # at least $10 gap between cheapest and priciest
