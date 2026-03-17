import os
import json
import anthropic
from models.opportunity import OpportunityAnalysis
from datetime import datetime

MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """You are a senior Amazon seller and sourcing expert with 10+ years of experience launching private label products.

Your job is to evaluate whether a supplier product from Alibaba represents a genuine opportunity to sell on Amazon.

Your analysis must be:
- Direct and honest — do not assume every product is a good opportunity
- Critical and realistic — if something is a bad idea, say so clearly
- Specific — avoid generic statements, always reference the actual data
- Business-focused — think in terms of margin, competition, and risk
- Concise — no fluff, no hype

You will be given supplier data from Alibaba and Amazon market data for the equivalent product category.
You must reason through the opportunity like an experienced operator would."""


def _build_prompt(product: dict, snapshot: dict) -> str:
    top_listings = json.loads(snapshot.get("raw_json") or "{}").get("top_listings", [])[:5]

    listings_text = ""
    for i, l in enumerate(top_listings, 1):
        listings_text += (
            f"  {i}. {l.get('title', '')[:80]}\n"
            f"     Price: ${l.get('price', '?')} | "
            f"Reviews: {l.get('reviews', '?')} | "
            f"Rating: {l.get('rating', '?')} | "
            f"Prime: {l.get('is_prime', False)} | "
            f"Best Seller: {l.get('is_best_seller', False)}\n"
        )

    supplier_price = product.get("price_range") or "Unknown"
    moq = f"{product.get('moq', '?')} {product.get('moq_unit', '')}".strip()

    prompt = f"""Analyse this product opportunity and respond ONLY with valid JSON.

---
ALIBABA SUPPLIER PRODUCT:
Title: {product.get('title', '')}
Supplier price: {supplier_price}
MOQ: {moq}
Supplier: {product.get('supplier_name', 'Unknown')} ({product.get('supplier_country', 'Unknown')})
Gold Supplier: {bool(product.get('is_gold_supplier'))}
Trade Assurance: {bool(product.get('has_trade_assurance'))}
Supplier trust score: {product.get('supplier_quality_score', 0)}/4
Employees: {product.get('employee_count', 'Unknown')}
Years active: {product.get('years_active', 'Unknown')}
Specifications: {product.get('spec_summary', 'Not available')}

---
AMAZON MARKET DATA (search: "{snapshot.get('search_query', '')}"):
Listings found: {snapshot.get('listings_analyzed', 0)}
Amazon price range: ${snapshot.get('min_price', '?')} – ${snapshot.get('max_price', '?')}
Price spread: ${snapshot.get('price_spread', '?')}
Average reviews: {snapshot.get('avg_reviews', '?')}
Median reviews: {snapshot.get('median_reviews', '?')}
Max reviews on one listing: {snapshot.get('max_reviews', '?')}
Listings with under 100 reviews: {snapshot.get('listings_under_100_reviews', 0)} (weak competitors)
Listings with 1000+ reviews: {snapshot.get('listings_over_1000_reviews', 0)} (dominant competitors)
Best seller listings: {snapshot.get('best_seller_count', 0)}
Prime listings: {snapshot.get('prime_listing_count', 0)}
Average rating: {snapshot.get('avg_rating', '?')}
Listings showing sales volume: {snapshot.get('listings_with_sales_volume', 0)}
Competition level (heuristic): {snapshot.get('competition_level', 'unknown')}

TOP AMAZON LISTINGS:
{listings_text if listings_text else '  No listings data available'}

---
MARGIN CONTEXT:
The supplier price is {supplier_price}. Amazon selling prices range from ${snapshot.get('min_price', '?')} to ${snapshot.get('max_price', '?')}.
Amazon FBA fees typically consume 30–40% of the selling price (referral fee ~15% + FBA fulfilment ~$3–6 per unit).
Factor in shipping from China (~$1–3/unit for small items by sea).

---
Respond with this exact JSON structure and nothing else:
{{
  "score": "strong" | "average" | "weak" | "avoid",
  "summary": "2–3 sentences: what this product is, who buys it, and the overall market situation",
  "margin_assessment": "Specific assessment of margin viability. Reference the actual supplier price and Amazon price range. Is there enough spread after FBA fees and shipping?",
  "competition_analysis": "Honest assessment of competition strength. Reference the actual review counts and listing quality. Is there room for a new entrant?",
  "differentiation_ideas": [
    "Specific idea 1",
    "Specific idea 2",
    "Specific idea 3"
  ],
  "risk_flags": [
    "Risk 1 (be specific)",
    "Risk 2 (be specific)"
  ],
  "final_recommendation": "Direct, specific recommendation. What should the seller do, and why? If it is a bad opportunity, say so plainly."
}}"""

    return prompt


async def analyse_opportunity(product: dict, snapshot: dict) -> OpportunityAnalysis:
    """
    Call Claude to analyse a product opportunity.
    Takes raw DB row dicts for both the product and its market snapshot.
    Returns a structured OpportunityAnalysis.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in environment")

    client = anthropic.Anthropic(api_key=api_key)

    prompt = _build_prompt(product, snapshot)

    message = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if Claude wraps in ```json ... ```
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    data = json.loads(raw)

    # Normalise score to valid values
    score = data.get("score", "weak").lower()
    if score not in ("strong", "average", "weak", "avoid"):
        score = "weak"

    return OpportunityAnalysis(
        product_id=product["product_id"],
        score=score,
        summary=data.get("summary", ""),
        margin_assessment=data.get("margin_assessment", ""),
        competition_analysis=data.get("competition_analysis", ""),
        differentiation_ideas=data.get("differentiation_ideas", []),
        risk_flags=data.get("risk_flags", []),
        final_recommendation=data.get("final_recommendation", ""),
        model_used=MODEL,
    )
