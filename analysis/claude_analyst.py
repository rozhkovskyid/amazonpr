import os
import json
import anthropic
from models.opportunity import OpportunityAnalysis
from datetime import datetime

MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """You are a seasoned Amazon private-label operator.
A product has already passed a rigorous mathematical filter (markup, competition gaps, demand).
Your job is a quick final check: confirm or deny the opportunity and flag any non-obvious risks.
Be direct. No fluff. Output only valid JSON."""


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

    markup = round((snapshot.get('avg_price') or 0) / max(product.get('price_low') or 1, 0.01), 1)

    prompt = f"""PRODUCT: {product.get('title', '')}
Supplier price: {supplier_price} | MOQ: {moq} | Markup vs Amazon avg: {markup}x
Supplier: {product.get('supplier_name', 'Unknown')} ({product.get('supplier_country', 'Unknown')}) | Gold: {bool(product.get('is_gold_supplier'))} | Trade Assurance: {bool(product.get('has_trade_assurance'))}

AMAZON ({snapshot.get('search_query', '')}):
Price range: ${snapshot.get('min_price', '?')}–${snapshot.get('max_price', '?')} avg ${snapshot.get('avg_price', '?')}
Listings: {snapshot.get('listings_analyzed', 0)} | Avg reviews: {snapshot.get('avg_reviews', '?')} | Avg rating: {snapshot.get('avg_rating', '?')}
Weak (<100 reviews): {snapshot.get('listings_under_100_reviews', 0)} | Dominant (1000+): {snapshot.get('listings_over_1000_reviews', 0)}
Competition: {snapshot.get('competition_level', '?')} | Best sellers: {snapshot.get('best_seller_count', 0)}

TOP LISTINGS:
{listings_text if listings_text else '  (none)'}

Respond ONLY with this JSON:
{{
  "score": "strong" | "average" | "weak" | "avoid",
  "summary": "1-2 sentences on the product and market",
  "margin_assessment": "margin viability after FBA fees (~35%) and shipping (~$2/unit)",
  "competition_analysis": "can a new entrant win?",
  "differentiation_ideas": ["idea 1", "idea 2"],
  "risk_flags": ["risk 1", "risk 2"],
  "final_recommendation": "one direct sentence"
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
        max_tokens=800,
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
