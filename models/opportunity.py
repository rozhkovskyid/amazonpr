from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class OpportunityAnalysis(BaseModel):
    """
    AI-generated opportunity assessment for a supplier product.
    Produced by Claude after reviewing both Alibaba supplier data
    and the Amazon market snapshot.
    """
    product_id: str

    # Verdict: strong / average / weak / avoid
    score: str

    # Human-readable analysis fields (empty for math-only records)
    summary: str = ""
    margin_assessment: str = ""
    competition_analysis: str = ""
    differentiation_ideas: list[str] = []
    risk_flags: list[str] = []
    final_recommendation: str = ""

    # "math" = scored by formula only, "claude-*" = AI analysed
    model_used: str = "math"
    analysed_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def score_color(self) -> str:
        return {
            "strong": "green",
            "average": "yellow",
            "weak": "orange",
            "avoid": "red",
        }.get(self.score, "muted")
