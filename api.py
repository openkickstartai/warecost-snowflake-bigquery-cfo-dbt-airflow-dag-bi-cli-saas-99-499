"""WareCost SaaS API — FastAPI backend for cost attribution."""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from warecost import CostEngine

app = FastAPI(
    title="WareCost API",
    version="1.0.0",
    description="Data warehouse query cost attribution & budget enforcement engine.",
)


class AnalyzeRequest(BaseModel):
    queries: list[dict] = Field(..., min_length=1)
    budgets: dict[str, float] = Field(default_factory=dict)
    z_threshold: float = 2.0
    credit_price: float = 3.0


def _build_engine(req: AnalyzeRequest) -> CostEngine:
    engine = CostEngine(credit_price=req.credit_price)
    try:
        engine.load(req.queries)
    except (TypeError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid query data: {exc}")
    for team, amount in req.budgets.items():
        engine.set_budget(team, amount)
    return engine


@app.get("/health")
def health():
    return {"status": "ok", "service": "warecost", "version": "1.0.0"}


@app.post("/v1/analyze")
def analyze(req: AnalyzeRequest):
    engine = _build_engine(req)
    return engine.summary()


@app.post("/v1/anomalies")
def detect_anomalies(req: AnalyzeRequest):
    engine = _build_engine(req)
    return {"anomalies": engine.anomalies(req.z_threshold)}


@app.post("/v1/breakdown/{dimension}")
def breakdown(dimension: str, req: AnalyzeRequest):
    valid_dims = {"team", "warehouse_name", "dbt_model", "dag_id", "user_name"}
    if dimension not in valid_dims:
        raise HTTPException(400, f"Invalid dimension '{dimension}'. Valid: {sorted(valid_dims)}")
    engine = _build_engine(req)
    return {"dimension": dimension, "breakdown": engine.breakdown(dimension)}


@app.post("/v1/budget-check")
def budget_check(req: AnalyzeRequest):
    if not req.budgets:
        raise HTTPException(400, "No budgets provided. Pass budgets: {team: amount}")
    engine = _build_engine(req)
    return {"alerts": engine.budget_alerts()}
