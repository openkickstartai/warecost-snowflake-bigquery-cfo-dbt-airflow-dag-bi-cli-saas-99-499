"""Tests for WareCost — cost attribution, anomaly detection, budget enforcement, API."""
import pytest
from fastapi.testclient import TestClient

from warecost import CostEngine, QueryRecord
from api import app

SAMPLE = [
    {"query_id": "q1", "query_text": "SELECT * FROM orders",
     "user_name": "analytics_bob", "warehouse_name": "ANALYTICS_WH",
     "credits_used": 2.5, "bytes_scanned": 1_000_000,
     "execution_time_ms": 3000, "start_time": "2024-01-15T10:00:00",
     "query_tag": "team=analytics;dbt:stg_orders"},
    {"query_id": "q2", "query_text": "SELECT count(*) FROM users",
     "user_name": "ml_alice", "warehouse_name": "ML_WH",
     "credits_used": 0.3, "bytes_scanned": 50_000,
     "execution_time_ms": 500, "start_time": "2024-01-15T11:00:00",
     "query_tag": "team=ml;dag=daily_train"},
    {"query_id": "q3", "query_text": "INSERT INTO reports SELECT *",
     "user_name": "analytics_carol", "warehouse_name": "ANALYTICS_WH",
     "credits_used": 15.0, "bytes_scanned": 50_000_000,
     "execution_time_ms": 45000, "start_time": "2024-01-15T12:00:00",
     "query_tag": "team=analytics;dbt:fct_revenue"},
    {"query_id": "q4", "query_text": "SELECT * FROM products",
     "user_name": "looker", "warehouse_name": "BI_WH",
     "credits_used": 0.1, "bytes_scanned": 10_000,
     "execution_time_ms": 200, "start_time": "2024-01-15T13:00:00",
     "query_tag": ""},
]


def test_query_record_team_from_tag():
    q = QueryRecord(**SAMPLE[0])
    assert q.team == "analytics"
    assert q.dbt_model == "stg_orders"
    assert q.dag_id is None
    assert q.cost_usd == 7.5


def test_query_record_dag_extraction():
    q = QueryRecord(**SAMPLE[1])
    assert q.team == "ml"
    assert q.dag_id == "daily_train"
    assert q.dbt_model is None


def test_unattributed_team_fallback():
    q = QueryRecord(**SAMPLE[3])
    assert q.team == "unattributed"


def test_cost_engine_breakdown_by_team():
    engine = CostEngine()
    engine.load(SAMPLE)
    by_team = engine.breakdown("team")
    assert "analytics" in by_team
    assert by_team["analytics"]["queries"] == 2
    assert by_team["analytics"]["cost_usd"] == 52.5


def test_anomaly_detection_catches_spike():
    engine = CostEngine()
    engine.load(SAMPLE)
    anoms = engine.anomalies(z_thresh=1.0)
    assert len(anoms) >= 1
    assert anoms[0]["query_id"] == "q3"


def test_anomaly_none_when_uniform():
    uniform = [{**SAMPLE[0], "query_id": f"u{i}", "credits_used": 1.0} for i in range(5)]
    engine = CostEngine()
    engine.load(uniform)
    assert engine.anomalies() == []


def test_budget_alert_over():
    engine = CostEngine()
    engine.load(SAMPLE)
    engine.set_budget("analytics", 50.0)
    alerts = engine.budget_alerts()
    assert len(alerts) == 1
    assert alerts[0]["status"] == "OVER"
    assert alerts[0]["pct"] > 100


def test_budget_no_alert_when_under():
    engine = CostEngine()
    engine.load(SAMPLE)
    engine.set_budget("ml", 100.0)
    assert engine.budget_alerts() == []


def test_summary_totals():
    engine = CostEngine()
    engine.load(SAMPLE)
    s = engine.summary()
    assert s["total_queries"] == 4
    assert s["total_cost_usd"] == 53.7


client = TestClient(app)


def test_api_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["service"] == "warecost"


def test_api_analyze_full():
    r = client.post("/v1/analyze", json={"queries": SAMPLE, "budgets": {"analytics": 50}})
    assert r.status_code == 200
    data = r.json()
    assert data["total_queries"] == 4
    assert len(data["budget_alerts"]) == 1


def test_api_breakdown_team():
    r = client.post("/v1/breakdown/team", json={"queries": SAMPLE})
    assert r.status_code == 200
    assert "analytics" in r.json()["breakdown"]


def test_api_invalid_dimension():
    r = client.post("/v1/breakdown/invalid", json={"queries": SAMPLE})
    assert r.status_code == 400
