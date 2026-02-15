# 💰 WareCost — Data Warehouse Query Cost Attribution & Budget Enforcement

> Stop the Snowflake/BigQuery bill shock. Attribute every dollar to the team, dbt model, and Airflow DAG that spent it.

[![CI](https://github.com/user/warecost/actions/workflows/ci.yml/badge.svg)](https://github.com/user/warecost/actions)

## The Problem

Your Snowflake bill went from $8k to $47k last month. Nobody knows why. The CFO is asking questions. You have 10,000 queries in `QUERY_HISTORY` and zero attribution.

**WareCost** traces every credit to the team, dbt model, Airflow DAG, and BI dashboard that consumed it — and alerts you *before* you blow the budget.

## 🚀 Quick Start

```bash
pip install -r requirements.txt

# Export query history from Snowflake:
# SELECT query_id, query_text, user_name, warehouse_name,
#        credits_used_cloud_services as credits_used, bytes_scanned,
#        total_elapsed_time as execution_time_ms, start_time, query_tag
# FROM snowflake.account_usage.query_history
# WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP());

# CLI — analyze exported JSON
python warecost.py analyze queries.json --budget analytics:500 --budget ml:200

# API — start the SaaS server
uvicorn api:app --reload
# POST /v1/analyze with your query data
```

## 📊 What You Get

| Dimension | Attribution Method |
|-----------|-------------------|
| **Team** | `query_tag` (`team=analytics`) or username prefix (`analytics_bob`) |
| **dbt Model** | `query_tag` (`dbt:stg_orders`) |
| **Airflow DAG** | `query_tag` (`dag=daily_train`) |
| **Warehouse** | Direct from query metadata |
| **Anomalies** | Z-score detection (configurable threshold) |
| **Budget** | Per-team alerts at 80% and 100% |

## 💰 Pricing

| Feature | Free (CLI) | Pro ($99/mo) | Enterprise ($499/mo) |
|---------|-----------|-------------|---------------------|
| JSON file analysis | ✅ | ✅ | ✅ |
| Cost breakdown by team/warehouse | ✅ | ✅ | ✅ |
| Anomaly detection | ✅ (basic) | ✅ (ML-powered) | ✅ (ML-powered) |
| Budget alerts (CLI) | ✅ | ✅ | ✅ |
| REST API access | ❌ | ✅ | ✅ |
| Direct Snowflake/BQ connection | ❌ | ✅ | ✅ |
| Slack/PagerDuty alerts | ❌ | ✅ | ✅ |
| Scheduled daily reports | ❌ | ✅ | ✅ |
| PDF cost reports | ❌ | ❌ | ✅ |
| SSO / RBAC | ❌ | ❌ | ✅ |
| Historical trend analysis | ❌ | ❌ | ✅ |
| SOC2 audit trail | ❌ | ❌ | ✅ |
| Support | Community | Email | Dedicated Slack |

## 🤑 Why Pay?

- A single unnoticed full-table-scan costs **$500+** in Snowflake credits
- Teams without cost attribution overspend by **30-60%** (industry data)
- WareCost Pro pays for itself if it catches **one** anomaly per month
- Alternative: hire a FinOps engineer at **$150k/year** salary

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/v1/analyze` | Full cost analysis with attribution |
| POST | `/v1/anomalies` | Detect cost anomalies |
| POST | `/v1/breakdown/{dim}` | Breakdown by team/warehouse/dbt_model/dag_id |
| POST | `/v1/budget-check` | Check budget alerts |

## License

BSL 1.1 — Free for non-production use. Production use requires a paid license.
