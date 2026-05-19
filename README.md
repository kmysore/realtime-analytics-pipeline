# Real-Time Analytics Pipeline with AI Monitoring

A production-style analytics pipeline that ingests financial market data,
transforms it with dbt Cloud on Snowflake, orchestrates jobs via Airflow,
and uses a Claude-powered agent to diagnose pipeline failures.

## Why This Project

Most analytics portfolio projects show a pipeline working once. This project
explores what happens after deployment: scheduled ingestion, freshness SLAs,
dbt test failures, cost management, and how an LLM agent acts as a
first-line responder to pipeline incidents.

## Architecture
Alpha Vantage API (stock prices, every 15 min)
↓
Airflow (Astro CLI / local Docker)
├── ingest_stocks_dag      → loads raw JSON to Snowflake RAW schema
├── trigger_dbt_cloud_dag  → kicks off dbt Cloud job via API
└── monitor_pipeline_dag   → runs AI agent every hour
↓
Snowflake (Standard edition, AWS us-west-2)
RAW.stock_prices_raw
↓
dbt Cloud (scheduled jobs + CI on PRs)
STAGING.stg_stock_prices   (incremental model)
MARTS.fct_daily_returns
MARTS.agg_volatility_by_ticker
↓
AI Monitoring Agent (Claude Sonnet + tool use)
→ reads Airflow logs, dbt test results, freshness checks
→ outputs markdown incident report with diagnosis + fix

## Production Practices

- Snowflake RBAC: separate ingestion, transform, and BI roles
- X-Small warehouse, auto-suspend = 60s (~$0.40/day during active dev)
- dbt incremental models with unique_key and merge strategy
- Source freshness SLAs (warn at 30 min, error at 1 hr)
- dbt tests: not_null, unique, accepted_values, relationships
- CI: dbt tests run automatically on every pull request
- Idempotent Airflow DAGs (safe to re-run without duplicating data)
- Secrets managed via .env locally, Airflow connections store in Docker

## Tech Stack

| Layer | Tool |
|---|---|
| Warehouse | Snowflake (Standard, AWS us-west-2) |
| Transformation | dbt Cloud (Developer tier, free) |
| Orchestration | Apache Airflow via Astro CLI |
| Ingestion | Alpha Vantage stock API |
| AI monitoring | Claude Sonnet (Anthropic) |
| CI/CD | GitHub Actions |
| Local environment | conda, Python 3.11 |

## Roadmap

- [x] Step 1: Repo scaffold, conda env, folder structure
- [ ] Step 2: Snowflake account, RBAC, warehouse, database setup
- [ ] Step 3: Alpha Vantage ingestion + Airflow ingest DAG
- [ ] Step 4: dbt Cloud project + staging models (incremental)
- [ ] Step 5: dbt marts + freshness tests + CI on PRs
- [ ] Step 6: dbt Cloud trigger DAG
- [ ] Step 7: AI monitoring agent (Claude + tool use)
- [ ] Step 8: Monitor DAG + end-to-end test
- [ ] Step 9: Runbook + cost analysis write-up

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/realtime-analytics-pipeline.git
cd realtime-analytics-pipeline
conda create -n analytics-pipeline python=3.11 -y
conda activate analytics-pipeline
pip install -r requirements.txt
cp .env.example .env
# Fill in .env with your credentials (see docs/setup.md)
```

## Cost Management

Designed to run entirely on the Snowflake free trial ($400 credits, 30 days).
With an X-Small warehouse and 60s auto-suspend, active development consumes
roughly $0.40-$1.00/day. Total estimated trial consumption: ~$15-25.

## Author

Karthik Mysore — [LinkedIn](https://linkedin.com/in/kmysore03)
