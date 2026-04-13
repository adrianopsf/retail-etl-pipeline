![Status](https://img.shields.io/badge/status-active-brightgreen)
![Python](https://img.shields.io/badge/python-3.11-blue)
![License](https://img.shields.io/badge/license-MIT-green)

# retail-etl-pipeline

A custom-built Python ETL pipeline that ingests, transforms, and loads 100k+ Olist e-commerce orders into a structured PostgreSQL warehouse — built without orchestration frameworks to demonstrate solid understanding of pipeline fundamentals.

---

## Problem Statement

Before relying on tools like Airflow or dbt, it's worth understanding what they abstract away. This project builds the full ETL lifecycle from scratch in Python: extraction from raw CSV files, domain-specific transformations (delivery performance metrics, customer segmentation, product sales aggregation), and loading into a two-layer PostgreSQL schema (staging + analytics).

The objective was to understand where real pipeline complexity lives — type coercion, partial failures, bulk loading performance, and data quality enforcement — before adding orchestration on top. For an Airflow + dbt version over the same dataset, see [ecommerce-modern-stack](https://github.com/adrianopsf/ecommerce-modern-stack).

---

## Architecture

```
CSV Files (Olist dataset — data/raw/)
            │
            ▼
┌───────────────────────┐
│     OlistExtractor    │   src/extract/
│  Reads CSVs, validates│
│  columns and dtypes   │
└──────────┬────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│          Transform Layer             │   src/transform/
│  ┌─────────────────────────────┐     │
│  │ OrdersTransformer           │     │   delivery metrics, status enrichment
│  │ CustomersTransformer        │     │   order frequency + recency segmentation
│  │ ProductsTransformer         │     │   revenue + review aggregation by category
│  └─────────────────────────────┘     │
└──────────┬───────────────────────────┘
           │
           ▼
┌───────────────────────┐
│     PostgresLoader    │   src/load/
│  staging schema       │   → typed, deduplicated source tables
│  analytics schema     │   → business-ready aggregations
└───────────────────────┘
           │
           ▼
     PostgreSQL (Docker)
     ├── staging.*      typed source data, one table per entity
     └── analytics.*    transformed and aggregated output tables
```

---

## Tech Stack

| Tool | Version | Why |
|------|---------|-----|
| Python | 3.11 | Pipeline language |
| pandas | 2.2.0 | Data manipulation and type-safe transformations |
| SQLAlchemy | 2.0.25 | Database abstraction — engine, connection pooling, bulk inserts |
| PostgreSQL | 15 | Two-layer data warehouse |
| pytest | 8.0.0 | Unit and integration tests |
| ruff | 0.2.0 | Linting and code style enforcement |
| Docker Compose | 2.x | Reproducible database environment |

**On pandas:** Used here for familiar, readable transformation logic. In a higher-volume scenario I'd move to DuckDB or Polars — see Next Steps.

---

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Python 3.11+
- Olist dataset CSVs → [download from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## How to Run

```bash
# 1. Clone and configure
git clone https://github.com/adrianopsf/retail-etl-pipeline.git
cd retail-etl-pipeline
cp .env.example .env      # fill in PostgreSQL credentials

# 2. Start PostgreSQL
make db-up

# 3. Set up Python environment
make setup

# 4. Place Olist CSV files in data/raw/ then run the pipeline
make run

# 5. Run the test suite
make test
```

### Available Make targets

| Target | Description |
|--------|-------------|
| `make db-up` | Start PostgreSQL container |
| `make db-down` | Stop PostgreSQL container |
| `make setup` | Create virtualenv and install dependencies |
| `make run` | Execute the full ETL pipeline |
| `make test` | Run pytest (unit + integration) |
| `make lint` | Run ruff linter |

---

## Project Structure

```
.
├── .github/
│   └── workflows/          CI: lint + pytest on every push
├── data/
│   └── raw/                Olist CSVs (gitignored — not committed)
├── src/
│   ├── extract/            OlistExtractor — CSV reading, column validation
│   ├── transform/          Domain transformers (Orders, Customers, Products)
│   ├── load/               PostgresLoader — staging and analytics writes
│   ├── utils/              Logger, DB connection helpers
│   └── pipeline.py         Main entry point — wires extract → transform → load
├── tests/                  Unit tests (transformers) + integration tests (DB writes)
├── sql/                    Schema DDL for staging and analytics tables
├── docker/
│   └── postgres/           DB initialization scripts
├── .env.example            Environment variable template
├── docker-compose.yml      PostgreSQL service definition
├── Makefile                Command shortcuts
└── pyproject.toml          Project config, dependency groups, ruff settings
```

---

## Transformations

**Orders:** Calculates delivery performance metrics — on-time rate, average delivery days by state, late delivery rate, and order status distribution.

**Customers:** Segments customers by order frequency and recency, identifying high-value cohorts for downstream BI consumption.

**Products:** Aggregates revenue, units sold, and average review score by product category, enabling product-level performance analysis.

---

## What I Learned / Next Steps

**What I learned building this:**

- Why separation of concerns matters even in simple pipelines: isolating extract, transform, and load made debugging failures much faster
- Schema validation at extraction time catches type mismatches before they silently corrupt transformed outputs downstream
- SQLAlchemy's bulk insert methods (e.g., `execute_many`) dramatically outperform row-by-row writes — critical for loading 100k+ records without timing out

**What I'd add in a production environment:**

- Replace pandas with DuckDB or Polars for columnar, memory-efficient processing at higher data volumes
- Add a checkpointing mechanism so failed runs resume from the last successful step rather than re-running everything
- Introduce YAML-based pipeline configuration to make the codebase dataset-agnostic
- Add post-load data quality assertions using Great Expectations or a lightweight custom framework

---

*Dataset: [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k+ orders, 2016–2018*
