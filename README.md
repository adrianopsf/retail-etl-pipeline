# retail-etl-pipeline

[![CI](https://github.com/adrianopsf/retail-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/adrianopsf/retail-etl-pipeline/actions)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/downloads/release/python-3110/)
[![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-316192?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Made with Pandas](https://img.shields.io/badge/Made%20with-Pandas-150458?style=flat-square&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![License MIT](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

---

## Overview

Olist is the largest B2B marketplace in Brazil, connecting small merchants to major retail channels. Between 2016 and 2018, over 100 000 orders were placed through the platform — covering customers in every Brazilian state, products across 70+ categories, and payment methods ranging from credit cards to Pix predecessors.

This project builds a production-grade ETL pipeline that ingests the raw Olist CSV exports, applies domain-specific transformations (delivery performance metrics, customer value segmentation, product sales aggregation), and loads the results into a structured PostgreSQL data warehouse with two layers — `staging` for raw data fidelity and `analytics` for BI-ready fact and dimension tables. The entire pipeline runs in a containerised environment and is tested against a real database in CI.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          data/raw/                                  │
│  olist_orders_dataset.csv          olist_customers_dataset.csv      │
│  olist_order_items_dataset.csv     olist_products_dataset.csv       │
│  olist_order_payments_dataset.csv  olist_sellers_dataset.csv        │
│  olist_order_reviews_dataset.csv   product_category_name_*.csv      │
│  olist_geolocation_dataset.csv                                      │
└────────────────────────────┬────────────────────────────────────────┘
                             │  pandas.read_csv + date parsing
                             ▼
              ┌──────────────────────────┐
              │     OlistExtractor       │
              │  src/extract/            │
              └──────────────┬───────────┘
                             │  dict[str, pd.DataFrame]
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
┌─────────────────┐ ┌─────────────────┐ ┌──────────────────┐
│ OrdersTransformer│ │CustomersTransf. │ │ProductsTransf.   │
│ • filter deliv. │ │ • deduplication │ │ • category transl│
│ • delivery_days │ │ • value segment │ │ • total_sold     │
│ • is_late flag  │ │ • first/last ts │ │ • avg_price      │
│ • join payments │ └────────┬────────┘ └────────┬─────────┘
└────────┬────────┘          │                   │
         └──────────────────┬┴───────────────────┘
                            │  cleaned DataFrames
                            ▼
              ┌──────────────────────────┐
              │     PostgresLoader       │
              │  src/load/               │
              │  chunksize=1 000 rows    │
              │  verify_load (1% tol.)  │
              └──────────────┬───────────┘
                             │
              ┌──────────────▼───────────────┐
              │         PostgreSQL 15         │
              │                               │
              │  staging.*                    │
              │  ├── orders                   │
              │  ├── customers                │
              │  ├── products                 │
              │  ├── order_items              │
              │  ├── order_payments           │
              │  ├── order_reviews            │
              │  └── sellers                  │
              │                               │
              │  analytics.*                  │
              │  ├── fact_orders              │
              │  ├── dim_customers            │
              │  ├── dim_products             │
              │  └── agg_monthly_sales        │
              └───────────────────────────────┘
```

### Layers

| Schema | Purpose | Consumers |
|---|---|---|
| `staging` | Raw data, loaded as-is plus computed fields | Data validation, reprocessing |
| `analytics` | Fact/dimension tables with indexes | Tableau, Metabase, BI tools |

---

## Dataset

The [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) is a real anonymised export from the Olist platform, widely used in data engineering and ML portfolios.

**Download** → place all CSV files in `data/raw/` before running the pipeline.

| File | Rows (approx.) | Description |
|---|---|---|
| `olist_orders_dataset.csv` | 99 441 | Order header — status, timestamps |
| `olist_order_items_dataset.csv` | 112 650 | Line items — product, seller, price |
| `olist_order_payments_dataset.csv` | 103 886 | Payment methods and values |
| `olist_order_reviews_dataset.csv` | 99 224 | Customer reviews (1–5 score) |
| `olist_customers_dataset.csv` | 99 441 | Customer location data |
| `olist_products_dataset.csv` | 32 951 | Product attributes and dimensions |
| `olist_sellers_dataset.csv` | 3 095 | Seller location data |
| `product_category_name_translation.csv` | 71 | Portuguese → English category names |
| `olist_geolocation_dataset.csv` | 1 000 163 | ZIP-code lat/lon coordinates |

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Python | 3.11 | Pipeline language |
| pandas | 2.2.0 | Data manipulation and transformation |
| SQLAlchemy | 2.0.25 | Database abstraction layer |
| psycopg2-binary | 2.9.9 | PostgreSQL driver |
| loguru | 0.7.2 | Structured logging with rotation |
| python-dotenv | 1.0.0 | Environment variable management |
| PostgreSQL | 15 | Data warehouse |
| Docker Compose | 2.x | Container orchestration |
| pytest | 8.0.0 | Test runner |
| pytest-cov | 4.1.0 | Coverage reporting |
| ruff | 0.2.0 | Linter and import sorter |
| GitHub Actions | — | CI: lint + unit + integration tests |

---

## Project Structure

```
retail-etl-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml              # Lint → unit tests → integration tests
├── data/
│   ├── raw/                    # Olist CSVs (gitignored)
│   └── processed/              # Intermediate artefacts (gitignored)
├── src/
│   ├── extract/
│   │   └── olist_extractor.py  # CSV → DataFrame; date-aware parsing
│   ├── transform/
│   │   ├── orders_transformer.py     # Delivery metrics, payment join
│   │   ├── customers_transformer.py  # Dedup, segmentation, dates
│   │   └── products_transformer.py   # Category translation, sales agg
│   ├── load/
│   │   └── postgres_loader.py  # to_sql wrapper + row-count verification
│   ├── utils/
│   │   ├── logger.py           # loguru setup (console + file rotation)
│   │   └── db_connection.py    # Engine factory with retry + pool
│   └── pipeline.py             # OlistETLPipeline orchestrator + CLI
├── tests/
│   ├── conftest.py             # Shared fixtures (engine, DataFrames)
│   ├── test_extract.py         # Extractor unit tests + column validation
│   ├── test_transform.py       # Transformer unit tests (all 3 domains)
│   └── test_load.py            # Loader unit + integration tests
├── sql/
│   ├── create_schemas.sql
│   ├── staging/
│   │   └── create_staging_tables.sql
│   └── analytics/
│       └── create_analytics_tables.sql
├── docker/
│   └── postgres/
│       └── init.sql            # Schema + permission bootstrap on first run
├── .env.example                # Environment variable template
├── .gitignore
├── docker-compose.yml          # PostgreSQL 15 with healthcheck
├── Makefile                    # Developer shortcuts
├── pyproject.toml              # Project metadata, pytest config, ruff config
├── requirements.txt            # Runtime dependencies
└── requirements-dev.txt        # Dev + test dependencies
```

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (free Kaggle account required)

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/adrianopsf/retail-etl-pipeline.git
cd retail-etl-pipeline

# 2. Set up the virtual environment and install dependencies
make setup

# 3. Configure environment variables
cp .env.example .env
# Edit .env if you need to change database credentials

# 4. Download the Olist dataset from Kaggle
#    https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
#    Extract all CSV files to:
mkdir -p data/raw
# Place all *.csv files inside data/raw/

# 5. Start PostgreSQL
make up

# 6. Run the full pipeline
make run

# 7. Run the test suite
make test
```

### Verify the load

After `make run`, connect to the database and inspect the results:

```bash
docker compose exec postgres psql -U olist_user -d olist_dw -c "\dt staging.*"
docker compose exec postgres psql -U olist_user -d olist_dw -c "\dt analytics.*"
docker compose exec postgres psql -U olist_user -d olist_dw \
  -c "SELECT order_month, total_orders, total_revenue FROM analytics.agg_monthly_sales ORDER BY 1;"
```

---

## Pipeline Details

### Extract

`OlistExtractor` reads all 9 Olist CSVs from `data/raw/`. Each dataset has an explicit list of date columns that are parsed directly by `pandas.read_csv` — no post-hoc `pd.to_datetime` calls in the transform layer. Missing files raise `FileNotFoundError` with the Kaggle download URL embedded in the message.

### Transform

| Transformer | Key transformations |
|---|---|
| `OrdersTransformer` | Filter to `delivered` status; compute `delivery_days` (sentinel -1 when delivery date is null); flag `is_late_delivery`; create `order_month` (YYYY-MM); join `order_items` for `total_items`; join `order_payments` for `total_order_value` |
| `CustomersTransformer` | Deduplicate on `customer_unique_id` (keep highest-spend record); normalise state to uppercase and city to title-case; compute `total_orders`, `total_spent`, `first_order_date`, `last_order_date`; segment customers as `high_value` (≥ R$500), `medium_value` (R$200–500), or `low_value` (< R$200) |
| `ProductsTransformer` | Join English category translation; fill missing categories with `"unknown"`; build ASCII-safe `category_slug`; aggregate `total_sold` and `avg_price` from `order_items` |

### Load

`PostgresLoader` writes DataFrames with `to_sql(if_exists="replace", chunksize=1_000)`. After each load, `verify_load` counts rows in the target table and raises `ValueError` if the discrepancy exceeds 1 % of the expected count — catching partial writes and silent failures before they reach the analytics layer.

---

## Data Model

```
                    analytics schema — star schema
                    ─────────────────────────────

        dim_customers              dim_products
        ─────────────              ────────────
        customer_unique_id PK      product_id PK
        customer_id                category
        customer_city              category_slug
        customer_state             product_weight_g
        customer_segment           total_sold
        total_orders               avg_price
        total_spent
        first_order_date    ┌──────────────────────┐
        last_order_date     │     fact_orders       │
              │             │     ────────────      │
              └────────────►│  order_id PK          │◄──── dim_products
                            │  customer_id FK        │      (via order_items)
                            │  order_month           │
                            │  delivery_days         │
                            │  is_late_delivery      │
                            │  total_items           │
                            │  total_order_value     │
                            │  review_score          │
                            └──────────┬─────────────┘
                                       │ aggregated
                                       ▼
                            agg_monthly_sales
                            ─────────────────
                            order_month PK
                            total_orders
                            total_revenue
                            avg_order_value
                            avg_delivery_days
                            late_deliveries
                            unique_customers
```

---

## Testing

```bash
# Run all unit tests (no database required)
make test

# Run only integration tests (requires make up first)
pytest tests/ -m integration -v

# Run with coverage report
pytest tests/ --cov=src --cov-report=term-missing

# Run a specific test file
pytest tests/test_transform.py -v
```

### Test suites

| File | Scope | Count |
|---|---|---|
| `test_extract.py` | Unit — uses `tmp_path` and `unittest.mock` | 9 tests |
| `test_transform.py` | Unit — in-memory DataFrames only | 25 tests |
| `test_load.py` | Unit (mock engine) + Integration (real DB) | 13 tests |

Integration tests are automatically **skipped** when PostgreSQL is not reachable, so `make test` works without Docker on a developer machine.

---

## Makefile Reference

| Target | Description |
|---|---|
| `make setup` | Create `.venv` and install all dev dependencies |
| `make up` | Start PostgreSQL container in the background |
| `make down` | Stop and remove the PostgreSQL container |
| `make run` | Execute the full ETL pipeline (`python -m src.pipeline`) |
| `make test` | Run pytest with coverage report |
| `make lint` | Run ruff linter on `src/` and `tests/` |
| `make clean` | Remove `__pycache__`, `.pytest_cache`, coverage artefacts |

---

## License

MIT — see [LICENSE](LICENSE) for details.
