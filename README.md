# retail-etl-pipeline

End-to-end ETL pipeline ingesting the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset into a structured PostgreSQL data warehouse.

![CI](https://github.com/adrianopsf/retail-etl-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-316192?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

## Architecture

```
data/raw/          ← Olist CSV files (gitignored, downloaded from Kaggle)
    │
    ▼
src/extract/       ← Reads CSVs into DataFrames
    │
    ▼
src/transform/     ← Cleans, types, and enriches each entity
    │
    ▼
src/load/          ← Writes to PostgreSQL (staging schema)
    │
    ▼
sql/analytics/     ← Builds the analytics layer on top of staging
```

**Schemas:**
- `staging` — raw data, loaded directly from source files
- `analytics` — fact and dimension tables ready for BI tools

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) downloaded into `data/raw/`

### Setup

```bash
# 1. Clone and enter the project
git clone https://github.com/adrianopsf/retail-etl-pipeline.git
cd retail-etl-pipeline

# 2. Create virtual environment and install dependencies
make setup

# 3. Copy and edit environment variables
cp .env.example .env

# 4. Start PostgreSQL
make up

# 5. Run the pipeline
make run
```

### Run tests

```bash
make test
```

### Lint

```bash
make lint
```

## Project Structure

```
retail-etl-pipeline/
├── .github/workflows/ci.yml    # GitHub Actions: lint + test on every push
├── src/
│   ├── extract/                # CSV → DataFrame
│   ├── transform/              # business logic per entity
│   ├── load/                   # DataFrame → PostgreSQL
│   ├── utils/                  # logger, db connection
│   └── pipeline.py             # orchestrator (extract → transform → load)
├── tests/                      # pytest unit tests with fixtures
├── sql/
│   ├── staging/                # DDL for the staging layer
│   └── analytics/              # DDL for the analytics layer
├── docker/postgres/init.sql    # schema bootstrap on container creation
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```

## Dataset

The [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) contains ~100k orders from 2016–2018 across the Brazilian e-commerce market. It includes orders, customers, products, sellers, payments, reviews, and geolocation data.

Download and place all CSV files in `data/raw/` before running the pipeline.

## License

MIT
