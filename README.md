![CI](https://github.com/jayhemnani9910/stock-data-platform/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-FDB515?style=flat&logo=timescale&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![License: MIT](https://img.shields.io/github/license/jayhemnani9910/stock-data-platform)

# Stock Data Platform

**End-to-end data engineering pipeline that ingests, transforms, and warehouses real-time stock market data — built to demonstrate production-grade streaming, orchestration, and dimensional modeling.**

<p align="center">
  <a href="https://jayhemnani9910.github.io/stock-data-platform/">
    <img src="architecture.png" alt="Stock Data Platform" width="800"/>
  </a>
  <br>
  <a href="https://jayhemnani9910.github.io/stock-data-platform/"><strong>View Live Site</strong></a> · <a href="https://jayhemnani9910.github.io/stock-data-platform/dashboard.html"><strong>Interactive Dashboard</strong></a>
</p>

---

## What It Does

Tracks **10 major US equities** (AAPL, AMZN, GOOG, META, MSFT, NFLX, NVDA, TSLA, JPM, DIS) through a fully containerized pipeline:

- **Streams** live market data via Kafka (producer → broker → consumer)
- **Orchestrates** 10 Airflow DAGs for ETL, fundamentals, earnings, SEC filings, and macro data
- **Warehouses** everything in a TimescaleDB star schema (4 dimension + 5 fact tables)
- **Visualizes** results on a Bloomberg Terminal-style [dashboard](https://jayhemnani9910.github.io/stock-data-platform/dashboard.html) with Chart.js

### Data Sources

| Source | Data | Method |
|--------|------|--------|
| **yfinance** | Prices, company info, earnings | Python API |
| **SEC EDGAR** | 10-K/10-Q financial statements | EdgarTools |
| **FRED** | Fed funds rate, CPI, GDP, unemployment | fredapi |

---

## Live Demo

Explore the platform's output — no setup required:

<p align="center">
  <a href="https://jayhemnani9910.github.io/stock-data-platform/">
    <img src="https://img.shields.io/badge/Landing_Page-View_Site-blue?style=for-the-badge" alt="Landing Page"/>
  </a>
  <a href="https://jayhemnani9910.github.io/stock-data-platform/dashboard.html">
    <img src="https://img.shields.io/badge/Dashboard-Live_Data-green?style=for-the-badge" alt="Dashboard"/>
  </a>
</p>

The landing page features a Bloomberg-style ticker tape and architecture overview. The dashboard displays interactive charts for price trends, fundamentals, earnings surprises, and macro indicators.

---

## Architecture

<img src="architecture.png" alt="Architecture Diagram" width="800"/>

**7 Docker containers** work together:

| Service | Image | Port | Role |
|---------|-------|------|------|
| `timescaledb` | timescale/timescaledb:latest-pg14 | 5432 | Star-schema warehouse |
| `airflow-webserver` | Custom (Dockerfile.airflow) | 8081 | DAG monitoring UI |
| `airflow-scheduler` | Custom (Dockerfile.airflow) | — | DAG execution engine |
| `zookeeper` | confluentinc/cp-zookeeper | 2181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka | 9092 | Message broker |
| `kafka-producer` | Custom (Dockerfile.kafka) | — | Market data ingestion |
| `kafka-consumer` | Custom (Dockerfile.kafka) | — | Kafka → TimescaleDB sink |

---

## Data Model (Star Schema)

| Table | Type | Description |
|-------|------|-------------|
| `dim_company` | Dimension | Ticker, company name, sector, industry, exchange (SCD Type 2) |
| `dim_date` | Dimension | Year, quarter, month, day, weekend flag |
| `dim_macro_indicator` | Dimension | FRED macro series metadata |
| `fact_stock_price_daily` | Fact | OHLCV data per ticker per day |
| `fact_stock_price_monthly` | Fact | Aggregated monthly averages and total volume |
| `fact_company_fundamentals` | Fact | Market cap, PE ratios, dividends, beta |
| `fact_earnings` | Fact | Quarterly EPS: estimate vs actual, surprise % |
| `fact_sec_financials` | Fact | SEC filing line items (income, balance sheet, cash flow) |
| `fact_macro_data` | Fact | Fed funds rate, CPI, unemployment, GDP time series |

Built on **TimescaleDB** for time-series optimized queries on PostgreSQL 14.

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `etl_stock_data_<ticker>` | Daily | Per-ticker ETL (one DAG per ticker) |
| `populate_dim_company` | On-demand | Load company dimension table |
| `populate_dim_date` | On-demand | Generate date dimension (1990-2035) |
| `fundamentals_daily` | Daily | Fetch company fundamentals |
| `earnings_weekly` | Weekly | Fetch earnings dates and EPS surprises |
| `sec_financials_quarterly` | Quarterly | Fetch SEC 10-K/10-Q financial statements |
| `macro_daily` | Daily | Fetch FRED macro indicators |
| `monthly_aggregate_dag` | Monthly | Compute monthly price aggregations |
| `csv_export_dag` | Triggered | Export last 30 days to CSV per ticker |

---

## Quickstart

### Prerequisites

- Docker and Docker Compose
- A free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html)

### Setup

```bash
# Clone and configure
git clone https://github.com/jayhemnani9910/stock-data-platform.git
cd stock-data-platform
cp .env.example .env    # Fill in your FRED_API_KEY and EDGAR_IDENTITY

# Start all 7 services
docker compose up -d

# Access Airflow UI
open http://localhost:8081   # admin / admin

# Initialize dimensions (run these DAGs first)
#   1. populate_dim_company
#   2. populate_dim_date
#   3. etl_stock_data_aapl (or any ticker DAG)

# Query the warehouse
docker exec -it timescaledb psql -U data226 -d stockdw \
  -c "SELECT * FROM fact_stock_price_daily ORDER BY date DESC LIMIT 10;"
```

---

## Project Structure

```
├── Dags/                          # Airflow DAG definitions
│   ├── dag_config.py              # Shared DAG defaults and ticker loading
│   ├── etl_stock_data_dag.py      # Per-ticker extract → transform → load
│   ├── populate_dags.py           # Dimension and fact table population
│   ├── fundamentals_dag.py        # Daily company fundamentals
│   ├── earnings_dag.py            # Weekly earnings data
│   ├── sec_financials_dag.py      # Quarterly SEC filings
│   ├── macro_dag.py               # Daily macro indicators
│   └── monthly_aggregate_dag.py   # Monthly rollups
├── scripts/                       # Data population and utilities
│   ├── db_utils.py                # Shared DB connection, batch insert, upsert SQL
│   ├── populate_dim_company.py
│   ├── populate_dim_date.py
│   ├── populate_company_fundamentals.py
│   ├── populate_earnings.py
│   ├── populate_sec_financials.py
│   └── populate_macro_data.py
├── SQL/                           # Schema and queries
│   ├── schema.sql                 # Star schema DDL (TimescaleDB)
│   └── aggregate_monthly.sql      # Monthly rollup query
├── tests/                         # Unit tests (pytest)
│   └── unit/                      # 62 tests across 7 modules
├── site/                          # GitHub Pages (landing page + dashboard)
├── docs/                          # Architecture diagrams (D2 format)
├── kafka_to_postgres.py           # Kafka consumer → TimescaleDB
├── live_from_kafka.py             # Real-time Kafka producer
├── Dockerfile.kafka               # Kafka producer/consumer image
├── Dockerfile.airflow             # Custom Airflow image
├── docker-compose.yml             # 7-service orchestration
├── ruff.toml                      # Linter and formatter config
├── Makefile                       # Common commands (up, down, test, lint)
├── .env.example                   # Environment variable template
└── requirements.txt               # Pinned Python dependencies
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Streaming** | Apache Kafka (Confluent) with Zookeeper |
| **Orchestration** | Apache Airflow 2.7 |
| **Database** | TimescaleDB (PostgreSQL 14 + time-series extensions) |
| **Containerization** | Docker Compose (7 services) |
| **CI/CD** | GitHub Actions (ruff lint + pytest) |
| **Frontend** | GitHub Pages + Chart.js |
| **Language** | Python 3.12 |

---

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions and guidelines.

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
