![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-FDB515?style=flat&logo=timescale&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

# Stock Data Platform

**Real-time stock data pipeline with Kafka streaming, Airflow orchestration, and a TimescaleDB star-schema warehouse.**

Tracks 10 major tickers (AAPL, AMZN, GOOG, META, MSFT, NFLX, NVDA, TSLA, JPM, DIS) through a fully containerized pipeline — from live market data ingestion to aggregated analytics.

---

## Architecture

<img src="architecture.png" alt="Architecture Diagram" width="800"/>

```
Market Data API
      │
      ▼
┌─────────────┐    ┌───────────┐    ┌──────────────────┐
│ Kafka        │    │ Kafka     │    │ TimescaleDB      │
│ Producer     │───▶│ Broker    │───▶│ (PostgreSQL 14)  │
│ (live data)  │    │           │    │                  │
└─────────────┘    └───────────┘    │ ┌──────────────┐ │
                                    │ │ dim_company   │ │
┌─────────────────────────────┐     │ │ dim_date      │ │
│ Airflow                     │     │ │ fact_daily    │ │
│ ┌─────────┐ ┌─────────────┐│     │ │ fact_monthly  │ │
│ │Scheduler│ │ Webserver   ││────▶│ └──────────────┘ │
│ └─────────┘ │ :8081       ││     └──────────────────┘
│             └─────────────┘│
│ DAGs:                      │
│  • ETL stock data          │
│  • Populate dimensions     │
│  • Monthly aggregation     │
└─────────────────────────────┘
```

---

## Data Model (Star Schema)

| Table | Type | Description |
|-------|------|-------------|
| `dim_company` | Dimension | Ticker, company name, sector, industry, exchange (SCD Type 2) |
| `dim_date` | Dimension | Year, quarter, month, day, weekend flag |
| `fact_stock_price_daily` | Fact | OHLCV data per ticker per day |
| `fact_stock_price_monthly` | Fact | Aggregated monthly averages and total volume |

Built on **TimescaleDB** for time-series optimized queries on top of PostgreSQL 14.

---

## Services (Docker Compose)

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `timescaledb` | timescale/timescaledb:latest-pg14 | 5432 | Data warehouse |
| `airflow-webserver` | apache/airflow:2.7.3 | 8081 | DAG monitoring UI |
| `airflow-scheduler` | apache/airflow:2.7.3 | — | DAG execution |
| `zookeeper` | confluentinc/cp-zookeeper | 2181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka | 9092 | Message broker |
| `kafka-producer` | Custom (Dockerfile.producer) | — | Market data ingestion |
| `kafka-consumer` | Custom (Dockerfile.consumer) | — | Kafka → TimescaleDB sink |

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `etl_stock_data_dag` | Daily | End-to-end stock data ETL |
| `populate_dim_company_dag` | On-demand | Load company dimension table |
| `populate_dim_date_dag` | On-demand | Generate date dimension |
| `populate_fact_stock_price_dag` | Daily | Load daily OHLCV facts |
| `monthly_aggregate_dag` | Monthly | Compute monthly price aggregations |

---

## Quickstart

```bash
# 1. Start all services
docker compose up -d

# 2. Access Airflow UI
open http://localhost:8081   # admin / admin

# 3. Enable DAGs and trigger:
#    - populate_dim_company_dag (first)
#    - populate_dim_date_dag
#    - etl_stock_data_dag

# 4. Query the warehouse
docker exec -it timescaledb psql -U data226 -d stockdw \
  -c "SELECT * FROM fact_stock_price_daily ORDER BY date DESC LIMIT 10;"
```

---

## Project Structure

```
├── Dags/                          # Airflow DAG definitions
│   ├── etl_stock_data_dag.py
│   ├── populate_dim_company_dag.py
│   ├── populate_dim_date_dag.py
│   ├── populate_fact_stock_price_dag.py
│   ├── monthly_aggregate_dag.py
│   └── stock_csvs/                # Sample data (10 tickers × 30 days)
├── SQL/
│   ├── schema.sql                 # Star schema DDL (TimescaleDB)
│   └── aggregate_monthly.sql      # Monthly rollup query
├── scripts/
│   ├── populate_dim_company.py
│   ├── populate_dim_date.py
│   └── populate_fact_stock_price.py
├── kafka_to_postgres.py           # Kafka consumer → TimescaleDB
├── live_from_kafka.py             # Real-time Kafka stream reader
├── Dockerfile.producer            # Market data producer container
├── Dockerfile.consumer            # Kafka→DB consumer container
├── docker-compose.yml             # 7-service orchestration
└── requirements.txt
```

---

## Tech Stack

- **Streaming**: Apache Kafka (Confluent) with Zookeeper
- **Orchestration**: Apache Airflow 2.7
- **Database**: TimescaleDB (PostgreSQL 14 + time-series extensions)
- **Containerization**: Docker Compose (7 services)
- **Language**: Python
