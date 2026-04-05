# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [1.0.0] - 2026-04-05

### Added
- Real-time Kafka streaming pipeline (producer + consumer)
- Airflow DAGs: ETL, fundamentals, earnings, SEC filings, macro data
- TimescaleDB star schema with 4 dimension and 5 fact tables
- Docker Compose orchestration (7 services)
- GitHub Pages landing page and interactive dashboard
- CI workflow with ruff linting and formatting
- Unit test suite (62 tests via pytest)
- Community files: CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md
- Issue and PR templates
- Dependabot for automated dependency updates

### Data Sources
- yfinance — stock prices, company info, earnings
- SEC EDGAR via EdgarTools — 10-K/10-Q financial statements
- FRED via fredapi — macro indicators (fed funds rate, CPI, GDP, unemployment)
