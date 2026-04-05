# Contributing

Thanks for your interest in contributing to the Stock Data Platform!

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- A free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html)

### Local Setup

```bash
# Clone the repo
git clone https://github.com/jayhemnani9910/stock-data-platform.git
cd stock-data-platform

# Copy environment template and fill in your values
cp .env.example .env

# Start all services
docker compose up -d

# Verify services are running
docker compose ps
```

### Running Lints

```bash
pip install ruff
ruff check .
ruff format --check .
```

## Making Changes

1. Fork the repo and create a branch from `master`
2. Make your changes
3. Run `ruff check .` and `ruff format --check .` — CI will enforce this
4. Commit with a clear message describing what and why
5. Open a pull request against `master`

## Project Structure

| Directory | What lives here |
|-----------|----------------|
| `Dags/` | Airflow DAG definitions |
| `scripts/` | Python scripts for data population |
| `SQL/` | Schema DDL and SQL queries |
| `site/` | GitHub Pages landing page and dashboard |
| `docs/` | Architecture diagrams (D2 format) |

## Code Style

- Python code is formatted and linted with [ruff](https://docs.astral.sh/ruff/)
- Follow existing patterns in the codebase
- Keep changes focused — one issue per PR

## Reporting Bugs

Use the [Bug Report](https://github.com/jayhemnani9910/stock-data-platform/issues/new?template=bug_report.yml) template.

## Suggesting Features

Use the [Feature Request](https://github.com/jayhemnani9910/stock-data-platform/issues/new?template=feature_request.yml) template.
