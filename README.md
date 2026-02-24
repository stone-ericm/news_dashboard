# News Dashboard

Evidence-driven dashboard for surfacing statistically significant real-world trend changes regardless of media coverage.

## The Idea

Media coverage is a poor proxy for what is actually changing in the world. A story can dominate headlines for weeks while the underlying data shows nothing unusual, and genuine shifts in food prices, air traffic, or disease incidence can go unreported entirely.

News Dashboard pulls structured data directly from authoritative sources, applies seasonality decomposition and statistical tests, and flags only the trends that represent a meaningful departure from historical baselines. The goal is to answer a simple question: what is actually changing right now, and by how much?

## Architecture

The pipeline follows a Bronze / Silver / Gold layered architecture:

- **Bronze** -- Raw ingestion. Data is pulled from external APIs and stored as-is with minimal transformation. Each source has its own ingestion module under `src/ingestion/`.
- **Silver** -- Cleaned and normalized. Timestamps are aligned, units are standardized, and missing values are handled. Seasonality decomposition happens here to separate signal from expected cyclical variation.
- **Gold** -- Analytical output. Statistical significance tests identify trend changes that exceed historical norms. These results feed the dashboard and real-time alerts.

Pipeline orchestration is handled by **Dagster**. Job definitions, schedules, and sensors live in `src/orchestration/`. Dagster manages dependencies between layers, retries on transient failures, and provides observability into each pipeline run.

A **caching layer** (`src/cache/manager.py`) sits between ingestion and the API to avoid redundant external calls and speed up repeated queries.

## Data Sources

| Source | Domain | What it provides |
|---|---|---|
| FAOSTAT | Economy, Food | Global food production and price indices |
| USDA NASS | Economy, Food | US agricultural statistics |
| Google Trends | Cross-domain | Search interest as a proxy for public attention |
| OpenSky Network | Transportation | Global air traffic volume |
| Wikipedia | Cross-domain | Pageview data as a cultural attention signal |

## Tech Stack

- **Python 3.10+**
- **Dagster** -- pipeline orchestration, scheduling, and monitoring
- **FastAPI** -- backend API with WebSocket support for real-time updates
- **Statistical processing** -- seasonality decomposition and significance testing
- **Tooling** -- black, ruff, mypy, pytest with coverage

## Getting Started

```
git clone https://github.com/stone-ericm/news_dashboard.git
cd news_dashboard
```

Create a virtual environment and install dependencies:

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start the Dagster development server to run pipelines:

```
dagster dev
```

Start the FastAPI backend:

```
uvicorn src.api.main:app --reload
```

## Project Structure

```
src/
  ingestion/       # Bronze layer -- source-specific data pullers
  processing/      # Silver/Gold layers -- seasonality, statistics
  orchestration/   # Dagster jobs, schedules, sensors
  cache/           # Caching layer for API and ingestion
  api/             # FastAPI backend with WebSocket support
dagster.yaml       # Dagster instance configuration
```

## Domains

The dashboard currently tracks trends across four domains:

- **Economy** -- food prices, agricultural output, trade flows
- **Public Health** -- disease incidence, health-related search trends
- **Crime** -- reported crime statistics and attention signals
- **Weather / Climate** -- temperature anomalies, extreme event frequency
