# News Dashboard

Evidence-driven dashboard for surfacing statistically significant real-world trend changes regardless of mainstream media coverage.

## Vision

Identify what is moving in the world using diverse short-term and long-term data sources, with seasonality-aware baselines and transparent methods. Rank topics by statistical movement and confidence, not by media volume.

## Scope (MVP)

- Domains: Economy, Public Health, Crime, Weather/Climate
- Cadence: Daily scoring (hourly as a stretch)
- Geography: National + top metros
- Outputs: "Today’s movers", topic detail pages, CSV export, basic alerts

## High-level Architecture

- Ingestion/orchestration: Dagster
- Processing: Python + dbt
- Storage: Bronze (raw) → Silver (curated) → Gold (serving)
- Serving: API (REST/GraphQL) + Next.js dashboard (Vega-Lite)

## International Data Sources (examples)

- Short-term: Google Trends, Wikipedia Pageviews, OpenAQ, ENTSO‑E, NASA FIRMS, USGS/EMSC, Eurocontrol/OpenSky, WFP HungerMap Live
- Long-term: World Bank, IMF IFS/WEO, OECD, Eurostat, UN Data/UNSD, ILOSTAT, UNODC, UNHCR, FAOSTAT, WHO GHO, Global Forest Watch

## Licensing and Compliance

Each source is tracked with cadence, lag, license, and TOS metadata in a source registry. Only public/allowed fields are stored; no PII.


