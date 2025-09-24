"""Dagster definitions for news dashboard."""

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define jobs
daily_ingestion_job = define_asset_job(
    name="daily_ingestion",
    selection=[
        "source_registry", 
        "topic_taxonomy", 
        "google_trends_raw", 
        "wikipedia_pageviews_raw",
        "faostat_raw",
        "opensky_raw"
    ],
    description="Daily data ingestion from all sources",
)

# Define schedules
daily_schedule = ScheduleDefinition(
    job=daily_ingestion_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    description="Daily ingestion schedule",
)

# Definitions
defs = Definitions(
    assets=all_assets,
    schedules=[daily_schedule],
)
