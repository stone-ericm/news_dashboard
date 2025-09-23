"""Dagster assets for news dashboard."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetMaterialization,
    MetadataValue,
    Output,
    asset,
)

from src.ingestion.google_trends import GoogleTrendsConnector
from src.ingestion.wikipedia import WikipediaPageviewsConnector
from src.models.source_registry import PREDEFINED_SOURCES, SourceRegistry
from src.models.taxonomy import TaxonomyManager, get_default_taxonomy


@asset(
    description="Source registry with all configured data sources",
    compute_kind="python",
)
def source_registry(context: AssetExecutionContext) -> Output[SourceRegistry]:
    """Initialize and populate source registry."""
    registry = SourceRegistry()
    
    # Add predefined sources
    for source in PREDEFINED_SOURCES:
        registry.add_source(source)
        context.log.info(f"Added source: {source.source_id}")
    
    registry.save()
    
    return Output(
        value=registry,
        metadata={
            "total_sources": len(registry.sources),
            "active_sources": len([s for s in registry.sources.values() if s.is_active]),
            "categories": list(set(s.category for s in registry.sources.values())),
        },
    )


@asset(
    description="Topic taxonomy for categorizing signals",
    compute_kind="python",
)
def topic_taxonomy(context: AssetExecutionContext) -> Output[TaxonomyManager]:
    """Initialize topic taxonomy."""
    taxonomy = get_default_taxonomy()
    
    # Save to disk
    taxonomy.save()
    
    topic_count = len(taxonomy.topics)
    context.log.info(f"Initialized taxonomy with {topic_count} topics")
    
    return Output(
        value=taxonomy,
        metadata={
            "total_topics": topic_count,
            "root_categories": len([t for t in taxonomy.topics.values() if t.parent_id is None]),
        },
    )


@asset(
    description="Google Trends data for configured topics",
    compute_kind="python",
    deps=[source_registry, topic_taxonomy],
)
def google_trends_raw(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch Google Trends data."""
    # Load dependencies
    registry = SourceRegistry()
    taxonomy = TaxonomyManager()
    taxonomy.load()
    
    # Get topics with Google Trends keywords
    topics_with_keywords = [
        topic for topic in taxonomy.topics.values()
        if topic.google_trends_queries
    ]
    
    context.log.info(f"Fetching trends for {len(topics_with_keywords)} topics")
    
    # Initialize connector
    connector = GoogleTrendsConnector()
    
    # Fetch data
    all_data = []
    for topic in topics_with_keywords[:5]:  # Limit for MVP
        for query in topic.google_trends_queries[:2]:  # Limit queries per topic
            try:
                data = connector.fetch_interest_over_time(
                    keywords=[query],
                    timeframe="today 3-m",
                    geo="US",
                )
                if data is not None and not data.empty:
                    data["topic_id"] = topic.topic_id
                    data["query"] = query
                    all_data.append(data)
                    context.log.info(f"Fetched data for {topic.name}: {query}")
            except Exception as e:
                context.log.warning(f"Failed to fetch {query}: {e}")
    
    # Combine results
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
    else:
        result_df = pd.DataFrame()
    
    # Save to bronze
    bronze_path = Path("data/bronze/google_trends")
    bronze_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = bronze_path / f"trends_{timestamp}.parquet"
    
    if not result_df.empty:
        result_df.to_parquet(file_path)
    
    # Update source health
    source = registry.get_source("google_trends")
    if source:
        from src.models.source_registry import HealthStatus
        
        registry.update_health(
            "google_trends",
            HealthStatus.HEALTHY if not result_df.empty else HealthStatus.DEGRADED,
            is_success=not result_df.empty,
        )
    
    return Output(
        value=result_df,
        metadata={
            "rows": len(result_df),
            "topics": len(result_df["topic_id"].unique()) if not result_df.empty else 0,
            "file_path": str(file_path),
        },
    )


@asset(
    description="Wikipedia pageview data for configured topics",
    compute_kind="python",
    deps=[source_registry, topic_taxonomy],
)
def wikipedia_pageviews_raw(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Fetch Wikipedia pageview data."""
    # Load dependencies
    registry = SourceRegistry()
    taxonomy = TaxonomyManager()
    taxonomy.load()
    
    # Get topics with Wikipedia articles
    topics_with_articles = [
        topic for topic in taxonomy.topics.values()
        if topic.wikipedia_articles
    ]
    
    context.log.info(f"Fetching pageviews for {len(topics_with_articles)} topics")
    
    # Initialize connector
    connector = WikipediaPageviewsConnector()
    
    # Time range
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=90)
    
    # Fetch data
    all_data = []
    for topic in topics_with_articles[:5]:  # Limit for MVP
        for article in topic.wikipedia_articles[:2]:  # Limit articles per topic
            try:
                data = connector.fetch_article_pageviews(
                    article=article,
                    start=start_date.strftime("%Y%m%d"),
                    end=end_date.strftime("%Y%m%d"),
                    project="en.wikipedia",
                )
                if data is not None and not data.empty:
                    data["topic_id"] = topic.topic_id
                    data["article"] = article
                    all_data.append(data)
                    context.log.info(f"Fetched data for {topic.name}: {article}")
            except Exception as e:
                context.log.warning(f"Failed to fetch {article}: {e}")
    
    # Combine results
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
    else:
        result_df = pd.DataFrame()
    
    # Save to bronze
    bronze_path = Path("data/bronze/wikipedia")
    bronze_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = bronze_path / f"pageviews_{timestamp}.parquet"
    
    if not result_df.empty:
        result_df.to_parquet(file_path)
    
    # Update source health
    source = registry.get_source("wikipedia_pageviews")
    if source:
        from src.models.source_registry import HealthStatus
        
        registry.update_health(
            "wikipedia_pageviews",
            HealthStatus.HEALTHY if not result_df.empty else HealthStatus.DEGRADED,
            is_success=not result_df.empty,
        )
    
    return Output(
        value=result_df,
        metadata={
            "rows": len(result_df),
            "topics": len(result_df["topic_id"].unique()) if not result_df.empty else 0,
            "file_path": str(file_path),
        },
    )
