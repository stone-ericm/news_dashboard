#!/usr/bin/env python
"""Test script to verify basic functionality."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.models.source_registry import PREDEFINED_SOURCES, SourceRegistry
from src.models.taxonomy import get_default_taxonomy


def test_source_registry():
    """Test source registry functionality."""
    print("\n=== Testing Source Registry ===")
    
    registry = SourceRegistry()
    
    # Add predefined sources
    for source in PREDEFINED_SOURCES:
        registry.add_source(source)
    
    # List sources
    sources = registry.list_sources()
    print(f"Total sources: {len(sources)}")
    
    for source in sources:
        print(f"  - {source.name} ({source.source_id}): {source.cadence_expected.value}")
    
    # Save and reload
    registry.save()
    registry2 = SourceRegistry()
    assert len(registry2.sources) == len(registry.sources)
    print("✓ Source registry save/load working")


def test_taxonomy():
    """Test taxonomy functionality."""
    print("\n=== Testing Taxonomy ===")
    
    taxonomy = get_default_taxonomy()
    
    # Count topics
    total = len(taxonomy.topics)
    root = len([t for t in taxonomy.topics.values() if t.parent_id is None])
    
    print(f"Total topics: {total}")
    print(f"Root categories: {root}")
    
    # Print hierarchy
    for topic in taxonomy.topics.values():
        if topic.parent_id is None:
            print(f"\n{topic.name}:")
            children = taxonomy.get_children(topic.topic_id)
            for child in children:
                print(f"  - {child.name}")
                if child.google_trends_queries:
                    print(f"    Trends: {', '.join(child.google_trends_queries[:2])}")
                if child.wikipedia_articles:
                    print(f"    Wiki: {', '.join(child.wikipedia_articles[:2])}")
    
    # Save
    taxonomy.save()
    print("\n✓ Taxonomy save working")


def test_connectors():
    """Test data connectors (without making actual API calls)."""
    print("\n=== Testing Connectors ===")
    
    # Test imports
    try:
        from src.ingestion.google_trends import GoogleTrendsConnector
        from src.ingestion.wikipedia import WikipediaPageviewsConnector
        
        # Initialize connectors
        trends = GoogleTrendsConnector()
        wiki = WikipediaPageviewsConnector()
        
        print("✓ Google Trends connector initialized")
        print("✓ Wikipedia connector initialized")
        
    except Exception as e:
        print(f"✗ Connector initialization failed: {e}")


def test_seasonality():
    """Test seasonality processing."""
    print("\n=== Testing Seasonality Processing ===")
    
    import numpy as np
    import pandas as pd
    
    from src.processing.seasonality import SeasonalityProcessor
    
    # Create synthetic data with weekly seasonality
    dates = pd.date_range("2023-01-01", periods=180, freq="D")
    
    # Trend + weekly pattern + noise
    trend = np.linspace(100, 120, len(dates))
    seasonal = 10 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
    noise = np.random.normal(0, 2, len(dates))
    
    series = pd.Series(trend + seasonal + noise, index=dates)
    
    # Process
    processor = SeasonalityProcessor(seasonal_period=7)
    
    # Compute features
    baseline = processor.compute_baseline(series, window_days=30)
    z_scores = processor.compute_zscore(series, baseline)
    anomalies = processor.detect_anomalies(series, z_threshold=2.5)
    
    print(f"Series length: {len(series)}")
    print(f"Mean z-score: {z_scores.mean():.3f}")
    print(f"Std z-score: {z_scores.std():.3f}")
    print(f"Anomalies detected: {anomalies.sum()}")
    
    print("✓ Seasonality processing working")


def test_dagster():
    """Test Dagster configuration."""
    print("\n=== Testing Dagster Configuration ===")
    
    try:
        from src.orchestration.definitions import defs
        
        print(f"Assets defined: {len(defs.assets or [])}")
        print(f"Schedules defined: {len(defs.schedules or [])}")
        
        print("✓ Dagster configuration loaded")
        
    except Exception as e:
        print(f"✗ Dagster configuration failed: {e}")


if __name__ == "__main__":
    print("News Dashboard - Setup Test")
    print("=" * 40)
    
    test_source_registry()
    test_taxonomy()
    test_connectors()
    test_seasonality()
    test_dagster()
    
    print("\n" + "=" * 40)
    print("All tests completed!")
    print("\nNext steps:")
    print("1. Install dependencies: pip install -r requirements.txt")
    print("2. Set up environment variables in .env file")
    print("3. Run Dagster UI: dagster dev -f src/orchestration/definitions.py")
    print("4. Trigger the daily_ingestion job to fetch data")
