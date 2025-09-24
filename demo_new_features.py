#!/usr/bin/env python
"""Demonstration script for new agriculture and transportation features."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.models.source_registry import SourceRegistry
from src.models.taxonomy import get_default_taxonomy
from src.ingestion.faostat import FAOSTATConnector
from src.ingestion.usda_nass import USDANASSConnector
from src.ingestion.opensky import OpenSkyConnector


def demo_source_registry():
    """Demonstrate the expanded source registry."""
    print("\n=== EXPANDED SOURCE REGISTRY ===")
    
    registry = SourceRegistry()
    
    # Show all sources by category
    categories = {}
    for source in registry.sources.values():
        if source.category not in categories:
            categories[source.category] = []
        categories[source.category].append(source)
    
    for category, sources in categories.items():
        print(f"\n{category.upper()} SOURCES:")
        for source in sources:
            print(f"  - {source.name}")
            print(f"    ID: {source.source_id}")
            print(f"    Cadence: {source.cadence_expected.value}")
            print(f"    Geographic Scope: {source.geographic_scope.value}")
            if source.auth_method.value != "none":
                print(f"    Auth Required: {source.auth_method.value}")
            print()


def demo_taxonomy():
    """Demonstrate the expanded taxonomy."""
    print("\n=== EXPANDED TAXONOMY ===")
    
    taxonomy = get_default_taxonomy()
    
    # Show root categories and their children
    root_topics = [t for t in taxonomy.topics.values() if t.parent_id is None]
    
    print(f"Total topics: {len(taxonomy.topics)}")
    print(f"Root categories: {len(root_topics)}")
    
    for root in sorted(root_topics, key=lambda x: x.priority):
        print(f"\n{root.name.upper()} ({root.color}):")
        print(f"  Description: {root.description}")
        
        children = taxonomy.get_children(root.topic_id)
        for child in children:
            print(f"  - {child.name}")
            if child.google_trends_queries:
                print(f"    Google Trends: {', '.join(child.google_trends_queries[:2])}")
            if child.wikipedia_articles:
                print(f"    Wikipedia: {', '.join(child.wikipedia_articles[:2])}")


def demo_agriculture_connectors():
    """Demonstrate agriculture data connectors."""
    print("\n=== AGRICULTURE DATA CONNECTORS ===")
    
    # FAOSTAT
    print("\nFAOSTAT Connector:")
    fao = FAOSTATConnector()
    print("✓ Initialized")
    
    try:
        # Try to get country codes
        print("  Attempting to fetch country codes...")
        countries = fao.get_country_codes()
        if countries:
            print(f"  ✓ Found {len(countries)} countries")
            # Show a few examples
            examples = list(countries.items())[:5]
            for name, code in examples:
                print(f"    {name}: {code}")
        else:
            print("  ⚠ Could not fetch country codes (API may be down)")
    except Exception as e:
        print(f"  ⚠ Error: {e}")
    
    # USDA NASS
    print("\nUSDA NASS Connector:")
    usda = USDANASSConnector()  # No API key provided
    print("✓ Initialized (requires API key for data access)")
    print("  Note: Register at https://quickstats.nass.usda.gov/api for free API key")


def demo_transportation_connectors():
    """Demonstrate transportation data connectors."""
    print("\n=== TRANSPORTATION DATA CONNECTORS ===")
    
    # OpenSky Network
    print("\nOpenSky Network Connector:")
    opensky = OpenSkyConnector()
    print("✓ Initialized")
    
    try:
        # Get current flight states for a small region (NYC area)
        print("  Fetching current aircraft states (NYC area)...")
        bbox = (40.0, 41.0, -74.0, -73.0)  # Small area to avoid rate limits
        states = opensky.fetch_states(bbox=bbox)
        
        if states is not None and not states.empty:
            print(f"  ✓ Found {len(states)} aircraft currently in the air")
            
            # Show some statistics
            on_ground = len(states[states['on_ground'] == True])
            in_air = len(states[states['on_ground'] == False])
            countries = states['origin_country'].nunique()
            
            print(f"    - In air: {in_air}")
            print(f"    - On ground: {on_ground}")
            print(f"    - Countries represented: {countries}")
            
            if not states.empty:
                avg_alt = states['baro_altitude'].mean()
                avg_vel = states['velocity'].mean()
                print(f"    - Average altitude: {avg_alt:.0f} meters")
                print(f"    - Average velocity: {avg_vel:.0f} m/s")
        else:
            print("  ⚠ No aircraft data returned (may be rate limited)")
            
    except Exception as e:
        print(f"  ⚠ Error: {e}")
    
    # Show major airports
    airports = opensky.get_major_airports()
    print(f"\n  Major airports configured: {len(airports)}")
    print(f"    Examples: {', '.join(airports[:5])}")


def demo_data_pipeline():
    """Demonstrate how the data pipeline would work."""
    print("\n=== DATA PIPELINE DEMONSTRATION ===")
    
    print("\nConfigured Dagster Assets:")
    assets = [
        "source_registry - Manages all data source configurations",
        "topic_taxonomy - Hierarchical topic categorization",
        "google_trends_raw - Search interest data",
        "wikipedia_pageviews_raw - Article attention data", 
        "faostat_raw - Agricultural production and yield data",
        "opensky_raw - Real-time aviation data"
    ]
    
    for asset in assets:
        print(f"  ✓ {asset}")
    
    print("\nDaily Ingestion Job:")
    print("  - Runs at 2 AM daily")
    print("  - Fetches data from all configured sources")
    print("  - Saves raw data to Bronze layer (Parquet files)")
    print("  - Updates source health monitoring")
    print("  - Provides metadata for each ingestion")
    
    print("\nData Storage Structure:")
    print("  data/bronze/")
    print("    ├── google_trends/")
    print("    ├── wikipedia/")
    print("    ├── faostat/")
    print("    └── opensky/")


def main():
    """Run all demonstrations."""
    print("NEWS DASHBOARD - NEW FEATURES DEMONSTRATION")
    print("=" * 50)
    
    demo_source_registry()
    demo_taxonomy()
    demo_agriculture_connectors()
    demo_transportation_connectors()
    demo_data_pipeline()
    
    print("\n" + "=" * 50)
    print("SUMMARY OF NEW FEATURES:")
    print("✓ Added 6 new data sources (3 agriculture, 3 transportation)")
    print("✓ Expanded taxonomy with 10 new topics")
    print("✓ Created FAOSTAT connector for global agricultural data")
    print("✓ Created USDA NASS connector for US agricultural data")
    print("✓ Created OpenSky connector for real-time aviation data")
    print("✓ Updated Dagster orchestration to include new sources")
    print("✓ All connectors include rate limiting and error handling")
    
    print("\nNEXT STEPS:")
    print("1. Obtain API keys for USDA NASS and other services")
    print("2. Set up environment variables in .env file")
    print("3. Install Dagster dependencies (resolve compilation issues)")
    print("4. Run data ingestion pipeline")
    print("5. Implement Silver layer processing for new data types")


if __name__ == "__main__":
    main()
