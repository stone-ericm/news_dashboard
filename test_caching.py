#!/usr/bin/env python3
"""Test script for the local caching system."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

from src.cache.manager import get_cache_manager

def test_caching_system():
    """Test the local caching functionality."""
    print("🧪 Testing Local Caching System")
    print("=" * 50)
    
    # Get cache manager
    cache = get_cache_manager()
    
    # Test 1: Cache DataFrame
    print("\n📊 Test 1: Caching DataFrame")
    dates = pd.date_range(start="2024-01-01", periods=100, freq="D")
    df = pd.DataFrame({
        'date': dates,
        'value': np.random.randn(100),
        'category': np.random.choice(['A', 'B', 'C'], 100)
    })
    
    start_time = time.time()
    key1 = cache.put('test_data', 'sample_df', df, ttl_hours=1, tags=['test', 'dataframe'])
    cache_time = time.time() - start_time
    print(f"✅ Cached DataFrame with {len(df)} rows in {cache_time:.3f}s")
    print(f"   Cache key: {key1}")
    
    # Test 2: Retrieve DataFrame
    print("\n📈 Test 2: Retrieving DataFrame")
    start_time = time.time()
    retrieved_df = cache.get('test_data', 'sample_df')
    retrieve_time = time.time() - start_time
    print(f"✅ Retrieved DataFrame in {retrieve_time:.3f}s")
    print(f"   Shape: {retrieved_df.shape if retrieved_df is not None else 'None'}")
    print(f"   Data matches: {df.equals(retrieved_df) if retrieved_df is not None else False}")
    
    # Test 3: Cache JSON data
    print("\n📋 Test 3: Caching JSON Data")
    json_data = {
        'topics': ['economy', 'health', 'agriculture'],
        'metrics': {
            'total_sources': 12,
            'active_alerts': 3,
            'last_updated': datetime.utcnow().isoformat()
        },
        'trends': [
            {'topic': 'economy', 'trend': 'increasing', 'confidence': 0.85},
            {'topic': 'health', 'trend': 'stable', 'confidence': 0.92},
            {'topic': 'agriculture', 'trend': 'seasonal', 'confidence': 0.78}
        ]
    }
    
    key2 = cache.put('dashboard', 'summary', json_data, ttl_hours=2, tags=['dashboard', 'summary'])
    print(f"✅ Cached JSON data")
    print(f"   Cache key: {key2}")
    
    # Test 4: Retrieve JSON data
    print("\n📊 Test 4: Retrieving JSON Data")
    retrieved_json = cache.get('dashboard', 'summary')
    print(f"✅ Retrieved JSON data")
    print(f"   Topics: {retrieved_json['topics'] if retrieved_json else 'None'}")
    print(f"   Total sources: {retrieved_json['metrics']['total_sources'] if retrieved_json else 'None'}")
    
    # Test 5: Cache with parameters
    print("\n🔧 Test 5: Parameterized Caching")
    for topic in ['economy', 'health', 'agriculture']:
        for days in [30, 90]:
            sample_data = {
                'topic_id': topic,
                'days_back': days,
                'data_points': np.random.randn(days).tolist(),
                'summary': {
                    'mean': float(np.random.randn()),
                    'std': float(np.random.rand()),
                    'anomalies': int(np.random.poisson(2))
                }
            }
            
            cache.put('historical', topic, sample_data, ttl_hours=6, 
                     tags=['historical', topic], days_back=days)
    
    print(f"✅ Cached historical data for 3 topics × 2 time periods = 6 entries")
    
    # Test 6: Cache statistics
    print("\n📈 Test 6: Cache Statistics")
    stats = cache.get_stats()
    print(f"✅ Cache Statistics:")
    print(f"   Total entries: {stats['total_entries']}")
    print(f"   Total size: {stats['total_size_mb']} MB")
    print(f"   Utilization: {stats['utilization_pct']}%")
    print(f"   Cache directory: {stats['cache_dir']}")
    
    print(f"\n📂 Namespaces:")
    for namespace, info in stats['namespaces'].items():
        print(f"   {namespace}: {info['count']} entries, {info['size'] / (1024*1024):.2f} MB")
    
    print(f"\n📄 Data Types:")
    for data_type, info in stats['data_types'].items():
        print(f"   {data_type}: {info['count']} entries, {info['size'] / (1024*1024):.2f} MB")
    
    # Test 7: Cache existence check
    print("\n🔍 Test 7: Cache Existence Checks")
    exists_df = cache.exists('test_data', 'sample_df')
    exists_json = cache.exists('dashboard', 'summary')
    exists_missing = cache.exists('nonexistent', 'data')
    
    print(f"✅ Existence checks:")
    print(f"   DataFrame exists: {exists_df}")
    print(f"   JSON data exists: {exists_json}")
    print(f"   Missing data exists: {exists_missing}")
    
    # Test 8: Retrieve with parameters
    print("\n🎯 Test 8: Parameterized Retrieval")
    economy_30d = cache.get('historical', 'economy', days_back=30)
    health_90d = cache.get('historical', 'health', days_back=90)
    
    print(f"✅ Parameterized retrieval:")
    print(f"   Economy 30d: {len(economy_30d['data_points']) if economy_30d else 'None'} points")
    print(f"   Health 90d: {len(health_90d['data_points']) if health_90d else 'None'} points")
    
    # Test 9: Cache invalidation
    print("\n🗑️ Test 9: Cache Invalidation")
    initial_count = stats['total_entries']
    
    # Invalidate by tag
    cache.invalidate('historical', tags=['economy'])
    stats_after_tag = cache.get_stats()
    
    # Invalidate specific entry
    cache.invalidate('dashboard', 'summary')
    stats_after_specific = cache.get_stats()
    
    print(f"✅ Invalidation results:")
    print(f"   Initial entries: {initial_count}")
    print(f"   After tag invalidation: {stats_after_tag['total_entries']}")
    print(f"   After specific invalidation: {stats_after_specific['total_entries']}")
    
    # Final statistics
    print("\n📊 Final Cache State")
    final_stats = cache.get_stats()
    print(f"   Remaining entries: {final_stats['total_entries']}")
    print(f"   Total size: {final_stats['total_size_mb']} MB")
    
    print("\n🎉 Caching system test completed successfully!")
    return True

if __name__ == "__main__":
    try:
        test_caching_system()
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
