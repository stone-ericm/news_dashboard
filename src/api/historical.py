"""Historical data API with local caching."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel

from src.cache.manager import get_cache_manager
from src.models.taxonomy import get_default_taxonomy
from src.ingestion.opensky import OpenSkyConnector
from src.ingestion.google_trends import GoogleTrendsConnector
from src.ingestion.wikipedia import WikipediaPageviewsConnector
from src.processing.seasonality import SeasonalityProcessor

router = APIRouter(prefix="/api/historical", tags=["historical"])


class HistoricalDataRequest(BaseModel):
    """Request for historical data."""
    topic_id: str
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD
    data_sources: Optional[List[str]] = None  # ['google_trends', 'wikipedia', 'aviation']
    include_processed: bool = True  # Include z-scores, trends, etc.


class DataPoint(BaseModel):
    """Single data point."""
    date: str
    value: float
    source: str
    raw_value: Optional[float] = None
    z_score: Optional[float] = None
    is_anomaly: Optional[bool] = None


class HistoricalDataResponse(BaseModel):
    """Historical data response."""
    topic_id: str
    topic_name: str
    start_date: str
    end_date: str
    data_points: List[DataPoint]
    summary: Dict
    cached: bool
    generated_at: str


@router.get("/data/{topic_id}")
async def get_historical_data(
    topic_id: str,
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    data_sources: Optional[List[str]] = Query(None, description="Data sources to include"),
    include_processed: bool = Query(True, description="Include processed metrics"),
    force_refresh: bool = Query(False, description="Force refresh from source")
) -> HistoricalDataResponse:
    """Get historical data for a topic with caching."""
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        if (end_dt - start_dt).days > 365:
            raise HTTPException(status_code=400, detail="Date range cannot exceed 365 days")
        
        # Get topic info
        taxonomy = get_default_taxonomy()
        topic = taxonomy.get_topic(topic_id)
        if not topic:
            raise HTTPException(status_code=404, detail="Topic not found")
        
        # Get cache manager
        cache = get_cache_manager()
        
        # Generate cache key
        cache_key_params = {
            'start_date': start_date,
            'end_date': end_date,
            'data_sources': ','.join(sorted(data_sources or [])),
            'include_processed': include_processed
        }
        
        # Check cache first (unless force refresh)
        cached_data = None
        if not force_refresh:
            cached_data = cache.get('historical_data', topic_id, **cache_key_params)
        
        if cached_data:
            return HistoricalDataResponse(**cached_data, cached=True)
        
        # Generate fresh data
        data_points = await _generate_historical_data(
            topic, start_dt, end_dt, data_sources, include_processed
        )
        
        # Calculate summary statistics
        values = [dp.value for dp in data_points]
        summary = {
            'total_points': len(data_points),
            'mean_value': float(np.mean(values)) if values else 0,
            'std_value': float(np.std(values)) if values else 0,
            'min_value': float(np.min(values)) if values else 0,
            'max_value': float(np.max(values)) if values else 0,
            'anomalies_detected': len([dp for dp in data_points if dp.is_anomaly]) if include_processed else 0,
            'data_sources_used': list(set(dp.source for dp in data_points))
        }
        
        # Create response
        response_data = {
            'topic_id': topic_id,
            'topic_name': topic.name,
            'start_date': start_date,
            'end_date': end_date,
            'data_points': [dp.dict() for dp in data_points],
            'summary': summary,
            'generated_at': datetime.utcnow().isoformat()
        }
        
        # Cache the result (TTL: 6 hours for recent data, 24 hours for older data)
        ttl_hours = 6 if end_dt > datetime.utcnow() - timedelta(days=7) else 24
        cache.put(
            'historical_data', 
            topic_id, 
            response_data, 
            ttl_hours=ttl_hours,
            tags=['historical', topic.parent_id or 'root'],
            **cache_key_params
        )
        
        return HistoricalDataResponse(**response_data, cached=False)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching historical data: {str(e)}")


async def _generate_historical_data(
    topic, start_dt: datetime, end_dt: datetime, 
    data_sources: Optional[List[str]], include_processed: bool
) -> List[DataPoint]:
    """Generate historical data for a topic."""
    data_points = []
    
    # Generate date range
    dates = pd.date_range(start=start_dt, end=end_dt, freq='D')
    
    # Default data sources based on topic
    if not data_sources:
        data_sources = []
        if topic.google_trends_queries:
            data_sources.append('google_trends')
        if topic.wikipedia_articles:
            data_sources.append('wikipedia')
        if 'aviation' in topic.topic_id or 'transportation' in topic.topic_id:
            data_sources.append('aviation')
    
    # Generate data for each source
    for source in data_sources:
        source_data = await _generate_source_data(topic, dates, source)
        
        # Process data if requested
        if include_processed and source_data:
            processor = SeasonalityProcessor(seasonal_period=7)
            values_series = pd.Series([dp['value'] for dp in source_data], 
                                    index=[pd.to_datetime(dp['date']) for dp in source_data])
            
            z_scores = processor.compute_zscore(values_series)
            anomalies = processor.detect_anomalies(values_series, z_threshold=2.5)
            
            # Add processed metrics
            for i, dp in enumerate(source_data):
                dp['z_score'] = float(z_scores.iloc[i]) if not pd.isna(z_scores.iloc[i]) else None
                dp['is_anomaly'] = bool(anomalies.iloc[i]) if i < len(anomalies) else False
        
        # Convert to DataPoint objects
        for dp in source_data:
            data_points.append(DataPoint(**dp))
    
    return data_points


async def _generate_source_data(topic, dates: pd.DatetimeIndex, source: str) -> List[Dict]:
    """Generate sample data for a specific source."""
    data = []
    
    if source == 'google_trends':
        # Simulate Google Trends data
        base_interest = 50
        if 'employment' in topic.topic_id:
            # Declining trend with weekly seasonality
            trend = base_interest - np.linspace(0, 20, len(dates))
            seasonal = 10 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        elif 'covid' in topic.topic_id:
            # Wave pattern
            trend = base_interest + 25 * np.sin(2 * np.pi * np.arange(len(dates)) / 60)
            seasonal = 5 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        elif 'agriculture' in topic.topic_id:
            # Strong seasonal pattern
            trend = base_interest + 20 * np.sin(2 * np.pi * np.arange(len(dates)) / 365)
            seasonal = 8 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        else:
            # Random walk
            trend = base_interest + np.cumsum(np.random.normal(0, 0.5, len(dates)))
            seasonal = 5 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        
        noise = np.random.normal(0, 3, len(dates))
        values = trend + seasonal + noise
        values = np.clip(values, 0, 100)  # Google Trends is 0-100
        
        for date, value in zip(dates, values):
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'value': float(value),
                'source': 'google_trends',
                'raw_value': float(value)
            })
    
    elif source == 'wikipedia':
        # Simulate Wikipedia pageviews
        base_views = 1000
        if 'covid' in topic.topic_id:
            # High interest with spikes
            trend = base_views + 500 * np.sin(2 * np.pi * np.arange(len(dates)) / 30)
            seasonal = 200 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        elif 'agriculture' in topic.topic_id:
            # Seasonal pattern
            trend = base_views + 300 * np.sin(2 * np.pi * np.arange(len(dates)) / 365)
            seasonal = 100 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        else:
            # Steady with weekly pattern
            trend = np.full(len(dates), base_views)
            seasonal = 150 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        
        noise = np.random.normal(0, 50, len(dates))
        values = trend + seasonal + noise
        values = np.clip(values, 0, None)  # No negative pageviews
        
        for date, value in zip(dates, values):
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'value': float(value),
                'source': 'wikipedia',
                'raw_value': float(value)
            })
    
    elif source == 'aviation':
        # Simulate aviation data (flight counts)
        base_flights = 5000
        if 'aviation' in topic.topic_id:
            # Recovery trend with weekly seasonality
            trend = base_flights + np.linspace(0, 1000, len(dates))  # Recovery
            seasonal = 800 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)  # Weekly pattern
        else:
            # Stable with seasonality
            trend = np.full(len(dates), base_flights)
            seasonal = 500 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        
        noise = np.random.normal(0, 200, len(dates))
        values = trend + seasonal + noise
        values = np.clip(values, 0, None)  # No negative flights
        
        for date, value in zip(dates, values):
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'value': float(value),
                'source': 'aviation',
                'raw_value': float(value)
            })
    
    return data


@router.get("/cache/stats")
async def get_cache_stats() -> Dict:
    """Get cache statistics."""
    try:
        cache = get_cache_manager()
        stats = cache.get_stats()
        
        # Add some additional metrics
        stats['cache_hit_rate'] = 0.75  # Simulated
        stats['avg_response_time_ms'] = 45  # Simulated
        stats['last_cleanup'] = datetime.utcnow().isoformat()
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting cache stats: {str(e)}")


@router.post("/cache/invalidate")
async def invalidate_cache(
    namespace: Optional[str] = None,
    topic_id: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> Dict:
    """Invalidate cache entries."""
    try:
        cache = get_cache_manager()
        
        if topic_id:
            cache.invalidate('historical_data', topic_id)
            message = f"Invalidated cache for topic: {topic_id}"
        elif namespace:
            cache.invalidate(namespace)
            message = f"Invalidated cache for namespace: {namespace}"
        elif tags:
            cache.invalidate('historical_data', tags=tags)
            message = f"Invalidated cache for tags: {', '.join(tags)}"
        else:
            raise HTTPException(status_code=400, detail="Must specify topic_id, namespace, or tags")
        
        return {
            'success': True,
            'message': message,
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error invalidating cache: {str(e)}")


@router.delete("/cache/clear")
async def clear_cache() -> Dict:
    """Clear all cache entries."""
    try:
        cache = get_cache_manager()
        cache.clear_all()
        
        return {
            'success': True,
            'message': 'All cache entries cleared',
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")


@router.get("/topics/{topic_id}/summary")
async def get_topic_summary(
    topic_id: str,
    days_back: int = Query(30, ge=1, le=365)
) -> Dict:
    """Get cached summary for a topic."""
    try:
        cache = get_cache_manager()
        
        # Check for cached summary
        cache_key = f"summary_{days_back}d"
        cached_summary = cache.get('topic_summary', topic_id, period=cache_key)
        
        if cached_summary:
            return {**cached_summary, 'cached': True}
        
        # Generate fresh summary
        taxonomy = get_default_taxonomy()
        topic = taxonomy.get_topic(topic_id)
        if not topic:
            raise HTTPException(status_code=404, detail="Topic not found")
        
        # Get recent historical data
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        # Generate sample summary data
        summary = {
            'topic_id': topic_id,
            'topic_name': topic.name,
            'period_days': days_back,
            'data_points': days_back * len(topic.google_trends_queries or []) + len(topic.wikipedia_articles or []),
            'avg_daily_value': float(np.random.uniform(40, 80)),
            'trend_direction': np.random.choice(['increasing', 'decreasing', 'stable']),
            'volatility_score': float(np.random.uniform(0.1, 0.8)),
            'anomalies_detected': int(np.random.poisson(2)),
            'data_sources': [],
            'last_updated': datetime.utcnow().isoformat()
        }
        
        if topic.google_trends_queries:
            summary['data_sources'].append('google_trends')
        if topic.wikipedia_articles:
            summary['data_sources'].append('wikipedia')
        if 'transportation' in topic_id:
            summary['data_sources'].append('aviation')
        
        # Cache the summary (TTL: 4 hours)
        cache.put(
            'topic_summary', 
            topic_id, 
            summary, 
            ttl_hours=4,
            tags=['summary', topic.parent_id or 'root'],
            period=cache_key
        )
        
        return {**summary, 'cached': False}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting topic summary: {str(e)}")


@router.post("/preload")
async def preload_data(
    background_tasks: BackgroundTasks,
    topic_ids: Optional[List[str]] = None,
    days_back: int = Query(90, ge=7, le=365)
) -> Dict:
    """Preload historical data for topics in background."""
    try:
        taxonomy = get_default_taxonomy()
        
        # Get topics to preload
        if topic_ids:
            topics = [taxonomy.get_topic(tid) for tid in topic_ids if taxonomy.get_topic(tid)]
        else:
            # Preload all topics with data sources
            topics = [t for t in taxonomy.topics.values() 
                     if t.google_trends_queries or t.wikipedia_articles]
        
        # Add background task
        background_tasks.add_task(_preload_topics, topics, days_back)
        
        return {
            'success': True,
            'message': f'Started preloading data for {len(topics)} topics',
            'topics': [t.topic_id for t in topics],
            'days_back': days_back,
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting preload: {str(e)}")


async def _preload_topics(topics: List, days_back: int):
    """Background task to preload topic data."""
    try:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        
        for topic in topics:
            try:
                # Generate and cache data
                data_points = await _generate_historical_data(
                    topic, start_date, end_date, None, True
                )
                
                # Cache the data
                cache = get_cache_manager()
                cache_data = {
                    'topic_id': topic.topic_id,
                    'topic_name': topic.name,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'data_points': [dp.dict() for dp in data_points],
                    'summary': {'preloaded': True},
                    'generated_at': datetime.utcnow().isoformat()
                }
                
                cache.put(
                    'historical_data',
                    topic.topic_id,
                    cache_data,
                    ttl_hours=24,
                    tags=['preloaded', topic.parent_id or 'root'],
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    data_sources='',
                    include_processed=True
                )
                
                print(f"Preloaded data for topic: {topic.topic_id}")
                
            except Exception as e:
                print(f"Error preloading topic {topic.topic_id}: {e}")
        
        print(f"Completed preloading {len(topics)} topics")
        
    except Exception as e:
        print(f"Error in preload task: {e}")
