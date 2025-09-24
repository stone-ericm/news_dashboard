"""Advanced analytics endpoints for News Dashboard."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.models.taxonomy import TaxonomyManager, get_default_taxonomy
from src.processing.seasonality import SeasonalityProcessor, MultiSignalProcessor
from src.ingestion.opensky import OpenSkyConnector

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


class AlertConfig(BaseModel):
    """Configuration for anomaly alerts."""
    topic_id: str
    z_threshold: float = 3.0
    min_consecutive_days: int = 2
    enabled: bool = True


class TrendAnalysis(BaseModel):
    """Trend analysis result."""
    topic_id: str
    topic_name: str
    trend_direction: str  # "increasing", "decreasing", "stable"
    trend_strength: float  # 0-1 scale
    significance_level: float  # p-value
    recent_change_pct: float
    anomaly_score: float
    last_updated: datetime


class Alert(BaseModel):
    """Anomaly alert."""
    id: str
    topic_id: str
    topic_name: str
    alert_type: str  # "anomaly", "trend_change", "threshold"
    severity: str  # "low", "medium", "high", "critical"
    message: str
    z_score: float
    timestamp: datetime
    is_active: bool


@router.get("/trends/analysis")
async def get_trend_analysis(
    topic_ids: Optional[List[str]] = Query(None),
    days_back: int = Query(30, ge=7, le=365),
    include_predictions: bool = Query(False)
) -> Dict:
    """Get comprehensive trend analysis for topics."""
    try:
        taxonomy = get_default_taxonomy()
        processor = SeasonalityProcessor(seasonal_period=7)
        
        # Get topics to analyze
        if topic_ids:
            topics = [taxonomy.get_topic(tid) for tid in topic_ids if taxonomy.get_topic(tid)]
        else:
            # Get all leaf topics (those with data sources)
            topics = [t for t in taxonomy.topics.values() 
                     if t.google_trends_queries or t.wikipedia_articles]
        
        analyses = []
        
        for topic in topics[:10]:  # Limit for performance
            # Generate sample time series data (in real implementation, fetch from storage)
            dates = pd.date_range(
                start=datetime.now() - timedelta(days=days_back),
                end=datetime.now(),
                freq='D'
            )
            
            # Simulate different trend patterns based on topic
            if "employment" in topic.topic_id:
                base_trend = 100 - np.linspace(0, 10, len(dates))  # Declining
                seasonal = 5 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
            elif "covid" in topic.topic_id:
                base_trend = 50 + 20 * np.sin(2 * np.pi * np.arange(len(dates)) / 30)  # Wave
                seasonal = np.zeros(len(dates))
            elif "agriculture" in topic.topic_id:
                base_trend = 80 + 15 * np.sin(2 * np.pi * np.arange(len(dates)) / 365)  # Seasonal
                seasonal = np.zeros(len(dates))
            else:
                base_trend = 75 + np.cumsum(np.random.normal(0, 0.5, len(dates)))  # Random walk
                seasonal = np.zeros(len(dates))
            
            noise = np.random.normal(0, 2, len(dates))
            values = pd.Series(base_trend + seasonal + noise, index=dates)
            
            # Perform analysis
            z_scores = processor.compute_zscore(values)
            slopes_7d = processor.compute_trend_slope(values, window_days=7)
            slopes_30d = processor.compute_trend_slope(values, window_days=30)
            volatility = processor.compute_volatility(values, window_days=14)
            
            # Determine trend direction and strength
            recent_slope = slopes_7d.iloc[-1] if not slopes_7d.empty else 0
            if abs(recent_slope) < 0.1:
                trend_direction = "stable"
                trend_strength = 0.0
            elif recent_slope > 0:
                trend_direction = "increasing"
                trend_strength = min(abs(recent_slope) / 2.0, 1.0)
            else:
                trend_direction = "decreasing"
                trend_strength = min(abs(recent_slope) / 2.0, 1.0)
            
            # Calculate recent change
            recent_change_pct = ((values.iloc[-1] - values.iloc[-7]) / values.iloc[-7] * 100) if len(values) >= 7 else 0
            
            # Anomaly score (based on recent z-scores)
            recent_z_scores = z_scores.tail(7)
            anomaly_score = float(np.mean(np.abs(recent_z_scores))) if not recent_z_scores.empty else 0
            
            analysis = TrendAnalysis(
                topic_id=topic.topic_id,
                topic_name=topic.name,
                trend_direction=trend_direction,
                trend_strength=float(trend_strength),
                significance_level=0.05,  # Placeholder
                recent_change_pct=float(recent_change_pct),
                anomaly_score=anomaly_score,
                last_updated=datetime.utcnow()
            )
            
            analyses.append(analysis.dict())
        
        return {
            "analyses": analyses,
            "summary": {
                "total_topics": len(analyses),
                "trending_up": len([a for a in analyses if a["trend_direction"] == "increasing"]),
                "trending_down": len([a for a in analyses if a["trend_direction"] == "decreasing"]),
                "stable": len([a for a in analyses if a["trend_direction"] == "stable"]),
                "high_anomaly": len([a for a in analyses if a["anomaly_score"] > 2.0]),
                "analysis_period_days": days_back,
                "generated_at": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error performing trend analysis: {str(e)}")


@router.get("/alerts/active")
async def get_active_alerts(
    severity: Optional[str] = Query(None),
    topic_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200)
) -> Dict:
    """Get active anomaly alerts."""
    try:
        taxonomy = get_default_taxonomy()
        
        # Generate sample alerts (in real implementation, fetch from database)
        alerts = []
        alert_types = ["anomaly", "trend_change", "threshold"]
        severities = ["low", "medium", "high", "critical"]
        
        # Get some topics for sample alerts
        topics = list(taxonomy.topics.values())[:10]
        
        for i, topic in enumerate(topics):
            if i % 3 == 0:  # Generate alerts for every 3rd topic
                alert_severity = np.random.choice(severities, p=[0.4, 0.3, 0.2, 0.1])
                alert_type = np.random.choice(alert_types)
                z_score = np.random.normal(0, 1) * (3 + i * 0.5)  # Some high z-scores
                
                if abs(z_score) > 2.0:  # Only create alerts for significant anomalies
                    alert = Alert(
                        id=f"alert_{topic.topic_id}_{int(datetime.utcnow().timestamp())}",
                        topic_id=topic.topic_id,
                        topic_name=topic.name,
                        alert_type=alert_type,
                        severity=alert_severity,
                        message=f"Anomalous activity detected in {topic.name} (Z-score: {z_score:.2f})",
                        z_score=float(z_score),
                        timestamp=datetime.utcnow() - timedelta(hours=np.random.randint(0, 24)),
                        is_active=True
                    )
                    
                    # Apply filters
                    if severity and alert.severity != severity:
                        continue
                    if topic_id and alert.topic_id != topic_id:
                        continue
                    
                    alerts.append(alert.dict())
        
        # Sort by severity and timestamp
        severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
        alerts.sort(key=lambda x: (severity_order.get(x["severity"], 0), x["timestamp"]), reverse=True)
        
        return {
            "alerts": alerts[:limit],
            "total": len(alerts),
            "summary": {
                "critical": len([a for a in alerts if a["severity"] == "critical"]),
                "high": len([a for a in alerts if a["severity"] == "high"]),
                "medium": len([a for a in alerts if a["severity"] == "medium"]),
                "low": len([a for a in alerts if a["severity"] == "low"]),
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")


@router.get("/correlations")
async def get_topic_correlations(
    primary_topic: str,
    days_back: int = Query(90, ge=30, le=365),
    min_correlation: float = Query(0.3, ge=0.0, le=1.0)
) -> Dict:
    """Find correlations between topics."""
    try:
        taxonomy = get_default_taxonomy()
        primary = taxonomy.get_topic(primary_topic)
        
        if not primary:
            raise HTTPException(status_code=404, detail="Primary topic not found")
        
        # Get other topics to correlate with
        other_topics = [t for t in taxonomy.topics.values() 
                       if t.topic_id != primary_topic and 
                       (t.google_trends_queries or t.wikipedia_articles)]
        
        correlations = []
        
        # Generate sample correlation data
        for topic in other_topics[:15]:  # Limit for performance
            # Simulate correlation based on topic relationships
            if primary.parent_id == topic.parent_id:
                # Same category - higher correlation
                correlation = np.random.uniform(0.4, 0.8)
            elif any(keyword in topic.keywords for keyword in primary.keywords):
                # Shared keywords - moderate correlation
                correlation = np.random.uniform(0.2, 0.6)
            else:
                # Random correlation
                correlation = np.random.uniform(-0.3, 0.5)
            
            # Add some noise
            correlation += np.random.normal(0, 0.1)
            correlation = np.clip(correlation, -1.0, 1.0)
            
            if abs(correlation) >= min_correlation:
                correlations.append({
                    "topic_id": topic.topic_id,
                    "topic_name": topic.name,
                    "correlation": float(correlation),
                    "p_value": float(np.random.uniform(0.001, 0.05)),  # Significant correlations
                    "relationship_type": "positive" if correlation > 0 else "negative",
                    "strength": "strong" if abs(correlation) > 0.7 else "moderate" if abs(correlation) > 0.4 else "weak"
                })
        
        # Sort by absolute correlation strength
        correlations.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        
        return {
            "primary_topic": {
                "id": primary.topic_id,
                "name": primary.name,
                "category": primary.parent_id or "root"
            },
            "correlations": correlations,
            "analysis_period_days": days_back,
            "min_correlation_threshold": min_correlation,
            "total_correlations_found": len(correlations)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating correlations: {str(e)}")


@router.get("/predictions/{topic_id}")
async def get_topic_predictions(
    topic_id: str,
    forecast_days: int = Query(7, ge=1, le=30)
) -> Dict:
    """Get predictions for a specific topic."""
    try:
        taxonomy = get_default_taxonomy()
        topic = taxonomy.get_topic(topic_id)
        
        if not topic:
            raise HTTPException(status_code=404, detail="Topic not found")
        
        # Generate sample historical data
        historical_days = 90
        dates = pd.date_range(
            start=datetime.now() - timedelta(days=historical_days),
            end=datetime.now(),
            freq='D'
        )
        
        # Create sample time series based on topic type
        if "employment" in topic_id:
            trend = 100 - np.linspace(0, 15, len(dates))
            seasonal = 8 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        elif "agriculture" in topic_id:
            trend = 80 + 20 * np.sin(2 * np.pi * np.arange(len(dates)) / 365)
            seasonal = 5 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        else:
            trend = 75 + np.cumsum(np.random.normal(0, 0.3, len(dates)))
            seasonal = 3 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
        
        noise = np.random.normal(0, 2, len(dates))
        historical_values = trend + seasonal + noise
        
        # Generate predictions (simple linear extrapolation with seasonality)
        future_dates = pd.date_range(
            start=datetime.now() + timedelta(days=1),
            periods=forecast_days,
            freq='D'
        )
        
        # Simple trend continuation
        last_trend = np.mean(np.diff(trend[-7:]))  # Recent trend
        future_trend = historical_values[-1] + np.arange(1, forecast_days + 1) * last_trend
        
        # Add seasonal component
        future_seasonal = 3 * np.sin(2 * np.pi * np.arange(len(dates), len(dates) + forecast_days) / 7)
        
        predictions = future_trend + future_seasonal
        
        # Calculate confidence intervals
        historical_std = np.std(noise)
        confidence_lower = predictions - 1.96 * historical_std
        confidence_upper = predictions + 1.96 * historical_std
        
        # Prepare response data
        historical_data = [
            {
                "date": date.strftime("%Y-%m-%d"),
                "value": float(val),
                "type": "historical"
            }
            for date, val in zip(dates[-30:], historical_values[-30:])  # Last 30 days
        ]
        
        prediction_data = [
            {
                "date": date.strftime("%Y-%m-%d"),
                "value": float(pred),
                "confidence_lower": float(lower),
                "confidence_upper": float(upper),
                "type": "prediction"
            }
            for date, pred, lower, upper in zip(future_dates, predictions, confidence_lower, confidence_upper)
        ]
        
        return {
            "topic": {
                "id": topic.topic_id,
                "name": topic.name
            },
            "historical_data": historical_data,
            "predictions": prediction_data,
            "forecast_summary": {
                "forecast_days": forecast_days,
                "trend_direction": "increasing" if last_trend > 0 else "decreasing" if last_trend < 0 else "stable",
                "confidence_level": 0.95,
                "model_type": "seasonal_trend",
                "generated_at": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating predictions: {str(e)}")


@router.get("/anomalies/detection")
async def detect_anomalies(
    topic_ids: Optional[List[str]] = Query(None),
    z_threshold: float = Query(2.5, ge=1.0, le=5.0),
    days_back: int = Query(30, ge=7, le=90)
) -> Dict:
    """Detect anomalies across topics using statistical methods."""
    try:
        taxonomy = get_default_taxonomy()
        processor = SeasonalityProcessor(seasonal_period=7)
        
        # Get topics to analyze
        if topic_ids:
            topics = [taxonomy.get_topic(tid) for tid in topic_ids if taxonomy.get_topic(tid)]
        else:
            topics = [t for t in taxonomy.topics.values() 
                     if t.google_trends_queries or t.wikipedia_articles][:10]
        
        anomalies = []
        
        for topic in topics:
            # Generate sample time series
            dates = pd.date_range(
                start=datetime.now() - timedelta(days=days_back),
                end=datetime.now(),
                freq='D'
            )
            
            # Create realistic data with some anomalies
            base_values = 50 + 10 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)
            noise = np.random.normal(0, 3, len(dates))
            
            # Inject some anomalies
            anomaly_indices = np.random.choice(len(dates), size=max(1, len(dates) // 15), replace=False)
            for idx in anomaly_indices:
                noise[idx] += np.random.choice([-1, 1]) * np.random.uniform(8, 15)
            
            values = pd.Series(base_values + noise, index=dates)
            
            # Detect anomalies
            z_scores = processor.compute_zscore(values)
            anomaly_mask = processor.detect_anomalies(values, z_threshold=z_threshold)
            
            # Extract anomaly points
            anomaly_dates = dates[anomaly_mask]
            anomaly_values = values[anomaly_mask]
            anomaly_z_scores = z_scores[anomaly_mask]
            
            for date, value, z_score in zip(anomaly_dates, anomaly_values, anomaly_z_scores):
                if pd.notna(z_score):
                    severity = "critical" if abs(z_score) > 4 else "high" if abs(z_score) > 3 else "medium"
                    
                    anomalies.append({
                        "topic_id": topic.topic_id,
                        "topic_name": topic.name,
                        "date": date.strftime("%Y-%m-%d"),
                        "value": float(value),
                        "z_score": float(z_score),
                        "severity": severity,
                        "deviation_type": "positive" if z_score > 0 else "negative",
                        "baseline_value": float(values.mean()),
                        "deviation_magnitude": float(abs(value - values.mean()))
                    })
        
        # Sort by severity and z-score
        severity_order = {"critical": 3, "high": 2, "medium": 1}
        anomalies.sort(key=lambda x: (severity_order.get(x["severity"], 0), abs(x["z_score"])), reverse=True)
        
        return {
            "anomalies": anomalies,
            "detection_summary": {
                "total_anomalies": len(anomalies),
                "critical": len([a for a in anomalies if a["severity"] == "critical"]),
                "high": len([a for a in anomalies if a["severity"] == "high"]),
                "medium": len([a for a in anomalies if a["severity"] == "medium"]),
                "z_threshold": z_threshold,
                "analysis_period_days": days_back,
                "topics_analyzed": len(topics)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting anomalies: {str(e)}")


@router.get("/insights/summary")
async def get_insights_summary() -> Dict:
    """Get a summary of key insights and findings."""
    try:
        taxonomy = get_default_taxonomy()
        
        # Generate sample insights
        insights = [
            {
                "id": "insight_1",
                "title": "Aviation Traffic Recovery Accelerating",
                "description": "Global aviation traffic shows 15% increase over past month, indicating strong recovery in transportation sector.",
                "category": "transportation",
                "confidence": 0.87,
                "impact_level": "high",
                "supporting_topics": ["transportation_aviation", "economy_employment"],
                "generated_at": datetime.utcnow() - timedelta(hours=2)
            },
            {
                "id": "insight_2", 
                "title": "Agricultural Commodity Prices Showing Seasonal Volatility",
                "description": "Crop prices exhibiting higher than normal seasonal variation, potentially indicating supply chain disruptions.",
                "category": "agriculture",
                "confidence": 0.73,
                "impact_level": "medium",
                "supporting_topics": ["agriculture_prices", "agriculture_crops"],
                "generated_at": datetime.utcnow() - timedelta(hours=6)
            },
            {
                "id": "insight_3",
                "title": "Employment Trends Diverging by Sector",
                "description": "Technology and healthcare employment rising while traditional manufacturing shows decline.",
                "category": "economic",
                "confidence": 0.91,
                "impact_level": "high",
                "supporting_topics": ["economy_employment"],
                "generated_at": datetime.utcnow() - timedelta(hours=12)
            }
        ]
        
        # Calculate summary statistics
        total_topics = len(taxonomy.topics)
        active_alerts = 7  # Sample number
        data_sources_healthy = 8  # Sample number
        
        return {
            "insights": [
                {
                    **insight,
                    "generated_at": insight["generated_at"].isoformat()
                }
                for insight in insights
            ],
            "dashboard_summary": {
                "total_topics_monitored": total_topics,
                "active_alerts": active_alerts,
                "healthy_data_sources": data_sources_healthy,
                "last_updated": datetime.utcnow().isoformat(),
                "system_status": "operational"
            },
            "key_metrics": {
                "anomalies_detected_24h": 12,
                "trending_topics": 8,
                "correlation_strength_avg": 0.34,
                "prediction_accuracy": 0.78
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating insights: {str(e)}")
