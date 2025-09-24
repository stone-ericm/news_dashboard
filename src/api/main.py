"""FastAPI web server for News Dashboard."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.models.source_registry import SourceRegistry
from src.models.taxonomy import TaxonomyManager, get_default_taxonomy
from src.ingestion.opensky import OpenSkyConnector
from src.processing.seasonality import SeasonalityProcessor
from src.api.analytics import router as analytics_router
from src.api.historical import router as historical_router
from src.api.export import router as export_router
from src.api.websockets import router as websocket_router
from src.api.realtime_dashboard import router as realtime_router
from src.api.enhanced_dashboard import router as enhanced_router

# Initialize FastAPI app
app = FastAPI(
    title="News Dashboard API",
    description="Evidence-driven dashboard for surfacing statistically significant real-world trend changes",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
static_path = Path(__file__).parent.parent.parent / "static"
static_path.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Include routers
app.include_router(analytics_router)
app.include_router(historical_router)
app.include_router(export_router)
app.include_router(websocket_router)
app.include_router(realtime_router)
app.include_router(enhanced_router)


@app.get("/")
async def root():
    """Root endpoint with dashboard options."""
    return {
        "message": "News Dashboard API - Enterprise Analytics Platform",
        "version": "2.0.0",
        "features": [
            "Advanced Analytics & Anomaly Detection",
            "Real-Time WebSocket Streaming", 
            "Intelligent Caching System",
            "Multi-Format Data Export",
            "Historical Data Analysis",
            "Interactive Visualizations"
        ],
        "dashboards": {
            "enhanced": "/enhanced",
            "static": "/dashboard", 
            "realtime": "/realtime"
        },
        "api": {
            "docs": "/docs",
            "openapi": "/openapi.json",
            "analytics": "/api/analytics/",
            "historical": "/api/historical/",
            "export": "/api/export/",
            "websocket_stats": "/ws/stats"
        },
        "websockets": [
            "/ws/dashboard",
            "/ws/aviation", 
            "/ws/alerts",
            "/ws/trends",
            "/ws/analytics"
        ]
    }


@app.get("/api/sources")
async def get_sources():
    """Get all configured data sources."""
    registry = SourceRegistry()
    
    sources_data = []
    for source in registry.sources.values():
        sources_data.append({
            "id": source.source_id,
            "name": source.name,
            "description": source.description,
            "category": source.category,
            "cadence": source.cadence_expected.value,
            "geographic_scope": source.geographic_scope.value,
            "health_status": source.health_status.value,
            "is_active": source.is_active,
            "auth_required": source.auth_method.value != "none",
            "last_success": source.last_success_ts.isoformat() if source.last_success_ts else None,
        })
    
    return {"sources": sources_data, "total": len(sources_data)}


@app.get("/api/topics")
async def get_topics():
    """Get all topics in the taxonomy."""
    taxonomy = get_default_taxonomy()
    
    topics_data = []
    for topic in taxonomy.topics.values():
        topics_data.append({
            "id": topic.topic_id,
            "name": topic.name,
            "description": topic.description,
            "parent_id": topic.parent_id,
            "color": topic.color,
            "priority": topic.priority,
            "keywords": topic.keywords,
            "google_trends_queries": topic.google_trends_queries,
            "wikipedia_articles": topic.wikipedia_articles,
            "is_active": topic.is_active,
        })
    
    return {"topics": topics_data, "total": len(topics_data)}


@app.get("/api/topics/hierarchy")
async def get_topic_hierarchy():
    """Get topics organized in hierarchical structure."""
    taxonomy = get_default_taxonomy()
    
    # Get root topics
    root_topics = [t for t in taxonomy.topics.values() if t.parent_id is None]
    
    hierarchy = []
    for root in sorted(root_topics, key=lambda x: x.priority):
        children = taxonomy.get_children(root.topic_id)
        hierarchy.append({
            "id": root.topic_id,
            "name": root.name,
            "description": root.description,
            "color": root.color,
            "priority": root.priority,
            "children": [
                {
                    "id": child.topic_id,
                    "name": child.name,
                    "description": child.description,
                    "keywords": child.keywords,
                    "google_trends_queries": child.google_trends_queries,
                    "wikipedia_articles": child.wikipedia_articles,
                }
                for child in children
            ]
        })
    
    return {"hierarchy": hierarchy}


@app.get("/api/data/aviation/live")
async def get_live_aviation_data():
    """Get live aviation data from OpenSky Network."""
    try:
        connector = OpenSkyConnector()
        
        # Define regions for data collection
        regions = {
            "north_america": (25.0, 50.0, -130.0, -60.0),
            "europe": (35.0, 70.0, -10.0, 40.0),
            "asia_pacific": (10.0, 50.0, 100.0, 150.0),
        }
        
        all_data = []
        region_stats = {}
        
        for region_name, bbox in regions.items():
            try:
                states = connector.fetch_states(bbox=bbox)
                if states is not None and not states.empty:
                    # Add region info
                    states["region"] = region_name
                    all_data.append(states)
                    
                    # Calculate region statistics
                    region_stats[region_name] = {
                        "total_aircraft": len(states),
                        "in_air": len(states[states["on_ground"] == False]),
                        "on_ground": len(states[states["on_ground"] == True]),
                        "countries": states["origin_country"].nunique(),
                        "avg_altitude": float(states["baro_altitude"].mean()) if not states["baro_altitude"].isna().all() else 0,
                        "avg_velocity": float(states["velocity"].mean()) if not states["velocity"].isna().all() else 0,
                    }
            except Exception as e:
                region_stats[region_name] = {"error": str(e)}
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Prepare data for visualization
            viz_data = []
            for _, row in combined_df.iterrows():
                if pd.notna(row["longitude"]) and pd.notna(row["latitude"]):
                    viz_data.append({
                        "longitude": float(row["longitude"]),
                        "latitude": float(row["latitude"]),
                        "altitude": float(row["baro_altitude"]) if pd.notna(row["baro_altitude"]) else 0,
                        "velocity": float(row["velocity"]) if pd.notna(row["velocity"]) else 0,
                        "country": row["origin_country"],
                        "region": row["region"],
                        "on_ground": bool(row["on_ground"]),
                        "callsign": row["callsign"] if pd.notna(row["callsign"]) else "",
                    })
            
            return {
                "data": viz_data,
                "region_stats": region_stats,
                "total_aircraft": len(viz_data),
                "timestamp": datetime.utcnow().isoformat(),
            }
        else:
            return {
                "data": [],
                "region_stats": region_stats,
                "total_aircraft": 0,
                "timestamp": datetime.utcnow().isoformat(),
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching aviation data: {str(e)}")


@app.get("/api/data/sample/trends")
async def get_sample_trends_data():
    """Generate sample trends data for visualization."""
    # Generate sample time series data
    dates = pd.date_range(start="2024-01-01", end="2024-12-31", freq="D")
    
    # Sample topics with different trend patterns
    topics = [
        {"id": "economy_employment", "name": "Employment", "color": "#2E7D32"},
        {"id": "health_covid", "name": "COVID-19", "color": "#1565C0"},
        {"id": "agriculture_crops", "name": "Crop Production", "color": "#8BC34A"},
        {"id": "transportation_aviation", "name": "Aviation", "color": "#FF9800"},
        {"id": "climate_temperature", "name": "Temperature", "color": "#388E3C"},
    ]
    
    data = []
    for topic in topics:
        # Generate different trend patterns
        if topic["id"] == "economy_employment":
            # Declining trend with seasonal variation
            base_trend = 100 - (dates - dates[0]).days * 0.01
            seasonal = 5 * pd.Series([1 if d.month in [6, 7, 8] else 0 for d in dates])
        elif topic["id"] == "health_covid":
            # Wave pattern
            base_trend = 50 + 30 * pd.Series([np.sin(2 * np.pi * i / 90) for i in range(len(dates))])
            seasonal = pd.Series([0] * len(dates))
        elif topic["id"] == "agriculture_crops":
            # Strong seasonal pattern
            base_trend = 80 + 20 * pd.Series([np.sin(2 * np.pi * d.dayofyear / 365) for d in dates])
            seasonal = pd.Series([0] * len(dates))
        elif topic["id"] == "transportation_aviation":
            # Recovery trend with dips
            base_trend = 60 + (dates - dates[0]).days * 0.02
            seasonal = -10 * pd.Series([1 if d.month in [1, 2] else 0 for d in dates])
        else:  # climate_temperature
            # Strong seasonal with warming trend
            base_trend = 15 + 10 * pd.Series([np.sin(2 * np.pi * (d.dayofyear - 80) / 365) for d in dates])
            seasonal = pd.Series([0] * len(dates))
        
        # Add noise
        noise = pd.Series(np.random.normal(0, 2, len(dates)))
        values = base_trend + seasonal + noise
        
        # Calculate z-scores using seasonality processor
        processor = SeasonalityProcessor(seasonal_period=7)
        z_scores = processor.compute_zscore(values)
        
        for i, (date, value, z_score) in enumerate(zip(dates, values, z_scores)):
            data.append({
                "date": date.strftime("%Y-%m-%d"),
                "topic_id": topic["id"],
                "topic_name": topic["name"],
                "value": float(value),
                "z_score": float(z_score) if pd.notna(z_score) else 0,
                "color": topic["color"],
                "is_anomaly": abs(z_score) > 2.0 if pd.notna(z_score) else False,
            })
    
    return {"data": data, "topics": topics}


@app.get("/api/data/sample/sources")
async def get_sample_source_health():
    """Get sample source health data for monitoring dashboard."""
    registry = SourceRegistry()
    
    health_data = []
    for source in registry.sources.values():
        # Simulate some health metrics
        success_rate = 0.95 if source.health_status.value == "healthy" else 0.75
        avg_response_time = 200 if source.cadence_expected.value == "realtime" else 1000
        
        health_data.append({
            "source_id": source.source_id,
            "name": source.name,
            "category": source.category,
            "health_status": source.health_status.value,
            "success_rate": success_rate,
            "avg_response_time_ms": avg_response_time,
            "last_success": source.last_success_ts.isoformat() if source.last_success_ts else None,
            "cadence": source.cadence_expected.value,
            "geographic_scope": source.geographic_scope.value,
        })
    
    return {"health_data": health_data}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard HTML page."""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>News Dashboard - Evidence-Driven Trend Analysis</title>
        <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
        <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
        <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
                text-align: center;
            }
            .header h1 {
                margin: 0;
                font-size: 2.5em;
                font-weight: 300;
            }
            .header p {
                margin: 10px 0 0 0;
                opacity: 0.9;
                font-size: 1.1em;
            }
            .dashboard-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            .chart-container {
                background: white;
                border-radius: 10px;
                padding: 20px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            .chart-title {
                font-size: 1.3em;
                font-weight: 600;
                margin-bottom: 15px;
                color: #333;
            }
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 30px;
            }
            .stat-card {
                background: white;
                padding: 20px;
                border-radius: 10px;
                text-align: center;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            .stat-number {
                font-size: 2em;
                font-weight: bold;
                color: #667eea;
            }
            .stat-label {
                color: #666;
                margin-top: 5px;
            }
            .loading {
                text-align: center;
                padding: 40px;
                color: #666;
            }
            .error {
                background: #fee;
                border: 1px solid #fcc;
                color: #c66;
                padding: 15px;
                border-radius: 5px;
                margin: 10px 0;
            }
            .refresh-btn {
                background: #667eea;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 5px;
                cursor: pointer;
                font-size: 1em;
                margin: 10px;
            }
            .refresh-btn:hover {
                background: #5a6fd8;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 News Dashboard</h1>
            <p>Evidence-driven analysis of statistically significant real-world trend changes</p>
            <button class="refresh-btn" onclick="loadAllData()">🔄 Refresh Data</button>
        </div>

        <div class="stats-grid" id="stats-grid">
            <div class="loading">Loading statistics...</div>
        </div>

        <div class="dashboard-grid">
            <div class="chart-container">
                <div class="chart-title">📈 Topic Trends Over Time</div>
                <div id="trends-chart" class="loading">Loading trends data...</div>
            </div>

            <div class="chart-container">
                <div class="chart-title">✈️ Live Aviation Traffic</div>
                <div id="aviation-chart" class="loading">Loading aviation data...</div>
            </div>

            <div class="chart-container">
                <div class="chart-title">🏗️ Data Source Health</div>
                <div id="sources-chart" class="loading">Loading source health...</div>
            </div>

            <div class="chart-container">
                <div class="chart-title">🗂️ Topic Taxonomy</div>
                <div id="taxonomy-chart" class="loading">Loading taxonomy...</div>
            </div>
        </div>

        <script>
            // Configuration
            const API_BASE = '';
            
            // Load all dashboard data
            async function loadAllData() {
                await Promise.all([
                    loadStats(),
                    loadTrendsChart(),
                    loadAviationChart(),
                    loadSourcesChart(),
                    loadTaxonomyChart()
                ]);
            }

            // Load statistics
            async function loadStats() {
                try {
                    const [sourcesRes, topicsRes, aviationRes] = await Promise.all([
                        fetch(`${API_BASE}/api/sources`),
                        fetch(`${API_BASE}/api/topics`),
                        fetch(`${API_BASE}/api/data/aviation/live`)
                    ]);

                    const sources = await sourcesRes.json();
                    const topics = await topicsRes.json();
                    const aviation = await aviationRes.json();

                    const statsHtml = `
                        <div class="stat-card">
                            <div class="stat-number">${sources.total}</div>
                            <div class="stat-label">Data Sources</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${topics.total}</div>
                            <div class="stat-label">Topics Tracked</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${aviation.total_aircraft}</div>
                            <div class="stat-label">Aircraft Tracked</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${Object.keys(aviation.region_stats).length}</div>
                            <div class="stat-label">Regions Monitored</div>
                        </div>
                    `;
                    
                    document.getElementById('stats-grid').innerHTML = statsHtml;
                } catch (error) {
                    document.getElementById('stats-grid').innerHTML = `<div class="error">Error loading stats: ${error.message}</div>`;
                }
            }

            // Load trends chart
            async function loadTrendsChart() {
                try {
                    const response = await fetch(`${API_BASE}/api/data/sample/trends`);
                    const data = await response.json();

                    const spec = {
                        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                        "width": 450,
                        "height": 300,
                        "data": {"values": data.data},
                        "mark": {"type": "line", "point": true, "strokeWidth": 2},
                        "encoding": {
                            "x": {
                                "field": "date",
                                "type": "temporal",
                                "title": "Date"
                            },
                            "y": {
                                "field": "z_score",
                                "type": "quantitative",
                                "title": "Z-Score (Statistical Significance)",
                                "scale": {"domain": [-4, 4]}
                            },
                            "color": {
                                "field": "topic_name",
                                "type": "nominal",
                                "title": "Topic",
                                "scale": {"range": ["#2E7D32", "#1565C0", "#8BC34A", "#FF9800", "#388E3C"]}
                            },
                            "tooltip": [
                                {"field": "topic_name", "title": "Topic"},
                                {"field": "date", "title": "Date"},
                                {"field": "value", "title": "Value", "format": ".2f"},
                                {"field": "z_score", "title": "Z-Score", "format": ".2f"}
                            ]
                        },
                        "resolve": {"scale": {"color": "independent"}}
                    };

                    vegaEmbed('#trends-chart', spec);
                } catch (error) {
                    document.getElementById('trends-chart').innerHTML = `<div class="error">Error loading trends: ${error.message}</div>`;
                }
            }

            // Load aviation chart
            async function loadAviationChart() {
                try {
                    const response = await fetch(`${API_BASE}/api/data/aviation/live`);
                    const data = await response.json();

                    const spec = {
                        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                        "width": 450,
                        "height": 300,
                        "data": {"values": data.data},
                        "mark": {"type": "circle", "size": 60, "opacity": 0.7},
                        "encoding": {
                            "x": {
                                "field": "longitude",
                                "type": "quantitative",
                                "title": "Longitude",
                                "scale": {"domain": [-180, 180]}
                            },
                            "y": {
                                "field": "latitude",
                                "type": "quantitative",
                                "title": "Latitude",
                                "scale": {"domain": [-90, 90]}
                            },
                            "color": {
                                "field": "region",
                                "type": "nominal",
                                "title": "Region"
                            },
                            "size": {
                                "field": "altitude",
                                "type": "quantitative",
                                "title": "Altitude (m)",
                                "scale": {"range": [20, 200]}
                            },
                            "tooltip": [
                                {"field": "callsign", "title": "Callsign"},
                                {"field": "country", "title": "Country"},
                                {"field": "altitude", "title": "Altitude (m)"},
                                {"field": "velocity", "title": "Velocity (m/s)"},
                                {"field": "region", "title": "Region"}
                            ]
                        }
                    };

                    vegaEmbed('#aviation-chart', spec);
                } catch (error) {
                    document.getElementById('aviation-chart').innerHTML = `<div class="error">Error loading aviation data: ${error.message}</div>`;
                }
            }

            // Load sources health chart
            async function loadSourcesChart() {
                try {
                    const response = await fetch(`${API_BASE}/api/data/sample/sources`);
                    const data = await response.json();

                    const spec = {
                        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                        "width": 450,
                        "height": 300,
                        "data": {"values": data.health_data},
                        "mark": {"type": "bar"},
                        "encoding": {
                            "x": {
                                "field": "success_rate",
                                "type": "quantitative",
                                "title": "Success Rate",
                                "scale": {"domain": [0, 1]}
                            },
                            "y": {
                                "field": "name",
                                "type": "nominal",
                                "title": "Data Source",
                                "sort": "-x"
                            },
                            "color": {
                                "field": "health_status",
                                "type": "nominal",
                                "title": "Health Status",
                                "scale": {
                                    "domain": ["healthy", "degraded", "down"],
                                    "range": ["#4CAF50", "#FF9800", "#F44336"]
                                }
                            },
                            "tooltip": [
                                {"field": "name", "title": "Source"},
                                {"field": "category", "title": "Category"},
                                {"field": "success_rate", "title": "Success Rate", "format": ".1%"},
                                {"field": "avg_response_time_ms", "title": "Avg Response (ms)"},
                                {"field": "cadence", "title": "Update Frequency"}
                            ]
                        }
                    };

                    vegaEmbed('#sources-chart', spec);
                } catch (error) {
                    document.getElementById('sources-chart').innerHTML = `<div class="error">Error loading source health: ${error.message}</div>`;
                }
            }

            // Load taxonomy chart
            async function loadTaxonomyChart() {
                try {
                    const response = await fetch(`${API_BASE}/api/topics/hierarchy`);
                    const data = await response.json();

                    // Flatten hierarchy for visualization
                    const flatData = [];
                    data.hierarchy.forEach(category => {
                        flatData.push({
                            name: category.name,
                            type: 'category',
                            count: category.children.length,
                            color: category.color
                        });
                        category.children.forEach(child => {
                            flatData.push({
                                name: child.name,
                                type: 'topic',
                                count: child.google_trends_queries.length + child.wikipedia_articles.length,
                                color: category.color,
                                parent: category.name
                            });
                        });
                    });

                    const spec = {
                        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                        "width": 450,
                        "height": 300,
                        "data": {"values": flatData.filter(d => d.type === 'category')},
                        "mark": {"type": "arc", "innerRadius": 50},
                        "encoding": {
                            "theta": {
                                "field": "count",
                                "type": "quantitative",
                                "title": "Number of Subtopics"
                            },
                            "color": {
                                "field": "color",
                                "type": "nominal",
                                "scale": null,
                                "title": "Category"
                            },
                            "tooltip": [
                                {"field": "name", "title": "Category"},
                                {"field": "count", "title": "Subtopics"}
                            ]
                        }
                    };

                    vegaEmbed('#taxonomy-chart', spec);
                } catch (error) {
                    document.getElementById('taxonomy-chart').innerHTML = `<div class="error">Error loading taxonomy: ${error.message}</div>`;
                }
            }

            // Load data on page load
            document.addEventListener('DOMContentLoaded', loadAllData);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
