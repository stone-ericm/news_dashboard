"""Real-time dashboard with WebSocket integration."""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

@router.get("/realtime", response_class=HTMLResponse)
async def realtime_dashboard():
    """Serve the real-time dashboard with WebSocket integration."""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>News Dashboard - Real-Time Analytics</title>
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
                position: relative;
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
            .connection-status {
                position: absolute;
                top: 20px;
                right: 20px;
                padding: 8px 16px;
                border-radius: 20px;
                font-size: 0.9em;
                font-weight: bold;
            }
            .connected {
                background-color: #4CAF50;
                color: white;
            }
            .disconnected {
                background-color: #f44336;
                color: white;
            }
            .connecting {
                background-color: #FF9800;
                color: white;
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
                position: relative;
            }
            .chart-title {
                font-size: 1.3em;
                font-weight: 600;
                margin-bottom: 15px;
                color: #333;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            .last-updated {
                font-size: 0.8em;
                color: #666;
                font-weight: normal;
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
                position: relative;
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
            .stat-change {
                position: absolute;
                top: 10px;
                right: 10px;
                font-size: 0.8em;
                padding: 2px 6px;
                border-radius: 10px;
            }
            .stat-up {
                background-color: #e8f5e8;
                color: #2e7d32;
            }
            .stat-down {
                background-color: #ffebee;
                color: #c62828;
            }
            .alerts-panel {
                background: white;
                border-radius: 10px;
                padding: 20px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                margin-bottom: 20px;
                max-height: 300px;
                overflow-y: auto;
            }
            .alert-item {
                padding: 10px;
                margin: 5px 0;
                border-radius: 5px;
                border-left: 4px solid;
                font-size: 0.9em;
            }
            .alert-critical {
                background-color: #ffebee;
                border-left-color: #f44336;
            }
            .alert-high {
                background-color: #fff3e0;
                border-left-color: #ff9800;
            }
            .alert-medium {
                background-color: #e3f2fd;
                border-left-color: #2196f3;
            }
            .alert-low {
                background-color: #f3e5f5;
                border-left-color: #9c27b0;
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
            .controls {
                background: white;
                padding: 15px;
                border-radius: 10px;
                margin-bottom: 20px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            .control-button {
                background: #667eea;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 5px;
                cursor: pointer;
                font-size: 0.9em;
                margin: 5px;
            }
            .control-button:hover {
                background: #5a6fd8;
            }
            .control-button:disabled {
                background: #ccc;
                cursor: not-allowed;
            }
            .pulse {
                animation: pulse 2s infinite;
            }
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }
        </style>
    </head>
    <body>
        <div class="header">
            <div class="connection-status connecting" id="connection-status">
                🔄 Connecting...
            </div>
            <h1>📊 Real-Time News Dashboard</h1>
            <p>Live analytics with WebSocket streaming</p>
        </div>

        <div class="controls">
            <button class="control-button" onclick="toggleConnection()" id="connection-toggle">
                🔌 Disconnect
            </button>
            <button class="control-button" onclick="requestUpdate()">
                🔄 Request Update
            </button>
            <button class="control-button" onclick="clearAlerts()">
                🗑️ Clear Alerts
            </button>
            <button class="control-button" onclick="exportData()">
                📤 Export Data
            </button>
        </div>

        <div class="stats-grid" id="stats-grid">
            <div class="loading">Connecting to real-time data...</div>
        </div>

        <div class="alerts-panel">
            <div class="chart-title">
                🚨 Live Alerts
                <span class="last-updated" id="alerts-updated">Waiting for data...</span>
            </div>
            <div id="alerts-container">
                <div class="loading">Waiting for alerts...</div>
            </div>
        </div>

        <div class="dashboard-grid">
            <div class="chart-container">
                <div class="chart-title">
                    📈 Live Trend Analysis
                    <span class="last-updated" id="trends-updated">Waiting for data...</span>
                </div>
                <div id="trends-chart" class="loading">Connecting to trend stream...</div>
            </div>

            <div class="chart-container">
                <div class="chart-title">
                    ✈️ Live Aviation Traffic
                    <span class="last-updated" id="aviation-updated">Waiting for data...</span>
                </div>
                <div id="aviation-chart" class="loading">Connecting to aviation stream...</div>
            </div>

            <div class="chart-container">
                <div class="chart-title">
                    📊 System Performance
                    <span class="last-updated" id="performance-updated">Waiting for data...</span>
                </div>
                <div id="performance-chart" class="loading">Connecting to performance stream...</div>
            </div>

            <div class="chart-container">
                <div class="chart-title">
                    🔗 WebSocket Connections
                    <span class="last-updated" id="connections-updated">Waiting for data...</span>
                </div>
                <div id="connections-chart" class="loading">Connecting to connection stats...</div>
            </div>
        </div>

        <script>
            // WebSocket connections
            let dashboardWs = null;
            let isConnected = false;
            let reconnectAttempts = 0;
            const maxReconnectAttempts = 5;
            
            // Data storage
            let trendsData = [];
            let aviationData = {};
            let alertsData = [];
            let performanceData = [];
            
            // Initialize WebSocket connection
            function initWebSocket() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws/dashboard`;
                
                updateConnectionStatus('connecting');
                
                dashboardWs = new WebSocket(wsUrl);
                
                dashboardWs.onopen = function(event) {
                    console.log('WebSocket connected');
                    isConnected = true;
                    reconnectAttempts = 0;
                    updateConnectionStatus('connected');
                    
                    // Send initial subscription
                    sendMessage({
                        type: 'subscribe',
                        topics: ['trends', 'aviation', 'alerts', 'performance']
                    });
                };
                
                dashboardWs.onmessage = function(event) {
                    try {
                        const data = JSON.parse(event.data);
                        handleWebSocketMessage(data);
                    } catch (error) {
                        console.error('Error parsing WebSocket message:', error);
                    }
                };
                
                dashboardWs.onclose = function(event) {
                    console.log('WebSocket disconnected');
                    isConnected = false;
                    updateConnectionStatus('disconnected');
                    
                    // Attempt to reconnect
                    if (reconnectAttempts < maxReconnectAttempts) {
                        reconnectAttempts++;
                        console.log(`Attempting to reconnect (${reconnectAttempts}/${maxReconnectAttempts})...`);
                        setTimeout(initWebSocket, 3000 * reconnectAttempts);
                    }
                };
                
                dashboardWs.onerror = function(error) {
                    console.error('WebSocket error:', error);
                    updateConnectionStatus('disconnected');
                };
            }
            
            function handleWebSocketMessage(data) {
                console.log('Received:', data.type);
                
                switch (data.type) {
                    case 'connection_established':
                        console.log('Connection established:', data);
                        break;
                        
                    case 'trend_update':
                        handleTrendUpdate(data);
                        break;
                        
                    case 'aviation_update':
                        handleAviationUpdate(data);
                        break;
                        
                    case 'new_alerts':
                        handleNewAlerts(data);
                        break;
                        
                    case 'system_heartbeat':
                        handleSystemHeartbeat(data);
                        break;
                        
                    case 'cache_update':
                        handleCacheUpdate(data);
                        break;
                        
                    default:
                        console.log('Unknown message type:', data.type);
                }
            }
            
            function handleTrendUpdate(data) {
                trendsData = data.trends || [];
                updateTrendsChart();
                updateTimestamp('trends-updated');
            }
            
            function handleAviationUpdate(data) {
                aviationData = data;
                updateAviationChart();
                updateStatsGrid();
                updateTimestamp('aviation-updated');
            }
            
            function handleNewAlerts(data) {
                alertsData = [...alertsData, ...data.alerts];
                // Keep only last 20 alerts
                if (alertsData.length > 20) {
                    alertsData = alertsData.slice(-20);
                }
                updateAlertsPanel();
                updateTimestamp('alerts-updated');
            }
            
            function handleSystemHeartbeat(data) {
                updatePerformanceChart(data);
                updateConnectionsChart(data.connections);
                updateTimestamp('performance-updated');
                updateTimestamp('connections-updated');
            }
            
            function handleCacheUpdate(data) {
                // Update performance metrics with cache data
                performanceData.push({
                    timestamp: data.timestamp,
                    cache_entries: data.stats.total_entries,
                    cache_size_mb: data.stats.total_size_mb,
                    hit_rate: data.performance.hit_rate,
                    response_time: data.performance.avg_response_time_ms
                });
                
                // Keep only last 50 data points
                if (performanceData.length > 50) {
                    performanceData = performanceData.slice(-50);
                }
            }
            
            function updateConnectionStatus(status) {
                const statusElement = document.getElementById('connection-status');
                const toggleButton = document.getElementById('connection-toggle');
                
                statusElement.className = `connection-status ${status}`;
                
                switch (status) {
                    case 'connected':
                        statusElement.textContent = '🟢 Connected';
                        toggleButton.textContent = '🔌 Disconnect';
                        toggleButton.disabled = false;
                        break;
                    case 'connecting':
                        statusElement.textContent = '🔄 Connecting...';
                        toggleButton.textContent = '🔌 Disconnect';
                        toggleButton.disabled = true;
                        break;
                    case 'disconnected':
                        statusElement.textContent = '🔴 Disconnected';
                        toggleButton.textContent = '🔌 Connect';
                        toggleButton.disabled = false;
                        break;
                }
            }
            
            function updateStatsGrid() {
                const statsGrid = document.getElementById('stats-grid');
                
                const totalAircraft = aviationData.total_aircraft || 0;
                const regions = Object.keys(aviationData.regions || {}).length;
                const alerts = alertsData.length;
                const trends = trendsData.length;
                
                statsGrid.innerHTML = `
                    <div class="stat-card">
                        <div class="stat-number pulse">${totalAircraft}</div>
                        <div class="stat-label">Aircraft Tracked</div>
                        <div class="stat-change stat-up">+${Math.floor(Math.random() * 10)}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${regions}</div>
                        <div class="stat-label">Regions Monitored</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number ${alerts > 0 ? 'pulse' : ''}">${alerts}</div>
                        <div class="stat-label">Active Alerts</div>
                        ${alerts > 0 ? '<div class="stat-change stat-up">New!</div>' : ''}
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${trends}</div>
                        <div class="stat-label">Topics Tracked</div>
                    </div>
                `;
            }
            
            function updateTrendsChart() {
                if (trendsData.length === 0) return;
                
                const spec = {
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                    "width": 450,
                    "height": 300,
                    "data": {"values": trendsData},
                    "mark": {"type": "circle", "size": 200},
                    "encoding": {
                        "x": {
                            "field": "current_value",
                            "type": "quantitative",
                            "title": "Current Value"
                        },
                        "y": {
                            "field": "z_score",
                            "type": "quantitative",
                            "title": "Z-Score",
                            "scale": {"domain": [-4, 4]}
                        },
                        "color": {
                            "field": "trend_direction",
                            "type": "nominal",
                            "title": "Trend",
                            "scale": {
                                "domain": ["increasing", "decreasing", "stable"],
                                "range": ["#4CAF50", "#f44336", "#FF9800"]
                            }
                        },
                        "stroke": {
                            "condition": {"test": "datum.is_anomaly", "value": "#ff0000"},
                            "value": "transparent"
                        },
                        "strokeWidth": {
                            "condition": {"test": "datum.is_anomaly", "value": 3},
                            "value": 0
                        },
                        "tooltip": [
                            {"field": "topic_name", "title": "Topic"},
                            {"field": "current_value", "title": "Value", "format": ".2f"},
                            {"field": "z_score", "title": "Z-Score", "format": ".2f"},
                            {"field": "trend_direction", "title": "Trend"},
                            {"field": "is_anomaly", "title": "Anomaly"}
                        ]
                    }
                };
                
                vegaEmbed('#trends-chart', spec);
            }
            
            function updateAviationChart() {
                if (!aviationData.regions) return;
                
                const regionData = Object.entries(aviationData.regions).map(([region, data]) => ({
                    region: region,
                    total_aircraft: data.total_aircraft,
                    in_air: data.in_air,
                    on_ground: data.on_ground,
                    avg_altitude: data.avg_altitude
                }));
                
                const spec = {
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                    "width": 450,
                    "height": 300,
                    "data": {"values": regionData},
                    "mark": {"type": "bar"},
                    "encoding": {
                        "x": {
                            "field": "region",
                            "type": "nominal",
                            "title": "Region"
                        },
                        "y": {
                            "field": "total_aircraft",
                            "type": "quantitative",
                            "title": "Total Aircraft"
                        },
                        "color": {
                            "field": "region",
                            "type": "nominal",
                            "title": "Region"
                        },
                        "tooltip": [
                            {"field": "region", "title": "Region"},
                            {"field": "total_aircraft", "title": "Total Aircraft"},
                            {"field": "in_air", "title": "In Air"},
                            {"field": "on_ground", "title": "On Ground"},
                            {"field": "avg_altitude", "title": "Avg Altitude (m)", "format": ".0f"}
                        ]
                    }
                };
                
                vegaEmbed('#aviation-chart', spec);
            }
            
            function updateAlertsPanel() {
                const container = document.getElementById('alerts-container');
                
                if (alertsData.length === 0) {
                    container.innerHTML = '<div class="loading">No alerts</div>';
                    return;
                }
                
                const alertsHtml = alertsData.slice(-10).reverse().map(alert => `
                    <div class="alert-item alert-${alert.severity}">
                        <strong>${alert.severity.toUpperCase()}</strong> - ${alert.message}
                        <br><small>Z-Score: ${alert.z_score.toFixed(2)} | ${new Date(alert.timestamp).toLocaleTimeString()}</small>
                    </div>
                `).join('');
                
                container.innerHTML = alertsHtml;
            }
            
            function updatePerformanceChart(data) {
                const perfData = [
                    {metric: 'Cache Entries', value: data.cache.entries},
                    {metric: 'Cache Size (MB)', value: data.cache.size_mb},
                    {metric: 'Cache Utilization (%)', value: data.cache.utilization_pct},
                    {metric: 'Total Connections', value: data.connections.total_connections}
                ];
                
                const spec = {
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                    "width": 450,
                    "height": 300,
                    "data": {"values": perfData},
                    "mark": {"type": "arc", "innerRadius": 50},
                    "encoding": {
                        "theta": {
                            "field": "value",
                            "type": "quantitative"
                        },
                        "color": {
                            "field": "metric",
                            "type": "nominal",
                            "title": "Metric"
                        },
                        "tooltip": [
                            {"field": "metric", "title": "Metric"},
                            {"field": "value", "title": "Value"}
                        ]
                    }
                };
                
                vegaEmbed('#performance-chart', spec);
            }
            
            function updateConnectionsChart(connectionData) {
                const connData = Object.entries(connectionData.connections_by_type).map(([type, count]) => ({
                    connection_type: type,
                    count: count
                }));
                
                const spec = {
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                    "width": 450,
                    "height": 300,
                    "data": {"values": connData},
                    "mark": {"type": "bar"},
                    "encoding": {
                        "x": {
                            "field": "connection_type",
                            "type": "nominal",
                            "title": "Connection Type"
                        },
                        "y": {
                            "field": "count",
                            "type": "quantitative",
                            "title": "Active Connections"
                        },
                        "color": {
                            "field": "connection_type",
                            "type": "nominal",
                            "title": "Type"
                        },
                        "tooltip": [
                            {"field": "connection_type", "title": "Type"},
                            {"field": "count", "title": "Connections"}
                        ]
                    }
                };
                
                vegaEmbed('#connections-chart', spec);
            }
            
            function updateTimestamp(elementId) {
                const element = document.getElementById(elementId);
                if (element) {
                    element.textContent = `Updated: ${new Date().toLocaleTimeString()}`;
                }
            }
            
            function sendMessage(message) {
                if (dashboardWs && dashboardWs.readyState === WebSocket.OPEN) {
                    dashboardWs.send(JSON.stringify(message));
                }
            }
            
            // Control functions
            function toggleConnection() {
                if (isConnected) {
                    dashboardWs.close();
                } else {
                    initWebSocket();
                }
            }
            
            function requestUpdate() {
                sendMessage({type: 'request_update'});
            }
            
            function clearAlerts() {
                alertsData = [];
                updateAlertsPanel();
            }
            
            function exportData() {
                const data = {
                    trends: trendsData,
                    aviation: aviationData,
                    alerts: alertsData,
                    performance: performanceData,
                    exported_at: new Date().toISOString()
                };
                
                const blob = new Blob([JSON.stringify(data, null, 2)], {type: 'application/json'});
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `dashboard_data_${new Date().toISOString().slice(0, 19)}.json`;
                a.click();
                URL.revokeObjectURL(url);
            }
            
            // Initialize on page load
            document.addEventListener('DOMContentLoaded', function() {
                initWebSocket();
                
                // Send periodic pings
                setInterval(() => {
                    if (isConnected) {
                        sendMessage({type: 'ping'});
                    }
                }, 30000);
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
