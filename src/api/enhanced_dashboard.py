"""Enhanced dashboard showcasing all new backend features."""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

@router.get("/enhanced", response_class=HTMLResponse)
async def enhanced_dashboard():
    """Serve the enhanced dashboard with all new features."""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>News Dashboard - Enhanced Analytics Platform</title>
        <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
        <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
        <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                color: #333;
            }
            
            .container {
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px;
            }
            
            .header {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                padding: 30px;
                margin-bottom: 30px;
                text-align: center;
                box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            }
            
            .header h1 {
                font-size: 3em;
                font-weight: 300;
                margin-bottom: 10px;
                background: linear-gradient(135deg, #667eea, #764ba2);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            
            .header p {
                font-size: 1.2em;
                color: #666;
                margin-bottom: 20px;
            }
            
            .feature-badges {
                display: flex;
                justify-content: center;
                gap: 10px;
                flex-wrap: wrap;
            }
            
            .badge {
                background: linear-gradient(135deg, #667eea, #764ba2);
                color: white;
                padding: 8px 16px;
                border-radius: 20px;
                font-size: 0.9em;
                font-weight: 500;
            }
            
            .nav-tabs {
                display: flex;
                background: rgba(255, 255, 255, 0.95);
                border-radius: 15px;
                padding: 10px;
                margin-bottom: 30px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                overflow-x: auto;
            }
            
            .nav-tab {
                flex: 1;
                padding: 15px 20px;
                text-align: center;
                border-radius: 10px;
                cursor: pointer;
                transition: all 0.3s ease;
                font-weight: 500;
                white-space: nowrap;
            }
            
            .nav-tab.active {
                background: linear-gradient(135deg, #667eea, #764ba2);
                color: white;
                transform: translateY(-2px);
                box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
            }
            
            .nav-tab:hover:not(.active) {
                background: rgba(102, 126, 234, 0.1);
            }
            
            .tab-content {
                display: none;
            }
            
            .tab-content.active {
                display: block;
            }
            
            .dashboard-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .card {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                padding: 25px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                transition: transform 0.3s ease, box-shadow 0.3s ease;
            }
            
            .card:hover {
                transform: translateY(-5px);
                box-shadow: 0 12px 40px rgba(0,0,0,0.15);
            }
            
            .card-title {
                font-size: 1.4em;
                font-weight: 600;
                margin-bottom: 20px;
                color: #333;
                display: flex;
                align-items: center;
                gap: 10px;
            }
            
            .card-title .icon {
                font-size: 1.2em;
            }
            
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .stat-card {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border-radius: 15px;
                padding: 25px;
                text-align: center;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                transition: transform 0.3s ease;
            }
            
            .stat-card:hover {
                transform: translateY(-3px);
            }
            
            .stat-number {
                font-size: 2.5em;
                font-weight: bold;
                background: linear-gradient(135deg, #667eea, #764ba2);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 10px;
            }
            
            .stat-label {
                color: #666;
                font-weight: 500;
            }
            
            .controls {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border-radius: 15px;
                padding: 20px;
                margin-bottom: 30px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            }
            
            .control-group {
                display: flex;
                gap: 15px;
                align-items: center;
                flex-wrap: wrap;
            }
            
            .btn {
                background: linear-gradient(135deg, #667eea, #764ba2);
                color: white;
                border: none;
                padding: 12px 24px;
                border-radius: 25px;
                cursor: pointer;
                font-size: 0.9em;
                font-weight: 500;
                transition: all 0.3s ease;
                text-decoration: none;
                display: inline-flex;
                align-items: center;
                gap: 8px;
            }
            
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
            }
            
            .btn-secondary {
                background: rgba(102, 126, 234, 0.1);
                color: #667eea;
            }
            
            .btn-success {
                background: linear-gradient(135deg, #4CAF50, #45a049);
            }
            
            .btn-warning {
                background: linear-gradient(135deg, #FF9800, #f57c00);
            }
            
            .btn-danger {
                background: linear-gradient(135deg, #f44336, #d32f2f);
            }
            
            .loading {
                text-align: center;
                padding: 40px;
                color: #666;
            }
            
            .spinner {
                border: 3px solid #f3f3f3;
                border-top: 3px solid #667eea;
                border-radius: 50%;
                width: 30px;
                height: 30px;
                animation: spin 1s linear infinite;
                margin: 0 auto 20px;
            }
            
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            
            .alert {
                padding: 15px;
                border-radius: 10px;
                margin: 10px 0;
                font-weight: 500;
            }
            
            .alert-success {
                background: rgba(76, 175, 80, 0.1);
                border: 1px solid rgba(76, 175, 80, 0.3);
                color: #2e7d32;
            }
            
            .alert-warning {
                background: rgba(255, 152, 0, 0.1);
                border: 1px solid rgba(255, 152, 0, 0.3);
                color: #f57c00;
            }
            
            .alert-error {
                background: rgba(244, 67, 54, 0.1);
                border: 1px solid rgba(244, 67, 54, 0.3);
                color: #d32f2f;
            }
            
            .feature-showcase {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .feature-card {
                background: rgba(255, 255, 255, 0.95);
                border-radius: 15px;
                padding: 20px;
                text-align: center;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            }
            
            .feature-icon {
                font-size: 3em;
                margin-bottom: 15px;
            }
            
            .feature-title {
                font-size: 1.2em;
                font-weight: 600;
                margin-bottom: 10px;
                color: #333;
            }
            
            .feature-description {
                color: #666;
                line-height: 1.5;
            }
            
            .api-endpoint {
                background: rgba(0, 0, 0, 0.05);
                border-radius: 8px;
                padding: 10px;
                font-family: 'Monaco', 'Menlo', monospace;
                font-size: 0.9em;
                margin: 5px 0;
                word-break: break-all;
            }
            
            .status-indicator {
                display: inline-block;
                width: 12px;
                height: 12px;
                border-radius: 50%;
                margin-right: 8px;
            }
            
            .status-online {
                background: #4CAF50;
                box-shadow: 0 0 10px rgba(76, 175, 80, 0.5);
            }
            
            .status-offline {
                background: #f44336;
            }
            
            .status-warning {
                background: #FF9800;
            }
            
            /* Visual Analytics Styles */
            .visual-summary {
                padding: 0;
            }
            
            .summary-header {
                text-align: center;
                margin-bottom: 20px;
                padding-bottom: 15px;
                border-bottom: 2px solid rgba(102, 126, 234, 0.1);
            }
            
            .summary-number {
                display: block;
                font-size: 2.5em;
                font-weight: bold;
                background: linear-gradient(135deg, #667eea, #764ba2);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 5px;
            }
            
            .summary-label {
                color: #666;
                font-weight: 500;
                font-size: 0.9em;
            }
            
            .summary-topic {
                display: block;
                font-size: 1.2em;
                font-weight: 600;
                color: #333;
                margin-top: 5px;
            }
            
            /* Trend Bars */
            .trend-bars {
                display: flex;
                flex-direction: column;
                gap: 12px;
            }
            
            .trend-item {
                display: flex;
                flex-direction: column;
                gap: 5px;
            }
            
            .trend-info {
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .trend-icon {
                font-size: 1.2em;
            }
            
            .trend-label {
                flex: 1;
                margin-left: 8px;
                font-weight: 500;
            }
            
            .trend-value {
                font-weight: bold;
                color: #333;
            }
            
            .trend-bar {
                height: 8px;
                background: rgba(0, 0, 0, 0.1);
                border-radius: 4px;
                overflow: hidden;
            }
            
            .trend-fill {
                height: 100%;
                border-radius: 4px;
                transition: width 0.3s ease;
            }
            
            /* Alert Grid */
            .alert-grid {
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 15px;
                margin-bottom: 20px;
            }
            
            .alert-item {
                text-align: center;
                padding: 15px;
                background: rgba(0, 0, 0, 0.02);
                border-radius: 10px;
                border: 1px solid rgba(0, 0, 0, 0.05);
            }
            
            .alert-icon {
                font-size: 1.5em;
                margin-bottom: 8px;
            }
            
            .alert-count {
                font-size: 1.8em;
                font-weight: bold;
                color: #333;
                margin-bottom: 5px;
            }
            
            .alert-severity {
                font-size: 0.85em;
                color: #666;
                font-weight: 500;
            }
            
            .recent-alerts {
                margin-top: 15px;
                padding-top: 15px;
                border-top: 1px solid rgba(0, 0, 0, 0.1);
            }
            
            .recent-alerts-title {
                font-size: 0.9em;
                font-weight: 600;
                color: #666;
                margin-bottom: 10px;
            }
            
            .recent-alert-item {
                display: flex;
                align-items: center;
                gap: 10px;
                padding: 8px 0;
                border-bottom: 1px solid rgba(0, 0, 0, 0.05);
            }
            
            .recent-alert-item:last-child {
                border-bottom: none;
            }
            
            .alert-indicator {
                width: 8px;
                height: 8px;
                border-radius: 50%;
                flex-shrink: 0;
            }
            
            .alert-text {
                font-size: 0.85em;
                color: #666;
                line-height: 1.3;
            }
            
            /* Correlation List */
            .correlation-list {
                display: flex;
                flex-direction: column;
                gap: 15px;
            }
            
            .correlation-item {
                padding: 12px;
                background: rgba(0, 0, 0, 0.02);
                border-radius: 8px;
                border: 1px solid rgba(0, 0, 0, 0.05);
            }
            
            .correlation-header {
                display: flex;
                align-items: center;
                gap: 8px;
                margin-bottom: 10px;
            }
            
            .correlation-icon {
                font-size: 1.1em;
            }
            
            .correlation-topic {
                font-weight: 600;
                color: #333;
                font-size: 0.9em;
            }
            
            .correlation-strength {
                display: flex;
                flex-direction: column;
                gap: 5px;
            }
            
            .strength-bar {
                height: 6px;
                background: rgba(0, 0, 0, 0.1);
                border-radius: 3px;
                overflow: hidden;
            }
            
            .strength-fill {
                height: 100%;
                border-radius: 3px;
                transition: width 0.3s ease;
            }
            
            .strength-info {
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            
            .strength-value {
                font-weight: bold;
                font-size: 0.85em;
                color: #333;
            }
            
            .strength-label {
                font-size: 0.8em;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            
            /* Anomaly Grid */
            .anomaly-grid {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 12px;
                margin-bottom: 15px;
            }
            
            .anomaly-item {
                text-align: center;
                padding: 12px;
                background: rgba(0, 0, 0, 0.02);
                border-radius: 8px;
                border: 1px solid rgba(0, 0, 0, 0.05);
            }
            
            .anomaly-icon {
                font-size: 1.3em;
                margin-bottom: 6px;
            }
            
            .anomaly-count {
                font-size: 1.5em;
                font-weight: bold;
                color: #333;
                margin-bottom: 4px;
            }
            
            .anomaly-label {
                font-size: 0.8em;
                color: #666;
                font-weight: 500;
            }
            
            .analysis-footer {
                text-align: center;
                padding-top: 15px;
                border-top: 1px solid rgba(0, 0, 0, 0.1);
            }
            
            .analysis-stat {
                display: inline-flex;
                align-items: center;
                gap: 6px;
                color: #666;
                font-size: 0.85em;
            }
            
            .stat-icon {
                font-size: 1em;
            }
            
            /* No Data State */
            .no-data {
                text-align: center;
                padding: 30px 20px;
                color: #666;
            }
            
            .no-data-icon {
                font-size: 2em;
                margin-bottom: 10px;
                opacity: 0.5;
            }
            
            .no-data-text {
                font-size: 0.9em;
            }
            
            @media (max-width: 768px) {
                .container {
                    padding: 10px;
                }
                
                .header h1 {
                    font-size: 2em;
                }
                
                .nav-tabs {
                    flex-direction: column;
                }
                
                .dashboard-grid {
                    grid-template-columns: 1fr;
                }
                
                .stats-grid {
                    grid-template-columns: repeat(2, 1fr);
                }
                
                /* Mobile responsive for visual analytics */
                .alert-grid {
                    grid-template-columns: 1fr;
                }
                
                .anomaly-grid {
                    grid-template-columns: repeat(2, 1fr);
                }
                
                .trend-info {
                    flex-direction: column;
                    align-items: flex-start;
                    gap: 5px;
                }
                
                .correlation-header {
                    flex-direction: column;
                    align-items: flex-start;
                    gap: 5px;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 News Dashboard</h1>
                <p>Enterprise-Grade Analytics Platform with Real-Time Intelligence</p>
                <div class="feature-badges">
                    <span class="badge">🚀 Real-Time Streaming</span>
                    <span class="badge">📈 Advanced Analytics</span>
                    <span class="badge">💾 Intelligent Caching</span>
                    <span class="badge">📤 Multi-Format Export</span>
                    <span class="badge">🔍 Anomaly Detection</span>
                    <span class="badge">⚡ WebSocket Powered</span>
                </div>
            </div>

            <div class="nav-tabs">
                <div class="nav-tab active" onclick="showTab('overview')">
                    <span class="icon">🏠</span> Overview
                </div>
                <div class="nav-tab" onclick="showTab('analytics')">
                    <span class="icon">📊</span> Analytics
                </div>
                <div class="nav-tab" onclick="showTab('historical')">
                    <span class="icon">📈</span> Historical Data
                </div>
                <div class="nav-tab" onclick="showTab('export')">
                    <span class="icon">📤</span> Export Tools
                </div>
                <div class="nav-tab" onclick="showTab('realtime')">
                    <span class="icon">⚡</span> Real-Time
                </div>
                <div class="nav-tab" onclick="showTab('api')">
                    <span class="icon">🔧</span> API Explorer
                </div>
            </div>

            <!-- Overview Tab -->
            <div id="overview" class="tab-content active">
                <div class="stats-grid" id="overview-stats">
                    <div class="loading">
                        <div class="spinner"></div>
                        Loading system statistics...
                    </div>
                </div>

                <div class="feature-showcase">
                    <div class="feature-card">
                        <div class="feature-icon">🧠</div>
                        <div class="feature-title">Advanced Analytics</div>
                        <div class="feature-description">
                            Statistical trend analysis, anomaly detection, correlation analysis, and predictive modeling
                        </div>
                    </div>
                    <div class="feature-card">
                        <div class="feature-icon">💾</div>
                        <div class="feature-title">Intelligent Caching</div>
                        <div class="feature-description">
                            Local file-based caching with LRU eviction, TTL support, and automatic cleanup
                        </div>
                    </div>
                    <div class="feature-card">
                        <div class="feature-icon">📤</div>
                        <div class="feature-title">Multi-Format Export</div>
                        <div class="feature-description">
                            Export data in CSV, JSON, Excel formats with rich metadata and bulk operations
                        </div>
                    </div>
                    <div class="feature-card">
                        <div class="feature-icon">⚡</div>
                        <div class="feature-title">Real-Time Streaming</div>
                        <div class="feature-description">
                            WebSocket-powered live updates with background task orchestration
                        </div>
                    </div>
                </div>

                <div class="dashboard-grid">
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📊</span>
                            System Health
                        </div>
                        <div id="system-health">
                            <div class="loading">Loading system health...</div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">💾</span>
                            Cache Performance
                        </div>
                        <div id="cache-performance">
                            <div class="loading">Loading cache stats...</div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Analytics Tab -->
            <div id="analytics" class="tab-content">
                <div class="controls">
                    <div class="control-group">
                        <button class="btn btn-secondary" onclick="loadAllAnalytics()">
                            🔄 Refresh Analytics
                        </button>
                    </div>
                </div>

                <div class="dashboard-grid">
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📈</span>
                            Trend Analysis
                        </div>
                        <div id="trend-analysis">
                            <div class="loading">
                                <div class="spinner"></div>
                                Loading trend analysis...
                            </div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">🚨</span>
                            Active Alerts
                        </div>
                        <div id="active-alerts">
                            <div class="loading">
                                <div class="spinner"></div>
                                Loading alerts...
                            </div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">🔗</span>
                            Topic Correlations
                        </div>
                        <div id="correlations">
                            <div class="loading">
                                <div class="spinner"></div>
                                Finding correlations...
                            </div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">🔍</span>
                            Anomaly Detection
                        </div>
                        <div id="anomalies">
                            <div class="loading">
                                <div class="spinner"></div>
                                Detecting anomalies...
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Historical Data Tab -->
            <div id="historical" class="tab-content">
                <div class="controls">
                    <div class="control-group">
                        <select id="topic-select" class="btn btn-secondary">
                            <option value="">Select Topic...</option>
                        </select>
                        <input type="date" id="start-date" class="btn btn-secondary" value="2024-01-01">
                        <input type="date" id="end-date" class="btn btn-secondary" value="2024-01-31">
                        <button class="btn" onclick="loadHistoricalData()">
                            📊 Load Data
                        </button>
                        <button class="btn btn-secondary" onclick="preloadData()">
                            🔄 Preload Cache
                        </button>
                    </div>
                </div>

                <div class="dashboard-grid">
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📈</span>
                            Historical Trends
                        </div>
                        <div id="historical-chart">
                            <div class="loading">Select a topic and date range to view historical data</div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📊</span>
                            Data Summary
                        </div>
                        <div id="data-summary">
                            <div class="loading">No data loaded</div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Export Tab -->
            <div id="export" class="tab-content">
                <div class="controls">
                    <div class="control-group">
                        <select id="export-topic" class="btn btn-secondary">
                            <option value="">Select Topic...</option>
                        </select>
                        <select id="export-format" class="btn btn-secondary">
                            <option value="csv">CSV</option>
                            <option value="json">JSON</option>
                            <option value="excel">Excel</option>
                        </select>
                        <button class="btn btn-success" onclick="exportData()">
                            📤 Export Data
                        </button>
                        <button class="btn btn-secondary" onclick="exportSources()">
                            🗂️ Export Sources
                        </button>
                    </div>
                </div>

                <div class="dashboard-grid">
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📋</span>
                            Export Formats
                        </div>
                        <div id="export-formats">
                            <div class="loading">Loading export options...</div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📊</span>
                            Export History
                        </div>
                        <div id="export-history">
                            <p>Recent exports will appear here</p>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Real-Time Tab -->
            <div id="realtime" class="tab-content">
                <div class="controls">
                    <div class="control-group">
                        <button class="btn" onclick="connectWebSocket()">
                            🔌 Connect WebSocket
                        </button>
                        <button class="btn btn-warning" onclick="disconnectWebSocket()">
                            🔌 Disconnect
                        </button>
                        <button class="btn btn-secondary" onclick="sendPing()">
                            📡 Send Ping
                        </button>
                        <span id="ws-status" class="status-indicator status-offline"></span>
                        <span id="ws-status-text">Disconnected</span>
                    </div>
                </div>

                <div class="dashboard-grid">
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">⚡</span>
                            Live Updates
                        </div>
                        <div id="live-updates">
                            <div class="loading">Connect WebSocket to see live updates</div>
                        </div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📡</span>
                            WebSocket Messages
                        </div>
                        <div id="ws-messages" style="max-height: 300px; overflow-y: auto;">
                            <p>WebSocket messages will appear here</p>
                        </div>
                    </div>
                </div>
            </div>

            <!-- API Explorer Tab -->
            <div id="api" class="tab-content">
                <div class="dashboard-grid">
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">🔧</span>
                            Analytics Endpoints
                        </div>
                        <div class="api-endpoint">GET /api/analytics/trends/analysis</div>
                        <div class="api-endpoint">GET /api/analytics/alerts/active</div>
                        <div class="api-endpoint">GET /api/analytics/correlations</div>
                        <div class="api-endpoint">GET /api/analytics/predictions/{topic_id}</div>
                        <div class="api-endpoint">GET /api/analytics/anomalies/detection</div>
                        <div class="api-endpoint">GET /api/analytics/insights/summary</div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📊</span>
                            Historical Data Endpoints
                        </div>
                        <div class="api-endpoint">GET /api/historical/data/{topic_id}</div>
                        <div class="api-endpoint">GET /api/historical/cache/stats</div>
                        <div class="api-endpoint">POST /api/historical/cache/invalidate</div>
                        <div class="api-endpoint">GET /api/historical/topics/{topic_id}/summary</div>
                        <div class="api-endpoint">POST /api/historical/preload</div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">📤</span>
                            Export Endpoints
                        </div>
                        <div class="api-endpoint">GET /api/export/topics/{topic_id}/csv</div>
                        <div class="api-endpoint">GET /api/export/topics/{topic_id}/json</div>
                        <div class="api-endpoint">GET /api/export/topics/{topic_id}/excel</div>
                        <div class="api-endpoint">GET /api/export/bulk/topics</div>
                        <div class="api-endpoint">GET /api/export/sources/registry</div>
                        <div class="api-endpoint">GET /api/export/formats</div>
                    </div>
                    <div class="card">
                        <div class="card-title">
                            <span class="icon">⚡</span>
                            WebSocket Endpoints
                        </div>
                        <div class="api-endpoint">WS /ws/dashboard</div>
                        <div class="api-endpoint">WS /ws/aviation</div>
                        <div class="api-endpoint">WS /ws/alerts</div>
                        <div class="api-endpoint">WS /ws/trends</div>
                        <div class="api-endpoint">WS /ws/analytics</div>
                        <div class="api-endpoint">GET /ws/stats</div>
                    </div>
                </div>

                <div class="controls">
                    <div class="control-group">
                        <a href="/docs" class="btn" target="_blank">
                            📖 API Documentation
                        </a>
                        <a href="/openapi.json" class="btn btn-secondary" target="_blank">
                            📋 OpenAPI Schema
                        </a>
                    </div>
                </div>
            </div>
        </div>

        <script>
            // Global variables
            let websocket = null;
            let topics = [];
            
            // Tab management
            function showTab(tabName) {
                // Hide all tabs
                document.querySelectorAll('.tab-content').forEach(tab => {
                    tab.classList.remove('active');
                });
                document.querySelectorAll('.nav-tab').forEach(tab => {
                    tab.classList.remove('active');
                });
                
                // Show selected tab
                document.getElementById(tabName).classList.add('active');
                event.target.classList.add('active');
                
                // Load tab-specific data
                if (tabName === 'overview') {
                    loadOverviewData();
                } else if (tabName === 'analytics') {
                    loadAllAnalytics();
                } else if (tabName === 'export') {
                    loadExportFormats();
                }
            }
            
            // Load overview data
            async function loadOverviewData() {
                try {
                    const [sourcesRes, topicsRes, cacheRes] = await Promise.all([
                        fetch('/api/sources'),
                        fetch('/api/topics'),
                        fetch('/api/historical/cache/stats')
                    ]);
                    
                    const sources = await sourcesRes.json();
                    const topicsData = await topicsRes.json();
                    const cache = await cacheRes.json();
                    
                    // Store topics globally - handle different response formats
                    topics = topicsData.topics || topicsData || [];
                    console.log('Loaded topics:', topics);
                    
                    // Update stats
                    document.getElementById('overview-stats').innerHTML = `
                        <div class="stat-card">
                            <div class="stat-number">${sources.total}</div>
                            <div class="stat-label">Data Sources</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${topicsData.total}</div>
                            <div class="stat-label">Topics Tracked</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${cache.total_entries}</div>
                            <div class="stat-label">Cache Entries</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">${cache.total_size_mb}</div>
                            <div class="stat-label">Cache Size (MB)</div>
                        </div>
                    `;
                    
                    // Update system health
                    document.getElementById('system-health').innerHTML = `
                        <div class="alert alert-success">
                            <span class="status-indicator status-online"></span>
                            All systems operational
                        </div>
                        <p><strong>API Endpoints:</strong> 25+ active</p>
                        <p><strong>WebSocket Endpoints:</strong> 5 available</p>
                        <p><strong>Background Tasks:</strong> Running</p>
                    `;
                    
                    // Update cache performance
                    document.getElementById('cache-performance').innerHTML = `
                        <p><strong>Utilization:</strong> ${cache.utilization_pct}%</p>
                        <p><strong>Namespaces:</strong> ${Object.keys(cache.namespaces || {}).length}</p>
                        <p><strong>Data Types:</strong> ${Object.keys(cache.data_types || {}).length}</p>
                        <div class="alert alert-success">Cache performing optimally</div>
                    `;
                    
                    // Populate topic selects
                    populateTopicSelects();
                    
                } catch (error) {
                    console.error('Error loading overview data:', error);
                    document.getElementById('overview-stats').innerHTML = `
                        <div class="alert alert-error">Error loading data: ${error.message}</div>
                    `;
                }
            }
            
            // Populate topic select dropdowns
            function populateTopicSelects() {
                const selects = ['topic-select', 'export-topic'];
                selects.forEach(selectId => {
                    const select = document.getElementById(selectId);
                    if (select && topics && Array.isArray(topics)) {
                        select.innerHTML = '<option value="">Select Topic...</option>';
                        topics.forEach(topic => {
                            const option = document.createElement('option');
                            option.value = topic.id || topic.topic_id;
                            option.textContent = topic.name;
                            select.appendChild(option);
                        });
                    }
                });
            }
            
            // Load all analytics automatically
            async function loadAllAnalytics() {
                console.log('Loading all analytics features automatically...');
                
                // Load all analytics features in parallel for better performance
                await Promise.all([
                    loadTrendAnalysis(),
                    loadAlerts(),
                    loadCorrelations(),
                    detectAnomalies()
                ]);
                
                console.log('All analytics features loaded successfully');
            }
            
            // Analytics functions
            async function loadTrendAnalysis() {
                document.getElementById('trend-analysis').innerHTML = '<div class="loading"><div class="spinner"></div>Loading trend analysis...</div>';
                
                try {
                    const response = await fetch('/api/analytics/trends/analysis?days_back=30');
                    const data = await response.json();
                    
                    // Create visual trend analysis
                    const trendData = [
                        { label: 'Trending Up', value: data.summary.trending_up, color: '#4CAF50', icon: '📈' },
                        { label: 'Trending Down', value: data.summary.trending_down, color: '#f44336', icon: '📉' },
                        { label: 'Stable', value: data.summary.stable, color: '#2196F3', icon: '➡️' },
                        { label: 'High Anomalies', value: data.summary.high_anomaly, color: '#FF9800', icon: '⚠️' }
                    ];
                    
                    const total = data.summary.total_topics;
                    let trendHtml = `
                        <div class="visual-summary">
                            <div class="summary-header">
                                <span class="summary-number">${total}</span>
                                <span class="summary-label">Topics Analyzed</span>
                            </div>
                            <div class="trend-bars">
                    `;
                    
                    trendData.forEach(item => {
                        const percentage = total > 0 ? (item.value / total * 100) : 0;
                        trendHtml += `
                            <div class="trend-item">
                                <div class="trend-info">
                                    <span class="trend-icon">${item.icon}</span>
                                    <span class="trend-label">${item.label}</span>
                                    <span class="trend-value">${item.value}</span>
                                </div>
                                <div class="trend-bar">
                                    <div class="trend-fill" style="width: ${percentage}%; background-color: ${item.color}"></div>
                                </div>
                            </div>
                        `;
                    });
                    
                    trendHtml += `
                            </div>
                        </div>
                    `;
                    
                    document.getElementById('trend-analysis').innerHTML = trendHtml;
                } catch (error) {
                    document.getElementById('trend-analysis').innerHTML = `
                        <div class="alert alert-error">Error: ${error.message}</div>
                    `;
                }
            }
            
            async function loadAlerts() {
                document.getElementById('active-alerts').innerHTML = '<div class="loading"><div class="spinner"></div>Loading alerts...</div>';
                
                try {
                    const response = await fetch('/api/analytics/alerts/active');
                    const data = await response.json();
                    
                    // Create visual alerts display
                    const alertCounts = { critical: 0, high: 0, medium: 0, low: 0 };
                    const alertIcons = { critical: '🔴', high: '🟠', medium: '🟡', low: '🟢' };
                    const alertColors = { critical: '#f44336', high: '#FF9800', medium: '#FFC107', low: '#4CAF50' };
                    
                    if (data.alerts && data.alerts.length > 0) {
                        data.alerts.forEach(alert => {
                            const severity = alert.severity.toLowerCase();
                            if (alertCounts.hasOwnProperty(severity)) {
                                alertCounts[severity]++;
                            }
                        });
                    }
                    
                    let alertsHtml = `
                        <div class="visual-summary">
                            <div class="summary-header">
                                <span class="summary-number">${data.total}</span>
                                <span class="summary-label">Total Alerts</span>
                            </div>
                            <div class="alert-grid">
                    `;
                    
                    Object.entries(alertCounts).forEach(([severity, count]) => {
                        alertsHtml += `
                            <div class="alert-item">
                                <div class="alert-icon" style="color: ${alertColors[severity]}">${alertIcons[severity]}</div>
                                <div class="alert-count">${count}</div>
                                <div class="alert-severity">${severity.charAt(0).toUpperCase() + severity.slice(1)}</div>
                            </div>
                        `;
                    });
                    
                    alertsHtml += `
                            </div>
                        </div>
                    `;
                    
                    // Add recent alerts if any
                    if (data.alerts && data.alerts.length > 0) {
                        alertsHtml += `
                            <div class="recent-alerts">
                                <div class="recent-alerts-title">Recent Alerts</div>
                        `;
                        data.alerts.slice(0, 3).forEach(alert => {
                            const severityColor = alertColors[alert.severity.toLowerCase()] || '#666';
                            alertsHtml += `
                                <div class="recent-alert-item">
                                    <div class="alert-indicator" style="background-color: ${severityColor}"></div>
                                    <div class="alert-text">${alert.message}</div>
                                </div>
                            `;
                        });
                        alertsHtml += `</div>`;
                    }
                    
                    document.getElementById('active-alerts').innerHTML = alertsHtml;
                } catch (error) {
                    document.getElementById('active-alerts').innerHTML = `
                        <div class="alert alert-error">Error: ${error.message}</div>
                    `;
                }
            }
            
            async function loadCorrelations() {
                document.getElementById('correlations').innerHTML = '<div class="loading"><div class="spinner"></div>Finding correlations...</div>';
                
                try {
                    const response = await fetch('/api/analytics/correlations?primary_topic=economy_employment');
                    const data = await response.json();
                    
                    // Create visual correlations display
                    let corrHtml = `
                        <div class="visual-summary">
                            <div class="summary-header">
                                <span class="summary-label">Primary Topic</span>
                                <span class="summary-topic">${data.primary_topic.name}</span>
                            </div>
                    `;
                    
                    if (data.correlations && data.correlations.length > 0) {
                        corrHtml += `<div class="correlation-list">`;
                        data.correlations.slice(0, 4).forEach(corr => {
                            const correlation = corr.correlation;
                            const absCorr = Math.abs(correlation);
                            const strength = corr.strength.toLowerCase();
                            
                            // Color based on correlation strength
                            let color = '#666';
                            if (strength === 'strong') color = '#4CAF50';
                            else if (strength === 'moderate') color = '#FF9800';
                            else if (strength === 'weak') color = '#2196F3';
                            
                            // Icon based on correlation direction
                            const icon = correlation > 0 ? '📈' : '📉';
                            
                            corrHtml += `
                                <div class="correlation-item">
                                    <div class="correlation-header">
                                        <span class="correlation-icon">${icon}</span>
                                        <span class="correlation-topic">${corr.topic_name}</span>
                                    </div>
                                    <div class="correlation-strength">
                                        <div class="strength-bar">
                                            <div class="strength-fill" style="width: ${absCorr * 100}%; background-color: ${color}"></div>
                                        </div>
                                        <div class="strength-info">
                                            <span class="strength-value">${correlation.toFixed(3)}</span>
                                            <span class="strength-label" style="color: ${color}">${strength}</span>
                                        </div>
                                    </div>
                                </div>
                            `;
                        });
                        corrHtml += `</div>`;
                    } else {
                        corrHtml += `
                            <div class="no-data">
                                <div class="no-data-icon">🔍</div>
                                <div class="no-data-text">No significant correlations found</div>
                            </div>
                        `;
                    }
                    
                    corrHtml += `</div>`;
                    document.getElementById('correlations').innerHTML = corrHtml;
                } catch (error) {
                    document.getElementById('correlations').innerHTML = `
                        <div class="alert alert-error">Error: ${error.message}</div>
                    `;
                }
            }
            
            async function detectAnomalies() {
                document.getElementById('anomalies').innerHTML = '<div class="loading"><div class="spinner"></div>Detecting anomalies...</div>';
                
                try {
                    const response = await fetch('/api/analytics/anomalies/detection?z_threshold=2.5');
                    const data = await response.json();
                    
                    // Create visual anomaly detection display
                    const summary = data.detection_summary;
                    const anomalyData = [
                        { label: 'Critical', value: summary.critical, color: '#f44336', icon: '🚨' },
                        { label: 'High', value: summary.high, color: '#FF9800', icon: '⚠️' },
                        { label: 'Medium', value: summary.medium, color: '#FFC107', icon: '⚡' }
                    ];
                    
                    let anomalyHtml = `
                        <div class="visual-summary">
                            <div class="summary-header">
                                <span class="summary-number">${summary.total_anomalies}</span>
                                <span class="summary-label">Anomalies Found</span>
                            </div>
                            <div class="anomaly-grid">
                    `;
                    
                    anomalyData.forEach(item => {
                        anomalyHtml += `
                            <div class="anomaly-item">
                                <div class="anomaly-icon" style="color: ${item.color}">${item.icon}</div>
                                <div class="anomaly-count">${item.value}</div>
                                <div class="anomaly-label">${item.label}</div>
                            </div>
                        `;
                    });
                    
                    anomalyHtml += `
                            </div>
                            <div class="analysis-footer">
                                <div class="analysis-stat">
                                    <span class="stat-icon">🔍</span>
                                    <span class="stat-text">${summary.topics_analyzed} topics analyzed</span>
                                </div>
                            </div>
                        </div>
                    `;
                    
                    document.getElementById('anomalies').innerHTML = anomalyHtml;
                } catch (error) {
                    document.getElementById('anomalies').innerHTML = `
                        <div class="alert alert-error">Error: ${error.message}</div>
                    `;
                }
            }
            
            // Historical data functions
            async function loadHistoricalData() {
                const topicId = document.getElementById('topic-select').value;
                const startDate = document.getElementById('start-date').value;
                const endDate = document.getElementById('end-date').value;
                
                if (!topicId) {
                    alert('Please select a topic');
                    return;
                }
                
                document.getElementById('historical-chart').innerHTML = '<div class="loading"><div class="spinner"></div>Loading historical data...</div>';
                
                try {
                    const response = await fetch(`/api/historical/data/${topicId}?start_date=${startDate}&end_date=${endDate}`);
                    
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                    }
                    
                    const data = await response.json();
                    
                    // Validate data structure
                    if (!data || !data.data_points) {
                        throw new Error('Invalid data format received');
                    }
                    
                    document.getElementById('historical-chart').innerHTML = `
                        <div class="alert alert-${data.cached ? 'success' : 'warning'}">
                            ${data.cached ? 'Loaded from cache' : 'Fresh data loaded'}
                        </div>
                        <p><strong>Topic:</strong> ${data.topic_name || 'Unknown'}</p>
                        <p><strong>Data Points:</strong> ${data.data_points.length || 0}</p>
                        <p><strong>Date Range:</strong> ${data.start_date || startDate} to ${data.end_date || endDate}</p>
                    `;
                    
                    // Safe access to summary data
                    const summary = data.summary || {};
                    document.getElementById('data-summary').innerHTML = `
                        <p><strong>Total Points:</strong> ${summary.total_points || 0}</p>
                        <p><strong>Mean Value:</strong> ${summary.mean_value ? summary.mean_value.toFixed(2) : 'N/A'}</p>
                        <p><strong>Std Deviation:</strong> ${summary.std_value ? summary.std_value.toFixed(2) : 'N/A'}</p>
                        <p><strong>Min/Max:</strong> ${summary.min_value ? summary.min_value.toFixed(2) : 'N/A'} / ${summary.max_value ? summary.max_value.toFixed(2) : 'N/A'}</p>
                        <p><strong>Anomalies:</strong> ${summary.anomalies_detected || 0}</p>
                    `;
                } catch (error) {
                    console.error('Historical data error:', error);
                    document.getElementById('historical-chart').innerHTML = `
                        <div class="alert alert-error">
                            <strong>Error loading historical data:</strong><br>
                            ${error.message}
                        </div>
                    `;
                    document.getElementById('data-summary').innerHTML = `
                        <div class="alert alert-error">Unable to load data summary</div>
                    `;
                }
            }
            
            async function preloadData() {
                try {
                    const response = await fetch('/api/historical/preload', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({days_back: 90})
                    });
                    const data = await response.json();
                    
                    alert(`Started preloading data for ${data.topics.length} topics`);
                } catch (error) {
                    alert(`Error: ${error.message}`);
                }
            }
            
            // Export functions
            async function loadExportFormats() {
                try {
                    const response = await fetch('/api/export/formats');
                    const data = await response.json();
                    
                    let formatsHtml = '';
                    Object.entries(data.formats).forEach(([format, info]) => {
                        formatsHtml += `
                            <div class="alert alert-success">
                                <strong>${format.toUpperCase()}:</strong> ${info.description}
                                <br><small>Best for: ${info.best_for}</small>
                            </div>
                        `;
                    });
                    
                    document.getElementById('export-formats').innerHTML = formatsHtml;
                } catch (error) {
                    document.getElementById('export-formats').innerHTML = `
                        <div class="alert alert-error">Error: ${error.message}</div>
                    `;
                }
            }
            
            async function exportData() {
                const topicId = document.getElementById('export-topic').value;
                const format = document.getElementById('export-format').value;
                
                if (!topicId) {
                    alert('Please select a topic');
                    return;
                }
                
                const url = `/api/export/topics/${topicId}/${format}?start_date=2024-01-01&end_date=2024-01-31`;
                window.open(url, '_blank');
                
                // Add to export history
                const historyDiv = document.getElementById('export-history');
                const exportItem = document.createElement('div');
                exportItem.className = 'alert alert-success';
                exportItem.innerHTML = `
                    <strong>Exported:</strong> ${topicId} as ${format.toUpperCase()}
                    <br><small>${new Date().toLocaleString()}</small>
                `;
                historyDiv.insertBefore(exportItem, historyDiv.firstChild);
            }
            
            async function exportSources() {
                window.open('/api/export/sources/registry?format=json', '_blank');
            }
            
            // WebSocket functions
            function connectWebSocket() {
                if (websocket) {
                    websocket.close();
                }
                
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws/dashboard`;
                
                websocket = new WebSocket(wsUrl);
                
                websocket.onopen = function(event) {
                    updateWSStatus('connected', 'Connected');
                    addWSMessage('Connected to WebSocket', 'success');
                };
                
                websocket.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    addWSMessage(`Received: ${data.type}`, 'info');
                    
                    // Update live updates
                    const liveDiv = document.getElementById('live-updates');
                    liveDiv.innerHTML = `
                        <div class="alert alert-success">
                            <strong>Last Update:</strong> ${data.type}
                            <br><small>${new Date().toLocaleString()}</small>
                        </div>
                        <pre>${JSON.stringify(data, null, 2)}</pre>
                    `;
                };
                
                websocket.onclose = function(event) {
                    updateWSStatus('offline', 'Disconnected');
                    addWSMessage('WebSocket disconnected', 'warning');
                };
                
                websocket.onerror = function(error) {
                    updateWSStatus('warning', 'Error');
                    addWSMessage(`WebSocket error: ${error}`, 'error');
                };
            }
            
            function disconnectWebSocket() {
                if (websocket) {
                    websocket.close();
                    websocket = null;
                }
            }
            
            function sendPing() {
                if (websocket && websocket.readyState === WebSocket.OPEN) {
                    websocket.send(JSON.stringify({
                        type: 'ping',
                        timestamp: new Date().toISOString()
                    }));
                    addWSMessage('Sent ping', 'info');
                } else {
                    alert('WebSocket not connected');
                }
            }
            
            function updateWSStatus(status, text) {
                const indicator = document.getElementById('ws-status');
                const statusText = document.getElementById('ws-status-text');
                
                indicator.className = `status-indicator status-${status}`;
                statusText.textContent = text;
            }
            
            function addWSMessage(message, type) {
                const messagesDiv = document.getElementById('ws-messages');
                const messageDiv = document.createElement('div');
                messageDiv.className = `alert alert-${type}`;
                messageDiv.innerHTML = `
                    <strong>${new Date().toLocaleTimeString()}:</strong> ${message}
                `;
                messagesDiv.insertBefore(messageDiv, messagesDiv.firstChild);
                
                // Keep only last 10 messages
                while (messagesDiv.children.length > 10) {
                    messagesDiv.removeChild(messagesDiv.lastChild);
                }
            }
            
            // Initialize on page load
            document.addEventListener('DOMContentLoaded', function() {
                loadOverviewData();
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
