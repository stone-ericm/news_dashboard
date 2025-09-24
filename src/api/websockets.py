"""WebSocket endpoints for real-time updates."""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set
import pandas as pd
import numpy as np
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.routing import APIRouter
import logging

from src.models.taxonomy import get_default_taxonomy
from src.ingestion.opensky import OpenSkyConnector
from src.processing.seasonality import SeasonalityProcessor
from src.cache.manager import get_cache_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()


class ConnectionManager:
    """Manages WebSocket connections and broadcasting."""
    
    def __init__(self):
        # Active connections by type
        self.connections: Dict[str, Set[WebSocket]] = {
            'dashboard': set(),
            'alerts': set(),
            'aviation': set(),
            'trends': set(),
            'analytics': set()
        }
        
        # Connection metadata
        self.connection_info: Dict[WebSocket, Dict] = {}
        
        # Background tasks
        self.background_tasks: Set[asyncio.Task] = set()
        self.running = False
    
    async def connect(self, websocket: WebSocket, connection_type: str = 'dashboard'):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        
        if connection_type not in self.connections:
            self.connections[connection_type] = set()
        
        self.connections[connection_type].add(websocket)
        self.connection_info[websocket] = {
            'type': connection_type,
            'connected_at': datetime.utcnow(),
            'last_ping': datetime.utcnow(),
            'message_count': 0
        }
        
        logger.info(f"New {connection_type} connection. Total: {len(self.connections[connection_type])}")
        
        # Send welcome message
        await self.send_personal_message(websocket, {
            'type': 'connection_established',
            'connection_type': connection_type,
            'timestamp': datetime.utcnow().isoformat(),
            'server_time': time.time()
        })
        
        # Start background tasks if not running
        if not self.running:
            await self.start_background_tasks()
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        if websocket in self.connection_info:
            connection_type = self.connection_info[websocket]['type']
            self.connections[connection_type].discard(websocket)
            del self.connection_info[websocket]
            logger.info(f"Disconnected {connection_type} connection. Remaining: {len(self.connections[connection_type])}")
    
    async def send_personal_message(self, websocket: WebSocket, message: dict):
        """Send message to a specific connection."""
        try:
            await websocket.send_text(json.dumps(message, default=str))
            if websocket in self.connection_info:
                self.connection_info[websocket]['message_count'] += 1
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
            self.disconnect(websocket)
    
    async def broadcast_to_type(self, connection_type: str, message: dict):
        """Broadcast message to all connections of a specific type."""
        if connection_type not in self.connections:
            return
        
        disconnected = set()
        for websocket in self.connections[connection_type].copy():
            try:
                await websocket.send_text(json.dumps(message, default=str))
                if websocket in self.connection_info:
                    self.connection_info[websocket]['message_count'] += 1
            except Exception as e:
                logger.error(f"Error broadcasting to {connection_type}: {e}")
                disconnected.add(websocket)
        
        # Clean up disconnected websockets
        for websocket in disconnected:
            self.disconnect(websocket)
    
    async def broadcast_to_all(self, message: dict):
        """Broadcast message to all connections."""
        for connection_type in self.connections:
            await self.broadcast_to_type(connection_type, message)
    
    def get_connection_stats(self) -> Dict:
        """Get connection statistics."""
        stats = {
            'total_connections': sum(len(conns) for conns in self.connections.values()),
            'connections_by_type': {
                conn_type: len(conns) for conn_type, conns in self.connections.items()
            },
            'uptime_seconds': 0,
            'total_messages_sent': sum(
                info['message_count'] for info in self.connection_info.values()
            )
        }
        
        if self.connection_info:
            oldest_connection = min(
                info['connected_at'] for info in self.connection_info.values()
            )
            stats['uptime_seconds'] = (datetime.utcnow() - oldest_connection).total_seconds()
        
        return stats
    
    async def start_background_tasks(self):
        """Start background tasks for real-time updates."""
        if self.running:
            return
        
        self.running = True
        
        # Create background tasks
        tasks = [
            asyncio.create_task(self.aviation_updates()),
            asyncio.create_task(self.trend_updates()),
            asyncio.create_task(self.alert_monitoring()),
            asyncio.create_task(self.system_heartbeat()),
            asyncio.create_task(self.cache_monitoring())
        ]
        
        self.background_tasks.update(tasks)
        logger.info("Started background WebSocket tasks")
    
    async def aviation_updates(self):
        """Stream live aviation data updates."""
        while self.running:
            try:
                if self.connections['aviation'] or self.connections['dashboard']:
                    # Get live aviation data
                    connector = OpenSkyConnector()
                    
                    # Sample regions for demo
                    regions = {
                        "north_america": (25.0, 50.0, -130.0, -60.0),
                        "europe": (35.0, 70.0, -10.0, 40.0),
                    }
                    
                    aviation_data = {
                        'type': 'aviation_update',
                        'timestamp': datetime.utcnow().isoformat(),
                        'regions': {}
                    }
                    
                    total_aircraft = 0
                    for region_name, bbox in regions.items():
                        try:
                            # Simulate aviation data (in production, use real API)
                            aircraft_count = np.random.poisson(150)
                            in_air = int(aircraft_count * 0.7)
                            
                            aviation_data['regions'][region_name] = {
                                'total_aircraft': aircraft_count,
                                'in_air': in_air,
                                'on_ground': aircraft_count - in_air,
                                'avg_altitude': float(np.random.uniform(8000, 12000)),
                                'avg_velocity': float(np.random.uniform(200, 500))
                            }
                            total_aircraft += aircraft_count
                            
                        except Exception as e:
                            logger.error(f"Error getting aviation data for {region_name}: {e}")
                    
                    aviation_data['total_aircraft'] = total_aircraft
                    
                    # Broadcast to aviation and dashboard connections
                    await self.broadcast_to_type('aviation', aviation_data)
                    await self.broadcast_to_type('dashboard', aviation_data)
                
                await asyncio.sleep(10)  # Update every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in aviation updates: {e}")
                await asyncio.sleep(30)
    
    async def trend_updates(self):
        """Stream trend analysis updates."""
        while self.running:
            try:
                if self.connections['trends'] or self.connections['dashboard']:
                    taxonomy = get_default_taxonomy()
                    processor = SeasonalityProcessor(seasonal_period=7)
                    
                    # Get sample topics
                    topics = list(taxonomy.topics.values())[:5]
                    
                    trend_data = {
                        'type': 'trend_update',
                        'timestamp': datetime.utcnow().isoformat(),
                        'trends': []
                    }
                    
                    for topic in topics:
                        # Generate sample trend data
                        dates = pd.date_range(
                            start=datetime.now() - timedelta(days=30),
                            end=datetime.now(),
                            freq='D'
                        )
                        
                        # Simulate different patterns
                        if 'employment' in topic.topic_id:
                            values = 100 - np.linspace(0, 15, len(dates)) + np.random.normal(0, 3, len(dates))
                        elif 'covid' in topic.topic_id:
                            values = 50 + 25 * np.sin(2 * np.pi * np.arange(len(dates)) / 30) + np.random.normal(0, 5, len(dates))
                        else:
                            values = 75 + np.cumsum(np.random.normal(0, 0.5, len(dates)))
                        
                        values_series = pd.Series(values, index=dates)
                        z_scores = processor.compute_zscore(values_series)
                        
                        # Current trend info
                        current_value = float(values[-1])
                        current_z_score = float(z_scores.iloc[-1]) if not pd.isna(z_scores.iloc[-1]) else 0
                        
                        # Trend direction
                        recent_slope = np.polyfit(range(7), values[-7:], 1)[0]
                        if recent_slope > 0.5:
                            trend_direction = 'increasing'
                        elif recent_slope < -0.5:
                            trend_direction = 'decreasing'
                        else:
                            trend_direction = 'stable'
                        
                        trend_data['trends'].append({
                            'topic_id': topic.topic_id,
                            'topic_name': topic.name,
                            'current_value': current_value,
                            'z_score': current_z_score,
                            'trend_direction': trend_direction,
                            'is_anomaly': abs(current_z_score) > 2.0,
                            'color': topic.color
                        })
                    
                    # Broadcast trend updates
                    await self.broadcast_to_type('trends', trend_data)
                    await self.broadcast_to_type('dashboard', trend_data)
                
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in trend updates: {e}")
                await asyncio.sleep(60)
    
    async def alert_monitoring(self):
        """Monitor and broadcast alerts."""
        while self.running:
            try:
                if self.connections['alerts'] or self.connections['dashboard']:
                    # Generate sample alerts
                    alerts = []
                    
                    # Simulate some alerts based on current time
                    current_hour = datetime.now().hour
                    if current_hour % 3 == 0:  # Every 3 hours
                        alert_types = ['anomaly', 'threshold', 'trend_change']
                        severities = ['low', 'medium', 'high', 'critical']
                        
                        for i in range(np.random.poisson(2)):  # 0-4 alerts typically
                            alerts.append({
                                'id': f"alert_{int(time.time())}_{i}",
                                'type': np.random.choice(alert_types),
                                'severity': np.random.choice(severities, p=[0.4, 0.3, 0.2, 0.1]),
                                'topic_id': f"topic_{np.random.randint(1, 10)}",
                                'message': f"Anomalous activity detected at {datetime.now().strftime('%H:%M')}",
                                'z_score': float(np.random.normal(0, 2)),
                                'timestamp': datetime.utcnow().isoformat(),
                                'is_active': True
                            })
                    
                    if alerts:
                        alert_data = {
                            'type': 'new_alerts',
                            'timestamp': datetime.utcnow().isoformat(),
                            'alerts': alerts,
                            'total_new': len(alerts)
                        }
                        
                        # Broadcast alerts
                        await self.broadcast_to_type('alerts', alert_data)
                        await self.broadcast_to_type('dashboard', alert_data)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in alert monitoring: {e}")
                await asyncio.sleep(120)
    
    async def system_heartbeat(self):
        """Send system status heartbeat."""
        while self.running:
            try:
                cache = get_cache_manager()
                cache_stats = cache.get_stats()
                
                heartbeat_data = {
                    'type': 'system_heartbeat',
                    'timestamp': datetime.utcnow().isoformat(),
                    'server_time': time.time(),
                    'system_status': 'operational',
                    'connections': self.get_connection_stats(),
                    'cache': {
                        'entries': cache_stats.get('total_entries', 0),
                        'size_mb': cache_stats.get('total_size_mb', 0),
                        'utilization_pct': cache_stats.get('utilization_pct', 0)
                    },
                    'uptime': time.time()  # Simplified uptime
                }
                
                # Broadcast heartbeat to all connections
                await self.broadcast_to_all(heartbeat_data)
                
                await asyncio.sleep(30)  # Heartbeat every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in system heartbeat: {e}")
                await asyncio.sleep(60)
    
    async def cache_monitoring(self):
        """Monitor cache performance and broadcast updates."""
        while self.running:
            try:
                if self.connections['analytics'] or self.connections['dashboard']:
                    cache = get_cache_manager()
                    stats = cache.get_stats()
                    
                    cache_data = {
                        'type': 'cache_update',
                        'timestamp': datetime.utcnow().isoformat(),
                        'stats': stats,
                        'performance': {
                            'hit_rate': 0.85,  # Simulated
                            'avg_response_time_ms': 45,  # Simulated
                            'operations_per_second': np.random.uniform(10, 50)
                        }
                    }
                    
                    await self.broadcast_to_type('analytics', cache_data)
                    
                await asyncio.sleep(45)  # Update every 45 seconds
                
            except Exception as e:
                logger.error(f"Error in cache monitoring: {e}")
                await asyncio.sleep(90)
    
    async def stop_background_tasks(self):
        """Stop all background tasks."""
        self.running = False
        
        for task in self.background_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        self.background_tasks.clear()
        logger.info("Stopped background WebSocket tasks")


# Global connection manager
manager = ConnectionManager()


@router.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    """WebSocket endpoint for dashboard real-time updates."""
    await websocket.accept()
    await manager.connect(websocket, 'dashboard')
    try:
        while True:
            # Listen for client messages
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                
                # Handle different message types
                if message.get('type') == 'ping':
                    await manager.send_personal_message(websocket, {
                        'type': 'pong',
                        'timestamp': datetime.utcnow().isoformat(),
                        'server_time': time.time()
                    })
                elif message.get('type') == 'subscribe':
                    # Handle subscription requests
                    topics = message.get('topics', [])
                    await manager.send_personal_message(websocket, {
                        'type': 'subscription_confirmed',
                        'topics': topics,
                        'timestamp': datetime.utcnow().isoformat()
                    })
                elif message.get('type') == 'request_update':
                    # Send immediate update
                    await manager.send_personal_message(websocket, {
                        'type': 'immediate_update',
                        'data': 'Update requested',
                        'timestamp': datetime.utcnow().isoformat()
                    })
                
            except json.JSONDecodeError:
                await manager.send_personal_message(websocket, {
                    'type': 'error',
                    'message': 'Invalid JSON format',
                    'timestamp': datetime.utcnow().isoformat()
                })
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@router.websocket("/ws/aviation")
async def websocket_aviation(websocket: WebSocket):
    """WebSocket endpoint for aviation data updates."""
    await websocket.accept()
    await manager.connect(websocket, 'aviation')
    try:
        while True:
            data = await websocket.receive_text()
            # Handle aviation-specific messages
            try:
                message = json.loads(data)
                if message.get('type') == 'request_regions':
                    regions = message.get('regions', ['north_america', 'europe'])
                    await manager.send_personal_message(websocket, {
                        'type': 'regions_configured',
                        'regions': regions,
                        'timestamp': datetime.utcnow().isoformat()
                    })
            except json.JSONDecodeError:
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@router.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """WebSocket endpoint for real-time alerts."""
    await websocket.accept()
    await manager.connect(websocket, 'alerts')
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get('type') == 'set_alert_threshold':
                    threshold = message.get('threshold', 2.0)
                    await manager.send_personal_message(websocket, {
                        'type': 'threshold_set',
                        'threshold': threshold,
                        'timestamp': datetime.utcnow().isoformat()
                    })
            except json.JSONDecodeError:
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@router.websocket("/ws/trends")
async def websocket_trends(websocket: WebSocket):
    """WebSocket endpoint for trend analysis updates."""
    await websocket.accept()
    await manager.connect(websocket, 'trends')
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get('type') == 'configure_trends':
                    topics = message.get('topics', [])
                    await manager.send_personal_message(websocket, {
                        'type': 'trends_configured',
                        'topics': topics,
                        'timestamp': datetime.utcnow().isoformat()
                    })
            except json.JSONDecodeError:
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@router.websocket("/ws/analytics")
async def websocket_analytics(websocket: WebSocket):
    """WebSocket endpoint for analytics and performance data."""
    await websocket.accept()
    await manager.connect(websocket, 'analytics')
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get('type') == 'request_stats':
                    stats = manager.get_connection_stats()
                    await manager.send_personal_message(websocket, {
                        'type': 'connection_stats',
                        'stats': stats,
                        'timestamp': datetime.utcnow().isoformat()
                    })
            except json.JSONDecodeError:
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# HTTP endpoint to get WebSocket connection stats
@router.get("/ws/stats")
async def get_websocket_stats():
    """Get WebSocket connection statistics."""
    return {
        'websocket_stats': manager.get_connection_stats(),
        'available_endpoints': [
            '/ws/dashboard',
            '/ws/aviation', 
            '/ws/alerts',
            '/ws/trends',
            '/ws/analytics'
        ],
        'message_types': {
            'dashboard': ['ping', 'subscribe', 'request_update'],
            'aviation': ['request_regions'],
            'alerts': ['set_alert_threshold'],
            'trends': ['configure_trends'],
            'analytics': ['request_stats']
        }
    }
