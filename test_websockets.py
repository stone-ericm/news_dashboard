#!/usr/bin/env python3
"""Test script for WebSocket functionality."""

import asyncio
import json
import time
import websockets
import requests
from datetime import datetime

async def test_websocket_connection(uri, connection_type="dashboard"):
    """Test a WebSocket connection."""
    print(f"🔌 Testing WebSocket: {uri}")
    
    try:
        async with websockets.connect(uri) as websocket:
            print(f"✅ Connected to {connection_type} WebSocket")
            
            # Send a ping message
            ping_message = {
                "type": "ping",
                "timestamp": datetime.utcnow().isoformat()
            }
            await websocket.send(json.dumps(ping_message))
            print(f"📤 Sent ping message")
            
            # Listen for messages for 30 seconds
            messages_received = 0
            start_time = time.time()
            
            while time.time() - start_time < 30:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    messages_received += 1
                    
                    print(f"📥 Received {data.get('type', 'unknown')} message")
                    
                    # Handle specific message types
                    if data.get('type') == 'connection_established':
                        print(f"   Connection type: {data.get('connection_type')}")
                    elif data.get('type') == 'pong':
                        print(f"   Pong received - latency OK")
                    elif data.get('type') == 'aviation_update':
                        total_aircraft = data.get('total_aircraft', 0)
                        regions = len(data.get('regions', {}))
                        print(f"   Aviation: {total_aircraft} aircraft across {regions} regions")
                    elif data.get('type') == 'trend_update':
                        trends = len(data.get('trends', []))
                        print(f"   Trends: {trends} topics updated")
                    elif data.get('type') == 'new_alerts':
                        alerts = len(data.get('alerts', []))
                        print(f"   Alerts: {alerts} new alerts")
                    elif data.get('type') == 'system_heartbeat':
                        connections = data.get('connections', {}).get('total_connections', 0)
                        print(f"   Heartbeat: {connections} total connections")
                    
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    print("❌ Connection closed unexpectedly")
                    break
            
            print(f"✅ Test completed - received {messages_received} messages in 30 seconds")
            return messages_received
            
    except Exception as e:
        print(f"❌ WebSocket test failed: {e}")
        return 0

async def test_all_websockets():
    """Test all WebSocket endpoints."""
    print("🚀 Testing WebSocket Functionality")
    print("=" * 60)
    
    base_url = "ws://localhost:8000"
    
    # Test endpoints
    endpoints = [
        ("dashboard", f"{base_url}/ws/dashboard"),
        ("aviation", f"{base_url}/ws/aviation"),
        ("alerts", f"{base_url}/ws/alerts"),
        ("trends", f"{base_url}/ws/trends"),
        ("analytics", f"{base_url}/ws/analytics")
    ]
    
    results = {}
    
    for name, uri in endpoints:
        print(f"\n📡 Testing {name.upper()} WebSocket")
        print("-" * 40)
        
        messages = await test_websocket_connection(uri, name)
        results[name] = messages
        
        # Wait between tests
        await asyncio.sleep(2)
    
    # Test WebSocket stats endpoint
    print(f"\n📊 Testing WebSocket Stats API")
    print("-" * 40)
    
    try:
        response = requests.get("http://localhost:8000/ws/stats")
        if response.status_code == 200:
            stats = response.json()
            print(f"✅ WebSocket stats retrieved")
            
            ws_stats = stats.get('websocket_stats', {})
            print(f"   Total connections: {ws_stats.get('total_connections', 0)}")
            print(f"   Messages sent: {ws_stats.get('total_messages_sent', 0)}")
            
            connections_by_type = ws_stats.get('connections_by_type', {})
            for conn_type, count in connections_by_type.items():
                print(f"   {conn_type}: {count} connections")
            
            endpoints_available = stats.get('available_endpoints', [])
            print(f"   Available endpoints: {len(endpoints_available)}")
            
        else:
            print(f"❌ WebSocket stats failed: {response.status_code}")
    except Exception as e:
        print(f"❌ WebSocket stats error: {e}")
    
    # Summary
    print(f"\n🎉 WebSocket Test Summary")
    print("=" * 60)
    
    total_messages = sum(results.values())
    successful_connections = len([r for r in results.values() if r > 0])
    
    print(f"✅ Successful connections: {successful_connections}/{len(endpoints)}")
    print(f"📥 Total messages received: {total_messages}")
    print(f"⚡ Average messages per connection: {total_messages/len(endpoints):.1f}")
    
    for name, count in results.items():
        status = "✅" if count > 0 else "❌"
        print(f"   {status} {name}: {count} messages")
    
    if successful_connections == len(endpoints):
        print(f"\n🎊 All WebSocket endpoints are working perfectly!")
    else:
        print(f"\n⚠️ Some WebSocket endpoints may need attention")
    
    print(f"\n🌐 Real-time Dashboard: http://localhost:8000/realtime")
    print(f"📖 API Documentation: http://localhost:8000/docs")

def test_http_endpoints():
    """Test HTTP endpoints before WebSocket tests."""
    print("🔍 Pre-flight HTTP Endpoint Check")
    print("-" * 40)
    
    endpoints = [
        ("Root", "http://localhost:8000/"),
        ("Health", "http://localhost:8000/api/sources"),
        ("WebSocket Stats", "http://localhost:8000/ws/stats")
    ]
    
    all_working = True
    
    for name, url in endpoints:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✅ {name}: OK")
            else:
                print(f"❌ {name}: HTTP {response.status_code}")
                all_working = False
        except Exception as e:
            print(f"❌ {name}: {e}")
            all_working = False
    
    if not all_working:
        print(f"\n⚠️ Some HTTP endpoints are not responding. WebSocket tests may fail.")
        return False
    
    print(f"✅ All HTTP endpoints are working")
    return True

async def main():
    """Main test function."""
    print("🧪 News Dashboard WebSocket Test Suite")
    print("=" * 60)
    
    # Test HTTP endpoints first
    if not test_http_endpoints():
        print("❌ Aborting WebSocket tests due to HTTP endpoint failures")
        return
    
    print("\n" + "=" * 60)
    
    # Wait for server to be fully ready
    print("⏳ Waiting for server to be fully ready...")
    await asyncio.sleep(5)
    
    # Test WebSocket functionality
    await test_all_websockets()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
