#!/usr/bin/env python
"""Startup script for the News Dashboard web interface."""

import sys
import webbrowser
from pathlib import Path
from threading import Timer

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

import uvicorn
from src.api.main import app


def open_browser():
    """Open the enhanced dashboard in the default browser."""
    webbrowser.open("http://localhost:8000/enhanced")


def main():
    """Run the dashboard server."""
    print("🚀 Starting News Dashboard...")
    print("📊 Available dashboards:")
    print("   • Enhanced: http://localhost:8000/enhanced (recommended)")
    print("   • Static:   http://localhost:8000/dashboard")
    print("   • Real-time: http://localhost:8000/realtime")
    print("📖 API documentation: http://localhost:8000/docs")
    print("🔄 Press Ctrl+C to stop the server")
    print()
    
    # Open enhanced dashboard after a short delay
    Timer(2.0, open_browser).start()
    
    # Start the server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True
    )


if __name__ == "__main__":
    main()
