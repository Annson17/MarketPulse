"""
MarketPulse API Module

FastAPI application with WebSocket streaming and Redis integration.
"""

from api.manager import ConnectionManager, manager
from api.main import app

__all__ = ["app", "manager", "ConnectionManager"]
