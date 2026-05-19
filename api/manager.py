"""
MarketPulse - WebSocket Connection Manager
===========================================

Purpose: Manage all active WebSocket connections grouped by ticker.

Think of this as a "broadcast controller":
- Tracks which clients are listening to which stocks
- Sends price updates to all interested clients simultaneously
- Handles client disconnections gracefully

This is the pattern used by major platforms:
- Bloomberg terminal: Manages connections for 100K+ traders
- Robinhood: Real-time price feed to millions of clients
- Discord: Manages channels with thousands of listeners each
"""

import json
import logging
from typing import Dict, List, Set
from fastapi import WebSocket

logger = logging.getLogger(__name__)

# ============================================================================
# CONNECTION MANAGER
# ============================================================================

class ConnectionManager:
    """
    Manages WebSocket connections grouped by stock ticker.
    
    Structure:
    {
        "TCS": [ws1, ws2, ws3],      # 3 clients listening to TCS
        "INFY": [ws4, ws5],          # 2 clients listening to INFY
        "RELIANCE": [ws6]            # 1 client listening to RELIANCE
    }
    
    Key Features:
    1. Thread-safe (no explicit locking needed - event loop is single-threaded)
    2. Fault tolerant (one disconnected client doesn't break others)
    3. Broadcast efficient (sends to all clients in one loop)
    
    Interview talking point:
    "This is a pub/sub consumer. The connection manager is the 'broker' between
    Redis Pub/Sub and WebSocket clients. It decouples the producer (market_maker)
    from consumers (browsers)."
    """
    
    def __init__(self):
        """Initialize empty connection dictionary."""
        # Maps ticker -> list of connected WebSockets
        self.active_connections: Dict[str, List[WebSocket]] = {}
        
        # Metrics for monitoring
        self.total_connections_ever = 0
        self.peak_connections = 0
    
    async def connect(self, ticker: str, websocket: WebSocket) -> None:
        """
        Register a new WebSocket connection for a ticker.
        
        Args:
            ticker: Stock ticker (e.g., "TCS")
            websocket: The WebSocket connection object
            
        Interview explanation:
        "When a browser connects via WebSocket to /ws/TCS, we:
        1. Accept the connection
        2. Add it to the TCS list
        3. Log the connection
        4. Update metrics"
        """
        
        # Accept the WebSocket connection
        await websocket.accept()
        
        # Add to connections dict
        if ticker not in self.active_connections:
            self.active_connections[ticker] = []
        
        self.active_connections[ticker].append(websocket)
        
        # Update metrics
        self.total_connections_ever += 1
        current_total = self._get_total_connections()
        if current_total > self.peak_connections:
            self.peak_connections = current_total
        
        logger.info(
            f"Client connected to {ticker} | "
            f"Ticker: {len(self.active_connections.get(ticker, []))} clients | "
            f"Total: {current_total} clients (peak: {self.peak_connections})"
        )
    
    async def disconnect(self, ticker: str, websocket: WebSocket) -> None:
        """
        Unregister a WebSocket connection.
        
        Args:
            ticker: Stock ticker
            websocket: The WebSocket to remove
            
        Interview explanation:
        "When a browser tab closes or network fails, we:
        1. Remove it from the connections list
        2. Clean up empty ticker entries
        3. Log the disconnection"
        """
        
        if ticker in self.active_connections:
            try:
                self.active_connections[ticker].remove(websocket)
                
                # Clean up empty lists
                if not self.active_connections[ticker]:
                    del self.active_connections[ticker]
                
                current_total = self._get_total_connections()
                logger.info(
                    f"Client disconnected from {ticker} | "
                    f"Ticker: {len(self.active_connections.get(ticker, []))} clients | "
                    f"Total: {current_total} clients"
                )
            except ValueError:
                logger.warning(f"Tried to disconnect {ticker} but socket not found")
    
    async def broadcast(self, ticker: str, message: dict) -> None:
        """
        Send a message to all clients listening to a ticker.
        
        This is the CORE of the system: every price update from Redis
        gets broadcasted to all connected browsers simultaneously.
        
        Args:
            ticker: Stock ticker
            message: Dict (will be JSON-serialized)
            
        Interview explanation:
        "This is the broadcast pattern:
        1. Receive message from Redis Pub/Sub
        2. Send to ALL clients interested in this ticker
        3. Handle disconnects gracefully
        4. This is how real-time systems work"
        
        Important: We iterate over a COPY of the list because
        connections might disconnect during iteration.
        """
        
        if ticker not in self.active_connections:
            # No one listening - this is fine
            return
        
        # Make a copy in case connections disconnect during iteration
        connections_copy = self.active_connections[ticker].copy()
        
        disconnected = []
        
        for websocket in connections_copy:
            try:
                # Send JSON-serialized message
                await websocket.send_json(message)
                
            except Exception as e:
                # Client disconnected or error occurred
                logger.warning(
                    f"Failed to send to {ticker} client: {e}"
                )
                disconnected.append(websocket)
        
        # Clean up disconnected clients
        for ws in disconnected:
            await self.disconnect(ticker, ws)
    
    async def broadcast_to_all(self, message: dict) -> None:
        """
        Broadcast a message to all connected clients (all tickers).
        
        Use case: System announcements, market status changes, etc.
        
        Args:
            message: Dict to broadcast
        """
        
        # Get all tickers currently active
        tickers = list(self.active_connections.keys())
        
        for ticker in tickers:
            await self.broadcast(ticker, message)
    
    def get_connection_stats(self) -> dict:
        """
        Return connection statistics (useful for monitoring/metrics).
        
        Interview explanation:
        "In production, you'd integrate this with:
        - Prometheus (metrics collection)
        - Grafana (visualization)
        - PagerDuty (alerting if connections drop)"
        
        Returns:
            Dict with connection statistics
        """
        
        stats = {
            "total_connected": self._get_total_connections(),
            "peak_connections": self.peak_connections,
            "total_connections_ever": self.total_connections_ever,
            "tickers_active": len(self.active_connections),
            "connections_by_ticker": {
                ticker: len(clients)
                for ticker, clients in self.active_connections.items()
            }
        }
        
        return stats
    
    def _get_total_connections(self) -> int:
        """Count total active connections across all tickers."""
        return sum(
            len(clients)
            for clients in self.active_connections.values()
        )


# ============================================================================
# MODULE-LEVEL INSTANCE
# ============================================================================

# Create a singleton instance (used by FastAPI routes)
# This ensures all routes access the same connection pool
manager = ConnectionManager()
