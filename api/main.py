"""
MarketPulse - FastAPI Core Application (Phase 3)
=================================================

Purpose: Central hub that connects market data (Redis) to clients (WebSocket).

This is the "brain" of the system:
1. Listens to Redis Pub/Sub in background (receives market data)
2. Manages WebSocket connections (serves clients)
3. Handles delta replay (new clients catch up on historical data)
4. Provides REST API for status/metrics

Architecture:
    market_maker → Redis Pub/Sub → FastAPI Listener → WebSocket → Browser
    
    + Redis Stream (for historical data when client reconnects)
"""

import asyncio
import json
import logging
import os
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
import redis.asyncio as redis
from sqlalchemy import desc

# Database imports
try:
    from db_init import MarketTick, get_session
    DB_AVAILABLE = True
except Exception as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Database import failed: {e}")
    DB_AVAILABLE = False

from api.manager import manager

# ============================================================================
# CONFIGURATION
# ============================================================================

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_PASSWORD = "redis_password"
# Real Indian stocks across different sectors
TICKERS = ["TCS", "SBIN", "SUNPHARMA", "MARUTI"]

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# GLOBAL STATE
# ============================================================================

# Redis client instance (shared across app)
redis_client: Optional[redis.Redis] = None

# Background listener task
listener_task: Optional[asyncio.Task] = None

# ============================================================================
# BACKGROUND REDIS LISTENER
# ============================================================================

async def redis_listener():
    """
    Background coroutine that listens to Redis Pub/Sub channels.
    
    This is the BRIDGE between the market data producer and WebSocket consumers:
    1. Redis publishes: market:live:TCS with {"price": 3520.45, ...}
    2. This listener receives it
    3. Broadcasts to all browsers listening to TCS
    
    Using PSUBSCRIBE to listen to ALL market:live:* channels at once.
    (vs SUBSCRIBE which would need to subscribe to each separately)
    
    Interview explanation:
    "This is the real-time data pipeline:
    - market_maker generates ticks (producer)
    - Redis stores them temporarily (broker)
    - This listener consumes them (consumer)
    - We broadcast to all connected clients (distribution)
    
    This is exactly how Bloomberg, Robinhood, and other trading platforms work."
    """
    
    logger.info("Starting Redis listener background task...")
    
    try:
        # Create a pubsub connection
        pubsub = redis_client.pubsub()
        
        # Subscribe to all market:live:* channels
        await pubsub.psubscribe("market:live:*")
        logger.info("✓ Subscribed to Redis Pub/Sub: market:live:*")
        
        # Listen forever
        async for message in pubsub.listen():
            
            # Skip subscription confirmation messages
            if message["type"] == "psubscribe":
                continue
            
            if message["type"] == "pmessage":
                # Extract channel name and data
                channel = message["channel"]  # e.g., "market:live:TCS"
                data = message["data"]         # e.g., '{"price": 3520.45, ...}'
                
                try:
                    # Parse JSON
                    tick = json.loads(data)
                    ticker = tick["ticker"]
                    
                    logger.debug(
                        f"[LISTENER] Received {ticker} from Redis: "
                        f"${tick['price']} (seq_id: {tick['sequence_id']})"
                    )
                    
                    # Broadcast to all clients listening to this ticker
                    await manager.broadcast(ticker, tick)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse Redis message: {e}")
    
    except asyncio.CancelledError:
        logger.info("Redis listener task cancelled")
    except Exception as e:
        logger.error(f"Fatal error in Redis listener: {e}")
    finally:
        await pubsub.close()
        logger.info("Redis listener stopped")

# ============================================================================
# APP LIFESPAN
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage app startup and shutdown.
    
    Startup: Initialize Redis connection and start background listener
    Shutdown: Close Redis and cancel background task
    """
    
    global redis_client, listener_task
    
    # ===== STARTUP =====
    logger.info("FastAPI Starting up...")
    
    try:
        # Connect to Redis
        redis_client = await redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_keepalive=True
        )
        
        # Test connection
        ping = await redis_client.ping()
        logger.info(f"✓ Redis connected: {ping}")
        
    except Exception as e:
        logger.error(f"✗ Failed to connect to Redis: {e}")
        logger.error("Make sure: docker-compose up -d")
        raise
    
    # Start background listener
    listener_task = asyncio.create_task(redis_listener())
    logger.info("✓ Background listener started")
    
    yield  # App runs here
    
    # ===== SHUTDOWN =====
    logger.info("FastAPI Shutting down...")
    
    if listener_task:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass
    
    if redis_client:
        await redis_client.close()
    
    logger.info("✓ Shutdown complete")

# ============================================================================
# CREATE FASTAPI APP
# ============================================================================

app = FastAPI(
    title="MarketPulse API",
    description="Real-time market data streaming",
    lifespan=lifespan
)

# ============================================================================
# REST ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check():
    """
    Health check endpoint (for Nginx and monitoring systems).
    
    Returns 200 if Redis is connected, 500 otherwise.
    """
    
    if redis_client is None:
        return JSONResponse(
            status_code=500,
            content={"status": "unhealthy", "reason": "Redis not connected"}
        )
    
    try:
        await redis_client.ping()
        return {"status": "healthy", "service": "marketpulse-api"}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "unhealthy", "error": str(e)}
        )

@app.get("/metrics")
async def get_metrics():
    """
    Return connection statistics and metrics.
    
    Interview explanation:
    "In production, this endpoint would be scraped by Prometheus.
    You could graph:
    - Total connected clients
    - Peak concurrent connections
    - Connections per ticker
    - Client churn rate"
    """
    
    stats = manager.get_connection_stats()
    return {
        "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
        "stats": stats
    }

@app.get("/tickers")
async def get_tickers():
    """List available stock tickers."""
    return {
        "tickers": TICKERS,
        "count": len(TICKERS)
    }

@app.get("/stats")
async def get_stats():
    """
    Dashboard statistics from PostgreSQL
    """

    if not DB_AVAILABLE:

        return {

            "historical_points": 0,

            "active_tickers": 0,

            "ticker_breakdown": {}

        }

    session = None

    try:

        session = get_session()

        total_history = session.query(
            MarketTick
        ).count()

        ticker_breakdown = {}

        active = 0

        for ticker in TICKERS:

            count = session.query(
                MarketTick
            ).filter(
                MarketTick.ticker
                ==
                ticker
            ).count()

            ticker_breakdown[
                ticker
            ] = count

            if count > 0:
                active += 1

        return {

            "historical_points":

            total_history,

            "active_tickers":

            active,

            "ticker_breakdown":

            ticker_breakdown

        }

    except Exception as e:

        logger.error(
            f"Stats error: {e}"
        )

        return {

            "historical_points": 0,

            "active_tickers": 0,

            "ticker_breakdown": {}

        }

    finally:

        if session:

            session.close()

@app.get("/dbtest")
async def dbtest():

    if not DB_AVAILABLE:

        return {

            "db": "not_loaded"

        }

    session = None

    try:

        session = get_session()

        total = session.query(
            MarketTick
        ).count()

        rows = session.query(
            MarketTick
        ).limit(5).all()

        sample = [

            {

                "ticker": r.ticker,

                "price": r.price,

                "seq_id": r.seq_id

            }

            for r in rows

        ]

        return {

            "db": "ok",

            "rows": total,

            "sample": sample

        }

    except Exception as e:

        return {

            "db":"failed",

            "error": str(e)

        }

    finally:

        if session:

            session.close()



@app.get("/history/{ticker}")
async def get_history(ticker: str, limit: int = 100):
    if not DB_AVAILABLE:
        return JSONResponse(
            status_code=503,
            content={"error": "Database not available"}
        )
    
    # Limit range for safety
    limit = min(limit, 500)
    
    try:
        session = get_session()
        
        # Query PostgreSQL for ticks (ordered by timestamp, newest first, then reverse)
        ticks = session.query(MarketTick).filter(
            MarketTick.ticker == ticker
        ).order_by(
            desc(MarketTick.timestamp)
        ).limit(limit).all()
        
        session.close()
        
        # Reverse to get oldest-to-newest order
        ticks = list(reversed(ticks))
        
        return {
            "ticker": ticker,
            "count": len(ticks),
            "ticks": [
                {
                    "timestamp": tick.timestamp.isoformat(),
                    "price": tick.price,
                    "seq_id": tick.seq_id
                }
                for tick in ticks
            ]
        }
    
    except Exception as e:
        logger.error(f"Error querying history for {ticker}: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Database query failed: {e}"}
        )

@app.get("/")
async def root():
    """Serve the dashboard HTML."""
    dashboard_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "dashboard.html"
    )
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path, media_type="text/html")
    else:
        return {
            "message": "MarketPulse API",
            "version": "1.0",
            "status": "running",
            "available_endpoints": {
                "dashboard": "GET /",
                "health": "GET /health",
                "metrics": "GET /metrics",
                "tickers": "GET /tickers",
                "websocket": "WS /ws/{ticker}"
            }
        }

# ============================================================================
# WEBSOCKET ENDPOINT (The Core Feature)
# ============================================================================

@app.websocket("/ws/{ticker}")
async def websocket_endpoint(
    websocket: WebSocket,
    ticker: str
):
    """
    WebSocket endpoint for real-time price streaming.
    
    URL: ws://localhost/ws/TCS.NS
    URL with replay: ws://localhost/ws/TCS.NS?last_sequence_id=100
    
    Path Parameters:
        ticker: Stock ticker (TCS.NS, SBIN.NS, SUNPHARMA.NS, MARUTI.NS)
    
    Query Parameters:
        last_sequence_id: Optional. If provided, client gets historical data
                         from this sequence ID onwards (delta replay)
    """
    
    # Extract last_sequence_id from query parameters
    last_sequence_id = None
    if websocket.query_params:
        try:
            last_sequence_id = int(websocket.query_params.get("last_sequence_id"))
        except (ValueError, TypeError):
            last_sequence_id = None
    
    logger.info(
        f"WebSocket connection attempt: {ticker} "
        f"(last_seq_id: {last_sequence_id})"
    )
    
    # Validate ticker
    if ticker not in TICKERS:
        await websocket.close(
            code=1008,
            reason=f"Invalid ticker: {ticker}. Valid: {TICKERS}"
        )
        return
    
    # Register the connection
    await manager.connect(ticker, websocket)
    
    try:
        # ===== PHASE 1: DELTA REPLAY (Catch up on history) =====
        if last_sequence_id is not None:
            logger.info(
                f"{ticker}: Replaying from sequence_id={last_sequence_id}"
            )
            
            # Query Redis Stream for missed ticks
            stream_key = f"market:stream:{ticker}"
            
            try:
                # XRANGE: Get all entries from stream (oldest to newest)
                history = await redis_client.xrange(
                    stream_key
                )
                
                missed_count = 0
                for stream_id, data in history:
                    tick = json.loads(data["data"])
                    
                    # Only send if sequence_id > last_sequence_id
                    if tick["sequence_id"] > last_sequence_id:
                        await websocket.send_json({
                            **tick,
                            "_replay": True  # Flag to indicate historical
                        })
                        missed_count += 1
                
                logger.info(
                    f"{ticker}: Replayed {missed_count} historical ticks"
                )
            
            except Exception as e:
                logger.error(f"Error replaying stream for {ticker}: {e}")
                # Continue anyway (don't fail if replay fails)
        
        # ===== PHASE 2: LIVE FEED =====
        # Now receive live ticks from the manager's broadcast
        logger.info(f"{ticker}: Now streaming live data")
        
        while True:
            try:
                # Wait for client messages with timeout (keep-alive check)
                # If no message within timeout, connection is still alive
                # If client closes, WebSocketDisconnect is raised
                data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=30  # 30 second timeout
                )
                logger.debug(f"{ticker}: Received keep-alive from client")
            except asyncio.TimeoutError:
                # Connection is alive but silent - this is normal
                # Server continues to broadcast to this client
                logger.debug(f"{ticker}: Keep-alive timeout (normal)")
                continue
    
    except WebSocketDisconnect:
        logger.info(f"{ticker}: Client disconnected")
        await manager.disconnect(ticker, websocket)
    
    except Exception as e:
        logger.error(f"{ticker}: WebSocket error: {e}")
        await manager.disconnect(ticker, websocket)

# ============================================================================
# RUN
# ============================================================================

if __name__ == "__main__":
    """
    Run the FastAPI app.
    
    Usage:
        uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
    
    Then connect WebSocket client:
        ws://localhost:8000/ws/TCS
    """
    import uvicorn
    
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
