"""
MarketPulse - Redis Consumer Examples
=====================================

This script demonstrates HOW TO READ from Redis in different ways.
Use this to understand and validate the market_maker.py is working.

This will help you in interviews: "Here's how I validate the data pipeline"
"""

import asyncio
import json
import logging
from datetime import datetime
import redis.asyncio as redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis connection settings
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_PASSWORD = "redis_password"

# ============================================================================
# EXAMPLE 1: SUBSCRIBE TO LIVE CHANNEL (Real-time)
# ============================================================================

async def consume_live_feed(ticker: str, duration_seconds: int = 10):
    """
    Listen to LIVE market updates via Pub/Sub.
    
    This is what a trading dashboard would do:
    - Subscribe to market:live:TCS
    - Every new price appears instantly (sub-millisecond)
    - Old prices are missed (unless you record them)
    
    Use case: Real-time price display on website
    """
    
    logger.info(f"[LIVE FEED] Subscribing to {ticker}...")
    
    redis_client = await redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"market:live:{ticker}")
    
    logger.info(f"✓ Listening to market:live:{ticker} for {duration_seconds}s")
    
    start_time = datetime.now()
    tick_count = 0
    
    try:
        async for message in pubsub.listen():
            # Skip subscription confirmation
            if message["type"] == "subscribe":
                continue
            
            if message["type"] == "message":
                tick_count += 1
                tick = json.loads(message["data"])
                
                print(f"\n[LIVE] {tick['ticker']}: ${tick['price']} "
                      f"(seq_id: {tick['sequence_id']})")
            
            # Stop after duration
            if (datetime.now() - start_time).total_seconds() > duration_seconds:
                break
    
    finally:
        await pubsub.close()
        await redis_client.close()
        logger.info(f"✓ Received {tick_count} live ticks in {duration_seconds}s")

# ============================================================================
# EXAMPLE 2: READ STREAM HISTORY (Persistent)
# ============================================================================

async def consume_stream_history(ticker: str, last_n_ticks: int = 5):
    """
    Read historical data from Redis Stream.
    
    This is what a data analyst or backtester would do:
    - Read stream market:stream:TCS
    - Get last N ticks
    - Can do historical analysis without losing data
    
    Use case: Charting, backtesting, audit logs
    """
    
    logger.info(f"[STREAM] Reading last {last_n_ticks} ticks for {ticker}...")
    
    redis_client = await redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    
    stream_key = f"market:stream:{ticker}"
    
    try:
        # XREVRANGE: Read stream in reverse (newest first)
        # -: Last entry
        # +: Oldest entry
        # COUNT: Number of entries
        results = await redis_client.xrevrange(stream_key, count=last_n_ticks)
        
        logger.info(f"✓ Found {len(results)} ticks in stream")
        
        for i, (stream_id, data) in enumerate(reversed(results), 1):
            tick = json.loads(data["data"])
            print(f"\n[HISTORY {i}] {tick['ticker']}: ${tick['price']} "
                  f"(seq_id: {tick['sequence_id']}, redis_id: {stream_id})")
    
    finally:
        await redis_client.close()

# ============================================================================
# EXAMPLE 3: CHECK REDIS KEY INFO
# ============================================================================

async def check_redis_info():
    """
    Diagnostic: See what's stored in Redis.
    
    Helpful for: Debugging, understanding data structure, monitoring
    """
    
    logger.info("[INFO] Checking Redis data structure...")
    
    redis_client = await redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    
    try:
        # Get all keys matching pattern
        live_channels = await redis_client.keys("market:live:*")
        stream_keys = await redis_client.keys("market:stream:*")
        
        print("\n=== REDIS DATA ===")
        print(f"Live Channels (Pub/Sub): {live_channels}")
        print(f"Stream Keys: {stream_keys}")
        
        # For each stream, show info
        for stream_key in stream_keys:
            info = await redis_client.xinfo_stream(stream_key)
            print(f"\n{stream_key}:")
            print(f"  - Length: {info['length']} ticks")
            print(f"  - First Entry: {info['first_entry']}")
            print(f"  - Last Entry: {info['last_entry']}")
    
    finally:
        await redis_client.close()

# ============================================================================
# MAIN: RUN EXAMPLES
# ============================================================================

async def main():
    """
    Interactive demo of all consumer patterns.
    """
    
    print("\n" + "="*60)
    print("MarketPulse - Redis Consumer Examples")
    print("="*60)
    print("\nBefore running, make sure market_maker.py is running!")
    print("  Terminal 1: python ingestion/market_maker.py")
    print("  Terminal 2: python ingestion/consumer_examples.py\n")
    
    # Example 1: Check what's in Redis
    print("\n--- STEP 1: Check Redis Data Structure ---")
    await check_redis_info()
    
    await asyncio.sleep(2)
    
    # Example 2: Read historical data from stream
    print("\n--- STEP 2: Read Historical Data from Stream ---")
    await consume_stream_history("TCS", last_n_ticks=5)
    
    await asyncio.sleep(2)
    
    # Example 3: Listen to live feed (10 seconds)
    print("\n--- STEP 3: Listen to Live Feed (10 seconds) ---")
    await consume_live_feed("TCS", duration_seconds=10)

if __name__ == "__main__":
    asyncio.run(main())
