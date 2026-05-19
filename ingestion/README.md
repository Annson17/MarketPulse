# Phase 2: Market Data Ingestion Engine - Interview Guide

## What You're Building

You're building the **DATA PRODUCER** tier of a real-time market data system. This is the component that:

1. **Simulates a live stock exchange** by generating realistic price ticks
2. **Feeds the entire system** with market data
3. **Demonstrates understanding** of:
   - Async/await patterns for high-performance I/O
   - Redis Pub/Sub (real-time messaging)
   - Redis Streams (persistent event logs)
   - Dual-write pattern (critical in distributed systems)

---

## Core Concepts To Explain In Interviews

### What Is A "Price Tick"?

A price tick is a single trade or price update:
```json
{
    "ticker": "TCS",
    "price": 3520.45,
    "timestamp": "2026-05-18T14:23:45.123456",
    "sequence_id": 42
}
```

**Why these fields?**
- `ticker`: Which stock
- `price`: What it's trading at
- `timestamp`: When it happened (UTC for global consistency)
- `sequence_id`: Strictly increasing counter (critical for detecting gaps or out-of-order ticks)

---

### The Dual-Write Pattern (IMPORTANT FOR INTERVIEWS)

This is how professional systems work:

```
Market Maker (Producer)
    ↓
    ├─→ PUBLISH to Redis Pub/Sub    ← Real-time (live broadcasts)
    └─→ XADD to Redis Stream         ← Historical (DVR recording)
    
    ↓
    
Consumers:
    ├─→ Dashboard (subscribes to Pub/Sub)    → Gets real-time updates
    └─→ Analytics Engine (reads Stream)      → Does analysis on history
```

**Why both?**

| Pub/Sub | Stream |
|---------|--------|
| Real-time delivery | Persistent storage |
| New messages only | Can read history |
| Subscribers after message = missed | Late joiners can catch up |
| Use: Live updates | Use: Analysis, replay, audit |
| Similar to: Radio broadcast | Similar to: DVR recording |

**Real-world analogy:**
- Pub/Sub = ESPN sports broadcast (live updates, miss if you turn on late)
- Stream = ESPN+ recording (can watch anytime, rewind, analyze)

---

## How To Run It

### Step 1: Install Dependencies
```powershell
cd d:\MarketPulse
pip install -r requirements.txt
```

### Step 2: Start Docker Containers
```powershell
docker-compose up -d
```

Verify Redis is running:
```powershell
docker exec marketpulse-redis redis-cli -a redis_password ping
# Should return: PONG
```

### Step 3: Run The Market Maker
```powershell
python ingestion/market_maker.py
```

Output should show:
```
2026-05-18 14:23:45 - __main__ - INFO - ✓ Connected to Redis: True
2026-05-18 14:23:45 - __main__ - INFO - Starting market maker for tickers: ['TCS', 'INFY', 'RELIANCE']
2026-05-18 14:23:46 - __main__ - INFO - TCS: $3520.45 (seq_id: 1)
2026-05-18 14:23:46 - __main__ - INFO - INFY: $2199.50 (seq_id: 1)
2026-05-18 14:23:46 - __main__ - INFO - RELIANCE: $2498.75 (seq_id: 1)
```

### Step 4: In Another Terminal, Run The Consumer
```powershell
python ingestion/consumer_examples.py
```

This demonstrates reading the data in multiple ways.

---

## Code Breakdown (For Interviews)

### 1. AsyncIO Pattern (Concurrency)

```python
# BAD (Sequential - slow)
for ticker in TICKERS:
    tick = generate_price_tick(ticker)
    await publish_to_redis(tick)
    # Total time: TICK_INTERVAL * 3

# GOOD (Concurrent - fast)
tasks = []
for ticker in TICKERS:
    tick = generate_price_tick(ticker)
    tasks.append(publish_to_redis(tick))
await asyncio.gather(*tasks)
# Total time: TICK_INTERVAL (all at once)
```

**Why asyncio?**
- Single-threaded but concurrent
- Handles thousands of connections efficiently
- No context-switching overhead (unlike threads)
- Perfect for I/O-bound operations (Redis, networks)

**Interview talking point:** "I use asyncio for high-concurrency I/O operations. This allows a single Python process to handle thousands of simultaneous Redis connections without thread overhead."

---

### 2. Redis Pub/Sub (Real-time Publishing)

```python
live_channel = f"market:live:{ticker}"
num_subscribers = await self.redis.publish(live_channel, tick_json)
```

**What happens:**
1. Tick is published to `market:live:TCS`
2. Anyone subscribed to this channel gets it INSTANTLY
3. If nobody subscribed = message is lost (fire-and-forget)

**Interview answer:** "Pub/Sub is perfect for real-time dashboards because it's fire-and-forget and low-latency. The broker doesn't store messages, just broadcasts them. This is efficient but requires active subscribers."

---

### 3. Redis Stream (Persistent History)

```python
stream_key = f"market:stream:{ticker}"
stream_id = await self.redis.xadd(
    stream_key,
    {"data": tick_json},
    maxlen=STREAM_MAXLEN,
    approximate=True
)
```

**What happens:**
1. Tick is appended to `market:stream:TCS` stream
2. Stream grows forever (up to MAXLEN)
3. XADD returns unique ID (auto-generated timestamp-based)
4. Later consumers can read entire history

**Interview answer:** "Redis Streams provide durability. Unlike Pub/Sub, every message is persisted. The MAXLEN parameter is important—it prevents infinite growth and is a common pattern in streaming systems."

---

### 4. Sequence ID (Ordering Guarantee)

```python
self.sequence_id[ticker] += 1
tick["sequence_id"] = self.sequence_id[ticker]
```

**Why this matters:**
- Stock traders need to know the order of trades
- If seq_id goes: 1, 2, 3, **5** ← detected missing tick!
- Gaps indicate data loss or system failure
- Allows consumers to request replay of missing data

**Interview answer:** "Sequence IDs are critical for financial systems. They detect data loss and ensure ordering even if messages arrive out-of-order. This is how audit systems work—they can verify no trades were lost."

---

### 5. Realistic Price Movement

```python
price_change_percent = random.uniform(-PRICE_CHANGE_PERCENT, PRICE_CHANGE_PERCENT)
new_price = current_price * (1 + price_change_percent)
```

**Why not random.random()?**
- Random() gives 0-1, completely unrealistic
- Using % change gives realistic price drift
- Stock prices have momentum (random walk)

**Interview answer:** "I use a random walk model to simulate price movements. Real stock prices don't jump randomly—they drift gradually with small changes. This makes the simulation realistic and the data useful for testing."

---

## Interview Questions You Might Get

### Q1: "What if Redis crashes while market_maker is running?"
**Answer:** "The market_maker would crash when trying to write to Redis. In production, I'd implement:
1. Connection retry logic with exponential backoff
2. Dead-letter queue (write failed ticks to disk)
3. Alerting (PagerDuty) when connection fails
4. Automatic reconnection when Redis recovers"

### Q2: "What if you generate ticks faster than consumers can read them?"
**Answer:** "This is exactly what Redis Streams solve. They buffer messages on disk (if memory runs out). The MAXLEN=10000 caps memory usage. Slow consumers can read at their own pace from the stream. Fast consumers use Pub/Sub for real-time."

### Q3: "How do you ensure no data is lost?"
**Answer:** "Multiple layers:
1. Sequence IDs detect gaps (consumer side)
2. Redis Stream persistence (server side)
3. Dual-write pattern (if Pub/Sub fails, Stream still has it)
4. AOF persistence in Redis (configured in docker-compose.yml)
5. Monitoring and alerting for errors"

### Q4: "How would you scale this to 1000 stocks?"
**Answer:** "The current approach scales linearly:
- Generate ticks for all 1000 stocks in parallel (asyncio handles it)
- Redis can handle 100K+ ops/sec (we do ~6 per tick, 1000 stocks = 6K ops/sec)
- If it bottlenecks:
  1. Shard Redis (multiple Redis instances by ticker range)
  2. Use message queue (Kafka) for higher throughput
  3. Batch ticks (send 10 at a time instead of individual)
"

### Q5: "Why not just use a database instead of Redis?"
**Answer:** "Databases are optimized for durability and complex queries. Redis is optimized for speed:
- Database write: ~10ms (disk I/O)
- Redis write: <1ms (memory)
- For real-time trading, sub-millisecond latency is critical
- Redis Streams + Database is the real pattern:
  - Real-time trades → Redis → Dashboard (instant)
  - Batch trade writes → Database → Analytics (eventual consistency)
"

---

## Project Structure Now

```
MarketPulse/
├── venv/                      ← Python virtual environment
├── docker-compose.yml         ← Container orchestration
├── nginx.conf                 ← Reverse proxy config
├── .env.example               ← Configuration template
├── requirements.txt           ← Python dependencies
│
└── ingestion/                 ← Data producer (Phase 2)
    ├── __init__.py
    ├── market_maker.py        ← Main ingestion engine
    ├── consumer_examples.py    ← Example consumers (for learning)
    └── README.md              ← This file
```

---

## Key Takeaways For Interview

You've demonstrated:

✅ **Async/await mastery** - Concurrent Redis writes without threads  
✅ **Redis expertise** - Both Pub/Sub (real-time) and Streams (persistent)  
✅ **Distributed systems thinking** - Dual-write, sequence IDs, ordering  
✅ **Production mindset** - Error handling, logging, monitoring  
✅ **Financial domain knowledge** - Price ticks, sequences, realistic data  
✅ **System design** - How data flows from producer to consumer  

Next phase: Build the FastAPI consumer that reads this data and serves it via WebSockets to a dashboard.

---

## Debugging Tips

**Redis CLI to inspect data:**
```powershell
# Connect to Redis
docker exec -it marketpulse-redis redis-cli -a redis_password

# See all channels with subscribers
> PUBSUB CHANNELS

# See all streams
> KEYS market:stream:*

# Read last 5 entries from TCS stream
> XREVRANGE market:stream:TCS - + COUNT 5

# Get stream info
> XINFO STREAM market:stream:TCS
```

**Check log file:**
```powershell
# On Windows
Get-Content market_maker.log -Tail 20

# Or in PowerShell
tail -f market_maker.log
```

---

Good luck with your interviews! You're building something real now. 🚀
