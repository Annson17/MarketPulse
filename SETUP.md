# Setup Instructions

## Prerequisites

- Python 3.9+
- Docker

## Running the Project

### Terminal 1: Start Docker Services

```bash
docker-compose up -d
```

Verify services are running:
```bash
docker-compose ps
```

### Terminal 2: Initialize Database (run once)

```bash
cd <project-directory>
python db_init.py
```

This creates the PostgreSQL schema and `market_ticks` table.

### Terminal 3: Start Continuous Importer

```bash
cd <project-directory>
python continuous_importer.py
```

This watches `market_maker.log` and imports new ticks to PostgreSQL. You should see:
```
Watching: market_maker.log
```

### Terminal 4: Start Market Maker

```bash
cd <project-directory>
python ingestion/market_maker.py
```

This generates market ticks every 500 ms and writes them to market_maker.log.

### Terminal 5: Start FastAPI Server

```bash
cd <project-directory>
python -m uvicorn api.main:app --reload
```

Server starts at `http://localhost:8000`

You should see:
```
Uvicorn running on http://0.0.0.0:8000
```

### Browser

Open your web browser:
```
http://localhost:8000
```

You should see:
- 4 charts (TCS, SBIN, SUNPHARMA, MARUTI)
- "Connected" status badges (green when live data is flowing)
- Live price updates every 500ms
- Charts populate with historical data after a few seconds

## Verification

### Check that data is flowing

1. **Market Maker** (Terminal 4): You should see new ticks logged every 500ms
2. **Continuous Importer** (Terminal 3): You should see "Importing tick" messages
3. **Dashboard** (Browser): Charts should show live updates with green badges

### Restart individual components

If something goes wrong, you can restart individual components without restarting Docker:

**Restart Market Maker:**
- Stop Terminal 4 (Ctrl+C)
- Terminal 4 again: `python ingestion/market_maker.py`

**Restart FastAPI:**
- Stop Terminal 5 (Ctrl+C)  
- Terminal 5 again: `python -m uvicorn api.main:app --reload`

**Restart Continuous Importer:**
- Stop Terminal 3 (Ctrl+C)
- Terminal 3 again: `python continuous_importer.py`

### Check Database

To verify data is in PostgreSQL:

```bash
docker exec -it <postgres-container-id> psql -U postgres -d marketpulse
```
```sql
SELECT ticker, price, seq_id, timestamp
FROM market_ticks
ORDER BY timestamp DESC
LIMIT 10;

SELECT ticker, COUNT(*)
FROM market_ticks
GROUP BY ticker;
```

## Troubleshooting

### "Port 8000 already in use"

```bash
# Find what's using port 8000
netstat -ano | findstr :8000

# Kill the process (replace PID with actual number)
taskkill /PID <PID> /F

# Try again
python -m uvicorn api.main:app --reload
```

### "Connection refused" in market_maker.py or continuous_importer.py

```bash
# Make sure Docker services are running
docker-compose ps

# If not running, start them
docker-compose up -d
```

### Dashboard shows "Loading chart..." but no data appears

1. Check that all services are running
2. Wait 10-15 seconds for historical data to import
3. Open browser console (F12) to check for errors
4. Hard refresh browser (Ctrl+Shift+R to clear cache)

### WebSocket connection fails in browser console

1. Make sure FastAPI is running (Terminal 5)
2. Check that market_maker is publishing data (Terminal 4)
3. Verify ticker names are correct in browser console:
   - Should be: TCS, SBIN, SUNPHARMA, MARUTI
   - NOT: TCS.NS, INFY, RELIANCE

### "No data in charts after 1 minute"

1. Check continuous_importer is running (Terminal 3)
2. Verify market_maker.log is being created and written to
3. Check database has data:
   ```bash
   docker exec -it <postgres-id> psql -U postgres -d marketpulse -c "SELECT COUNT(*) FROM market_ticks;"
   ```

## Stopping Everything

When done:

```bash
# In Terminal 1
docker-compose down
```

This stops PostgreSQL and Redis. Individual Python services will stop when you press Ctrl+C in their terminals.

## Environment Configuration

Copy `.env.example` to `.env` if configuration changes are needed.

## Architecture Summary

```
Market Maker
    ↓
Redis Pub/Sub + PostgreSQL
    ↓
FastAPI
    ↓
Dashboard
```

For more details, see README.md
