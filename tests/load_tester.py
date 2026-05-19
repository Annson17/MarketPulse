"""
MarketPulse - WebSocket Load Tester

Purpose:
Load test MarketPulse WebSocket endpoints using concurrent clients.

Usage:
python tests/load_tester.py
"""

import asyncio
import logging
import statistics
from datetime import datetime

import aiohttp

# ============================================================
# CONFIGURATION
# ============================================================

WS_URL = "ws://localhost:8000/ws/{ticker}"

TOTAL_CLIENTS = 200

CLIENTS_PER_BATCH = 20

BATCH_DELAY = 0.2

TEST_DURATION = 5

TICKERS = [
    "TCS",
    "SBIN",
    "SUNPHARMA",
    "MARUTI"
]

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(
    __name__
)

# ============================================================
# METRICS
# ============================================================

class Metrics:

    def __init__(self):

        self.messages = 0

        self.connected = 0

        self.failed = 0

        self.latencies = []

        self.lock = asyncio.Lock()

    async def add_message(self):

        async with self.lock:

            self.messages += 1

            self.latencies.append(0)

    async def success(self):

        async with self.lock:

            self.connected += 1

    async def failure(self):

        async with self.lock:

            self.failed += 1

    def percentile(
        self,
        values,
        p
    ):

        if not values:
            return 0

        values = sorted(
            values
        )

        idx = int(
            (
                len(values)-1
            ) * p
        )

        return round(
            values[idx],
            2
        )

    def stats(self):

        if not self.latencies:
            return {}

        return {

            "mean":

            round(

                statistics.mean(
                    self.latencies
                ),

                2

            ),

            "p95":

            self.percentile(
                self.latencies,
                0.95
            ),

            "p99":

            self.percentile(
                self.latencies,
                0.99
            )

        }


metrics = Metrics()

# ============================================================
# CLIENT
# ============================================================

async def simulate_client(
    client_id,
    ticker,
    session
):

    url = WS_URL.format(
        ticker=ticker
    )

    start = datetime.now()

    try:

        async with session.ws_connect(

            url,

            heartbeat=20

        ) as ws:

            await metrics.success()

            while True:

                elapsed = (

                    datetime.now()

                    -

                    start

                ).total_seconds()

                if (
                    elapsed
                    >=
                    TEST_DURATION
                ):
                    break

                try:

                    msg = await asyncio.wait_for(

                        ws.receive_json(),

                        timeout=2

                    )

                    # Count every received message

                    await metrics.add_message()

                except asyncio.TimeoutError:

                    break

                except Exception:

                    break

    except Exception as e:

        logger.warning(
            f"Client "
            f"{client_id} "
            f"failed: {e}"
        )

        await metrics.failure()

# ============================================================
# SPAWN CLIENTS
# ============================================================

async def spawn_clients():

    connector = aiohttp.TCPConnector(

        limit=50,

        limit_per_host=25,

        enable_cleanup_closed=True

    )

    async with aiohttp.ClientSession(

        connector=
        connector

    ) as session:

        tasks = []

        logger.info(
            f"Starting "
            f"{TOTAL_CLIENTS} "
            f"clients"
        )

        for i in range(
            TOTAL_CLIENTS
        ):

            ticker = TICKERS[
                i %
                len(
                    TICKERS
                )
            ]

            task = asyncio.create_task(

                simulate_client(

                    i,

                    ticker,

                    session

                )

            )

            tasks.append(
                task
            )

            if (

                i + 1

            ) % CLIENTS_PER_BATCH == 0:

                logger.info(

                    f"Spawned "

                    f"{i+1}/"

                    f"{TOTAL_CLIENTS}"

                )

                await asyncio.sleep(
                    BATCH_DELAY
                )

        await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

# ============================================================
# MAIN
# ============================================================

async def main():

    print(
        "\n"
        +
        "="*70
    )

    print(
        "MARKETPULSE LOAD TEST"
    )

    print(
        "="*70
    )

    start = datetime.now()

    await spawn_clients()

    duration = (

        datetime.now()

        -

        start

    ).total_seconds()

    print("\nRESULTS")

    print("="*70)

    print(
        f"Clients: "
        f"{TOTAL_CLIENTS}"
    )

    print(
        f"Connected: "
        f"{metrics.connected}"
    )

    print(
        f"Failed: "
        f"{metrics.failed}"
    )

    print(
        f"Messages: "
        f"{metrics.messages}"
    )

    print(
        f"Duration: "
        f"{duration:.2f}s"
    )

    if duration > 0:

        print(

            f"Messages/sec: "

            f"{metrics.messages / duration:.2f}"

        )

if __name__ == "__main__":

    asyncio.run(
        main()
    )