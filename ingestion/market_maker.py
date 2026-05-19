"""
MarketPulse - Realistic Market Data Generator
GBM + Mean Reversion + Momentum + Noise

Flow:
Market Maker
    ↓
Redis Pub/Sub
    ↓
FastAPI WebSocket
    ↓
Dashboard + PostgreSQL History
"""

import asyncio
import json
import random
import logging
from datetime import datetime

import redis.asyncio as redis

# ============================================================
# CONFIG
# ============================================================

TICKERS = [
    "TCS",
    "SBIN",
    "SUNPHARMA",
    "MARUTI"
]

TICKER_INFO = {
    "TCS": "IT/Tech",
    "SBIN": "Banking",
    "SUNPHARMA": "Pharma",
    "MARUTI": "Auto"
}

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_PASSWORD = "redis_password"

TICK_INTERVAL = 0.5
STREAM_MAXLEN = 10000

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s - "
        "%(name)s - "
        "%(levelname)s - "
        "%(message)s"
    ),
    handlers=[
        logging.FileHandler(
            "market_maker.log"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ============================================================
# MARKET MAKER
# ============================================================

class MarketMaker:

    def __init__(
        self,
        redis_client
    ):

        self.redis = redis_client

        # Base prices

        self.prices = {
            "TCS": 3520.50,
            "SBIN": 750.25,
            "SUNPHARMA": 680.80,
            "MARUTI": 8250.00
        }

        self.base_prices = (
            self.prices.copy()
        )

        self.sequence_id = {
            ticker: 0
            for ticker in TICKERS
        }

        logger.info(
            f"Initialized "
            f"{len(TICKERS)} tickers"
        )

    async def generate_tick(
        self,
        ticker
    ):

        current = (
            self.prices[
                ticker
            ]
        )

        base = (
            self.base_prices[
                ticker
            ]
        )

        settings = {

            "TCS": {
                "vol": 0.010,
                "noise": 8
            },

            "SBIN": {
                "vol": 0.015,
                "noise": 3
            },

            "SUNPHARMA": {
                "vol": 0.012,
                "noise": 4
            },

            "MARUTI": {
                "vol": 0.011,
                "noise": 15
            }

        }

        vol = (
            settings[
                ticker
            ]["vol"]
        )

        noise = (
            settings[
                ticker
            ]["noise"]
        )

        trend = random.gauss(
            0,
            vol
        )

        momentum = (
            current
            -
            base
        ) / base

        reversion = (
            -0.05
            *
            momentum
        )

        move = (
            trend
            +
            reversion
        )

        new_price = (
            current
            *
            (
                1
                +
                move
            )
        )

        # micro fluctuations

        new_price += (
            random.uniform(
                -noise,
                noise
            )
        )

        # prevent explosions

        upper = (
            base
            *
            1.5
        )

        lower = (
            base
            *
            0.7
        )

        new_price = min(
            upper,
            max(
                lower,
                new_price
            )
        )

        new_price = round(
            new_price,
            2
        )

        self.prices[
            ticker
        ] = new_price

        self.sequence_id[
            ticker
        ] += 1

        tick = {

            "ticker":
            ticker,

            "price":
            new_price,

            "timestamp":

            datetime.utcnow()
            .isoformat(),

            "sequence_id":

            self.sequence_id[
                ticker
            ],

            "sector":

            TICKER_INFO[
                ticker
            ]

        }

        return tick

    async def publish_tick(
        self,
        tick
    ):

        ticker = (
            tick[
                "ticker"
            ]
        )

        tick_json = json.dumps(
            tick
        )

        await self.redis.publish(

            f"market:live:{ticker}",

            tick_json

        )

        await self.redis.xadd(

            f"market:stream:{ticker}",

            {
                "data":
                tick_json
            },

            maxlen=
            STREAM_MAXLEN,

            approximate=
            True

        )

    async def run(
        self
    ):

        logger.info(
            "Starting realistic market maker..."
        )

        while True:

            try:

                for ticker in TICKERS:

                    tick = await (
                        self
                        .generate_tick(
                            ticker
                        )
                    )

                    await (
                        self
                        .publish_tick(
                            tick
                        )
                    )

                    logger.info(

                        f"{ticker}: "

                        f"Rs"

                        f"{tick['price']} "

                        f"(seq: "

                        f"{tick['sequence_id']})"

                    )

                await asyncio.sleep(
                    TICK_INTERVAL
                )

            except Exception as e:

                logger.error(
                    f"Tick error: {e}"
                )

                await asyncio.sleep(
                    1
                )

# ============================================================
# MAIN
# ============================================================

async def main():

    logger.info(
        "Starting MarketPulse..."
    )

    redis_client = None

    try:

        redis_client = await redis.Redis(

            host=
            REDIS_HOST,

            port=
            REDIS_PORT,

            password=
            REDIS_PASSWORD,

            decode_responses=
            True

        )

        await redis_client.ping()

        logger.info(
            "Connected to Redis"
        )

        maker = MarketMaker(
            redis_client
        )

        await maker.run()

    except KeyboardInterrupt:

        logger.info(
            "Shutdown requested"
        )

    except Exception as e:

        logger.error(
            f"Connection failed: {e}"
        )

    finally:

        if redis_client:

            await redis_client.aclose()

            logger.info(
                "Closed Redis connection"
            )

if __name__ == "__main__":

    asyncio.run(
        main()
    )