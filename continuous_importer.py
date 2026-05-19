"""
Continuous Log Importer
=======================

Watches market_maker.log and continuously imports
new ticks into PostgreSQL.

Architecture:

market_maker.log
        ↓
continuous_importer.py
        ↓
PostgreSQL
        ↓
History API
        ↓
Chart.js
"""

import os
import time
import re
import logging
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError

from db_init import (
    get_session,
    MarketTick
)

logging.basicConfig(
    level=logging.INFO,
    format=(
        '%(asctime)s - '
        '%(name)s - '
        '%(levelname)s - '
        '%(message)s'
    )
)

logger = logging.getLogger(__name__)

# ROOT log file
LOG_FILE = "market_maker.log"

# Example:
# 2026-05-18 20:10:42,088 - __main__ - INFO - TCS: $3443.44 (seq_id: 1)

REGEX = re.compile(

    r'('
    r'\d{4}-\d{2}-\d{2} '
    r'\d{2}:\d{2}:\d{2}'
    r')'

    r',\d+'

    r'.*?INFO - '

    r'([A-Z]+)'

    r': (?:Rs|\$)'

    r'([0-9,.]+)'

    r' \(seq(?:_id)?: '

    r'(\d+)'

    r'\)'

)


def parse_log_line(line):

    match = REGEX.search(
        line
    )

    if not match:
        return None

    try:

        timestamp_str, ticker, price_str, seq_id = (
            match.groups()
        )

        return {

            "timestamp":
            datetime.strptime(
                timestamp_str,
                "%Y-%m-%d %H:%M:%S"
            ),

            "ticker":
            ticker.strip(),

            "price":
            float(
                price_str.replace(
                    ",",
                    ""
                )
            ),

            "seq_id":
            int(seq_id)

        }

    except Exception:

        return None


def import_new_lines():

    logger.info(
        f"Watching: {LOG_FILE}"
    )

    session = get_session()

    imported_total = 0

    # start at END of file
    last_position = (
        os.path.getsize(
            LOG_FILE
        )
        if os.path.exists(
            LOG_FILE
        )
        else 0
    )

    try:

        while True:

            if not os.path.exists(
                LOG_FILE
            ):

                time.sleep(
                    1
                )

                continue

            current_size = (
                os.path.getsize(
                    LOG_FILE
                )
            )

            # file reset / rotated

            if current_size < last_position:

                logger.info(
                    "Log rotated"
                )

                last_position = 0

            with open(
                LOG_FILE,
                "r",
                encoding="utf-8"
            ) as f:

                f.seek(
                    last_position
                )

                lines = (
                    f.readlines()
                )

                last_position = (
                    f.tell()
                )

            if not lines:

                time.sleep(
                    0.5
                )

                continue

            new_ticks = []

            for line in lines:

                tick = (
                    parse_log_line(
                        line
                    )
                )

                if not tick:
                    continue

                existing = (
                    session.query(
                        MarketTick
                    )
                    .filter(
                        MarketTick.ticker == tick["ticker"]
                    )
                    .filter(
                        MarketTick.seq_id == tick["seq_id"]
                    )
                    .first()
                )

                if existing:
                    continue

                new_ticks.append(

                    MarketTick(

                        ticker=
                        tick[
                            "ticker"
                        ],

                        price=
                        tick[
                            "price"
                        ],

                        seq_id=
                        tick[
                            "seq_id"
                        ],

                        timestamp=
                        tick[
                            "timestamp"
                        ]

                    )

                )

            if new_ticks:

                session.bulk_save_objects(
                    new_ticks
                )

                session.commit()

                imported_total += (
                    len(
                        new_ticks
                    )
                )

                tickers = set(

                    t.ticker

                    for t in
                    new_ticks

                )

                logger.info(

                    f"Imported "
                    f"{len(new_ticks)} "

                    f"ticks | "

                    f"Tickers: "
                    f"{tickers} | "

                    f"Total: "
                    f"{imported_total}"

                )

            time.sleep(
                0.5
            )

    except KeyboardInterrupt:

        logger.info(
            "Stopped"
        )

    except SQLAlchemyError as e:

        session.rollback()

        logger.error(
            e
        )

    except Exception as e:

        logger.exception(
            e
        )

    finally:

        session.close()


if __name__ == "__main__":

    logger.info(
        "Starting "
        "Continuous "
        "Importer..."
    )

    import_new_lines()