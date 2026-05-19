import re
from datetime import datetime
from sqlalchemy.orm import Session
from db_init import MarketTick, get_session

LOG_PATTERN = re.compile(
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+'
    r'.*INFO - '
    r'([A-Z]+): Rs([0-9.]+)'
    r' \(seq: (\d+)\)'
)


def parse_log_line(line):

    match = LOG_PATTERN.search(line)

    if not match:
        return None

    timestamp_str, ticker, price_str, seq_id = match.groups()

    timestamp = datetime.strptime(
        timestamp_str,
        "%Y-%m-%d %H:%M:%S"
    )

    return (
        timestamp,
        ticker,
        float(price_str),
        int(seq_id)
    )


def bulk_import(
    log_file_path: str,
    session: Session
):

    print(
        f"Reading {log_file_path}"
    )

    ticks = []

    with open(
        log_file_path,
        "r",
        encoding="utf-8"
    ) as f:

        for line in f:

            parsed = parse_log_line(
                line.strip()
            )

            if parsed:

                timestamp, ticker, price, seq_id = parsed

                ticks.append(

                    MarketTick(

                        ticker=ticker,

                        price=price,

                        seq_id=seq_id,

                        timestamp=timestamp

                    )
                )

    print(
        f"Parsed {len(ticks)} ticks"
    )

    print(
        "Tickers:",
        sorted(
            list(
                set(
                    t.ticker
                    for t in ticks
                )
            )
        )
    )

    if ticks:

        session.bulk_save_objects(
            ticks
        )

        session.commit()

        print(
            f"Imported {len(ticks)} rows"
        )

    else:

        print(
            "No new ticks"
        )

    return len(ticks)


if __name__ == "__main__":

    session = get_session()

    count = bulk_import(
        "market_maker.log",
        session
    )

    session.close()

    print(
        f"\nDone: {count}"
    )