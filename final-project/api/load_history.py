"""
load_history.py — единоразово заполняет БД историческими данными.
Запускается один раз вручную после развёртывания сервера.

Использование:
    python load_history.py --start 2023-01-01 --end 2023-12-31

По умолчанию — весь 2023 год.
"""

import argparse
import time
import logging
from datetime import date, timedelta

from fetcher import fetch_and_store, get_conn, ensure_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DELAY_BETWEEN_DAYS = 1.5   # сек между запросами — не DDoSим API


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Historical data loader")
    parser.add_argument("--start", default="2023-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   default="2023-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--delay", type=float, default=DELAY_BETWEEN_DAYS,
                        help="Delay between API requests (sec)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    total_days = (end - start).days + 1

    log.info(f"Loading {start} → {end} ({total_days} days)")

    # Убеждаемся, что схема создана
    with get_conn() as conn:
        ensure_schema(conn)

    total_rows = 0
    errors = []

    for i, d in enumerate(daterange(start, end), 1):
        try:
            n = fetch_and_store(d)
            total_rows += n
            pct = i / total_days * 100
            log.info(f"  [{i}/{total_days}  {pct:.1f}%]  {d}: {n} rows  (total: {total_rows})")
        except Exception as e:
            log.error(f"  FAILED {d}: {e}")
            errors.append((d, str(e)))

        time.sleep(args.delay)

    log.info(f"\n=== Done. {total_rows} total rows loaded. {len(errors)} errors. ===")
    if errors:
        log.warning("Failed dates:")
        for d, e in errors:
            log.warning(f"  {d}: {e}")


if __name__ == "__main__":
    main()
