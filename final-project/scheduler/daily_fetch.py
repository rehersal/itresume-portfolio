"""
daily_fetch.py — ежедневный скрипт для cron.
Забирает данные за вчера. Запускается в 07:00 через cron:

    0 7 * * * /usr/bin/python3 /opt/marketplace/scheduler/daily_fetch.py >> /var/log/marketplace_fetch.log 2>&1
"""

import logging
import sys
import os
from datetime import date, timedelta

# добавляем путь к api-модулям
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

from fetcher import fetch_and_store, get_conn, ensure_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def main():
    yesterday = date.today() - timedelta(days=1)
    log.info(f"Daily fetch started for {yesterday}")

    try:
        with get_conn() as conn:
            ensure_schema(conn)
        n = fetch_and_store(yesterday)
        log.info(f"Daily fetch completed: {n} rows for {yesterday}")
    except Exception as e:
        log.error(f"Daily fetch FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
