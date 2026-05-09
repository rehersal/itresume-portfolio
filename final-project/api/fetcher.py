"""
fetcher.py — забирает данные с API за указанную дату и сохраняет в PostgreSQL.
Используется как модуль в scheduler и в историческом заполнении.
"""

import os
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import date, timedelta
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

API_URL   = os.getenv("API_URL",  "http://final-project.simulative.ru/data")
DB_DSN    = os.getenv("DB_DSN",   "postgresql://analyst:CHANGE_ME@localhost:5432/marketplace")
API_DELAY = float(os.getenv("API_DELAY", "1.0"))   # пауза между запросами, сек


# ── Подключение к БД ─────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DB_DSN)


# ── Создание таблицы (идемпотентно) ──────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS raw_orders (
    id              SERIAL PRIMARY KEY,
    order_id        TEXT,
    order_date      DATE,
    order_datetime  TIMESTAMP,
    customer_id     TEXT,
    customer_name   TEXT,
    customer_email  TEXT,
    customer_city   TEXT,
    customer_gender TEXT,
    product_id      TEXT,
    product_name    TEXT,
    category        TEXT,
    subcategory     TEXT,
    brand           TEXT,
    price           NUMERIC(12,2),
    cost_price      NUMERIC(12,2),
    quantity        INTEGER,
    discount_pct    NUMERIC(5,2),
    discount_amount NUMERIC(12,2),
    revenue         NUMERIC(12,2),
    profit          NUMERIC(12,2),
    payment_method  TEXT,
    delivery_days   INTEGER,
    is_returned     BOOLEAN,
    rating          NUMERIC(3,1),
    fetched_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(order_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_orders_date       ON raw_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer   ON raw_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_product    ON raw_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_category   ON raw_orders(category);
"""

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    log.info("Schema OK")


# ── Запрос к API ──────────────────────────────────────────────────────────────
def fetch_day(target_date: date) -> list[dict]:
    """
    Запрашивает данные за один день. Обрабатывает пагинацию, если она есть.
    Возвращает список записей (dict).
    """
    params = {"date": target_date.strftime("%Y-%m-%d")}
    all_records = []
    page = 1

    while True:
        if page > 1:
            params["page"] = page

        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"API error on {target_date} page {page}: {e}")
            break

        data = resp.json()

        # API может вернуть список или объект с ключами data/results/items
        if isinstance(data, list):
            records = data
            has_more = False
        elif isinstance(data, dict):
            # пробуем стандартные ключи
            records = (
                data.get("data") or
                data.get("results") or
                data.get("items") or
                data.get("orders") or
                []
            )
            has_more = data.get("has_more", False) or data.get("next") is not None
        else:
            records = []
            has_more = False

        all_records.extend(records)
        log.info(f"  {target_date} page {page}: got {len(records)} records")

        if not has_more or len(records) == 0:
            break
        page += 1
        time.sleep(API_DELAY)

    return all_records


# ── Нормализация одной записи ─────────────────────────────────────────────────
def normalize(rec: dict) -> Optional[dict]:
    """
    Приводит запись из API к единому формату для вставки в БД.
    Поля могут называться по-разному — обрабатываем оба варианта.
    """
    def g(*keys, default=None):
        for k in keys:
            if k in rec:
                return rec[k]
        return default

    def num(v, default=None):
        try:
            return float(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    def to_bool(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in ("true", "1", "yes")
        return bool(v) if v is not None else False

    order_id = g("order_id", "id", "orderId")
    if not order_id:
        return None

    price    = num(g("price", "unit_price", "unitPrice"))
    cost     = num(g("cost_price", "costPrice", "cost"))
    qty      = int(g("quantity", "qty", default=1) or 1)
    disc_pct = num(g("discount_pct", "discount", "discountPct"), 0)

    revenue  = num(g("revenue", "total", "amount"))
    if revenue is None and price is not None:
        disc_amount = price * qty * (disc_pct / 100)
        revenue     = price * qty - disc_amount
    else:
        disc_amount = num(g("discount_amount", "discountAmount"), 0)

    profit = num(g("profit", "margin"))
    if profit is None and revenue is not None and cost is not None:
        profit = revenue - cost * qty

    return {
        "order_id":        str(order_id),
        "order_date":      g("order_date", "date", "orderDate"),
        "order_datetime":  g("order_datetime", "datetime", "created_at", "createdAt"),
        "customer_id":     g("customer_id", "customerId", "client_id"),
        "customer_name":   g("customer_name", "customerName", "name"),
        "customer_email":  g("customer_email", "email"),
        "customer_city":   g("customer_city", "city", "region"),
        "customer_gender": g("customer_gender", "gender"),
        "product_id":      g("product_id", "productId", "sku"),
        "product_name":    g("product_name", "productName", "product"),
        "category":        g("category", "product_category"),
        "subcategory":     g("subcategory", "sub_category", "subCategory"),
        "brand":           g("brand"),
        "price":           price,
        "cost_price":      cost,
        "quantity":        qty,
        "discount_pct":    disc_pct,
        "discount_amount": disc_amount,
        "revenue":         revenue,
        "profit":          profit,
        "payment_method":  g("payment_method", "payment", "paymentMethod"),
        "delivery_days":   g("delivery_days", "deliveryDays", "delivery"),
        "is_returned":     to_bool(g("is_returned", "returned", "isReturned")),
        "rating":          num(g("rating", "review_score", "score")),
    }


# ── Вставка в БД ──────────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO raw_orders (
    order_id, order_date, order_datetime,
    customer_id, customer_name, customer_email, customer_city, customer_gender,
    product_id, product_name, category, subcategory, brand,
    price, cost_price, quantity, discount_pct, discount_amount,
    revenue, profit, payment_method, delivery_days, is_returned, rating
) VALUES (
    %(order_id)s, %(order_date)s, %(order_datetime)s,
    %(customer_id)s, %(customer_name)s, %(customer_email)s,
    %(customer_city)s, %(customer_gender)s,
    %(product_id)s, %(product_name)s, %(category)s, %(subcategory)s, %(brand)s,
    %(price)s, %(cost_price)s, %(quantity)s, %(discount_pct)s, %(discount_amount)s,
    %(revenue)s, %(profit)s, %(payment_method)s, %(delivery_days)s,
    %(is_returned)s, %(rating)s
)
ON CONFLICT (order_id) DO NOTHING;
"""

def upsert_records(conn, records: list[dict]) -> int:
    rows = [normalize(r) for r in records]
    rows = [r for r in rows if r is not None]
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)


# ── Публичный интерфейс ───────────────────────────────────────────────────────
def fetch_and_store(target_date: date) -> int:
    """Забирает данные за один день и записывает в БД. Возвращает кол-во строк."""
    log.info(f"Fetching {target_date}...")
    records = fetch_day(target_date)
    if not records:
        log.info(f"  No data for {target_date}")
        return 0
    with get_conn() as conn:
        n = upsert_records(conn, records)
    log.info(f"  Stored {n} rows for {target_date}")
    return n


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today() - timedelta(days=1)
    with get_conn() as conn:
        ensure_schema(conn)
    fetch_and_store(d)
