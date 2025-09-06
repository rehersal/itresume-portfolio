#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
from datetime import datetime
import shutil

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, Numeric, DateTime,     ForeignKey, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine

from utils import ensure_dirs, get_logger, load_config, load_env
import os

from sqlalchemy.dialects.postgresql import insert as pg_insert

REQUIRED_COLUMNS = ["doc_id", "item", "category", "amount", "price", "discount"]
FILENAME_RE = re.compile(r"^(?P<shop>\d+)_(?P<cash>\d+)\.csv$", re.IGNORECASE)

def get_engine_from_env() -> Engine:
    load_env()  # loads .env if present
    db_url = os.getenv("DATABASE_URL", "sqlite:///sales.db")
    return create_engine(db_url, future=True)

def define_schema(metadata: MetaData):
    shops = Table(
        "shops", metadata,
        Column("shop_num", Integer, primary_key=True)
    )
    cash_registers = Table(
        "cash_registers", metadata,
        Column("shop_num", Integer, ForeignKey("shops.shop_num", ondelete="CASCADE"), nullable=False),
        Column("cash_num", Integer, nullable=False),
        UniqueConstraint("shop_num", "cash_num", name="pk_cash"),
    )
    sales_lines = Table(
        "sales_lines", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("doc_id", Text, nullable=False),
        Column("shop_num", Integer, ForeignKey("shops.shop_num", ondelete="RESTRICT"), nullable=False),
        Column("cash_num", Integer, nullable=False),
        Column("row_num", Integer, nullable=False),
        Column("item", Text, nullable=False),
        Column("category", Text, nullable=False),
        Column("amount", Integer, nullable=False),
        Column("price", Numeric(12,2), nullable=False),
        Column("discount", Numeric(12,2), nullable=False),
        Column("line_total", Numeric(12,2), nullable=False),
        Column("load_ts", DateTime, nullable=False, default=datetime.utcnow),
        Column("source_file", Text, nullable=False),
        UniqueConstraint("doc_id", "shop_num", "cash_num", "row_num", name="uniq_doc_row")
    )
    return shops, cash_registers, sales_lines

def coerce_and_validate(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize columns to lower-case
    df.columns = [c.strip().lower() for c in df.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Type coercion
    df["amount"] = pd.to_numeric(df["amount"], errors="raise", downcast="integer")
    for col in ["price", "discount"]:
        df[col] = pd.to_numeric(df[col], errors="raise").round(2)

    # Basic validation
    if (df["amount"] < 0).any():
        raise ValueError("Negative amounts are not allowed")
    if (df["price"] < 0).any() or (df["discount"] < 0).any():
        raise ValueError("Negative price/discount are not allowed")

    return df

def process_file(engine: Engine, tables, file_path: Path, shop_num: int, cash_num: int, logger):
    shops, cash_registers, sales_lines = tables

    df = pd.read_csv(file_path, encoding="utf-8")
    df = coerce_and_validate(df)
    df.insert(0, "row_num", range(1, len(df)+1))
    df["line_total"] = (df["amount"] * df["price"] - df["discount"]).round(2)
    df["shop_num"] = shop_num
    df["cash_num"] = cash_num
    df["source_file"] = str(file_path)

    with engine.begin() as conn:
        # UPSERT (PostgreSQL): не ломаем транзакцию, если запись уже существует
        stmt_shop = pg_insert(shops).values(shop_num=shop_num) \
            .on_conflict_do_nothing(index_elements=[shops.c.shop_num])
        conn.execute(stmt_shop)

        stmt_cash = pg_insert(cash_registers).values(shop_num=shop_num, cash_num=cash_num) \
            .on_conflict_do_nothing(index_elements=[cash_registers.c.shop_num, cash_registers.c.cash_num])
        conn.execute(stmt_cash)

        # Insert lines (skip duplicates)
        cols = ["doc_id","shop_num","cash_num","row_num","item","category","amount","price","discount","line_total","source_file"]
        for rec in df[cols].to_dict(orient="records"):
            try:
                conn.execute(sales_lines.insert().values(**rec))
            except IntegrityError:
                # duplicate line -> ignore
                logger.info(f"Duplicate skipped for {file_path.name}: {rec['doc_id']}/{rec['row_num']}")
                continue

def main():
    ap = argparse.ArgumentParser(description="Load CSV files into the database")
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    data_dir = Path(cfg.get("data_dir", "data/incoming"))
    processed_dir = Path(cfg.get("processed_dir", "data/processed"))
    rejected_dir = Path(cfg.get("rejected_dir", "data/rejected"))
    ensure_dirs(data_dir, processed_dir, rejected_dir)

    logger = get_logger("load_to_db", cfg.get("logs_dir", "logs"))
    engine = get_engine_from_env()

    metadata = MetaData()
    tables = define_schema(metadata)
    metadata.create_all(engine)

    for file in sorted(data_dir.iterdir()):
        if not file.is_file():
            continue
        m = FILENAME_RE.match(file.name)
        if not m or file.suffix.lower() != ".csv":
            logger.warning(f"Ignored non-matching file: {file.name}")
            # optionally move to rejected
            shutil.move(str(file), rejected_dir / file.name)
            continue

        shop_num = int(m.group("shop"))
        cash_num = int(m.group("cash"))
        try:
            process_file(engine, tables, file, shop_num, cash_num, logger)
        except Exception as e:
            logger.exception(f"Failed to process {file.name}: {e}")
            shutil.move(str(file), rejected_dir / file.name)
        else:
            # Move to processed/YYYY-MM-DD
            date_dir = processed_dir / datetime.now().strftime("%Y-%m-%d")
            ensure_dirs(date_dir.as_posix())
            shutil.move(str(file), date_dir / file.name)
            logger.info(f"Processed {file.name} -> {date_dir}")

    logger.info("Done.")

if __name__ == "__main__":
    main()
