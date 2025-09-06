#!/usr/bin/env python3
import argparse
import random
import string
from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml

from utils import ensure_dirs, get_logger, load_config

REQUIRED_COLUMNS = ["doc_id", "item", "category", "amount", "price", "discount"]

def rand_doc_id(shop:int, cash:int) -> str:
    # DOC-<YYYYMMDD>-<shop>-<cash>-<6alnum>
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"DOC-{datetime.now():%Y%m%d}-{shop}-{cash}-{suffix}"

def main():
    ap = argparse.ArgumentParser(description="Generate CSV dumps per shop/cash into data/")
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    gen = cfg.get("generator", {})
    random.seed(gen.get("seed", None))

    data_dir = Path(cfg.get("data_dir", "data/incoming"))
    ensure_dirs(data_dir)

    logger = get_logger("generate_data", cfg.get("logs_dir", "logs"))

    shops = int(gen.get("shops", 3))
    cash_per_shop = int(gen.get("cash_per_shop", 2))
    rec_min, rec_max = gen.get("receipts_per_cash", [50, 120])
    lines_min, lines_max = gen.get("lines_per_receipt", [1, 5])

    catalog = gen.get("items", [])
    if not catalog:
        raise ValueError("No items defined in config.yaml under generator.items")

    for shop in range(1, shops + 1):
        for cash in range(1, cash_per_shop + 1):
            rows = []
            receipts = random.randint(rec_min, rec_max)
            for _ in range(receipts):
                doc_id = rand_doc_id(shop, cash)
                n_lines = random.randint(lines_min, lines_max)
                for _ in range(n_lines):
                    item_rec = random.choice(catalog)
                    price = round(random.uniform(*item_rec["price_range"]), 2)
                    amount = random.randint(1, 5)
                    # Discount: sometimes 0, sometimes up to 20% of gross line
                    if random.random() < 0.75:
                        discount = 0.0
                    else:
                        discount = round(price * amount * random.uniform(0.05, 0.2), 2)

                    rows.append({
                        "doc_id": doc_id,
                        "item": item_rec["name"],
                        "category": item_rec["category"],
                        "amount": amount,
                        "price": price,
                        "discount": discount,
                    })

            df = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)

            filename = f"{shop}_{cash}.csv"
            out_path = data_dir / filename
            df.to_csv(out_path, index=False, encoding="utf-8")
            logger.info(f"Generated {out_path} with {len(df)} rows")

    logger.info("Done.")

if __name__ == "__main__":
    main()
