-- PostgreSQL DDL for the project (run this once if you don't use auto-creation from Python)

CREATE TABLE IF NOT EXISTS shops (
    shop_num INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS cash_registers (
    shop_num INTEGER NOT NULL REFERENCES shops(shop_num) ON DELETE CASCADE,
    cash_num INTEGER NOT NULL,
    PRIMARY KEY (shop_num, cash_num)
);

CREATE TABLE IF NOT EXISTS sales_lines (
    id BIGSERIAL PRIMARY KEY,
    doc_id TEXT NOT NULL,
    shop_num INTEGER NOT NULL REFERENCES shops(shop_num) ON DELETE RESTRICT,
    cash_num INTEGER NOT NULL,
    row_num INTEGER NOT NULL,
    item TEXT NOT NULL,
    category TEXT NOT NULL,
    amount INTEGER NOT NULL CHECK (amount >= 0),
    price NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    discount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (discount >= 0),
    line_total NUMERIC(12,2) NOT NULL,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file TEXT NOT NULL,
    CONSTRAINT fk_cash FOREIGN KEY (shop_num, cash_num)
        REFERENCES cash_registers (shop_num, cash_num) ON DELETE RESTRICT,
    CONSTRAINT uniq_doc_row UNIQUE (doc_id, shop_num, cash_num, row_num)
);
