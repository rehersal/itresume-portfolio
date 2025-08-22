import os
import pandas as pd
import configparser
from datetime import datetime, timedelta

import yfinance as yf

from pgdb import PGDatabase

config = configparser.ConfigParser()
config.read('config.ini')

def prev_business_day(d):
    # Пн=0 … Вс=6
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

start = prev_business_day(datetime.today().date() - timedelta(days=1))
end   = start + timedelta(days=1)  # end — верхняя (исключающая) граница

companies = eval(config['Companies']['Companies'])
sales_path = config['Files']['sales_path']
database_creds = config['Database']

sales_df = pd.DataFrame()
#проверка на существование файла
if os.path.exists(sales_path):
    sales_df = pd.read_csv(sales_path)
    os.remove(sales_path)

historical_d = {}

for company in companies:
    historical_d[company] = yf.download(
        tickers = company,
        start = start,
        end = end,
        interval = '1d',
        auto_adjust=False,
        progress=False
    ).reset_index()
    print(company, historical_d[company].head())


database = PGDatabase(
    host = database_creds['host'],
    database = database_creds['database'],
    user = database_creds['user'],
    password = database_creds['password']
)

for i, row in sales_df.iterrows():
    dt = pd.to_datetime(row['dt']).strftime("%Y-%m-%d")  # нормализуем дату
    query = f"insert into sales values ('{dt}', '{row['company']}', '{row['transaction_type']}', {row['amount']})"
    print(query)
    database.post(query, args = None)

for company, data in historical_d.items():
    if data.empty:
        continue

    if isinstance(data.columns, pd.MultiIndex):
        # дата — это колонка с первым уровнем 'Date' (второй уровень обычно пустой)
        date_col = next(c for c in data.columns if isinstance(c, tuple) and c[0] == 'Date')
        open_col = ('Open',  company)
        close_col = ('Close', company)
    else:
        date_col = 'Date'
        open_col = 'Open'
        close_col = 'Close'

    for _, row in data.iterrows():
        date_val = row[date_col]
        # приводим к 'YYYY-MM-DD' надёжно
        dt_iso = pd.to_datetime(date_val).strftime('%Y-%m-%d')

        open_val = row[open_col]
        close_val = row[close_col]

        query = f"insert into stock values ('{dt_iso}', '{company}', {open_val}, {close_val})"
        database.post(query)g