# -*- coding: utf-8 -*-
"""
Retail case: products × orders
Задачи:
1) Самая ходовая товарная группа (таблица + barchart)
2) Распределение продаж по подкатегориям (таблица)
3) Средний чек на 13.01.2022
4) Доля промо в категории "Сыры" (в штуках) + piechart
5) Маржа по категориям (руб и %) + 2 горизонтальных барчарта
6) ABC-анализ по подкатегориям: по количеству и по выручке + итоговая группа

Правило: во всех задачах, кроме среднего чека, игнорируем товары, которых нет в products (inner join).
"""

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------
# CONFIG — укажите пути к своим файлам, если нужно
# ---------------------------------------------------------------------
ORDERS_PATH   = Path(r"orders.xlsx")   # orders
PRODUCTS_PATH = Path(r"products.xlsx")    # products
RESULTS_DIR   = Path("results_case")
CATEGORY_FOR_PROMO = "Сыры"
ABC_THRESHOLDS = (0.80, 0.95)  # A до 80%, B до 95%, далее C

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------
orders = pd.read_excel(ORDERS_PATH, sheet_name=0, parse_dates=["accepted_at"])
products = pd.read_excel(PRODUCTS_PATH, sheet_name=0)

# Для задач с категориями: берём только совпавшие product_id
ord_prod = orders.merge(products, on="product_id", how="inner")

# Подготовим базовые величины, пригодятся дальше
ord_prod["revenue"] = ord_prod["price"] * ord_prod["quantity"]
ord_prod["cost"]    = ord_prod["cost_price"] * ord_prod["quantity"]
ord_prod["date"]    = ord_prod["accepted_at"].dt.date

# ---------------------------------------------------------------------
# 1) Самая ходовая товарная группа
# ---------------------------------------------------------------------
cat_units = (ord_prod.groupby("level1", as_index=False)["quantity"]
                    .sum()
                    .rename(columns={"quantity": "units_sold"})
                    .sort_values("units_sold", ascending=False))

top_cat, top_units = cat_units.iloc[0]["level1"], int(cat_units.iloc[0]["units_sold"])
print("=== Самая ходовая товарная группа ===")
print(f"Категория: {top_cat} — продано {top_units} шт.")
print("\nТаблица по категориям (шт):")
print(cat_units.to_string(index=False))

# Barchart
plt.figure(figsize=(11,5))
plt.bar(cat_units["level1"], cat_units["units_sold"])
plt.title("Проданные позиции по категориям (шт)")
plt.xlabel("Категория (level1)")
plt.ylabel("Штук")
plt.xticks(rotation=30, ha="right")
for x, y in zip(range(len(cat_units)), cat_units["units_sold"]):
    plt.text(x, y, str(int(y)), ha="center", va="bottom", fontsize=9)
plt.tight_layout()
plt.savefig(RESULTS_DIR / "01_barchart_units_by_category.png", dpi=150)

# ---------------------------------------------------------------------
# 2) Распределение продаж по подкатегориям в разрезе категорий
# ---------------------------------------------------------------------
subcat_units = (ord_prod.groupby(["level1","level2"], as_index=False)["quantity"]
                       .sum()
                       .rename(columns={"quantity": "units_sold"}))
subcat_units["units_in_cat"] = subcat_units.groupby("level1")["units_sold"].transform("sum")
subcat_units["share_in_cat"] = subcat_units["units_sold"] / subcat_units["units_in_cat"]
subcat_units = subcat_units.sort_values(["level1","units_sold"], ascending=[True, False])

print("\n=== Распределение по подкатегориям (шт и доля в категории) ===")
print(subcat_units[["level1","level2","units_sold","share_in_cat"]].to_string(index=False))

subcat_units.to_excel(RESULTS_DIR / "02_subcategory_distribution.xlsx", index=False)

# ---------------------------------------------------------------------
# 3) Средний чек на 13.01.2022 (orders только)
# ---------------------------------------------------------------------
target_date = pd.Timestamp("2022-01-13").date()
orders_day = orders[orders["accepted_at"].dt.date == target_date]
bill_sums = (orders_day.assign(sum_item=orders_day["price"] * orders_day["quantity"])
                        .groupby("order_id", as_index=False)["sum_item"].sum())
avg_check = float(bill_sums["sum_item"].mean()) if len(bill_sums) else np.nan
print(f"\n=== Средний чек на {target_date.strftime('%d.%m.%Y')} ===")
print(f"Средний чек: {avg_check:.2f} ₽")

# ---------------------------------------------------------------------
# 4) Доля промо в категории "Сыры" (в штуках) + piechart
#    Промо: price < regular_price
# ---------------------------------------------------------------------
cheese = ord_prod[ord_prod["level1"] == CATEGORY_FOR_PROMO].copy()
promo_units = nonpromo_units = total_units = 0
promo_share = np.nan
if not cheese.empty:
    cheese["is_promo"] = cheese["price"] < cheese["regular_price"]
    promo_units    = int(cheese.loc[cheese["is_promo"], "quantity"].sum())
    total_units    = int(cheese["quantity"].sum())
    nonpromo_units = total_units - promo_units
    promo_share    = (promo_units / total_units) if total_units else 0.0

print(f"\n=== Доля промо (шт) в категории '{CATEGORY_FOR_PROMO}' ===")
print(f"Промо-шт: {promo_units}, Не промо-шт: {nonpromo_units}, Доля промо: {promo_share:.1%}")

plt.figure(figsize=(6,6))
plt.pie([promo_units, nonpromo_units],
        labels=["Промо", "Не промо"],
        autopct="%.1f%%",
        startangle=90)
plt.title(f"Доля промо в категории '{CATEGORY_FOR_PROMO}' (в штуках)")
plt.tight_layout()
plt.savefig(RESULTS_DIR / "04_pie_promo_share_cheese.png", dpi=150)

# ---------------------------------------------------------------------
# 5) Маржа по категориям (руб и %) + 2 горизонтальных барчарта
# ---------------------------------------------------------------------
margins = (ord_prod.groupby("level1", as_index=False)[["revenue","cost"]].sum())
margins["margin_rub"] = margins["revenue"] - margins["cost"]
margins["margin_pct"] = np.where(
    margins["revenue"] > 0, margins["margin_rub"] / margins["revenue"] * 100, np.nan
)
print("\n=== Маржа по категориям (руб и %) ===")
tmp = margins[["level1","margin_rub","margin_pct"]].copy()
tmp["margin_pct"] = tmp["margin_pct"].map(lambda x: f"{x:.1f}%" if pd.notnull(x) else "—")
print(tmp.to_string(index=False))
margins.to_excel(RESULTS_DIR / "05_margins_by_category.xlsx", index=False)

# barh: руб
margins_sorted_rub = margins.sort_values("margin_rub", ascending=False)
plt.figure(figsize=(10,6))
plt.barh(margins_sorted_rub["level1"], margins_sorted_rub["margin_rub"])
plt.title("Маржа по категориям (руб)")
plt.xlabel("Маржа, руб")
plt.ylabel("Категория")
for y, v in zip(range(len(margins_sorted_rub)), margins_sorted_rub["margin_rub"]):
    plt.text(v, y, f"{int(v)}", va="center", ha="left", fontsize=9)
plt.tight_layout()
plt.savefig(RESULTS_DIR / "05_barh_margin_rub.png", dpi=150)

# barh: %
margins_sorted_pct = margins.sort_values("margin_pct", ascending=False)
plt.figure(figsize=(10,6))
plt.barh(margins_sorted_pct["level1"], margins_sorted_pct["margin_pct"])
plt.title("Маржа по категориям (%)")
plt.xlabel("Маржа, %")
plt.ylabel("Категория")
for y, v in zip(range(len(margins_sorted_pct)), margins_sorted_pct["margin_pct"]):
    if pd.notnull(v):
        plt.text(v, y, f"{v:.1f}%", va="center", ha="left", fontsize=9)
plt.tight_layout()
plt.savefig(RESULTS_DIR / "05_barh_margin_pct.png", dpi=150)

# ---------------------------------------------------------------------
# 6) ABC-анализ по подкатегориям (level2)
# ---------------------------------------------------------------------
abc_base = (ord_prod.groupby("level2", as_index=False)
                     .agg(units_sold=("quantity","sum"),
                          sales=("revenue","sum")))

def abc_class(series: pd.Series, thresholds=(0.80, 0.95)) -> pd.Series:
    """Вернёт A/B/C в порядке исходного индекса series,
    где A — кум. доля до t0, B — до t1, остальное — C."""
    s = series.sort_values(ascending=False)
    cum = s.cumsum() / s.sum()
    labels_sorted = pd.cut(
        cum, bins=[-np.inf, thresholds[0], thresholds[1], np.inf],
        labels=["A","B","C"]
    )
    # вернуть к исходному индексу
    return series.index.to_series().map(dict(zip(s.index, labels_sorted)))

abc = abc_base.copy()
abc["ABC_qty"]   = abc_class(abc.set_index("level2")["units_sold"], ABC_THRESHOLDS).values
abc["ABC_sales"] = abc_class(abc.set_index("level2")["sales"],      ABC_THRESHOLDS).values
abc["ABC_both"]  = abc["ABC_qty"].astype(str) + " " + abc["ABC_sales"].astype(str)

abc = abc.sort_values(["ABC_sales","sales"], ascending=[True, False])
print("\n=== ABC-анализ по подкатегориям ===")
print(abc.to_string(index=False))

abc.to_excel(RESULTS_DIR / "06_abc_by_subcategory.xlsx", index=False)

# Показать все графики в интерактиве
plt.show()

print(f"\nГотово. Файлы с таблицами и графиками сохранены в: {RESULTS_DIR.resolve()}")