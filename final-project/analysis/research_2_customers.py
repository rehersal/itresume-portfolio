"""
research_2_customers.py
─────────────────────────────────────────────────────────────────────────────
Исследование 2: Работа с клиентской базой. Как увеличить LTV?

Методы:
  - RFM-сегментация
  - Когортный анализ удержания
  - Анализ LTV по сегментам
  - Воронка повторных покупок
  - Анализ времени между заказами (inter-purchase interval)
─────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from datetime import timedelta
warnings.filterwarnings('ignore')

import matplotlib.font_manager as fm
for f in ['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
          '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf']:
    if os.path.exists(f):
        fm.fontManager.addfont(f)
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 130

PALETTE = ['#2E4057', '#048A81', '#E4572E', '#FFB703', '#8338EC',
           '#54C6EB', '#06D6A0', '#EF476F']

OUT = 'output/research2'
os.makedirs(OUT, exist_ok=True)

def save(name):
    path = f'{OUT}/{name}.png'
    plt.savefig(path, bbox_inches='tight', facecolor='white')
    plt.close('all')
    return path


# ════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА
# ════════════════════════════════════════════════════════════════════════════
def load_data():
    dsn = os.getenv("DB_DSN")
    if dsn:
        try:
            import psycopg2
            conn = psycopg2.connect(dsn)
            df = pd.read_sql("""
                SELECT order_id, order_date, customer_id, customer_city,
                       customer_gender, category, revenue, profit, is_returned, rating
                FROM raw_orders
                WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
            """, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"DB failed: {e} — demo mode")

    # Demo
    np.random.seed(7)
    N = 25000
    cats = ['Электроника','Одежда','Дом и сад','Спорт','Красота','Книги','Детские товары']
    cat_w = [0.20, 0.22, 0.15, 0.12, 0.13, 0.08, 0.10]

    # Клиенты с разной частотой покупок (Парето)
    n_clients = 4000
    client_ids = [f'C{i:05d}' for i in range(1, n_clients+1)]
    client_freq = np.random.pareto(1.5, n_clients) + 1
    client_freq = client_freq / client_freq.sum()

    customers = np.random.choice(client_ids, N, p=client_freq)
    categories = np.random.choice(cats, N, p=cat_w)
    price_m = {'Электроника': 18000, 'Одежда': 3500, 'Дом и сад': 5000,
               'Спорт': 4500, 'Красота': 1800, 'Книги': 600, 'Детские товары': 3000}
    price_s = {'Электроника': 10000, 'Одежда': 2000, 'Дом и сад': 3000,
               'Спорт': 3000, 'Красота': 900, 'Книги': 300, 'Детские товары': 1800}
    revenue = np.array([max(50, np.random.normal(price_m[c], price_s[c]))
                        for c in categories])
    profit = revenue * np.random.uniform(0.20, 0.55, N)

    dates = pd.to_datetime('2023-01-01') + pd.to_timedelta(
        np.random.randint(0, 365, N), unit='D')
    cities = np.random.choice(['Москва','СПб','Новосибирск','Екатеринбург','Краснодар'],
                               N, p=[0.30,0.20,0.15,0.15,0.20])
    gender = np.random.choice(['M','F',''], N, p=[0.42, 0.50, 0.08])
    return_prob = np.where(np.array(categories) == 'Одежда', 0.12, 0.05)
    is_returned = np.random.binomial(1, return_prob).astype(bool)

    return pd.DataFrame({
        'order_id':       [f'O{i:06d}' for i in range(N)],
        'order_date':     dates,
        'customer_id':    customers,
        'customer_city':  cities,
        'customer_gender':gender,
        'category':       categories,
        'revenue':        np.round(revenue, 2),
        'profit':         np.round(profit, 2),
        'is_returned':    is_returned,
        'rating':         np.clip(np.round(np.random.normal(4.0, 0.9, N), 1), 1, 5),
    })


df = load_data()
df['order_date'] = pd.to_datetime(df['order_date'])
df_clean = df[~df['is_returned']].copy()
SNAPSHOT = pd.Timestamp('2024-01-01')


# ════════════════════════════════════════════════════════════════════════════
# RFM
# ════════════════════════════════════════════════════════════════════════════
rfm = df_clean.groupby('customer_id').agg(
    last_date=('order_date', 'max'),
    frequency=('order_id', 'count'),
    monetary=('revenue', 'sum'),
).reset_index()
rfm['recency'] = (SNAPSHOT - rfm['last_date']).dt.days

rfm['R'] = pd.qcut(rfm['recency'],   5, labels=[5,4,3,2,1]).astype(int)
rfm['F'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5]).astype(int)
rfm['M'] = pd.qcut(rfm['monetary'].rank(method='first'),  5, labels=[1,2,3,4,5]).astype(int)
rfm['rfm_score'] = rfm['R']*100 + rfm['F']*10 + rfm['M']

def segment(row):
    r, f, m = row['R'], row['F'], row['M']
    if r >= 4 and f >= 4:             return 'Champions'
    if r >= 3 and f >= 3:             return 'Loyal'
    if r >= 4 and f < 2:              return 'New Customers'
    if r >= 3 and f < 3:              return 'Potential Loyal'
    if r == 2 and f >= 3:             return 'At Risk'
    if r <= 2 and f <= 2 and m >= 3:  return 'Can\'t Lose'
    if r <= 2:                        return 'Lost'
    return 'Others'

rfm['segment'] = rfm.apply(segment, axis=1)
seg_stats = rfm.groupby('segment').agg(
    count=('customer_id', 'count'),
    avg_monetary=('monetary', 'mean'),
    avg_frequency=('frequency', 'mean'),
    avg_recency=('recency', 'mean'),
).reset_index()
seg_stats['share'] = seg_stats['count'] / seg_stats['count'].sum() * 100

print("\n=== RFM Segments ===")
print(seg_stats.sort_values('count', ascending=False).to_string(index=False))


# ════════════════════════════════════════════════════════════════════════════
# КОГОРТНЫЙ АНАЛИЗ
# ════════════════════════════════════════════════════════════════════════════
df_clean['cohort_month'] = df_clean.groupby('customer_id')['order_date'] \
    .transform('min').dt.to_period('M')
df_clean['order_month']  = df_clean['order_date'].dt.to_period('M')
df_clean['month_number'] = (df_clean['order_month'] - df_clean['cohort_month']).apply(
    lambda x: x.n if hasattr(x, 'n') else int(x))

cohort_data = df_clean.groupby(['cohort_month', 'month_number'])['customer_id'] \
    .nunique().reset_index(name='customers')
cohort_pivot = cohort_data.pivot(index='cohort_month', columns='month_number', values='customers')
cohort_size  = cohort_pivot[0]
retention    = cohort_pivot.div(cohort_size, axis=0) * 100
retention    = retention.iloc[:, :12]  # первые 12 месяцев


# ════════════════════════════════════════════════════════════════════════════
# INTER-PURCHASE INTERVAL
# ════════════════════════════════════════════════════════════════════════════
repeat = df_clean[df_clean['customer_id'].isin(
    df_clean.groupby('customer_id')['order_id'].count()[
        lambda x: x >= 2].index)].copy()
repeat = repeat.sort_values(['customer_id', 'order_date'])
repeat['prev_date'] = repeat.groupby('customer_id')['order_date'].shift(1)
repeat['interval_days'] = (repeat['order_date'] - repeat['prev_date']).dt.days
intervals = repeat['interval_days'].dropna()
median_interval = intervals.median()
print(f"\nМедианный интервал между заказами: {median_interval:.0f} дней")


# ════════════════════════════════════════════════════════════════════════════
# LTV по сегментам
# ════════════════════════════════════════════════════════════════════════════
ltv = rfm[['customer_id', 'segment', 'monetary', 'frequency']].copy()
ltv['ltv_projected'] = ltv['monetary'] * (ltv['frequency'] / 12 * 24)  # проекция на 24 мес


# ════════════════════════════════════════════════════════════════════════════
# ГРАФИКИ
# ════════════════════════════════════════════════════════════════════════════

# ── Г1: RFM-сегменты — bubble chart ─────────────────────────────────────────
seg_stats_sorted = seg_stats.sort_values('avg_monetary', ascending=False)
fig, ax = plt.subplots(figsize=(12, 6))
fig.patch.set_facecolor('white')
colors_seg = PALETTE[:len(seg_stats_sorted)]
scatter = ax.scatter(seg_stats_sorted['avg_frequency'],
                     seg_stats_sorted['avg_monetary'],
                     s=seg_stats_sorted['count'] * 1.5,
                     c=colors_seg, alpha=0.75, edgecolors='white', lw=2)
for _, row in seg_stats_sorted.iterrows():
    ax.annotate(f'{row["segment"]}\n({row["count"]} кл., {row["share"]:.1f}%)',
                (row['avg_frequency'], row['avg_monetary']),
                fontsize=8, ha='center', va='bottom',
                xytext=(0, 10), textcoords='offset points')
ax.set_xlabel('Средняя частота покупок', fontsize=11)
ax.set_ylabel('Средняя выручка (LTV за год), ₽', fontsize=11)
ax.set_title('RFM-сегменты клиентской базы 2023\n(размер кружка = число клиентов)',
             fontsize=13, fontweight='bold')
ax.spines[['top','right']].set_visible(False)
ax.grid(alpha=0.3)
plt.tight_layout()
save('01_rfm_segments')


# ── Г2: Когортное удержание — heatmap ────────────────────────────────────────
fig, ax = plt.subplots(figsize=(14, 6))
fig.patch.set_facecolor('white')
ret_display = retention.fillna(0)
try:
    sns.heatmap(ret_display, annot=True, fmt='.0f', cmap='RdYlGn',
                ax=ax, linewidths=0.3, linecolor='white',
                vmin=0, vmax=100,
                annot_kws={'size': 8})
except Exception:
    im = ax.imshow(ret_display.values, cmap='RdYlGn', aspect='auto', vmin=0, vmax=100)
    for i in range(ret_display.shape[0]):
        for j in range(ret_display.shape[1]):
            v = ret_display.values[i, j]
            if not np.isnan(v):
                ax.text(j, i, f'{v:.0f}', ha='center', va='center', fontsize=7)
    plt.colorbar(im, ax=ax)
    ax.set_xticks(range(len(ret_display.columns)))
    ax.set_yticks(range(len(ret_display.index)))
    ax.set_xticklabels(ret_display.columns)
    ax.set_yticklabels([str(p) for p in ret_display.index], rotation=0)
ax.set_title('Когортное удержание клиентов, % (по месяцу первой покупки)', fontsize=12, fontweight='bold')
ax.set_xlabel('Месяц с момента первой покупки', fontsize=10)
ax.set_ylabel('Когорта (месяц первой покупки)', fontsize=10)
plt.tight_layout()
save('02_cohort_retention')


# ── Г3: Воронка повторных покупок ─────────────────────────────────────────────
purchase_counts = df_clean.groupby('customer_id')['order_id'].count()
funnel_labels = ['1 покупка', '2 покупки', '3–5 покупок', '6–10 покупок', '11+ покупок']
funnel_vals = [
    (purchase_counts == 1).sum(),
    (purchase_counts == 2).sum(),
    ((purchase_counts >= 3) & (purchase_counts <= 5)).sum(),
    ((purchase_counts >= 6) & (purchase_counts <= 10)).sum(),
    (purchase_counts >= 11).sum(),
]
total_clients = sum(funnel_vals)

fig, ax = plt.subplots(figsize=(10, 5))
fig.patch.set_facecolor('white')
bars = ax.barh(funnel_labels[::-1], [v/total_clients*100 for v in funnel_vals[::-1]],
               color=PALETTE[:5][::-1], alpha=0.85, edgecolor='white')
for bar, val, pct in zip(bars, funnel_vals[::-1], [v/total_clients*100 for v in funnel_vals[::-1]]):
    ax.text(pct + 0.3, bar.get_y() + bar.get_height()/2,
            f'{val:,} клиентов ({pct:.1f}%)'.replace(',', ' '),
            va='center', fontsize=9)
ax.set_xlabel('Доля клиентов, %', fontsize=10)
ax.set_title('Воронка повторных покупок (2023)', fontsize=13, fontweight='bold')
ax.spines[['top','right']].set_visible(False)
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
save('03_purchase_funnel')


# ── Г4: Распределение интервалов между заказами ───────────────────────────────
fig, ax = plt.subplots(figsize=(11, 4.5))
fig.patch.set_facecolor('white')
ax.hist(intervals[intervals <= 200], bins=40, color=PALETTE[0], alpha=0.8, edgecolor='white')
ax.axvline(median_interval, color=PALETTE[2], lw=2.5, ls='--',
           label=f'Медиана: {median_interval:.0f} дней')
ax.axvline(intervals.mean(), color=PALETTE[1], lw=2, ls=':',
           label=f'Среднее: {intervals.mean():.0f} дней')
ax.set_xlabel('Дней между заказами', fontsize=10)
ax.set_ylabel('Число пар заказов', fontsize=10)
ax.set_title('Распределение интервалов между покупками (повторные клиенты)',
             fontsize=12, fontweight='bold')
ax.legend(fontsize=10)
ax.spines[['top','right']].set_visible(False)
plt.tight_layout()
save('04_purchase_intervals')


# ── Г5: LTV по сегментам ──────────────────────────────────────────────────────
ltv_seg = ltv.groupby('segment').agg(
    customers=('customer_id', 'count'),
    avg_ltv=('monetary', 'mean'),
    total_ltv=('monetary', 'sum'),
).sort_values('avg_ltv', ascending=False).reset_index()

fig, ax = plt.subplots(figsize=(10, 5))
fig.patch.set_facecolor('white')
bars = ax.bar(range(len(ltv_seg)), ltv_seg['avg_ltv'],
              color=PALETTE[:len(ltv_seg)], alpha=0.85, edgecolor='white')
ax.set_xticks(range(len(ltv_seg)))
ax.set_xticklabels(ltv_seg['segment'], rotation=20, ha='right', fontsize=9)
for bar, val, n in zip(bars, ltv_seg['avg_ltv'], ltv_seg['customers']):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200,
            f'{val:,.0f} ₽\n({n} кл.)'.replace(',', ' '),
            ha='center', va='bottom', fontsize=8)
ax.set_ylabel('Средний LTV за 2023, ₽', fontsize=10)
ax.set_title('Средний LTV по RFM-сегментам', fontsize=12, fontweight='bold')
ax.spines[['top','right']].set_visible(False)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
save('05_ltv_by_segment')


# ════════════════════════════════════════════════════════════════════════════
# ВЫВОДЫ
# ════════════════════════════════════════════════════════════════════════════
one_time = funnel_vals[0] / total_clients * 100
top2_segs = ltv_seg.head(2)['segment'].tolist()
champions_ltv = ltv_seg[ltv_seg['segment']=='Champions']['avg_ltv'].values
loyal_ltv     = ltv_seg[ltv_seg['segment']=='Loyal']['avg_ltv'].values

print("\n" + "="*60)
print("ВЫВОДЫ И РЕКОМЕНДАЦИИ (Исследование 2)")
print("="*60)
print(f"""
1. Воронка повторных покупок:
   • {one_time:.1f}% клиентов сделали только 1 заказ.
   РЕКОМЕНДАЦИЯ: запустить триггерную email-цепочку через {median_interval:.0f} дней
   после первой покупки (это медианный интервал) — «Не видели вас давно, вот
   персональная подборка». Конверсия таких цепочек: 8–15%.

2. Когортное удержание:
   • Типичное удержание на M3 составляет ~{retention.iloc[:,3].mean():.0f}%.
   РЕКОМЕНДАЦИЯ: внедрить программу лояльности с накопительными баллами —
   клиент видит прогресс и имеет стимул вернуться до «сгорания» баллов.
   Целевой KPI: поднять удержание M3 до 25%.

3. RFM-сегменты — приоритет:
   • Сегмент «Champions» — самый ценный по LTV. Нужно их удерживать:
     персональный менеджер, early access к новинкам, подарок на ДР.
   • Сегмент «At Risk» — бывшие активные клиенты. Реактивация через
     win-back кампанию: «Соскучились! Вот скидка 15% только для вас».
   • Сегмент «Lost» — не тратить бюджет; возможен 1 последний оффер.

4. Интервалы между покупками:
   • Медиана {median_interval:.0f} дней → оптимальное окно для ремаркетинга.
   РЕКОМЕНДАЦИЯ: настроить пуш-уведомление/email на день ({median_interval:.0f} - 5) после
   последней покупки с персональной рекомендацией из той же категории.

5. Метрики для роста LTV:
   ┌──────────────────────────────────────────────────────────┐
   │ Метрика          │ Текущее  │ Цель     │ Инструмент      │
   ├──────────────────┼──────────┼──────────┼─────────────────┤
   │ Retention M1     │ ~{retention.iloc[:,1].mean():.0f}%     │ 35%      │ Welcome-серия   │
   │ Avg. frequency   │ {rfm['frequency'].mean():.1f}      │ 4.5      │ Программа лояльн│
   │ Avg. order value │ текущий  │ +10%     │ Upsell/cross    │
   │ 1-time buyers    │ {one_time:.0f}%     │ <40%     │ Триггер письма  │
   └──────────────────┴──────────┴──────────┴─────────────────┘
""")

print(f"Графики сохранены в {OUT}/")
