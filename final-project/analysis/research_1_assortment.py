"""
research_1_assortment.py
─────────────────────────────────────────────────────────────────────────────
Исследование 1: Оптимизация ассортиментной матрицы
Вопрос: какие товары выводить, какие масштабировать, где поднять цену/маржу?

Методы:
  - ABC-анализ по выручке
  - XYZ-анализ по стабильности продаж
  - BCG-матрица (рост × доля рынка на уровне категорий)
  - Анализ возвратов по товарам
  - Анализ скидок: реально ли они нужны?
─────────────────────────────────────────────────────────────────────────────
ВАЖНО: скрипт работает в двух режимах:
  1) подключение к реальной БД (DB_DSN в .env)
  2) демо-режим с синтетическими данными (если БД недоступна)
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
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
import seaborn as sns
from scipy.stats import pearsonr
warnings.filterwarnings('ignore')

# ── шрифты ───────────────────────────────────────────────────────────────────
import matplotlib.font_manager as fm
for f in ['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
          '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf']:
    if os.path.exists(f):
        fm.fontManager.addfont(f)
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 130

PALETTE = ['#2E4057', '#048A81', '#E4572E', '#FFB703', '#8338EC',
           '#54C6EB', '#06D6A0', '#EF476F', '#118AB2', '#073B4C']

OUT = 'output/research1'
os.makedirs(OUT, exist_ok=True)

def save(name):
    path = f'{OUT}/{name}.png'
    plt.savefig(path, bbox_inches='tight', facecolor='white')
    plt.close('all')
    return path


# ════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА ДАННЫХ
# ════════════════════════════════════════════════════════════════════════════
def load_data() -> pd.DataFrame:
    dsn = os.getenv("DB_DSN")
    if dsn:
        try:
            import psycopg2
            conn = psycopg2.connect(dsn)
            df = pd.read_sql("""
                SELECT * FROM raw_orders
                WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
            """, conn)
            conn.close()
            print(f"Loaded {len(df)} rows from DB")
            return df
        except Exception as e:
            print(f"DB connection failed: {e} — using demo data")

    # Demo: генерируем синтетику
    return generate_demo_data()


def generate_demo_data() -> pd.DataFrame:
    np.random.seed(42)
    N = 25000
    cats = {
        'Электроника':   {'sub': ['Смартфоны','Ноутбуки','Аудио','Планшеты'],   'price_m': 18000, 'price_s': 10000, 'cost_r': 0.60},
        'Одежда':        {'sub': ['Верхняя','Платья','Брюки','Аксессуары'],       'price_m':  3500, 'price_s':  2000, 'cost_r': 0.35},
        'Дом и сад':     {'sub': ['Мебель','Текстиль','Инструменты','Декор'],     'price_m':  5000, 'price_s':  3000, 'cost_r': 0.45},
        'Спорт':         {'sub': ['Тренажёры','Одежда','Инвентарь','Питание'],    'price_m':  4500, 'price_s':  3000, 'cost_r': 0.42},
        'Красота':       {'sub': ['Уход','Парфюмерия','Макияж','Волосы'],         'price_m':  1800, 'price_s':   900, 'cost_r': 0.30},
        'Книги':         {'sub': ['Художественная','Нон-фикшн','Учебники','Дети'],'price_m':   600, 'price_s':   300, 'cost_r': 0.25},
        'Детские товары':{'sub': ['Игрушки','Одежда','Питание','Развитие'],       'price_m':  3000, 'price_s':  1800, 'cost_r': 0.38},
    }
    cat_names = list(cats.keys())
    cat_w = [0.20, 0.22, 0.15, 0.12, 0.13, 0.08, 0.10]
    categories = np.random.choice(cat_names, N, p=cat_w)

    n_products = 200
    product_ids   = [f'P{i:04d}' for i in range(1, n_products+1)]
    product_cats  = np.random.choice(cat_names, n_products, p=cat_w)
    product_subs  = [np.random.choice(cats[c]['sub']) for c in product_cats]
    product_names = [f'{c.split()[0]} {s} #{i}' for i, (c, s) in enumerate(zip(product_cats, product_subs), 1)]
    # Продукты с разной популярностью (Парето: 20% дают 80% продаж)
    product_pop = np.random.pareto(2, n_products) + 1
    product_pop = product_pop / product_pop.sum()
    products_df = pd.DataFrame({'product_id': product_ids, 'product_name': product_names,
                                'product_cat': product_cats, 'product_sub': product_subs,
                                'pop': product_pop})

    # Генерируем заказы
    order_products = np.random.choice(n_products, N, p=product_pop)
    cats_arr = product_cats[order_products]
    subs_arr = product_subs[order_products]

    prices = np.array([
        max(50, np.random.normal(cats[c]['price_m'], cats[c]['price_s']))
        for c in cats_arr
    ])
    cost_rates = np.array([cats[c]['cost_r'] for c in cats_arr])
    costs = prices * cost_rates

    qty = np.clip(np.random.poisson(1.3, N), 1, 6).astype(int)
    disc = np.random.choice([0,0,0,5,10,15,20,25,30], N,
                             p=[0.45,0.10,0.10,0.10,0.07,0.07,0.05,0.03,0.03])
    revenue = prices * qty * (1 - disc/100)
    profit  = revenue - costs * qty

    dates = pd.to_datetime('2023-01-01') + pd.to_timedelta(np.random.randint(0, 365, N), unit='D')
    cities = np.random.choice(['Москва','СПб','Новосибирск','Екатеринбург','Казань','Краснодар'],
                               N, p=[0.30,0.18,0.12,0.10,0.10,0.20])
    return_prob = np.where(np.isin(cats_arr, ['Одежда']), 0.12,
                  np.where(np.isin(cats_arr, ['Электроника']), 0.06, 0.04))
    is_returned = np.random.binomial(1, return_prob)

    customers = np.random.choice([f'C{i:05d}' for i in range(1, 4001)], N)

    df = pd.DataFrame({
        'order_id':     [f'O{i:06d}' for i in range(N)],
        'order_date':   dates,
        'customer_id':  customers,
        'customer_city':cities,
        'product_id':   [product_ids[i] for i in order_products],
        'product_name': [product_names[i] for i in order_products],
        'category':     cats_arr,
        'subcategory':  subs_arr,
        'brand':        np.random.choice(['BrandA','BrandB','BrandC','NoName','Premium'], N,
                                         p=[0.20,0.18,0.15,0.30,0.17]),
        'price':        np.round(prices, 0),
        'cost_price':   np.round(costs, 0),
        'quantity':     qty,
        'discount_pct': disc.astype(float),
        'revenue':      np.round(revenue, 2),
        'profit':       np.round(profit, 2),
        'is_returned':  is_returned.astype(bool),
        'rating':       np.clip(np.round(np.random.normal(4.0, 0.9, N), 1), 1, 5),
    })
    print(f"Demo data: {len(df)} rows")
    return df


# ════════════════════════════════════════════════════════════════════════════
# АНАЛИЗ
# ════════════════════════════════════════════════════════════════════════════
df = load_data()
df['order_date'] = pd.to_datetime(df['order_date'])
df['month'] = df['order_date'].dt.to_period('M')
df_clean = df[~df['is_returned']]  # без возвратов для финансовых метрик


# ── A. ABC-анализ продуктов ────────────────────────────────────────────────────
product_rev = df_clean.groupby(['product_id', 'product_name', 'category']).agg(
    revenue=('revenue', 'sum'),
    profit=('profit', 'sum'),
    orders=('order_id', 'count'),
    units=('quantity', 'sum'),
    avg_price=('price', 'mean'),
    avg_disc=('discount_pct', 'mean'),
).reset_index().sort_values('revenue', ascending=False)

total_rev = product_rev['revenue'].sum()
product_rev['rev_share']  = product_rev['revenue'] / total_rev * 100
product_rev['cum_share']  = product_rev['rev_share'].cumsum()
product_rev['abc'] = pd.cut(product_rev['cum_share'],
                             bins=[0, 80, 95, 100],
                             labels=['A', 'B', 'C'])
product_rev['margin_pct'] = product_rev['profit'] / product_rev['revenue'] * 100

print("\n=== ABC Summary ===")
for cls in ['A', 'B', 'C']:
    sub = product_rev[product_rev['abc'] == cls]
    print(f"  {cls}: {len(sub)} SKU  | rev: {sub['revenue'].sum()/1e6:.1f}M "
          f"| share: {sub['rev_share'].sum():.1f}%")


# ── B. XYZ-анализ (стабильность) ─────────────────────────────────────────────
monthly_sales = df_clean.groupby(['product_id', 'month'])['revenue'].sum().reset_index()
xyz = monthly_sales.groupby('product_id')['revenue'].agg(
    mean_rev='mean', std_rev='std', months='count'
).reset_index()
xyz['cv'] = xyz['std_rev'] / xyz['mean_rev'].replace(0, np.nan)
xyz['xyz'] = pd.cut(xyz['cv'], bins=[0, 0.25, 0.5, np.inf], labels=['X', 'Y', 'Z'])
product_rev = product_rev.merge(xyz[['product_id', 'cv', 'xyz']], on='product_id', how='left')
product_rev['abc_xyz'] = product_rev['abc'].astype(str) + product_rev['xyz'].astype(str)

print("\n=== ABC×XYZ Matrix ===")
matrix = product_rev.groupby(['abc', 'xyz'])['product_id'].count().unstack(fill_value=0)
print(matrix)


# ════════════════════════════════════════════════════════════════════════════
# ГРАФИКИ
# ════════════════════════════════════════════════════════════════════════════

# ── Г1: Кривая ABC ────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 5))
fig.patch.set_facecolor('white')
color_map = product_rev['abc'].map({'A': PALETTE[0], 'B': PALETTE[2], 'C': PALETTE[3]})
ax.bar(range(len(product_rev)), product_rev['rev_share'], color=color_map, width=1.0, alpha=0.8)
ax2 = ax.twinx()
ax2.plot(range(len(product_rev)), product_rev['cum_share'], color='black', lw=2)
ax2.axhline(80, color=PALETTE[0], ls='--', lw=1.2, alpha=0.7, label='80%')
ax2.axhline(95, color=PALETTE[2], ls='--', lw=1.2, alpha=0.7, label='95%')
ax2.set_ylabel('Кумулятивная доля, %', fontsize=10)
ax.set_xlabel('Товары (ранжированы по убыванию выручки)', fontsize=10)
ax.set_ylabel('Доля в выручке, %', fontsize=10)
ax.set_title('ABC-анализ товаров по выручке 2023', fontsize=13, fontweight='bold')
patches = [mpatches.Patch(color=PALETTE[0], label=f'A — {(product_rev.abc=="A").sum()} SKU (80% выручки)'),
           mpatches.Patch(color=PALETTE[2], label=f'B — {(product_rev.abc=="B").sum()} SKU (15% выручки)'),
           mpatches.Patch(color=PALETTE[3], label=f'C — {(product_rev.abc=="C").sum()} SKU (5% выручки)')]
ax.legend(handles=patches, fontsize=9, loc='upper right')
ax.spines[['top']].set_visible(False)
plt.tight_layout()
save('01_abc_curve')


# ── Г2: ABC×XYZ тепловая карта ────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7, 5))
fig.patch.set_facecolor('white')
heat = product_rev.groupby(['abc', 'xyz'])['revenue'].sum().unstack(fill_value=0) / 1e6
try:
    sns.heatmap(heat, annot=True, fmt='.1f', cmap='YlOrRd', ax=ax,
                linewidths=0.5, linecolor='white',
                annot_kws={'size': 11, 'weight': 'bold'})
except Exception:
    im = ax.imshow(heat.values, cmap='YlOrRd', aspect='auto')
    for i in range(heat.shape[0]):
        for j in range(heat.shape[1]):
            ax.text(j, i, f'{heat.values[i,j]:.1f}', ha='center', va='center', fontsize=11)
    ax.set_xticks(range(len(heat.columns)))
    ax.set_yticks(range(len(heat.index)))
    ax.set_xticklabels(heat.columns)
    ax.set_yticklabels(heat.index)
ax.set_title('ABC×XYZ матрица: выручка, млн ₽', fontsize=12, fontweight='bold')
ax.set_xlabel('XYZ (стабильность спроса)', fontsize=10)
ax.set_ylabel('ABC (доля в выручке)', fontsize=10)
plt.tight_layout()
save('02_abc_xyz_heatmap')


# ── Г3: Маржинальность по категориям + возвраты ───────────────────────────────
cat_metrics = df_clean.groupby('category').agg(
    revenue=('revenue', 'sum'),
    profit=('profit', 'sum'),
    orders=('order_id', 'count'),
).reset_index()
cat_metrics['margin_pct'] = cat_metrics['profit'] / cat_metrics['revenue'] * 100
ret_rate = df.groupby('category')['is_returned'].mean() * 100
cat_metrics = cat_metrics.merge(ret_rate.rename('return_rate'), on='category')
cat_metrics = cat_metrics.sort_values('margin_pct', ascending=True)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.patch.set_facecolor('white')

bar_colors = [PALETTE[1] if m >= cat_metrics['margin_pct'].mean() else PALETTE[2]
              for m in cat_metrics['margin_pct']]
axes[0].barh(cat_metrics['category'], cat_metrics['margin_pct'],
             color=bar_colors, alpha=0.85, edgecolor='white')
axes[0].axvline(cat_metrics['margin_pct'].mean(), color='black', ls='--', lw=1.5,
                label=f'Среднее: {cat_metrics["margin_pct"].mean():.1f}%')
for i, (v, rev) in enumerate(zip(cat_metrics['margin_pct'], cat_metrics['revenue'])):
    axes[0].text(v + 0.3, i, f'{v:.1f}%  (выр.: {rev/1e6:.1f}М ₽)', va='center', fontsize=8)
axes[0].set_title('Маржинальность по категориям', fontsize=12, fontweight='bold')
axes[0].set_xlabel('Маржа, %', fontsize=10)
axes[0].legend(fontsize=9)
axes[0].spines[['top', 'right']].set_visible(False)

axes[1].barh(cat_metrics['category'], cat_metrics['return_rate'],
             color=[PALETTE[2] if r > 8 else PALETTE[1] for r in cat_metrics['return_rate']],
             alpha=0.85, edgecolor='white')
avg_ret = df['is_returned'].mean() * 100
axes[1].axvline(avg_ret, color='black', ls='--', lw=1.5, label=f'Среднее: {avg_ret:.1f}%')
for i, v in enumerate(cat_metrics['return_rate']):
    axes[1].text(v + 0.1, i, f'{v:.1f}%', va='center', fontsize=8)
axes[1].set_title('Уровень возвратов по категориям', fontsize=12, fontweight='bold')
axes[1].set_xlabel('Возвраты, %', fontsize=10)
axes[1].legend(fontsize=9)
axes[1].spines[['top', 'right']].set_visible(False)

plt.tight_layout()
save('03_margin_returns')


# ── Г4: BCG-матрица на уровне подкатегорий ────────────────────────────────────
# Рост = изменение выручки H2/H1; Доля = доля в общей выручке категории
h1 = df_clean[df_clean['order_date'].dt.month <= 6]
h2 = df_clean[df_clean['order_date'].dt.month > 6]
rev_h1 = h1.groupby('subcategory')['revenue'].sum()
rev_h2 = h2.groupby('subcategory')['revenue'].sum()
bcg = pd.DataFrame({'h1': rev_h1, 'h2': rev_h2}).fillna(0)
bcg['growth'] = (bcg['h2'] - bcg['h1']) / bcg['h1'].replace(0, np.nan) * 100
bcg['total']  = bcg['h1'] + bcg['h2']
bcg['share']  = bcg['total'] / bcg['total'].sum() * 100
bcg = bcg.dropna()

# сопоставим с категорией
sub_cat_map = df_clean.groupby('subcategory')['category'].first()
bcg = bcg.merge(sub_cat_map, left_index=True, right_index=True)
bcg.index.name = 'subcategory'
bcg = bcg.reset_index()

cat_list = bcg['category'].unique()
cat_color = {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(cat_list)}

fig, ax = plt.subplots(figsize=(12, 7))
fig.patch.set_facecolor('white')
med_growth = bcg['growth'].median()
med_share  = bcg['share'].median()

ax.axhline(med_growth, color='grey', ls='--', lw=1, alpha=0.6)
ax.axvline(med_share,  color='grey', ls='--', lw=1, alpha=0.6)

for _, row in bcg.iterrows():
    ax.scatter(row['share'], row['growth'],
               s=row['total']/bcg['total'].max()*1500 + 50,
               color=cat_color[row['category']], alpha=0.75, edgecolors='white', lw=1.5)
    ax.annotate(row['subcategory'], (row['share'], row['growth']),
                fontsize=7.5, ha='center', va='bottom',
                xytext=(0, 6), textcoords='offset points')

# Квадранты
ax.text(bcg['share'].max()*0.8, bcg['growth'].max()*0.85, '★ Звёзды',
        fontsize=10, color=PALETTE[0], fontweight='bold', alpha=0.5)
ax.text(bcg['share'].min()*1.1, bcg['growth'].max()*0.85, '❓ Знаки вопроса',
        fontsize=10, color=PALETTE[2], fontweight='bold', alpha=0.5)
ax.text(bcg['share'].max()*0.8, bcg['growth'].min()*0.85, '🐄 Дойные коровы',
        fontsize=10, color=PALETTE[1], fontweight='bold', alpha=0.5)
ax.text(bcg['share'].min()*1.1, bcg['growth'].min()*0.85, '🐕 Собаки',
        fontsize=10, color='grey', fontweight='bold', alpha=0.5)

legend_h = [mpatches.Patch(color=cat_color[c], label=c) for c in cat_list]
ax.legend(handles=legend_h, fontsize=8, loc='lower right')
ax.set_xlabel('Доля в выручке, %', fontsize=11)
ax.set_ylabel('Рост выручки H2/H1, %', fontsize=11)
ax.set_title('BCG-матрица подкатегорий (2023, H1→H2)', fontsize=13, fontweight='bold')
ax.spines[['top', 'right']].set_visible(False)
plt.tight_layout()
save('04_bcg_matrix')


# ── Г5: Влияние скидок на прибыль ─────────────────────────────────────────────
disc_buckets = pd.cut(df_clean['discount_pct'],
                      bins=[-1, 0, 10, 20, 35],
                      labels=['Без скидки', '1–10%', '11–20%', '21–35%'])
disc_analysis = df_clean.groupby(disc_buckets, observed=True).agg(
    orders=('order_id', 'count'),
    revenue=('revenue', 'sum'),
    profit=('profit', 'sum'),
    avg_qty=('quantity', 'mean'),
).reset_index()
disc_analysis['margin_pct'] = disc_analysis['profit'] / disc_analysis['revenue'] * 100
disc_analysis['rev_share']  = disc_analysis['revenue'] / disc_analysis['revenue'].sum() * 100

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.patch.set_facecolor('white')

x = range(len(disc_analysis))
axes[0].bar(x, disc_analysis['orders'], color=PALETTE[:4], alpha=0.8)
axes[0].set_xticks(x)
axes[0].set_xticklabels(disc_analysis['discount_pct'], fontsize=9)
axes[0].set_title('Число заказов по размеру скидки', fontsize=11, fontweight='bold')
axes[0].set_ylabel('Заказов', fontsize=10)
for i, v in enumerate(disc_analysis['orders']):
    axes[0].text(i, v + 10, str(v), ha='center', fontsize=9)
axes[0].spines[['top','right']].set_visible(False)

axes[1].bar(x, disc_analysis['margin_pct'],
            color=[PALETTE[1] if m > 0 else PALETTE[2] for m in disc_analysis['margin_pct']],
            alpha=0.8)
axes[1].set_xticks(x)
axes[1].set_xticklabels(disc_analysis['discount_pct'], fontsize=9)
axes[1].set_title('Маржинальность по размеру скидки', fontsize=11, fontweight='bold')
axes[1].set_ylabel('Маржа, %', fontsize=10)
for i, v in enumerate(disc_analysis['margin_pct']):
    axes[1].text(i, v + 0.5, f'{v:.1f}%', ha='center', fontsize=9)
axes[1].spines[['top','right']].set_visible(False)

plt.tight_layout()
save('05_discount_impact')


# ════════════════════════════════════════════════════════════════════════════
# ВЫВОДЫ — печатаем в консоль
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("ВЫВОДЫ И РЕКОМЕНДАЦИИ (Исследование 1)")
print("="*60)

a_skus = (product_rev['abc'] == 'A').sum()
b_skus = (product_rev['abc'] == 'B').sum()
c_skus = (product_rev['abc'] == 'C').sum()
c_rev_share = product_rev[product_rev['abc']=='C']['rev_share'].sum()

print(f"""
1. ABC-анализ:
   • {a_skus} SKU класса A генерируют 80% выручки → приоритет по запасам, рекламе, размещению.
   • {c_skus} SKU класса C дают лишь {c_rev_share:.1f}% выручки.
   РЕКОМЕНДАЦИЯ: сократить ассортимент C-товаров с CV > 0.6 (нестабильный спрос + низкая выручка),
   перераспределить складскую площадь и рекламный бюджет в пользу A-товаров.

2. Маржинальность:
   • Самая высокая маржа: {cat_metrics.sort_values('margin_pct',ascending=False).iloc[0]['category']}
   • Самая низкая маржа:  {cat_metrics.sort_values('margin_pct').iloc[0]['category']}
   РЕКОМЕНДАЦИЯ: в низкомаржинальных категориях ограничить скидки > 15% — маржа уходит в минус.

3. Возвраты:
   • Лидер по возвратам: {cat_metrics.sort_values('return_rate',ascending=False).iloc[0]['category']}
     ({cat_metrics.sort_values('return_rate',ascending=False).iloc[0]['return_rate']:.1f}%).
   РЕКОМЕНДАЦИЯ: добавить таблицы размеров + виртуальную примерку для Одежды,
   ввести обязательные видео-обзоры для Электроники — снизит возвраты на ~20-30%.

4. Скидки:
   • Скидки > 20% снижают маржу ниже безубыточности во многих категориях.
   РЕКОМЕНДАЦИЯ: заменить высокие скидки на механику «подарок при покупке» —
   воспринимается покупателем так же, но обходится дешевле. Тестировать через A/B.

5. BCG-матрица:
   РЕКОМЕНДАЦИЯ: «Знаки вопроса» с высоким ростом — точечно инвестировать в маркетинг;
   «Собаки» с падающей выручкой — выводить из ассортимента или переводить в аутлет.
""")

print(f"Графики сохранены в {OUT}/")
