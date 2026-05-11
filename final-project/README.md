# 🛒 Marketplace Analytics — Дипломный проект

Полный аналитический стек для онлайн-маркетплейса:
сбор данных по API → PostgreSQL → cron → Metabase + 2 аналитических исследования.

---

## 📋 Данные для сдачи

| Параметр | Значение |
|---|---|
| **GitHub** | https://github.com/rehersal/itresume-portfolio/tree/main/final-project |
| **IP сервера** | 85.239.42.190 |
| **Metabase дашборд** | http://85.239.42.190:3000/public/dashboard/e2ba13ef-e7fd-449f-bb7f-a5e1abe774df |

### Доступы к PostgreSQL
```
Host:     85.239.42.190
Port:     5432
Database: marketplace
User:     analyst
Password: GxVngDp5UgncA6YlRTaMzA==
```

---

## 📁 Структура репозитория

```
final-project/
├── api/
│   ├── fetcher.py          # Сбор данных с API (один день)
│   └── load_history.py     # Загрузка исторических данных
├── scheduler/
│   └── daily_fetch.py      # Cron-скрипт (07:00 UTC ежедневно)
├── db/
│   └── views.sql           # Аналитические представления для Metabase
├── analysis/
│   ├── research_1_assortment.py   # Исследование 1: ассортимент
│   └── research_2_customers.py    # Исследование 2: клиенты / LTV
├── setup_server.sh         # Установка стека на Ubuntu
├── requirements.txt
└── README.md
```

---

## 🗄️ База данных

**Таблица `raw_orders`** — 2 040 075 строк за 2023 год

| Поле | Тип | Описание |
|---|---|---|
| client_id | BIGINT | ID клиента |
| gender | TEXT | Пол (M/F) |
| purchase_datetime | DATE | Дата покупки |
| purchase_time_as_seconds_from_midnight | INTEGER | Время покупки |
| product_id | BIGINT | ID товара |
| quantity | INTEGER | Количество |
| price_per_item | NUMERIC | Цена за единицу, руб. |
| discount_per_item | NUMERIC | Скидка за единицу, руб. |
| total_price | NUMERIC | Итоговая сумма, руб. |

**Аналитические представления:** `v_daily_stats`, `v_monthly_stats`, `v_product_stats`, `v_client_stats`, `v_top_products`, `v_discount_analysis`

---

## 📊 Metabase дашборд — 6 графиков

1. Выручка по дням
2. Клиенты и транзакции по месяцам
3. Топ-20 товаров по выручке
4. Клиенты по полу
5. Средний чек и скидка по месяцам
6. Выручка по размеру скидки

---

## ⚙️ Cron (07:00 UTC ежедневно)

```
0 7 * * * cd /opt/marketplace && source venv/bin/activate && \
  export DB_DSN="postgresql://analyst:GxVngDp5UgncA6YlRTaMzA==@localhost:5432/marketplace" && \
  python3 scheduler/daily_fetch.py >> /var/log/marketplace_fetch.log 2>&1
```

---

## 🔬 Исследования

### Исследование 1: Оптимизация ассортиментной матрицы

**Методы:** ABC-анализ, анализ скидок, сезонность

**Ключевые находки:**
- 27 000 A-товаров (54% ассортимента) → 80% выручки
- 11 500 C-товаров → лишь 5% выручки, кандидаты на вывод
- 100% транзакций со скидками — скидка встроена в ценообразование
- Большие скидки (>5 000 руб.) стимулируют выбор дорогих товаров

**Рекомендации:**
1. Сфокусировать рекламный бюджет на A-товарах
2. Вывести C-позиции без продаж за 90+ дней
3. Внедрить пороговые скидки вместо фиксированных

---

### Исследование 2: Клиентская база. Как увеличить LTV?

**Методы:** RFM-сегментация, воронка повторных покупок, интервальный анализ

**Ключевые находки:**
- 864 420 уникальных клиентов
- 31.3% купили только 1 раз — главная точка роста
- Медианный интервал между покупками — 68 дней
- Champions (22.5%) и Loyal (20.9%) — наиболее ценные сегменты

| Сегмент | Клиентов | Ср. LTV |
|---|---|---|
| Champions | 194 727 | 3.1 млн руб. |
| Loyal | 180 526 | 2.2 млн руб. |
| At Risk | 95 441 | 2.3 млн руб. |
| Lost | 180 605 | 0.76 млн руб. |

**Рекомендации:**
1. Welcome-серия с триггером на 62-й день → снизить долю «одноразовых» с 31% до 20%
2. Win-back для «At Risk»: скидка 15% на 48 часов
3. Программа лояльности с баллами (сгорают через 60 дней)
4. Цель 2024: частота с 2.1 до 3.5 покупок в год → +67% LTV

---

## 🔧 Технологии

Python 3.12 · PostgreSQL 16 · Metabase 0.50 · cron · Ubuntu 24.04 · Timeweb Cloud
