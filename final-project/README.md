# 🛒 Marketplace Analytics — Дипломный проект

Полный аналитический стек для онлайн-маркетплейса:
сбор данных по API → PostgreSQL → cron → Metabase + 2 аналитических исследования.

---

## 📁 Структура репозитория

```
final-project/
├── api/
│   ├── fetcher.py          # Модуль получения данных с API (один день)
│   └── load_history.py     # Однократная загрузка исторических данных
├── scheduler/
│   └── daily_fetch.py      # Скрипт для cron (ежедневно в 07:00)
├── db/
│   └── views.sql           # Аналитические представления для Metabase
├── analysis/
│   ├── research_1_assortment.py   # Исследование 1: ассортимент
│   └── research_2_customers.py    # Исследование 2: клиентская база / LTV
├── setup_server.sh         # Скрипт установки всего стека на Ubuntu 22.04
├── requirements.txt
└── README.md
```

---

## 🚀 Быстрый старт (на сервере)

### 1. Арендовать VPS (Ubuntu 22.04, 2 vCPU / 2 GB RAM)

Рекомендованные провайдеры:
- [Timeweb Cloud](https://timeweb.cloud) — ~300 ₽/мес (РФ)
- [Hetzner](https://hetzner.com) — €4.5/мес (EU)
- [DigitalOcean](https://digitalocean.com) — $12/мес (US)

### 2. Подключиться и запустить установку

```bash
ssh root@YOUR_SERVER_IP
curl -sO https://raw.githubusercontent.com/rehersal/itresume-portfolio/main/final-project/setup_server.sh
bash setup_server.sh
```

Скрипт автоматически:
- Установит PostgreSQL, создаст БД и пользователя
- Склонирует репозиторий в `/opt/marketplace`
- Создаст виртуальное окружение и установит зависимости
- Создаст схему таблиц и представлений
- Добавит cron-задачу на 07:00 UTC
- Установит и запустит Metabase

### 3. Загрузить исторические данные

```bash
cd /opt/marketplace
source venv/bin/activate
export $(cat .env | xargs)
python3 api/load_history.py --start 2023-01-01 --end 2023-12-31
```

Процесс займёт ~10–15 минут (365 запросов к API с паузой 1.5 сек).

### 4. Открыть Metabase

```
http://YOUR_SERVER_IP:3000
```

При первом входе:
1. Создать admin-аккаунт
2. Подключить БД: тип PostgreSQL, host `localhost`, port `5432`,
   db `marketplace`, user `analyst`, пароль из `.env`
3. Собрать дашборд на основе представлений `v_*`

---

## ⚙️ Переменные окружения (`.env`)

```env
API_URL=http://final-project.simulative.ru/data
DB_DSN=postgresql://analyst:PASSWORD@localhost:5432/marketplace
API_DELAY=1.5
```

---

## 📊 Аналитические представления (для Metabase)

| Представление | Описание |
|---|---|
| `v_daily_revenue` | Ежедневная выручка, заказы, AOV, возвраты |
| `v_monthly_revenue` | Помесячная динамика |
| `v_category_metrics` | Метрики по категориям + маржа |
| `v_product_abc` | ABC-анализ товаров |
| `v_customer_rfm` | RFM-сегментация клиентов |
| `v_cohort_retention` | Когортное удержание |
| `v_city_metrics` | Метрики по городам |

---

## 🔬 Исследования

### Исследование 1: Оптимизация ассортимента
```bash
python3 analysis/research_1_assortment.py
```
- ABC-анализ по выручке
- XYZ-анализ по стабильности спроса
- BCG-матрица подкатегорий
- Влияние скидок на маржинальность

### Исследование 2: Клиентская база и LTV
```bash
python3 analysis/research_2_customers.py
```
- RFM-сегментация
- Когортный анализ удержания
- Воронка повторных покупок
- Анализ интервалов между заказами

---

## 📋 Cron

```
0 7 * * * cd /opt/marketplace && source venv/bin/activate && \
  export $(cat .env | xargs) && python3 scheduler/daily_fetch.py \
  >> /var/log/marketplace_fetch.log 2>&1
```

---

## 🔧 Технологии

| Компонент | Технология |
|---|---|
| Язык | Python 3.11 |
| База данных | PostgreSQL 15 |
| BI-инструмент | Metabase 0.50 |
| Планировщик | cron |
| Зависимости | requests, psycopg2, pandas, matplotlib |
