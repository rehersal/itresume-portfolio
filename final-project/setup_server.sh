#!/usr/bin/env bash
# =============================================================
# setup_server.sh — разворачивает всю инфраструктуру на Ubuntu 22.04
# Запускать от root: bash setup_server.sh
# =============================================================

set -e

DB_NAME="marketplace"
DB_USER="analyst"
DB_PASS="$(openssl rand -base64 16)"   # случайный пароль — запишите!
PROJECT_DIR="/opt/marketplace"

echo "============================="
echo " Marketplace Analytics Setup"
echo "============================="

# ── 1. Обновляем систему ──────────────────────────────────────────────────────
apt-get update -y && apt-get upgrade -y
apt-get install -y python3 python3-pip python3-venv curl git cron

# ── 2. PostgreSQL ─────────────────────────────────────────────────────────────
apt-get install -y postgresql postgresql-contrib
systemctl enable postgresql && systemctl start postgresql

sudo -u postgres psql <<EOF
CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';
CREATE DATABASE $DB_NAME OWNER $DB_USER;
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
EOF

echo ""
echo ">>> PostgreSQL ready."
echo "    DB: $DB_NAME  User: $DB_USER  Password: $DB_PASS"
echo "    DSN: postgresql://$DB_USER:$DB_PASS@localhost:5432/$DB_NAME"
echo ""

# ── 3. Разворачиваем проект ───────────────────────────────────────────────────
mkdir -p $PROJECT_DIR
cd $PROJECT_DIR

# Клонируем репозиторий
git clone https://github.com/rehersal/itresume-portfolio.git .

# Virtualenv
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# .env файл
cat > $PROJECT_DIR/.env <<ENVEOF
API_URL=http://final-project.simulative.ru/data
DB_DSN=postgresql://$DB_USER:$DB_PASS@localhost:5432/$DB_NAME
API_DELAY=1.5
ENVEOF

echo ">>> Project deployed to $PROJECT_DIR"

# ── 4. Создаём схему БД ───────────────────────────────────────────────────────
cd $PROJECT_DIR
source venv/bin/activate
export DB_DSN="postgresql://$DB_USER:$DB_PASS@localhost:5432/$DB_NAME"
python3 api/fetcher.py  # создаст схему и попробует вчерашний день

psql "postgresql://$DB_USER:$DB_PASS@localhost:5432/$DB_NAME" -f db/views.sql
echo ">>> Schema and views created."

# ── 5. Cron — ежедневно в 07:00 UTC ──────────────────────────────────────────
CRON_CMD="0 7 * * * cd $PROJECT_DIR && source venv/bin/activate && export \$(cat .env | xargs) && python3 scheduler/daily_fetch.py >> /var/log/marketplace_fetch.log 2>&1"
(crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
echo ">>> Cron job added (07:00 UTC daily)."

# ── 6. Metabase ───────────────────────────────────────────────────────────────
apt-get install -y default-jdk-headless
mkdir -p /opt/metabase
cd /opt/metabase
curl -sL https://downloads.metabase.com/v0.50.0/metabase.jar -o metabase.jar

# systemd unit для Metabase
cat > /etc/systemd/system/metabase.service <<SVCEOF
[Unit]
Description=Metabase BI
After=network.target postgresql.service

[Service]
User=root
WorkingDirectory=/opt/metabase
Environment="MB_DB_TYPE=postgres"
Environment="MB_DB_DBNAME=$DB_NAME"
Environment="MB_DB_PORT=5432"
Environment="MB_DB_USER=$DB_USER"
Environment="MB_DB_PASS=$DB_PASS"
Environment="MB_DB_HOST=localhost"
ExecStart=/usr/bin/java -jar /opt/metabase/metabase.jar
Restart=on-failure

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable metabase
systemctl start metabase

echo ""
echo "============================="
echo " УСТАНОВКА ЗАВЕРШЕНА"
echo "============================="
echo " PostgreSQL DSN:"
echo "   postgresql://$DB_USER:$DB_PASS@localhost:5432/$DB_NAME"
echo ""
echo " Metabase:"
echo "   http://$(curl -s ifconfig.me):3000"
echo ""
echo " Следующий шаг — загрузить историю:"
echo "   cd $PROJECT_DIR && source venv/bin/activate"
echo "   export \$(cat .env | xargs)"
echo "   python3 api/load_history.py --start 2023-01-01 --end 2023-12-31"
echo "============================="
