Проект имитирует работу кассового софта:
каждый день генерируются CSV-файлы по магазинам и кассам, а затем они загружаются в базу данных.

Что используется

Python 3 + библиотеки (requirements.txt)

PostgreSQL (локально) или SQLite для теста

Планировщик задач Windows для автоматизации

Установка и запуск

Клонировать проект и открыть консоль в папке:

git clone <repo_url>
cd Auto_my_project


Создать виртуальное окружение и установить зависимости:

py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt


Скопировать .env.example → .env и прописать строку подключения.
Для PostgreSQL:

DATABASE_URL=postgresql+psycopg2://postgres:ПАРОЛЬ@localhost:5432/salesdb


(если БД нет — создать salesdb в DBeaver/pgAdmin)

Сгенерировать тестовые данные:

python scripts\generate_data.py --config config.yaml


Файлы появятся в data/incoming/.

Загрузить их в БД:

python scripts\load_to_db.py --config config.yaml


Проверить в БД (например, в DBeaver):

SELECT COUNT(*) FROM sales_lines;
SELECT * FROM sales_lines LIMIT 10;

Автоматизация

В Windows Планировщике задач созданы две задачи (см. скрины в img/):

GenerateSales — запуск генерации (пн–сб в 07:00)

LoadSalesToDB — загрузка в БД (ежедневно в 07:10)

📂 В папке img/ — подтверждения (скрины Планировщика и DBeaver).
📂 В папке sql/ — DDL для таблиц.