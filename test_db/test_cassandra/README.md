# Запуск теста CassandraDB:

Требуемая версия питона: 3.10

1. Создайте и активируйте виртуальное окружение
```commandline
python3.10 -m venv venv
. venv/bin/activate
```
2. Установите зависимости
```commandline
pip install -r requirements.txt
```
3. Запустите кластер Cassandra:
```commandline
docker-compose up -d
```
Команда так же запустит начальную миграцию.

Схема таблицы:
```cql
CREATE TABLE IF NOT EXISTS movies.stats (
    id text PRIMARY KEY,
    user_id text,
    movie_id text,
    timestamp bigint
);
```
4. Сгенерируйте записи в базу:
```commandline
python generate_data.py
```
5. Для таблиц с числом записей >2000000 база перестает отдавать данные за разумное время, поэтому запуск дальнейших тестов не требуется.