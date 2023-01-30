# Запуск теста ClickHouse

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
3. Запустите кластер ClickHouse:
```commandline
docker-compose up -d
```
4. При первом запуске проведите миграцию:
```commandline
python migrate.py
```
Схема таблицы в БД:
```sql
CREATE TABLE test.stats (
    user_id UUID, 
    movie_id UUID, 
    timestamp UInt32
) Engine=MergeTree() ORDER BY (user_id, movie_id)
```
5. Далее можно запускать тесты. Команда запуска всех тестов на чтение:
```commandline
python run_test.py all
```
Так же можно указать название конкретного теста:
- `select_average_timestamps` - достать `id` всех фильмов со средним временем их просмотра
```sql
SELECT movie_id, AVG(timestamp) 
FROM test.stats 
GROUP BY movie_id
```
- `select_max_timestamp_by_user_movie` - достать timestamp, на котором остановился пользователь при просмотре фильма
```sql
SELECT MAX (timestamp) 
FROM test.stats 
WHERE movie_id = %(movie_id)s 
AND user_id = %(user_id)s
```
- `select_max_timestamps_by_user` - достать timestamp-ы, на которых остановился пользователь для всех фильмов
```sql
SELECT movie_id, max(timestamp) 
FROM test.stats 
WHERE user_id = %(user_id)s 
GROUP BY movie_id
```
- `select_movies_by_user` - достать id фильмов, которые просматривал пользователь
```sql
SELECT DISTINCT (movie_id) 
FROM test.stats 
WHERE user_id = %(user_id)s
```
- `select_users_by_movie` - достать id пользователей, которые просматривали фильм
```sql
SELECT DISTINCT (user_id) FROM test.stats WHERE movie_id = %(movie_id)s
```

Опциональные аргументы:
- `-i`, `--iterations` - количество итераций для усреднения
- `-l`, `--loaded` - работает ли БД под нагрузкой, будет отображено в названии файла с результатами
6. Запуск тестов на запись:
```commandline
python run_test_insert.py
```

Опциональные аргументы:
- `-s`, `--size` - размер батча
- `-i`, `--iterations` - количество итераций для усреднения
- `-l`, `--loaded` - работает ли БД под нагрузкой

7. Для запуска нагрузки на БД (операции вставки):
```sql
python run_load.py
```

Опциональные агрументы:
- `-s`, `--size` - размер батча для вставки
- `-d`, `--delay` - промежутки времени между вставками