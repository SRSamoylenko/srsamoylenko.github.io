# Запуск теста MongoDB

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
3. Запустите кластер Mongo:
```commandline
docker-compose up -d
```
4. Генерируем данные:
```commandline
python generate_data.py
```
5. Далее можно запускать тесты. Команда запуска всех тестов на чтение:
```commandline
python run_test.py all
```
Так же можно указать название конкретного теста:
- `user_favorites` - достать список id любимых фильмов пользователя
- `movie_likes` - количество лайков у фильма
- `postpones` - список id отложенных фильмов
- `average_score` - средняя оценка фильма

Опциональные аргументы:
- `-i`, `--iterations` - количество итераций для усреднения
- `-l`, `--loaded` - работает ли БД под нагрузкой, будет отображено в названии файла с результатами

6Для запуска нагрузки на БД (операции вставки):
```sql
python run_load.py
```
