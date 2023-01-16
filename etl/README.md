# Kafka to Clickhouse ETL

## ETL
При `docker-compose up --build` создастся бд в clickhouse и подключится прием данных из kafka.

## Kafka
Топик **movies** должен создаться автоматически. Если нет, создаем топик **movies** в админке kafka `http://localhost:9021`
Если *kafka* брокер не может стартовать из-за ошибки несоответствия id кластера, нужно удалить контейнер брокера и попробывать еще.

Отправка данных в Kafka происходит в сервисе асинхронного апи через декоратор `log_view` https://github.com/SRSamoylenko/Async_API_sprint_2/blob/main/src/api/v1/views/films.py#L81

## Test data
Загружаем тестовые данные в кафку 
```
curl https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson -o ~/Downloads/github_all_columns.ndjson

cat ~/Downloads/github_all_columns.ndjson|docker run -i --network=host edenhill/kcat:1.7.1 -b localhost:9092 -t movies -P
```

## Clickhouse
Тестируем наличие данных в *clickhouse*. На хосте выполняем:
```
from clickhouse_driver import Client
client = Client(host='localhost') 
c.execute('select * from movies.events')
```
