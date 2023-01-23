# Kafka to Clickhouse ETL

## Если ничего не получается [ cамая важная информация наверху :) ]
Попробуйте удалить все относящиеся к проекту контейнеры выполнив
```bash
docker ps -a|grep moviesugc-|awk '{print $1}'|xargs docker rm
```

## Api
Отправка данных в Kafka происходит через [апи](../api/README.md) на Flask. Адрес апи [ugc.localhost](http://ugc.localhost). Чтобы этот урл был доступен, должен быть поднят `traefik` из корня репозитория [https://github.com/MaximIglinRest/CommandMonoRepo](https://github.com/MaximIglinRest/CommandMonoRepo). Также должен быть поднят этот стек.

## ETL
При `docker-compose up --build` создастся бд в clickhouse и подключится прием данных из kafka.

## Kafka
Топик **views** должен создаться автоматически. Если нет, создаем топик **views** в админке kafka `http://localhost:9021`
Если *kafka* брокер не может стартовать из-за ошибки несоответствия id кластера, нужно удалить контейнер брокера и попробывать еще.

## Test data
Загружаем тестовые данные в кафку 
```
curl -XPOST 'ugc.localhost/write-timestamp' -d'{"user_id":"063af08f-cb57-4aac-9fca-c57029597cd0","movie_id":"063af08f-cb57-4aac-9fca-c57029597cd0","ts":'$(date +%s)'}' -H'Content-Type: application/json'
```

## Clickhouse
Тестируем наличие данных в *clickhouse*. На хосте выполняем:
```
from clickhouse_driver import Client
client = Client(host='localhost') 
c.execute('select * from movies.views')
```
