# UGC Api

Отправляйте ваши данные в формате json на `http://ugc.localhost/write-timestamp`

Например так:
```bash
curl -XPOST 'ugc.localhost/write-timestamp' -d'{"user_id":"063af08f-cb57-4aac-9fca-c57029597cd0","movie_id":"063af08f-cb57-4aac-9fca-c57029597cd0","ts":'$(date +%s)'}' -H'Content-Type: application/json'
```
