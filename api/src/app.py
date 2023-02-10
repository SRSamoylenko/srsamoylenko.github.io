import logging
from http import HTTPStatus
from typing import Tuple
from uuid import UUID

import logstash
from aiokafka import AIOKafkaProducer
from flask import Flask, request
from pydantic import BaseModel, ValidationError

from .settings import settings

USER_ID = "user_id"
MOVIE_ID = "movie_id"
TIMESTAMP = "ts"

if settings.sentry_dsn:
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        integrations=[
            FlaskIntegration(),
        ],
        traces_sample_rate=settings.sentry_traces_sample_rate,
    )

app = Flask(__name__)
logger = logging.getLogger(settings.logger_name)
app.logger = logger
app.logger.setLevel(logging.INFO)
logstash_handler = logstash.LogstashHandler(
    settings.logstash_host, settings.logstash_port, version=1
)
app.logger.addHandler(logstash_handler)


class RequestParams(BaseModel):
    user_id: UUID
    movie_id: UUID
    timestamp: int


@app.route("/write-timestamp", methods=["POST"])
async def write_timestamp() -> Tuple[dict, int]:
    producer = AIOKafkaProducer(
        bootstrap_servers=f"{settings.kafka_host}:{settings.kafka_port}",
    )

    json_data = request.json
    if not json_data:
        return (
            {"details": "No json data provided"},
            HTTPStatus.BAD_REQUEST,
        )

    try:
        params = RequestParams(
            user_id=json_data.get(USER_ID),
            movie_id=json_data.get(MOVIE_ID),
            timestamp=json_data.get(TIMESTAMP),
        )
    except ValidationError as exception:
        return (
            {"details": str(exception)},
            HTTPStatus.BAD_REQUEST,
        )

    await producer.start()
    try:
        await producer.send(
            settings.kafka_topic,
            value=params.json().encode(),
            key=f"{params.user_id}+{params.movie_id}".encode(),
        )
    finally:
        await producer.stop()

    return {"status": "success"}, HTTPStatus.CREATED
