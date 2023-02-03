from http import HTTPStatus
from typing import Tuple
from uuid import UUID

from aiokafka import AIOKafkaProducer
from flask import Flask, request
from pydantic import BaseModel, ValidationError

from .settings import settings

USER_ID = "user_id"
MOVIE_ID = "movie_id"
TIMESTAMP = "ts"

app = Flask(__name__)


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
