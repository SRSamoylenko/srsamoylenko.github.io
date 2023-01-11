from pydantic import BaseSettings, Field


class Config(BaseSettings):
    CLICKHOUSE_CLUSTER: str = Field("company_cluster", env="CLICKHOUSE_CLUSTER")
    CLICKHOUSE_HOST: str = Field("localhost", env="CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = Field(9000, env="CLICKHOUSE_PORT")
    CLICKHOUSE_DB: str = Field("movies", env="CLICKHOUSE_DB")

    KAFKA_HOST: str = Field(
        "broker", env="KAFKA_HOST"
    )  # must be available from clickhouse
    KAFKA_PORT: int = Field(29092, env="KAFKA_PORT")
    KAFKA_TOPIC: str = Field("movies", env="KAFKA_TOPIC")
    KAFKA_CONSUMER_GROUP: str = Field("clickhouse", env="KAFKA_CONSUMER_GROUP")
