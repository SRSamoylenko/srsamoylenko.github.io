from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    kafka_host: str = Field("127.0.0.1", env="KAFKA_HOST")
    kafka_port: int = Field(9092, env="KAFKA_PORT")
    kafka_topic: str = Field("movies", env="KAFKA_TOPIC")


settings = Settings()
