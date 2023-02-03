from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    kafka_host: str = Field("127.0.0.1", env="KAFKA_HOST")
    kafka_port: int = Field(9092, env="KAFKA_PORT")
    kafka_topic: str = Field("movies", env="KAFKA_TOPIC")

    logstash_host: str = Field("logstash", env="LOGSTASH_HOST")
    logstash_port: int = Field(5044, env="LOGSTASH_PORT")

    logger_name = "ugc"
    sentry_dsn: str = Field("", env="SENTRY_DSN")


settings = Settings()
