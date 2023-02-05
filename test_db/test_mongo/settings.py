from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    mongo_username: str = Field("root", env="MONGO_INITDB_ROOT_USERNAME")
    mongo_password: str = Field("example", env="MONGO_INITDB_ROOT_PASSWORD")


settings = Settings()
