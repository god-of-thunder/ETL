from pydantic_settings import BaseSettings, SettingsConfigDict
import os

print(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
class Settings(BaseSettings):

    PROJECT_NAME: str = "Production E-commerce API"

    DATABASE_URL: str

    SECRET_KEY: str

    ALGORITHM: str

    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    class Config:
        model_config = SettingsConfigDict(
            env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
            extra="ignore"
        )

settings = Settings()