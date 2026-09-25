from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Robot Data Pipeline Backend"
    APP_VERSION: str = "1.0.0"
    DATABASE_URL: str = "sqlite:///./robot_data.db"
    API_V1_PREFIX: str = "/api/v1"
    # 幂等记录存活时长（小时）：写入起超过该时长即过期，到期边界（含）失效
    IDEMPOTENCY_TTL_HOURS: int = 24

    class Config:
        env_file = ".env"


settings = Settings()
