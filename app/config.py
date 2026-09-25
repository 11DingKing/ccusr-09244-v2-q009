from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Robot Data Pipeline Backend"
    APP_VERSION: str = "1.0.0"
    DATABASE_URL: str = "sqlite:///./robot_data.db"
    API_V1_PREFIX: str = "/api/v1"

    # 幂等记录 TTL（秒）：到期后记录不再参与重放，但在宽限期结束前不允许复用键
    IDEMPOTENCY_TTL_SECONDS: int = 7 * 24 * 3600
    # 到期后的额外宽限期（秒）：只有超过 TTL+宽限期的记录才会被回收删除
    IDEMPOTENCY_GRACE_SECONDS: int = 24 * 3600
    # 单次回收最多删除的记录数
    IDEMPOTENCY_REAP_BATCH_SIZE: int = 1000

    class Config:
        env_file = ".env"


settings = Settings()
