from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuração do worker de ingestão (FastF1 -> PostgreSQL)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    f1_season: int = 2024
    fastf1_cache_dir: str = "/tmp/fastf1_cache"

    # Postgres OLTP (o Debezium lê o WAL deste banco)
    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_user: str = "f1user"
    pg_password: str = "f1pass"
    pg_database: str = "f1oltp"

    log_level: str = "INFO"


settings = Settings()
