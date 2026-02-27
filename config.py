# ============================================================
# DBMA - Database Management Agent
# config.py — Central Configuration Management
# ============================================================

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv

# Load .env file
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")


class MySQLConfig(BaseSettings):
    """MySQL connection configuration."""
    host: str = Field(default="localhost", env="MYSQL_HOST")
    port: int = Field(default=3306, env="MYSQL_PORT")
    user: str = Field(default="root", env="MYSQL_USER")
    password: str = Field(default="", env="MYSQL_PASSWORD")
    default_database: Optional[str] = Field(default=None, env="MYSQL_DEFAULT_DATABASE")

    class Config:
        env_prefix = "MYSQL_"
        extra = "ignore"

    def get_connection_params(self, database: Optional[str] = None) -> dict:
        params = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "autocommit": True,
            "connection_timeout": 30,
        }
        if database:
            params["database"] = database
        return params


class PostgreSQLConfig(BaseSettings):
    """PostgreSQL configuration for chat persistence."""
    host: str = Field(default="localhost", env="POSTGRES_HOST")
    port: int = Field(default=5432, env="POSTGRES_PORT")
    user: str = Field(default="postgres", env="POSTGRES_USER")
    password: str = Field(default="Dhankoli@90", env="POSTGRES_PASSWORD")
    db: str = Field(default="dbma_persistence", env="POSTGRES_DB")

    class Config:
        extra = "ignore"

    def get_dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )

    def get_connection_params(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": self.db,
        }


class OllamaConfig(BaseSettings):
    """Ollama LLM configuration."""
    base_url: str = Field(default="http://localhost:11434", env="OLLAMA_BASE_URL")
    model: str = Field(default="qwen3:8b", env="OLLAMA_MODEL")
    timeout: int = Field(default=120, env="OLLAMA_TIMEOUT")
    temperature: float = Field(default=0.1, env="AGENT_TEMPERATURE")

    class Config:
        extra = "ignore"


class AppConfig(BaseSettings):
    """Application-level configuration."""
    name: str = Field(default="DBMA", env="APP_NAME")
    version: str = Field(default="1.0.0", env="APP_VERSION")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: str = Field(default="logs/dbma.log", env="LOG_FILE")
    max_chat_history: int = Field(default=100, env="MAX_CHAT_HISTORY")
    max_sql_retries: int = Field(default=3, env="MAX_SQL_RETRIES")

    class Config:
        extra = "ignore"


# ── Singleton Config Instances ────────────────────────────────
mysql_config = MySQLConfig()
postgres_config = PostgreSQLConfig()
ollama_config = OllamaConfig()
app_config = AppConfig()

# ── Ensure log directory exists ───────────────────────────────
os.makedirs(BASE_DIR / "logs", exist_ok=True)



# Add this section to your existing config.py

import os
from dotenv import load_dotenv
load_dotenv()













