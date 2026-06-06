import json
from typing import Any, List, Union
from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Pydantic v2 Settings Config
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=True, extra="ignore"
    )

    # Application Configuration
    APP_NAME: str = "Plataforma NLP RAG Enterprise"
    APP_ENV: str = "development"
    DEBUG: bool = True
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = "super-secret-key-change-in-production"

    # Server Configuration
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # CORS Configuration
    BACKEND_CORS_ORIGINS: List[str] = []

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            try:
                if isinstance(v, str):
                    return json.loads(v)
                return v
            except Exception:
                return []
        raise ValueError(v)

    # Database & Storage
    CHROMADB_PATH: str = "./data/chromadb"
    PERSIST_DIRECTORY: str = "./data/chromadb"
    UPLOAD_DIR: str = "./data/uploads"
    MAX_FILE_SIZE_MB: int = 10
    CHUNK_SIZE: int = 500
    CHUNK_OVERLAP: int = 50
    EMBEDDING_MODEL_NAME: str = "sentence-transformers/all-MiniLM-L6-v2"

    # Cache & Queues
    REDIS_URL: str = "redis://localhost:6379/0"
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_DOCUMENT_QUEUE: str = "document_processing"
    RABBITMQ_RAG_QUEUE: str = "rag_requests"
    CACHE_TTL_SECONDS: int = 3600  # Tempo de vida padrão do cache Redis (1 hora)

    # LLM Providers
    GOOGLE_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-2.5-flash"
    LLM_TEMPERATURE: float = 0.2
    LLM_MAX_TOKENS: int = 1024

    # RAG Pipeline
    RAG_CONTEXT_CHUNKS: int = 4
    RAG_MIN_SIMILARITY: float = 0.3


# Instanciação global das configurações
settings = Settings()
