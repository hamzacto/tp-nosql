import os
from typing import Any, Dict, Optional

from pydantic import PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = "your-secret-key"  # Default value for development
    ALGORITHM: str = "HS256"  # Default value for development
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # PostgreSQL
    DATABASE_URL: PostgresDsn = PostgresDsn("postgresql+asyncpg://postgres:postgres@localhost:5432/mydb")  # Default value for development
    
    # Neo4j
    NEO4J_URI: str = "bolt://localhost:7687"  # Default value for development
    NEO4J_USER: str = "neo4j"  # Default value for development
    NEO4J_PASSWORD: str = "password"  # Default value for development
    
    # Server
    PORT: int = 8000
    
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)


settings = Settings() 