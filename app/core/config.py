"""
Application configuration management
"""
import os
from typing import List, Optional, Union
from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""
    
    # Project
    PROJECT_NAME: str = "CIMCO ML Analytics"
    API_V1_STR: str = "/api/v1"
    
    # CORS
    BACKEND_CORS_ORIGINS: str = "http://localhost:3000,http://localhost:8080"
    
    @property
    def CORS_ORIGINS_LIST(self) -> List[str]:
        """Convert CORS origins string to list"""
        return [origin.strip() for origin in self.BACKEND_CORS_ORIGINS.split(",")]
    
    # CIMCO MySQL Database (Railway)
    CIMCO_DB_HOST: str = "gondola.proxy.rlwy.net"
    CIMCO_DB_PORT: int = 21632
    CIMCO_DB_USER: str = "root"
    CIMCO_DB_PASSWORD: str = "DjYsEncznrHnsEKzAmDcHfJCvuoOoOuP"
    CIMCO_DB_NAME: str = "railway"
    CIMCO_MYSQL_PORT: int = 3306
    
    @property
    def CIMCO_DATABASE_URL(self) -> str:
        return f"mysql+aiomysql://{self.CIMCO_DB_USER}:{self.CIMCO_DB_PASSWORD}@{self.CIMCO_DB_HOST}:{self.CIMCO_DB_PORT}/{self.CIMCO_DB_NAME}"
    
    # PostgreSQL Database (Analytics)
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "cimco_analytics"
    
    @property
    def POSTGRES_DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    
    @property
    def REDIS_URL(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
    
    # Security
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # ML Models
    MODEL_STORAGE_PATH: str = "models/"
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    model_config = {
        "env_file": ".env",
        "case_sensitive": True
    }


settings = Settings()