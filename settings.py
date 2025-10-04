# settings.py
import os
from pydantic import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings for Polymarket blockchain indexer.
    """

    # Database Configuration
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:polymarket123@localhost:5432/core"
    )

    # Redis Configuration
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Blockchain Configuration
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
    START_BLOCK: int = int(os.getenv("START_BLOCK", "50000000"))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))

    # Polymarket Contract Addresses (Polygon Mainnet)
    CONDITIONAL_TOKENS_ADDRESS: str = os.getenv(
        "CONDITIONAL_TOKENS_ADDRESS",
        "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    )
    CTF_EXCHANGE_ADDRESS: str = os.getenv(
        "CTF_EXCHANGE_ADDRESS",
        "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
    )
    NEG_RISK_ADAPTER_ADDRESS: str = os.getenv(
        "NEG_RISK_ADAPTER_ADDRESS",
        "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    )

    # Indexer Configuration
    INDEXER_INTERVAL_MINUTES: int = int(os.getenv("INDEXER_INTERVAL_MINUTES", "5"))
    TRIGGER_IMMEDIATE: bool = os.getenv("TRIGGER_IMMEDIATE", "false").lower() == "true"
    MAX_RETRY_ATTEMPTS: int = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))

    # API Configuration
    API_RATE_LIMIT: int = int(os.getenv("API_RATE_LIMIT", "1000"))
    PORT: int = int(os.getenv("PORT", "8000"))
    WORKERS: int = int(os.getenv("WORKERS", "2"))

    # External API Configuration (optional)
    POLYMARKET_API_URL: str = os.getenv(
        "POLYMARKET_API_URL",
        "https://gamma-api.polymarket.com"
    )
    UMA_API_URL: str = os.getenv(
        "UMA_API_URL",
        "https://api.uma.xyz"
    )
    IPFS_GATEWAY_URL: str = os.getenv(
        "IPFS_GATEWAY_URL",
        "https://ipfs.io/ipfs"
    )

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE_PATH: str = os.getenv("LOG_FILE_PATH", "/logs/core.log")

    # Performance Configuration
    CONNECTION_POOL_SIZE: int = int(os.getenv("CONNECTION_POOL_SIZE", "20"))
    QUERY_TIMEOUT: int = int(os.getenv("QUERY_TIMEOUT", "60"))

    # Data Retention Configuration
    PRICE_HISTORY_RETENTION_DAYS: int = int(os.getenv("PRICE_HISTORY_RETENTION_DAYS", "90"))
    EVENT_LOG_RETENTION_DAYS: int = int(os.getenv("EVENT_LOG_RETENTION_DAYS", "30"))

    # Development/Testing Configuration
    ENABLE_METADATA_ENRICHMENT: bool = os.getenv("ENABLE_METADATA_ENRICHMENT", "true").lower() == "true"
    ENABLE_EXTERNAL_API_CALLS: bool = os.getenv("ENABLE_EXTERNAL_API_CALLS", "true").lower() == "true"
    MOCK_BLOCKCHAIN_DATA: bool = os.getenv("MOCK_BLOCKCHAIN_DATA", "false").lower() == "true"

    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()


# Validation
def validate_settings():
    """
    Validate critical settings on startup.
    """
    errors = []

    # Validate database URL
    if not settings.DATABASE_URL.startswith(("postgresql://", "postgres://")):
        errors.append("DATABASE_URL must be a valid PostgreSQL connection string")

    # Validate Redis URL
    if not settings.REDIS_URL.startswith("redis://"):
        errors.append("REDIS_URL must be a valid Redis connection string")

    # Validate Polygon RPC URL
    if not settings.POLYGON_RPC_URL.startswith(("http://", "https://")):
        errors.append("POLYGON_RPC_URL must be a valid HTTP(S) URL")

    # Validate contract addresses
    if not all(addr.startswith("0x") and len(addr) == 42 for addr in [
        settings.CONDITIONAL_TOKENS_ADDRESS,
        settings.CTF_EXCHANGE_ADDRESS,
        settings.NEG_RISK_ADAPTER_ADDRESS
    ]):
        errors.append("Contract addresses must be valid Ethereum addresses")

    # Validate intervals
    if settings.INDEXER_INTERVAL_MINUTES < 1 or settings.INDEXER_INTERVAL_MINUTES > 60:
        errors.append("INDEXER_INTERVAL_MINUTES must be between 1 and 60")

    if settings.BATCH_SIZE < 1 or settings.BATCH_SIZE > 10000:
        errors.append("BATCH_SIZE must be between 1 and 10000")

    if errors:
        raise ValueError(f"Settings validation failed: {'; '.join(errors)}")

    return True


# Validate on import
validate_settings()