"""Configuration management for MarketingJobDiscovery."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Application configuration."""

    # Paths
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent)
    db_path: Path = field(default=None)
    cache_path: Path = field(default=None)
    input_csv_path: Path = field(default=None)

    # HTTP settings
    user_agent: str = "MarketingJobDiscovery/1.0 (hiring-intelligence-bot)"
    http_timeout: float = 30.0
    http_cache_ttl: int = 3600  # 1 hour

    # Rate limiting
    delay_between_requests: float = 1.0  # seconds
    delay_between_companies: float = 2.0  # seconds

    # ATS detection
    ats_cache_ttl_days: int = 7

    # Relevance scoring
    relevance_threshold: float = 60.0

    # Pipeline settings
    max_jobs_per_company: int = 100

    def __post_init__(self):
        """Set default paths based on base_dir."""
        if self.db_path is None:
            self.db_path = Path("discovery.db")
        if self.cache_path is None:
            self.cache_path = self.base_dir / "data" / "cache"
        if self.input_csv_path is None:
            self.input_csv_path = self.base_dir / "apollo-accounts-export (4).csv"

        # Ensure cache directory exists
        self.cache_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            http_timeout=float(os.getenv("HTTP_TIMEOUT", "30.0")),
            delay_between_requests=float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0")),
            delay_between_companies=float(os.getenv("DELAY_BETWEEN_COMPANIES", "2.0")),
            relevance_threshold=float(os.getenv("RELEVANCE_THRESHOLD", "60.0")),
            max_jobs_per_company=int(os.getenv("MAX_JOBS_PER_COMPANY", "100")),
        )
