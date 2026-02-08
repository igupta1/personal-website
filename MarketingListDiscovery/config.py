"""Configuration management for MarketingListDiscovery."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

# Load .env from the MarketingListDiscovery package directory
load_dotenv(Path(__file__).parent / ".env")


@dataclass
class Config:
    """Application configuration."""

    # Paths
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent)
    db_path: Path = field(default=None)
    cache_path: Path = field(default=None)

    # GitHub source
    github_repo: str = "jobright-ai/2026-Marketing-New-Grad"

    # HTTP settings
    user_agent: str = "MarketingListDiscovery/1.0 (hiring-intelligence-bot)"
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

    # Gemini Decision Maker settings
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-2.5-flash"
    gemini_batch_size: int = 5
    enable_decision_maker_lookup: bool = True

    # Apollo Email Lookup settings
    apollo_api_key: Optional[str] = None
    apollo_batch_size: int = 10
    enable_email_lookup: bool = True

    # Job verification settings
    enable_job_verification: bool = True
    job_verification_timeout: float = 5.0
    job_verification_batch_size: int = 20


    def __post_init__(self):
        """Set default paths based on base_dir."""
        if self.db_path is None:
            self.db_path = Path("list_discovery.db")
        if self.cache_path is None:
            self.cache_path = self.base_dir / "data" / "cache"

        # Ensure cache directory exists
        self.cache_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            github_repo=os.getenv(
                "GITHUB_REPO", "jobright-ai/2026-Marketing-New-Grad"
            ),
            http_timeout=float(os.getenv("HTTP_TIMEOUT", "30.0")),
            delay_between_requests=float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0")),
            delay_between_companies=float(os.getenv("DELAY_BETWEEN_COMPANIES", "2.0")),
            relevance_threshold=float(os.getenv("RELEVANCE_THRESHOLD", "60.0")),
            max_jobs_per_company=int(os.getenv("MAX_JOBS_PER_COMPANY", "100")),
            gemini_api_key=os.getenv("GEMINI_API_KEY"),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            gemini_batch_size=int(os.getenv("GEMINI_BATCH_SIZE", "5")),
            enable_decision_maker_lookup=os.getenv(
                "ENABLE_DECISION_MAKER_LOOKUP", "true"
            ).lower()
            == "true",
            apollo_api_key=os.getenv("APOLLO_API_KEY"),
            apollo_batch_size=int(os.getenv("APOLLO_BATCH_SIZE", "10")),
            enable_email_lookup=os.getenv(
                "ENABLE_EMAIL_LOOKUP", "true"
            ).lower()
            == "true",
            enable_job_verification=os.getenv(
                "ENABLE_JOB_VERIFICATION", "true"
            ).lower()
            == "true",
            job_verification_timeout=float(
                os.getenv("JOB_VERIFICATION_TIMEOUT", "5.0")
            ),
            job_verification_batch_size=int(
                os.getenv("JOB_VERIFICATION_BATCH_SIZE", "20")
            ),
        )
