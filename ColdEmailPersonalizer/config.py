"""Configuration management for ColdEmailPersonalizer."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")


@dataclass
class Config:
    """Application configuration."""

    # I/O
    input_csv: Path = field(default_factory=lambda: Path("AllMarketingAgencies.csv"))
    output_csv: Optional[Path] = None

    # Gemini
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-2.5-flash"

    # Concurrency
    scrape_concurrency: int = 20
    llm_concurrency: int = 15
    scrape_timeout: float = 10.0
    subpage_delay: float = 0.5

    # Content limits
    homepage_char_limit: int = 2000
    subpage_char_limit: int = 1000
    max_subpages: int = 5

    # Validation & retries
    max_retries: int = 1

    # Limit (0 = all rows)
    limit: int = 0

    # Resume
    resume: bool = False

    def __post_init__(self):
        if self.output_csv is None:
            stem = self.input_csv.stem
            self.output_csv = self.input_csv.parent / f"{stem}_personalized.csv"

    @classmethod
    def from_env(cls, **overrides) -> "Config":
        """Create config from environment variables with optional overrides."""
        kwargs = {
            "gemini_api_key": os.getenv("GEMINI_API_KEY"),
        }
        env_model = os.getenv("GEMINI_MODEL")
        if env_model:
            kwargs["gemini_model"] = env_model

        kwargs.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**kwargs)
