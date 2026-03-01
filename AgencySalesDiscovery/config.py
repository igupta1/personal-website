"""Configuration management for AgencySalesDiscovery."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List

from dotenv import load_dotenv

# Load .env from the AgencySalesDiscovery package directory
load_dotenv(Path(__file__).parent / ".env")


@dataclass
class Config:
    """Application configuration."""

    # Paths
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent)
    seen_companies_path: Path = field(default=None)
    metro_state_path: Path = field(default=None)
    output_dir: Path = field(default=None)

    # SerpAPI settings
    serpapi_api_key: Optional[str] = None
    max_searches_per_run: int = 2
    metros_per_run: int = 2

    # Gemini Decision Maker settings
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-2.5-flash"
    gemini_batch_size: int = 5

    # Company size filter
    max_employee_count: int = 100

    # Metro areas for search
    metro_areas: List[str] = field(default_factory=lambda: [
        "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
        "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
        "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL",
        "Fort Worth, TX", "Columbus, OH", "Charlotte, NC", "Indianapolis, IN",
        "San Francisco, CA", "Seattle, WA", "Denver, CO", "Washington, DC",
    ])

    # SerpAPI query: sales/marketing roles at marketing agencies
    search_query: str = (
        '("Sales Manager" OR "Account Executive" OR "Business Development" '
        'OR "Marketing Manager" OR "Sales Representative" OR "BDR" '
        'OR "Sales Director" OR "Marketing Director") '
        '("marketing agency" OR "digital marketing agency" OR "advertising agency" '
        'OR "creative agency" OR "digital agency" OR "media agency")'
    )

    def __post_init__(self):
        """Set default paths based on base_dir."""
        if self.seen_companies_path is None:
            self.seen_companies_path = self.base_dir / "seen_companies.json"
        if self.metro_state_path is None:
            self.metro_state_path = self.base_dir / "metro_state.json"
        if self.output_dir is None:
            self.output_dir = self.base_dir / "output"

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            serpapi_api_key=os.getenv("SERPAPI_API_KEY"),
            max_searches_per_run=int(os.getenv("MAX_SEARCHES_PER_RUN", "2")),
            metros_per_run=int(os.getenv("METROS_PER_RUN", "2")),
            gemini_api_key=os.getenv("GEMINI_API_KEY"),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            gemini_batch_size=int(os.getenv("GEMINI_BATCH_SIZE", "5")),
            max_employee_count=int(os.getenv("MAX_EMPLOYEE_COUNT", "100")),
        )
