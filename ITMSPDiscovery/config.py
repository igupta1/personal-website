"""Configuration management for ITMSPDiscovery."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List

from dotenv import load_dotenv

# Load .env from the ITMSPDiscovery package directory
load_dotenv(Path(__file__).parent / ".env")


@dataclass
class Config:
    """Application configuration."""

    # Paths
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent)
    db_path: Path = field(default=None)

    # SerpAPI settings
    serpapi_api_key: Optional[str] = None
    max_searches_per_run: int = 3
    metros_per_run: int = 3
    metro_state_path: Path = field(default=None)

    # Gemini Decision Maker settings
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-2.5-flash"
    gemini_batch_size: int = 5
    enable_decision_maker_lookup: bool = True

    # Upload settings
    upload_api_key: Optional[str] = None
    vercel_api_url: str = "https://www.ishaangpta.com"
    upload_location: str = "it-msp-discovery"

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

    # Single combined query (all 10 IT titles in one search to save credits)
    search_query: str = (
        '("IT Manager" OR "Help Desk" OR "Systems Administrator" OR "Network Admin" '
        'OR "IT Support" OR "IT Technician" OR "IT Coordinator" OR "Desktop Support" '
        'OR "IT Specialist" OR "Network Engineer")'
    )

    def __post_init__(self):
        """Set default paths based on base_dir."""
        if self.db_path is None:
            self.db_path = self.base_dir / "it_msp_discovery.db"
        if self.metro_state_path is None:
            self.metro_state_path = self.base_dir / "metro_state.json"

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            serpapi_api_key=os.getenv("SERPAPI_API_KEY"),
            max_searches_per_run=int(os.getenv("MAX_SEARCHES_PER_RUN", "3")),
            metros_per_run=int(os.getenv("METROS_PER_RUN", "3")),
            gemini_api_key=os.getenv("GEMINI_API_KEY"),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            gemini_batch_size=int(os.getenv("GEMINI_BATCH_SIZE", "5")),
            enable_decision_maker_lookup=os.getenv(
                "ENABLE_DECISION_MAKER_LOOKUP", "true"
            ).lower() == "true",
            upload_api_key=os.getenv("LEADS_UPLOAD_API_KEY"),
            vercel_api_url=os.getenv("VERCEL_API_URL", "https://www.ishaangpta.com"),
            max_employee_count=int(os.getenv("MAX_EMPLOYEE_COUNT", "100")),
        )
