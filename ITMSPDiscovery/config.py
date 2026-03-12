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
    max_searches_per_run: int = 8
    metros_per_run: int = 4
    metro_state_path: Path = field(default=None)

    # Relevancy screening settings
    enable_relevancy_screening: bool = True
    relevancy_screening_threshold: int = 40
    max_enrichment_companies: int = 30

    # Gemini Decision Maker settings
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-2.5-flash"
    gemini_batch_size: int = 5
    enable_decision_maker_lookup: bool = True

    # Insight generation settings
    enable_insight_generation: bool = True

    # Priority classification settings
    enable_priority_classification: bool = True

    # Outreach draft generation settings
    enable_outreach_generation: bool = True

    # Job verification settings
    enable_job_verification: bool = True
    job_verification_timeout: float = 5.0
    job_verification_batch_size: int = 20

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

    # Split into two query clusters for better coverage with 8 daily SERP calls
    search_queries: List[str] = field(default_factory=lambda: [
        '("IT Manager" OR "Help Desk" OR "IT Support" OR "IT Technician" OR "Desktop Support")',
        '("Systems Administrator" OR "Network Administrator" OR "IT Coordinator" OR "IT Specialist" OR "Network Engineer")',
    ])

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
            max_searches_per_run=int(os.getenv("MAX_SEARCHES_PER_RUN", "8")),
            metros_per_run=int(os.getenv("METROS_PER_RUN", "4")),
            enable_relevancy_screening=os.getenv(
                "ENABLE_RELEVANCY_SCREENING", "true"
            ).lower() == "true",
            relevancy_screening_threshold=int(os.getenv("RELEVANCY_SCREENING_THRESHOLD", "40")),
            max_enrichment_companies=int(os.getenv("MAX_ENRICHMENT_COMPANIES", "30")),
            gemini_api_key=os.getenv("GEMINI_API_KEY"),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            gemini_batch_size=int(os.getenv("GEMINI_BATCH_SIZE", "5")),
            enable_decision_maker_lookup=os.getenv(
                "ENABLE_DECISION_MAKER_LOOKUP", "true"
            ).lower() == "true",
            enable_insight_generation=os.getenv(
                "ENABLE_INSIGHT_GENERATION", "true"
            ).lower() == "true",
            enable_priority_classification=os.getenv(
                "ENABLE_PRIORITY_CLASSIFICATION", "true"
            ).lower() == "true",
            enable_outreach_generation=os.getenv(
                "ENABLE_OUTREACH_GENERATION", "true"
            ).lower() == "true",
            enable_job_verification=os.getenv(
                "ENABLE_JOB_VERIFICATION", "true"
            ).lower() == "true",
            job_verification_timeout=float(os.getenv("JOB_VERIFICATION_TIMEOUT", "5.0")),
            job_verification_batch_size=int(os.getenv("JOB_VERIFICATION_BATCH_SIZE", "20")),
            upload_api_key=os.getenv("LEADS_UPLOAD_API_KEY"),
            vercel_api_url=os.getenv("VERCEL_API_URL", "https://www.ishaangpta.com"),
            max_employee_count=int(os.getenv("MAX_EMPLOYEE_COUNT", "100")),
        )
