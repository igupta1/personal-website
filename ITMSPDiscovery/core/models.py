"""Data models for IT MSP lead discovery."""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Dict, Any


@dataclass
class SerpJobListing:
    """A single job listing from SerpAPI Google Jobs."""

    title: str
    company_name: str
    location: str
    posted_at: str  # Raw string, e.g. "2 days ago"
    posting_date: Optional[date] = None  # Computed from posted_at
    job_url: Optional[str] = None  # From apply_options[0].link
    source: Optional[str] = None  # e.g. "LinkedIn", "Indeed"
    description_snippet: Optional[str] = None
    search_metro: Optional[str] = None  # Which metro search found this
    raw_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DecisionMakerResult:
    """Result of a decision maker lookup for one company."""

    company_name: str
    person_name: Optional[str] = None
    title: Optional[str] = None
    source_url: Optional[str] = None
    confidence: Optional[str] = None  # "High", "Medium", or None
    employee_count: Optional[int] = None
    industry: Optional[str] = None
    not_found_reason: Optional[str] = None
    raw_text: Optional[str] = None
