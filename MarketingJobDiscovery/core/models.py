"""Data models for job discovery."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any


@dataclass
class Company:
    """Company data from Apollo CSV."""

    name: str
    domain: str
    website: Optional[str] = None
    industry: Optional[str] = None
    keywords: Optional[str] = None
    employee_count: Optional[int] = None
    ats_provider: Optional[str] = None
    ats_board_token: Optional[str] = None
    careers_page_url: Optional[str] = None
    id: Optional[int] = None

    @classmethod
    def from_csv_row(cls, row: Dict[str, str]) -> "Company":
        """Create Company from CSV row."""
        from urllib.parse import urlparse

        website = row.get("Website", "")
        domain = ""
        if website:
            url = website if website.startswith("http") else f"https://{website}"
            parsed = urlparse(url)
            domain = parsed.netloc.replace("www.", "")

        employee_count = None
        emp_str = row.get("# Employees", "")
        if emp_str:
            try:
                employee_count = int(emp_str.replace(",", ""))
            except ValueError:
                pass

        return cls(
            name=row.get("Company Name", ""),
            domain=domain,
            website=website,
            industry=row.get("Industry", ""),
            keywords=row.get("Keywords", ""),
            employee_count=employee_count,
        )


@dataclass
class JobPosting:
    """A job posting from an ATS."""

    external_id: str
    title: str
    job_url: str
    department: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    posting_date: Optional[datetime] = None
    raw_data: Dict[str, Any] = field(default_factory=dict)

    # Added by relevance scorer
    relevance_score: Optional[float] = None
    matched_category: Optional[str] = None

    # Database fields
    id: Optional[int] = None
    company_id: Optional[int] = None
    discovered_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    is_active: bool = True


@dataclass
class ATSDetectionResult:
    """Result of ATS detection for a company."""

    provider: Optional[str]  # greenhouse, lever, ashby, workable, jobvite, unknown
    board_token: Optional[str]  # Company identifier for API calls
    confidence: float  # 0-1 confidence score
    detection_method: str  # url_pattern, html_signature, api_probe, redirect


@dataclass
class RelevanceResult:
    """Result of job relevance scoring."""

    score: float  # 0-100
    matched_category: str  # Best matching category
    matched_keywords: List[str]
    is_relevant: bool  # Score >= threshold


@dataclass
class JobChange:
    """A change in job status between runs."""

    job_id: int
    external_id: str
    title: str
    company_name: str
    change_type: str  # 'new', 'removed'
    job_url: str


@dataclass
class ChangeReport:
    """Report of changes detected in a run."""

    run_id: str
    run_date: datetime
    company_id: int
    company_name: str
    new_jobs: List[JobChange]
    removed_jobs: List[JobChange]
    total_active: int


@dataclass
class RunSummary:
    """Summary of a complete discovery run."""

    run_date: datetime
    elapsed_seconds: float
    companies_processed: int
    companies_successful: int
    total_jobs_found: int
    total_new_jobs: int
    total_removed_jobs: int
    by_status: Dict[str, int]
    by_ats: Dict[str, int]
    details: List[Dict[str, Any]]
