"""Role relevance scoring using fuzzy matching."""

from typing import List, Tuple, Dict
from rapidfuzz import fuzz

from .models import RelevanceResult


class RoleRelevanceScorer:
    """
    Score job titles for marketing relevance using fuzzy matching.

    Uses RapidFuzz for fast, accurate fuzzy string matching.
    """

    # Primary marketing role categories with keywords
    ROLE_CATEGORIES = {
        "growth_marketing": [
            "growth",
            "growth marketing",
            "growth marketer",
            "head of growth",
            "growth lead",
            "growth manager",
            "user acquisition",
            "acquisition marketing",
            "growth hacker",
        ],
        "content_marketing": [
            "content",
            "content marketing",
            "content strategist",
            "content manager",
            "content lead",
            "copywriter",
            "editor",
            "editorial",
            "content writer",
            "content creator",
            "blog",
            "blogger",
        ],
        "seo": [
            "seo",
            "search engine",
            "organic",
            "seo manager",
            "seo specialist",
            "search marketing",
            "organic growth",
            "seo lead",
        ],
        "performance_marketing": [
            "performance",
            "performance marketing",
            "paid media",
            "paid acquisition",
            "paid social",
            "ppc",
            "sem",
            "digital advertising",
            "media buyer",
            "paid marketing",
            "digital media",
            "paid ads",
        ],
        "lifecycle_crm": [
            "lifecycle",
            "crm",
            "email marketing",
            "retention",
            "engagement",
            "customer marketing",
            "lifecycle marketing",
            "marketing automation",
            "email",
            "retention marketing",
            "customer engagement",
        ],
        "brand_marketing": [
            "brand",
            "brand marketing",
            "brand manager",
            "brand strategist",
            "brand lead",
            "creative marketing",
            "brand director",
            "brand specialist",
        ],
        "demand_generation": [
            "demand gen",
            "demand generation",
            "lead generation",
            "pipeline",
            "b2b marketing",
            "field marketing",
            "lead gen",
            "pipeline marketing",
            "demand marketing",
        ],
        "product_marketing": [
            "product marketing",
            "pmm",
            "product marketer",
            "go-to-market",
            "gtm",
            "product launch",
            "product marketing manager",
            "solutions marketing",
        ],
        "marketing_leadership": [
            "vp marketing",
            "vp of marketing",
            "head of marketing",
            "marketing director",
            "cmo",
            "chief marketing",
            "marketing lead",
            "director of marketing",
            "svp marketing",
            "evp marketing",
        ],
        "general_marketing": [
            "marketing",
            "marketing manager",
            "marketing specialist",
            "marketing coordinator",
            "digital marketing",
            "marketing associate",
            "marketing analyst",
        ],
        "social_media": [
            "social media",
            "social media manager",
            "community manager",
            "social marketing",
            "social media specialist",
            "community",
        ],
        "communications": [
            "communications",
            "pr",
            "public relations",
            "corporate communications",
            "comms",
            "communications manager",
        ],
        "partnerships": [
            "partner marketing",
            "partnership marketing",
            "channel marketing",
            "affiliate",
            "affiliate marketing",
        ],
    }

    # Keywords that indicate non-marketing roles to exclude
    EXCLUSION_KEYWORDS = [
        "engineer",
        "developer",
        "software",
        "backend",
        "frontend",
        "full stack",
        "fullstack",
        "data scientist",
        "machine learning",
        "devops",
        "sre",
        "site reliability",
        "data engineer",
        "ml engineer",
        "ai engineer",
        "sales representative",
        "account executive",
        "sdr",
        "bdr",
        "sales development",
        "business development rep",
        "recruiter",
        "hr",
        "people ops",
        "talent acquisition",
        "human resources",
        "finance",
        "accounting",
        "legal",
        "compliance",
        "general counsel",
        "accountant",
        "financial analyst",
        "customer success",
        "customer support",
        "support engineer",
        "technical support",
        "operations manager",
        "office manager",
        "executive assistant",
        "administrative",
        "facilities",
        "security engineer",
        "infrastructure",
        "platform engineer",
        "qa engineer",
        "quality assurance",
        "test engineer",
    ]

    def __init__(self, relevance_threshold: float = 60.0):
        self.threshold = relevance_threshold
        # Flatten all keywords for initial screening
        self._all_keywords: List[Tuple[str, str]] = []
        for category, keywords in self.ROLE_CATEGORIES.items():
            for kw in keywords:
                self._all_keywords.append((kw, category))

    def score(self, job_title: str, job_description: str = "") -> RelevanceResult:
        """
        Score a job's relevance to marketing roles.

        Algorithm:
        1. Check exclusion keywords (fast rejection)
        2. Fuzzy match title against all marketing keywords
        3. Boost score if description contains marketing terms
        4. Return best category match and overall score
        """
        title_lower = job_title.lower().strip()

        # Step 1: Fast exclusion check
        for exclusion in self.EXCLUSION_KEYWORDS:
            if exclusion in title_lower:
                return RelevanceResult(
                    score=0.0,
                    matched_category="excluded",
                    matched_keywords=[],
                    is_relevant=False,
                )

        # Step 2: Fuzzy match against all keywords
        best_matches: List[Tuple[float, str, str]] = []
        for keyword, category in self._all_keywords:
            # Use token_set_ratio for better partial matching
            ratio = fuzz.token_set_ratio(keyword, title_lower)
            if ratio >= 50:  # Minimum threshold for consideration
                best_matches.append((ratio, keyword, category))

        if not best_matches:
            return RelevanceResult(
                score=0.0,
                matched_category="none",
                matched_keywords=[],
                is_relevant=False,
            )

        # Sort by score descending
        best_matches.sort(reverse=True, key=lambda x: x[0])

        # Take top match
        top_score, top_keyword, top_category = best_matches[0]

        # Step 3: Description boost
        description_boost = 0.0
        if job_description:
            desc_lower = job_description.lower()
            marketing_terms = [
                "marketing",
                "campaign",
                "brand",
                "content",
                "seo",
                "growth",
                "acquisition",
                "funnel",
                "conversion",
                "analytics",
                "strategy",
            ]
            matches = sum(1 for term in marketing_terms if term in desc_lower)
            description_boost = min(matches * 3, 15)  # Max 15 point boost

        final_score = min(top_score + description_boost, 100.0)

        # Collect all matched keywords above threshold
        matched_keywords = [kw for score, kw, cat in best_matches if score >= 50]

        return RelevanceResult(
            score=final_score,
            matched_category=top_category,
            matched_keywords=matched_keywords[:5],  # Top 5
            is_relevant=final_score >= self.threshold,
        )

    def filter_relevant_jobs(
        self, jobs: List[Dict]
    ) -> List[Tuple[Dict, RelevanceResult]]:
        """
        Filter and score a list of jobs, returning only relevant ones.

        Args:
            jobs: List of job dicts with 'title' and optionally 'description'

        Returns:
            List of (job_data, RelevanceResult) tuples for relevant jobs
        """
        results = []
        for job in jobs:
            title = job.get("title", "")
            description = job.get("description", "")
            result = self.score(title, description)
            if result.is_relevant:
                results.append((job, result))

        # Sort by relevance score
        results.sort(key=lambda x: x[1].score, reverse=True)
        return results
