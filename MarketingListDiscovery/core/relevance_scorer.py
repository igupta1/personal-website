"""Role relevance scoring using explicit marketing term matching."""

from typing import List, Tuple, Dict

from .models import RelevanceResult


class RoleRelevanceScorer:
    """
    Score job titles for marketing relevance using explicit term matching.

    Requires job titles to contain explicit marketing signals (like "marketing",
    "SEO", "copywriter") rather than fuzzy matching which catches false positives.
    """

    # REQUIRED: Job title must contain at least one of these marketing signals
    # These are explicit terms that unambiguously indicate a marketing role
    MARKETING_SIGNALS = [
        # Primary signal - catches most legitimate marketing roles
        "marketing",
        # Specific marketing functions (unambiguous)
        "seo",
        "ppc",
        "copywriter",
        "copywriting",
        "social media",
        "social strategist",
        "brand manager",
        "brand director",
        "brand strategist",
        "demand gen",
        "content strategist",
        "content manager",
        "influencer",
        "paid media",
        "paid social",
        "paid acquisition",
        "growth marketing",
        "growth marketer",
        "cmo",
        "chief marketing",
        "public relations",
        "communications manager",
        "communications director",
        "pr manager",
        "pr director",
        "media buyer",
    ]

    # Map signals to categories for reporting
    SIGNAL_TO_CATEGORY = {
        "marketing": "general_marketing",
        "seo": "seo",
        "ppc": "performance_marketing",
        "copywriter": "content_marketing",
        "copywriting": "content_marketing",
        "social media": "social_media",
        "brand manager": "brand_marketing",
        "brand director": "brand_marketing",
        "brand strategist": "brand_marketing",
        "demand gen": "demand_generation",
        "content strategist": "content_marketing",
        "influencer": "influencer_marketing",
        "paid media": "performance_marketing",
        "paid social": "performance_marketing",
        "paid acquisition": "performance_marketing",
        "growth marketing": "growth_marketing",
        "growth marketer": "growth_marketing",
        "cmo": "marketing_leadership",
        "chief marketing": "marketing_leadership",
        "public relations": "communications",
        "communications manager": "communications",
        "communications director": "communications",
        "pr manager": "communications",
        "pr director": "communications",
        "media buyer": "performance_marketing",
    }

    # Keywords that indicate non-marketing roles to exclude
    # Even if a title contains "marketing", these exclusions take precedence
    EXCLUSION_KEYWORDS = [
        # Engineering/Tech
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
        "security engineer",
        "infrastructure",
        "platform engineer",
        "qa engineer",
        "quality assurance",
        "test engineer",
        # Sales (not marketing)
        "sales representative",
        "account executive",
        "sdr",
        "bdr",
        "sales development",
        "business development",
        "sales manager",
        "director of sales",
        "sales training",
        "sales enablement",
        "sales operations",
        "sales analyst",
        "sales associate",
        "sales coordinator",
        "account manager",
        "key account",
        # HR/Recruiting
        "recruiter",
        "people ops",
        "talent acquisition",
        "talent partner",
        "human resources",
        # Finance/Legal
        "finance",
        "accounting",
        "legal",
        "compliance",
        "general counsel",
        "accountant",
        "financial analyst",
        "financial planning",
        "investor relations",
        "capital markets",
        # Customer/Support
        "customer success",
        "customer support",
        "customer service",
        "support engineer",
        "technical support",
        "student success",
        # Operations/Admin
        "operations manager",
        "director of operations",
        "operations analyst",
        "operations associate",
        "operations coordinator",
        "operations executive",
        "office manager",
        "executive assistant",
        "administrative",
        "facilities",
        "supply chain",
        "implementation",
        "revenue operations",
        "business operations",
        "general manager",
        # Product (not product marketing)
        "product manager",
        "product designer",
        "product owner",
        "product operations",
        # Shipping/Logistics (not marketing)
        "shipping",
        "freight",
        "logistics",
        "marine",
        "surveyor",
        "move coordinator",
        "buyer",
        # Growth roles without marketing context
        "growth manager",
        "growth associate",
        "growth hacker",
        "head of growth",
        # Education/Childcare (false positives from "lead")
        "teacher",
        "childcare",
        "preschool",
        "early childhood",
        "learning specialist",
        "education manager",
        "academic",
        "instructor",
        "tutor",
        # Other non-marketing
        "research mentor",
        "enrollment manager",
        "head of partnerships",
        "partnerships manager",
        "video editor",
        "scriptwriter",
        "founder in residence",
        "online events",
        "vip manager",
        "avionics",
        "mission development",
        "project manager",
        "program manager",
    ]

    def __init__(self, relevance_threshold: float = 60.0):
        self.threshold = relevance_threshold

    def score(self, job_title: str, job_description: str = "") -> RelevanceResult:
        """
        Score a job's relevance to marketing roles.

        Algorithm:
        1. Check exclusion keywords (fast rejection)
        2. Check for explicit marketing signals (required)
        3. Boost score if description reinforces marketing context
        4. Return category match and score
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

        # Step 2: Check for explicit marketing signals (REQUIRED)
        matched_signal = None
        for signal in self.MARKETING_SIGNALS:
            if signal in title_lower:
                matched_signal = signal
                break

        if not matched_signal:
            # No marketing signal found - not a marketing role
            return RelevanceResult(
                score=0.0,
                matched_category="no_marketing_signal",
                matched_keywords=[],
                is_relevant=False,
            )

        # Step 3: Determine category from matched signal
        category = self.SIGNAL_TO_CATEGORY.get(matched_signal, "general_marketing")

        # Refine category based on title keywords
        if "director" in title_lower or "vp" in title_lower or "head of" in title_lower:
            category = "marketing_leadership"
        elif "product marketing" in title_lower:
            category = "product_marketing"
        elif "content" in title_lower:
            category = "content_marketing"
        elif "brand" in title_lower:
            category = "brand_marketing"
        elif "demand" in title_lower:
            category = "demand_generation"
        elif "growth" in title_lower:
            category = "growth_marketing"
        elif "social" in title_lower:
            category = "social_media"
        elif "seo" in title_lower:
            category = "seo"
        elif "paid" in title_lower or "ppc" in title_lower or "performance" in title_lower:
            category = "performance_marketing"
        elif "email" in title_lower or "lifecycle" in title_lower or "retention" in title_lower:
            category = "lifecycle_crm"

        # Step 4: Base score = 80, with description boost up to 20
        base_score = 80.0
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
            description_boost = min(matches * 4, 20)  # Max 20 point boost

        final_score = min(base_score + description_boost, 100.0)

        return RelevanceResult(
            score=final_score,
            matched_category=category,
            matched_keywords=[matched_signal],
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
