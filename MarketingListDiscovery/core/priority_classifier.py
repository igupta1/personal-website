"""Priority tier classification using Anthropic Claude."""

import json
import logging
import re
from typing import Dict, List, Optional, Any

from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)

VALID_TIERS = {"P1", "P2", "P3", "P4", "P5"}


class PriorityClassifier:
    """
    Classify companies into priority tiers (P1-P5) based on
    relevancy to a marketing agency's ideal client profile.
    """

    PROMPT_TEMPLATE = (
        'You are a lead prioritization system for a marketing agency that serves '
        'small and medium businesses. For each company below, classify it into a '
        'priority tier (P1 through P5) based on how strong a prospect it is.\n\n'
        'STEP 1 — Determine relevancy tier:\n\n'
        'Automatic LOW — disqualify these immediately:\n'
        '- Company IS a marketing, advertising, PR, media, or creative agency '
        '(they are competitors, not clients)\n'
        '- Company is a large enterprise (Fortune 500, publicly traded global '
        'brands, 1000+ employees)\n'
        '- Role is clearly non-marketing (fundraising, research operations, '
        'brand protection/legal, corporate relations)\n'
        '- Company appears to be a direct sales / MLM organization\n\n'
        'HIGH — all of these must be true:\n'
        '- Company is NOT in the marketing/advertising/media space\n'
        '- Company is an SMB (likely under 200 employees)\n'
        '- Job role is a core marketing function: Marketing Coordinator, '
        'Marketing Specialist, Marketing Manager, Digital Marketing, Social '
        'Media Specialist/Manager, Content Strategist, Growth Marketing, SEO '
        'Specialist, Paid Media, GTM roles\n'
        '- Company type is in a strong-fit vertical: local/home services, B2B '
        'SaaS, healthcare practices, real estate, DTC/e-commerce brands, '
        'professional services (engineering, accounting, legal), tech startups, '
        'consumer brands\n\n'
        'MEDIUM — any of these conditions:\n'
        '- Non-profit or membership organization (budget constraints likely)\n'
        '- Role is tangential: Event Coordinator, Brand Ambassador, Community '
        'Manager, Market Research, Communications Associate\n'
        '- Company is in a regulated industry (financial services, law, '
        'insurance) where agency adoption is slower\n'
        '- Role is part-time or short-term contract\n'
        '- Company has existing internal marketing leadership (VP/Director of '
        'Marketing) suggesting less need for external agency\n'
        '- Company fits High criteria but insufficient context to confirm\n\n'
        'STEP 2 — Assign priority:\n'
        '- P1 = High relevancy + Decision Maker identified\n'
        '- P2 = Medium relevancy + Decision Maker identified\n'
        '- P3 = High relevancy + No Decision Maker identified\n'
        '- P4 = Medium relevancy + No Decision Maker identified\n'
        '- P5 = Low relevancy (regardless of Decision Maker)\n\n'
        'For each company, I am providing: company name, domain, industry, '
        'approximate employee count, job roles being hired, and whether a '
        'decision maker has been identified.\n\n'
        'IMPORTANT: Return your results as a JSON array. Each element must be '
        'an object with these exact keys: "company_name", "priority_tier", "reason".\n'
        'The priority_tier must be exactly one of: "P1", "P2", "P3", "P4", "P5".\n'
        'The reason should be 1 short sentence explaining the classification.\n\n'
        'Companies:\n{company_list}'
    )

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-6",
        batch_size: int = 10,
    ):
        self.model = model
        self.batch_size = batch_size
        self.client = AsyncAnthropic(api_key=api_key, max_retries=3)

    async def classify_priorities(
        self, companies: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, str]]:
        """
        Classify priority tiers for a list of companies.

        Args:
            companies: List of dicts with keys:
                - company_name (str)
                - domain (str)
                - industry (str)
                - employee_count (int or None)
                - roles (list of str)
                - has_decision_maker (bool)

        Returns:
            Dict mapping company_name -> {"priority_tier": "P1", "reason": "..."}
        """
        all_results: Dict[str, Dict[str, str]] = {}

        batches = [
            companies[i : i + self.batch_size]
            for i in range(0, len(companies), self.batch_size)
        ]

        logger.info(
            f"Classifying priorities for {len(companies)} companies "
            f"in {len(batches)} batch(es)"
        )

        for batch_idx, batch in enumerate(batches, 1):
            logger.info(
                f"Priority batch {batch_idx}/{len(batches)}: "
                f"{[c['company_name'] for c in batch]}"
            )
            try:
                batch_results = await self._process_batch(batch)
                all_results.update(batch_results)
            except Exception as e:
                logger.error(f"Failed priority batch {batch_idx}: {e}")

        return all_results

    async def _process_batch(
        self, batch: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, str]]:
        """Process a single batch via Anthropic Claude."""
        lines = []
        for c in batch:
            emp = f"~{c['employee_count']} employees" if c.get("employee_count") else "unknown size"
            roles_str = ", ".join(c.get("roles", [])) or "unknown roles"
            dm_str = "Yes" if c.get("has_decision_maker") else "No"
            industry = c.get("industry") or "unknown"
            line = (
                f"- {c['company_name']} (domain: {c.get('domain', '')}, "
                f"industry: {industry}, {emp}, "
                f"hiring: {roles_str}, DM: {dm_str})"
            )
            lines.append(line)

        company_list = "\n".join(lines)
        prompt = self.PROMPT_TEMPLATE.format(company_list=company_list)

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=1500,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )

        return self._parse_response(response.content[0].text, batch)

    def _parse_response(
        self, raw_text: str, batch: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, str]]:
        """Parse LLM response into company_name -> {priority_tier, reason} mapping."""
        batch_names = {c["company_name"] for c in batch}
        results: Dict[str, Dict[str, str]] = {}

        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1]).strip()

        parsed = None
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            match = re.search(r"\[[\s\S]*\]", raw_text)
            if match:
                try:
                    parsed = json.loads(match.group())
                except json.JSONDecodeError:
                    pass

        if parsed and isinstance(parsed, list):
            for entry in parsed:
                name = entry.get("company_name", "")
                tier = entry.get("priority_tier", "")
                reason = entry.get("reason", "")
                matched = self._match_company_name(name, batch_names)
                if matched and tier:
                    if tier.upper() not in VALID_TIERS:
                        tier = "P5"
                    results[matched] = {
                        "priority_tier": tier.upper(),
                        "reason": reason,
                    }

        return results

    @staticmethod
    def _match_company_name(name: str, candidates: set) -> Optional[str]:
        """Match company name from response to batch list."""
        if not name:
            return None
        name_lower = name.lower().strip()
        for candidate in candidates:
            if candidate.lower() == name_lower:
                return candidate
            if name_lower in candidate.lower() or candidate.lower() in name_lower:
                return candidate
        return None
