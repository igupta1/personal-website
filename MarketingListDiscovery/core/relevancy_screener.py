"""Early relevancy screening using Anthropic Claude.

Runs on all scraped companies BEFORE expensive enrichment stages
(DM lookup, outreach, etc.) to filter to the most promising prospects.
"""

import json
import logging
import re
from typing import Dict, List, Optional, Any

from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)


class RelevancyScreener:
    """
    Screen companies for relevancy to a marketing agency before
    running expensive enrichment. Returns a score (1-100) per company.
    """

    PROMPT_TEMPLATE = (
        'You are a lead qualification system for a marketing agency that serves '
        'small and medium businesses. For each company below, score how strong a '
        'prospect it is on a scale of 1-100.\n\n'
        'Scoring guide:\n\n'
        '**Score 1-20 (Disqualified):**\n'
        '- Company IS a marketing, advertising, PR, media, or creative agency '
        '(they are competitors, not clients)\n'
        '- Company is a large enterprise (Fortune 500, publicly traded global '
        'brands, 1000+ employees)\n'
        '- Role is clearly non-marketing (fundraising, research operations, '
        'brand protection/legal, corporate relations)\n'
        '- Company appears to be a direct sales / MLM organization\n\n'
        '**Score 70-100 (High):**\n'
        '- Company is NOT in the marketing/advertising/media space\n'
        '- Company is likely an SMB (under 200 employees)\n'
        '- Job role is a core marketing function: Marketing Coordinator, '
        'Marketing Specialist, Marketing Manager, Digital Marketing, Social '
        'Media Specialist/Manager, Content Strategist, Growth Marketing, SEO '
        'Specialist, Paid Media, GTM roles\n'
        '- Company type is in a strong-fit vertical: local/home services, B2B '
        'SaaS, healthcare practices, real estate, DTC/e-commerce brands, '
        'professional services, tech startups, consumer brands\n\n'
        '**Score 40-69 (Medium):**\n'
        '- Non-profit or membership organization\n'
        '- Role is tangential: Event Coordinator, Brand Ambassador, Community '
        'Manager, Market Research, Communications Associate\n'
        '- Company is in a regulated industry (financial services, law, insurance)\n'
        '- Role is part-time or short-term contract\n'
        '- Company fits High criteria but insufficient context to confirm\n\n'
        '**Score 21-39 (Low):**\n'
        '- Borderline disqualified but not clearly a competitor or enterprise\n'
        '- Very niche or unclear company with weak marketing signal\n\n'
        'For each company, I am providing: company name, domain, and job '
        'roles being hired.\n\n'
        'IMPORTANT: Return your results as a JSON array. Each element must be '
        'an object with these exact keys: "company_name", "score", "reason".\n'
        'The score must be an integer from 1 to 100.\n'
        'The reason should be 1 short sentence explaining the score.\n\n'
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

    async def screen_companies(
        self, companies: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Score a list of companies by relevancy.

        Args:
            companies: List of dicts with keys:
                - company_name (str)
                - domain (str)
                - roles (list of str)

        Returns:
            Dict mapping company_name -> {"score": int, "reason": str}
        """
        all_results: Dict[str, Dict[str, Any]] = {}

        batches = [
            companies[i : i + self.batch_size]
            for i in range(0, len(companies), self.batch_size)
        ]

        logger.info(
            f"Screening {len(companies)} companies for relevancy "
            f"in {len(batches)} batch(es)"
        )

        for batch_idx, batch in enumerate(batches, 1):
            logger.info(
                f"Relevancy batch {batch_idx}/{len(batches)}: "
                f"{[c['company_name'] for c in batch]}"
            )
            try:
                batch_results = await self._process_batch(batch)
                all_results.update(batch_results)
            except Exception as e:
                logger.error(f"Failed relevancy batch {batch_idx}: {e}")
                # Assign default medium score for failed batches so they aren't dropped
                for c in batch:
                    all_results[c["company_name"]] = {
                        "score": 50,
                        "reason": "Screening failed, assigned default score",
                    }

        return all_results

    async def _process_batch(
        self, batch: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Process a single batch via Anthropic Claude."""
        lines = []
        for c in batch:
            roles_str = ", ".join(c.get("roles", [])) or "unknown roles"
            line = (
                f"- {c['company_name']} (domain: {c.get('domain', '')}, "
                f"hiring: {roles_str})"
            )
            lines.append(line)

        company_list = "\n".join(lines)
        prompt = self.PROMPT_TEMPLATE.format(company_list=company_list)

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )

        return self._parse_response(response.content[0].text, batch)

    def _parse_response(
        self, raw_text: str, batch: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Parse LLM response into company_name -> {score, reason} mapping."""
        batch_names = {c["company_name"] for c in batch}
        results: Dict[str, Dict[str, Any]] = {}

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
                score = entry.get("score", 50)
                reason = entry.get("reason", "")
                matched = self._match_company_name(name, batch_names)
                if matched:
                    try:
                        score = int(score)
                        score = max(1, min(100, score))
                    except (ValueError, TypeError):
                        score = 50
                    results[matched] = {"score": score, "reason": reason}

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
