"""Early relevancy screening using Anthropic Claude.

Runs on all scraped companies BEFORE expensive enrichment stages
(DM lookup, outreach, etc.) to filter to the most promising MSP prospects.
"""

import json
import logging
import re
from typing import Dict, List, Optional, Any

from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)


class RelevancyScreener:
    """
    Screen companies for relevancy to an IT MSP before
    running expensive enrichment. Returns a score (1-100) per company.
    """

    PROMPT_TEMPLATE = (
        'You are a lead qualification system for an IT Managed Service Provider (MSP) '
        'that serves small and medium businesses. For each company below, score how strong '
        'a prospect it is on a scale of 1-100.\n\n'
        'Scoring guide:\n\n'
        '**Score 1-20 (Disqualified):**\n'
        '- Company IS an IT MSP, managed services provider, IT consulting firm, '
        'or cybersecurity vendor (they are competitors, not clients)\n'
        '- Company is a large enterprise (Fortune 500, publicly traded, 500+ employees)\n'
        '- Company is a technology company that clearly manages its own IT\n'
        '- Government agency or military branch\n'
        '- Staffing/recruiting firm posting on behalf of clients '
        '(e.g., TEKsystems, Robert Half, Kforce, Insight Global)\n\n'
        '**Score 70-100 (High):**\n'
        '- Company is NOT in IT services/MSP/technology space\n'
        '- Company is likely a small business (under 100 employees)\n'
        '- Job role indicates IT growing pains: IT Manager, Help Desk, Systems '
        'Administrator, Network Admin, IT Support, Desktop Support\n'
        '- Strong-fit vertical: Healthcare, Legal, Financial Services, Manufacturing, '
        'Professional Services, Construction, Real Estate, Retail, Education, '
        'Nonprofits, Food & Beverage\n\n'
        '**Score 40-69 (Medium):**\n'
        '- Company has 100-300 employees\n'
        '- Role is tangential or unclear (generic "tech support")\n'
        '- Industry where outsourced IT adoption is slower\n'
        '- Company already has IT Director suggesting existing IT maturity\n'
        '- Fits High criteria but insufficient context to confirm\n\n'
        '**Score 21-39 (Low):**\n'
        '- Borderline disqualified but not clearly a competitor or enterprise\n'
        '- Very niche or unclear company with weak signal\n\n'
        'For each company, I am providing: company name, job roles being hired, '
        'location, and a snippet from the job description.\n\n'
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
        batch_size: int = 15,
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
                - roles (list of str)
                - location (str, optional)
                - description_snippet (str, optional)

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
            location = c.get("location", "")
            snippet = c.get("description_snippet", "")
            line = f"- {c['company_name']} (hiring: {roles_str}"
            if location:
                line += f", location: {location}"
            if snippet:
                # Truncate snippet for prompt efficiency
                line += f', snippet: "{snippet[:200]}"'
            line += ")"
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
    ) -> Dict[str, Dict[str, Any]]:
        """Parse Claude response into company_name -> {score, reason} mapping."""
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
