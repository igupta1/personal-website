"""AI-generated outreach insights using Anthropic Claude."""

import json
import logging
import re
from typing import Dict, List, Optional, Any

from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)


class InsightGenerator:
    """
    Generate 1-2 sentence outreach insights for companies,
    explaining why a marketing agency should reach out.
    """

    PROMPT_TEMPLATE = (
        'You are a marketing agency strategist. For each company below, '
        'write exactly ONE sentence (max 20 words) explaining why a marketing '
        'agency should reach out. Be specific to the role, company type, and hiring signal.\n\n'
        'Do NOT mention the company name. Do NOT start with "This company" or '
        '"They are". Start with a verb or observation.\n\n'
        'GOOD examples:\n'
        '- "Hiring a CRM Specialist without a dedicated marketing team signals agency opportunity for retention strategy."\n'
        '- "Expanding into DTC e-commerce while hiring entry-level suggests they need senior strategic guidance externally."\n'
        '- "Opening a Marketing Coordinator role at a 50-person SaaS startup indicates building the function from scratch."\n\n'
        'BAD examples (too generic — avoid these patterns):\n'
        '- "Hiring a marketing role suggests a need for marketing support."\n'
        '- "Could benefit from agency expertise for their marketing needs."\n'
        '- "Investing in marketing suggests a need for strategic support."\n\n'
        'Focus on: what the specific hiring signal reveals about their marketing maturity, '
        'budget gaps, or capability holes that an agency could fill.\n\n'
        'IMPORTANT: Return your results as a JSON array. Each element must be '
        'an object with these exact keys: "company_name", "insight".\n\n'
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

    async def generate_insights(
        self, companies: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        Generate insights for a list of companies.

        Args:
            companies: List of dicts with keys:
                - company_name (str)
                - domain (str)
                - roles (list of str): job titles being hired

        Returns:
            Dict mapping company_name -> insight string
        """
        all_insights: Dict[str, str] = {}

        batches = [
            companies[i : i + self.batch_size]
            for i in range(0, len(companies), self.batch_size)
        ]

        logger.info(
            f"Generating insights for {len(companies)} companies "
            f"in {len(batches)} batch(es)"
        )

        for batch_idx, batch in enumerate(batches, 1):
            logger.info(
                f"Insight batch {batch_idx}/{len(batches)}: "
                f"{[c['company_name'] for c in batch]}"
            )
            try:
                batch_insights = await self._process_batch(batch)
                all_insights.update(batch_insights)
            except Exception as e:
                logger.error(f"Failed insight batch {batch_idx}: {e}")

        return all_insights

    async def _process_batch(
        self, batch: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Process a single batch via Anthropic Claude."""
        lines = []
        for c in batch:
            roles_str = ", ".join(c.get("roles", []))
            line = f"- {c['company_name']} (website: {c.get('domain', '')})"
            if roles_str:
                line += f" — hiring: {roles_str}"
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
    ) -> Dict[str, str]:
        """Parse LLM response into company_name -> insight mapping."""
        batch_names = {c["company_name"] for c in batch}
        results: Dict[str, str] = {}

        # Strip markdown code fences
        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1]).strip()

        # Try JSON parsing (same pattern as decision_maker.py)
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
                insight = entry.get("insight", "")
                matched = self._match_company_name(name, batch_names)
                if matched and insight:
                    results[matched] = insight.strip()

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
