"""Combined insight generation + priority classification in a single Claude call."""

import json
import logging
import re
from typing import Dict, List, Optional, Any

from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)

VALID_TIERS = {"P1", "P2", "P3", "P4", "P5"}


class InsightAndPriorityGenerator:
    """Generate insights and classify priority tiers in a single LLM call."""

    PROMPT_TEMPLATE = (
        'You are an IT managed services provider (MSP) strategist and lead prioritization system. '
        'For each company below, do TWO things:\n\n'
        '**Task 1 — Insight:** Write exactly ONE sentence (max 20 words) explaining '
        'why an IT MSP should reach out. Be specific to the role, company type, '
        'and hiring signal.\n'
        '- Do NOT mention the company name. Do NOT start with "This company" or "They are".\n'
        '- Start with a verb or observation.\n'
        '- Focus on what the hiring signal reveals about IT maturity, infrastructure gaps, '
        'or support capacity holes.\n\n'
        '**Task 2 — Priority Tier:** Classify the company into P1-P5.\n\n'
        'Step A — Determine relevancy:\n'
        'Automatic LOW:\n'
        '- Company IS an IT MSP/managed services/IT consulting/cybersecurity provider (competitor)\n'
        '- Large enterprise (Fortune 500, 500+ employees)\n'
        '- Tech company managing own IT\n'
        '- Senior/executive IT roles (CIO, VP of IT) suggesting an established IT department\n\n'
        'HIGH (all must be true):\n'
        '- Not in IT services/MSP/technology space\n'
        '- Small business (under 100 employees)\n'
        '- Role indicates IT growing pains (IT Manager, Systems Admin, Help Desk, IT Support, '
        'Network Admin, IT Coordinator, Desktop Support, IT Specialist, IT Technician, Network Engineer)\n'
        '- Strong-fit vertical (Healthcare, Legal, Financial Services, Manufacturing, '
        'Professional Services, Construction, Real Estate, Retail/E-commerce, Education, '
        'Nonprofits, Food & Beverage)\n\n'
        'MEDIUM (any of these):\n'
        '- 50-100 employees\n'
        '- Tangential roles (not core IT growing-pains roles)\n'
        '- Slower adoption industry\n'
        '- Temp or project-based IT roles\n'
        '- IT Director present (some internal IT capability)\n'
        '- Fits High criteria but insufficient context\n\n'
        'Step B — Assign tier:\n'
        '- P1 = High relevancy + Decision Maker identified\n'
        '- P2 = Medium relevancy + Decision Maker identified\n'
        '- P3 = High relevancy + No Decision Maker\n'
        '- P4 = Medium relevancy + No Decision Maker\n'
        '- P5 = Low relevancy (regardless of DM)\n\n'
        'IMPORTANT: Return a JSON array. Each element must have these exact keys: '
        '"company_name", "insight", "priority_tier", "reason".\n'
        'priority_tier must be exactly one of: "P1", "P2", "P3", "P4", "P5".\n'
        'reason should be 1 short sentence for the priority classification.\n\n'
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

    async def generate(
        self, companies: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, str]]:
        """
        Generate insights and priority tiers for a list of companies.

        Args:
            companies: List of dicts with keys:
                - company_name, domain, industry, employee_count, roles, has_decision_maker

        Returns:
            Dict mapping company_name -> {"insight": str, "priority_tier": str, "reason": str}
        """
        all_results: Dict[str, Dict[str, str]] = {}

        batches = [
            companies[i : i + self.batch_size]
            for i in range(0, len(companies), self.batch_size)
        ]

        logger.info(
            f"Generating insights + priorities for {len(companies)} companies "
            f"in {len(batches)} batch(es)"
        )

        for batch_idx, batch in enumerate(batches, 1):
            logger.info(
                f"Insight+priority batch {batch_idx}/{len(batches)}: "
                f"{[c['company_name'] for c in batch]}"
            )
            try:
                batch_results = await self._process_batch(batch)
                all_results.update(batch_results)
            except Exception as e:
                logger.error(f"Failed insight+priority batch {batch_idx}: {e}")

        return all_results

    async def _process_batch(
        self, batch: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, str]]:
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
            max_tokens=2000,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )

        return self._parse_response(response.content[0].text, batch)

    def _parse_response(
        self, raw_text: str, batch: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, str]]:
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
                insight = entry.get("insight", "")
                tier = entry.get("priority_tier", "")
                reason = entry.get("reason", "")
                matched = self._match_company_name(name, batch_names)
                if matched:
                    if tier and tier.upper() not in VALID_TIERS:
                        tier = "P5"
                    results[matched] = {
                        "insight": insight.strip() if insight else "",
                        "priority_tier": tier.upper() if tier else "P5",
                        "reason": reason,
                    }

        return results

    @staticmethod
    def _match_company_name(name: str, candidates: set) -> Optional[str]:
        if not name:
            return None
        name_lower = name.lower().strip()
        for candidate in candidates:
            if candidate.lower() == name_lower:
                return candidate
            if name_lower in candidate.lower() or candidate.lower() in name_lower:
                return candidate
        return None
