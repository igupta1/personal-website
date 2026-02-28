"""Decision maker lookup using Google Gemini with Search grounding for IT MSP pipeline."""

import json
import logging
import re
from typing import Dict, List, Optional, Any

from google import genai
from google.genai import types

from .models import DecisionMakerResult

logger = logging.getLogger(__name__)


VALID_INDUSTRIES = {
    "Healthcare", "Legal", "Financial Services", "Manufacturing",
    "Professional Services", "Construction", "Real Estate",
    "Retail / E-commerce", "Education", "Nonprofits",
    "Food & Beverage", "Other",
}


class ITDecisionMakerFinder:
    """
    Find IT purchasing decision makers at small companies using Google Gemini
    with Google Search grounding.

    Batches companies into groups to minimize API calls.
    """

    PROMPT_TEMPLATE = (
        'You have access to Google Search grounding. Your task is to identify '
        'the single most appropriate current decision maker responsible for '
        'IT purchasing, technology operations, or general business operations '
        'at each of the companies listed below.\n\n'
        'For each company, return exactly one person, chosen using this '
        'strict priority order:\n'
        '1. Owner, CEO, Founder, or Co-Founder (most common IT buyer at small businesses)\n'
        '2. IT Director, IT Manager, or CTO\n'
        '3. Office Manager, COO, or Operations Manager\n\n'
        'You must use only publicly verifiable sources such as LinkedIn '
        'profiles, company "About" or "Team" pages, or reputable press '
        'articles. Do not guess, infer, or hallucinate names or titles. If '
        'you cannot confidently identify a suitable person, explicitly return '
        '"Not confidently identifiable" and briefly state why.\n\n'
        'Do not return multiple people, do not list alternatives, and do not '
        'select individual contributors. Exclude recruiters, HR, '
        'engineers, designers, consultants, agencies, and former employees.\n\n'
        'For each company, output the company name, the decision maker\'s full '
        'name, exact current title, a source URL proving the role, a '
        'confidence level (High if the LinkedIn title clearly matches and is '
        'current, Medium if there is one strong but slightly ambiguous source), '
        'and the approximate employee count for the company (use LinkedIn or '
        'other public sources; return as an integer, e.g. 15, 50, 200). '
        'Also determine the industry category for each company. Choose exactly '
        'one from this list: Healthcare, Legal, Financial Services, Manufacturing, '
        'Professional Services, Construction, Real Estate, '
        'Retail / E-commerce, Education, Nonprofits, Food & Beverage, Other.\n\n'
        'Prefer accuracy over completeness.\n\n'
        'IMPORTANT: Return your results as a JSON array. Each element must be '
        'an object with these exact keys: "company_name", "person_name", '
        '"title", "source_url", "confidence", "employee_count", "industry". '
        'If not identifiable, set person_name to "Not confidently identifiable" '
        'and add a "reason" key.\n\n'
        'Companies:\n{company_list}'
    )

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.5-flash",
        batch_size: int = 5,
    ):
        self.model = model
        self.batch_size = batch_size

        client_kwargs = {}
        if api_key:
            client_kwargs["api_key"] = api_key
        self.client = genai.Client(**client_kwargs)

    async def find_decision_makers(
        self, companies: List[Dict[str, Any]]
    ) -> List[DecisionMakerResult]:
        """
        Look up decision makers for a list of companies.

        Args:
            companies: List of dicts, each with at least a "company" key (name)
                       and optionally a "domain" key.

        Returns:
            List of DecisionMakerResult, one per company.
        """
        all_results: List[DecisionMakerResult] = []

        batches = [
            companies[i : i + self.batch_size]
            for i in range(0, len(companies), self.batch_size)
        ]

        logger.info(
            f"Looking up decision makers for {len(companies)} companies "
            f"in {len(batches)} batch(es)"
        )

        for batch_idx, batch in enumerate(batches, 1):
            batch_names = [c["company"] for c in batch]
            logger.info(
                f"Decision maker batch {batch_idx}/{len(batches)}: {batch_names}"
            )

            try:
                batch_results = await self._process_batch(batch)
                all_results.extend(batch_results)
            except Exception as e:
                logger.error(
                    f"Failed to process decision maker batch {batch_idx}: {e}"
                )
                for company in batch:
                    all_results.append(
                        DecisionMakerResult(
                            company_name=company["company"],
                            not_found_reason=f"API error: {e}",
                        )
                    )

        return all_results

    async def _process_batch(
        self, batch: List[Dict[str, Any]]
    ) -> List[DecisionMakerResult]:
        """Process a single batch of companies via Gemini."""
        lines = []
        for c in batch:
            domain = c.get("domain", "")
            if domain:
                lines.append(f"- {c['company']} (website: {domain})")
            else:
                lines.append(f"- {c['company']}")
        company_list = "\n".join(lines)
        prompt = self.PROMPT_TEMPLATE.format(company_list=company_list)

        config = types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0.0,
        )

        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=prompt,
            config=config,
        )

        raw_text = response.text
        logger.debug(f"Gemini raw response:\n{raw_text}")

        return self._parse_response(raw_text, batch)

    def _parse_response(
        self,
        raw_text: str,
        batch: List[Dict[str, Any]],
    ) -> List[DecisionMakerResult]:
        """Parse Gemini's response into structured results."""
        batch_company_names = {c["company"] for c in batch}
        results_by_company: Dict[str, DecisionMakerResult] = {}

        parsed = self._try_parse_json(raw_text)
        if parsed:
            for entry in parsed:
                name = entry.get("company_name", "")
                matched = self._match_company_name(name, batch_company_names)
                if not matched:
                    continue

                person = entry.get("person_name", "")
                if person and "not confidently" in person.lower():
                    results_by_company[matched] = DecisionMakerResult(
                        company_name=matched,
                        not_found_reason=entry.get("reason", person),
                        raw_text=str(entry),
                    )
                else:
                    emp_count = entry.get("employee_count")
                    if emp_count is not None:
                        try:
                            emp_count = int(emp_count)
                        except (ValueError, TypeError):
                            emp_count = None
                    industry = entry.get("industry")
                    if industry and industry not in VALID_INDUSTRIES:
                        industry = "Other"
                    results_by_company[matched] = DecisionMakerResult(
                        company_name=matched,
                        person_name=person or None,
                        title=entry.get("title"),
                        source_url=entry.get("source_url"),
                        confidence=entry.get("confidence"),
                        employee_count=emp_count,
                        industry=industry,
                        raw_text=str(entry),
                    )
        else:
            logger.warning("JSON parsing failed, using regex fallback")
            results_by_company = self._regex_parse(raw_text, batch_company_names)

        # Ensure every company in the batch has a result
        for company_dict in batch:
            company_name = company_dict["company"]
            if company_name not in results_by_company:
                results_by_company[company_name] = DecisionMakerResult(
                    company_name=company_name,
                    not_found_reason="Not found in Gemini response",
                    raw_text=raw_text[:500],
                )

        return list(results_by_company.values())

    @staticmethod
    def _try_parse_json(text: str) -> Optional[List[Dict]]:
        """Try to extract and parse a JSON array from the response."""
        cleaned = text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1]).strip()

        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass

        match = re.search(r"\[[\s\S]*\]", text)
        if match:
            try:
                parsed = json.loads(match.group())
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass

        return None

    @staticmethod
    def _match_company_name(name: str, candidates: set) -> Optional[str]:
        """Match a company name from Gemini's response to the batch list."""
        if not name:
            return None
        name_lower = name.lower().strip()
        for candidate in candidates:
            if candidate.lower() == name_lower:
                return candidate
            if name_lower in candidate.lower() or candidate.lower() in name_lower:
                return candidate
        return None

    def _regex_parse(
        self,
        text: str,
        company_names: set,
    ) -> Dict[str, DecisionMakerResult]:
        """Fallback regex parsing for non-JSON responses."""
        results: Dict[str, DecisionMakerResult] = {}

        for company_name in company_names:
            pattern = re.compile(
                re.escape(company_name) + r"[:\s\-]*(.+?)(?=\n\n|\n-|\Z)",
                re.IGNORECASE | re.DOTALL,
            )
            match = pattern.search(text)
            if not match:
                continue

            block = match.group(1).strip()

            if "not confidently" in block.lower():
                results[company_name] = DecisionMakerResult(
                    company_name=company_name,
                    not_found_reason=block[:200],
                    raw_text=block,
                )
                continue

            person_match = re.search(
                r"(?:name|person|decision maker)[:\s]*"
                r"([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)",
                block,
                re.IGNORECASE,
            )
            title_match = re.search(
                r"(?:title|role|position)[:\s]*(.+?)(?:\n|,|$)",
                block,
                re.IGNORECASE,
            )
            url_match = re.search(
                r"(?:source|url|link)[:\s]*(https?://\S+)",
                block,
                re.IGNORECASE,
            )
            conf_match = re.search(
                r"(?:confidence)[:\s]*(high|medium)",
                block,
                re.IGNORECASE,
            )

            results[company_name] = DecisionMakerResult(
                company_name=company_name,
                person_name=person_match.group(1).strip() if person_match else None,
                title=title_match.group(1).strip() if title_match else None,
                source_url=url_match.group(1).strip() if url_match else None,
                confidence=conf_match.group(1).capitalize() if conf_match else None,
                raw_text=block,
            )

        return results
