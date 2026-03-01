"""Decision maker lookup using Google Gemini with Search grounding for MSP sales pipeline."""

import json
import logging
import re
from typing import Dict, List, Optional, Any

from google import genai
from google.genai import types

from .models import DecisionMakerResult

logger = logging.getLogger(__name__)


class MSPDecisionMakerFinder:
    """
    Verify IT MSP status, get employee count, and find decision makers
    using Google Gemini with Google Search grounding.

    Batches companies into groups to minimize API calls.
    """

    PROMPT_TEMPLATE = (
        'You have access to Google Search grounding. For each company listed below, '
        'perform THREE tasks:\n\n'
        '**Task 1: MSP Verification**\n'
        'Determine whether the company is an IT Managed Service Provider (MSP), '
        'Managed IT Services company, or IT services/support company that provides '
        'outsourced IT services to other businesses.\n\n'
        'IMPORTANT: If the company listed is a staffing/recruiting agency or a job '
        'board (e.g. Dice, VirtualVocations, Indeed) that is posting a job on behalf '
        'of an actual IT MSP client, try to identify the underlying MSP client '
        'company. If you can identify the MSP client, set is_msp to true and use '
        'the MSP client\'s name as company_name, their website, employee count, '
        'and decision maker — NOT the staffing agency\'s or job board\'s. '
        'If you cannot identify the underlying MSP client, set is_msp to false.\n\n'
        'Companies that are software vendors, general marketing firms, or non-IT '
        'consulting firms should be marked as is_msp: false. Staffing agencies '
        'hiring for their own internal roles (not on behalf of an MSP) should also '
        'be marked as is_msp: false.\n\n'
        '**Task 2: Employee Count**\n'
        'Find the approximate current employee count using LinkedIn, company website, '
        'or other public sources. Return as an integer.\n\n'
        '**Task 3: Decision Maker**\n'
        'Identify the single most appropriate decision maker responsible for '
        'sales, marketing, or overall business growth at this company. '
        'For small MSPs (<=100 employees), this is almost always the Owner, CEO, '
        'Founder, or President. Use this strict priority order:\n'
        '1. Owner, CEO, Founder, President, or Co-Founder\n'
        '2. VP of Sales, Sales Director, or Chief Revenue Officer\n'
        '3. VP of Marketing, Marketing Director, or CMO\n\n'
        'Use only publicly verifiable sources (LinkedIn profiles, company "About" '
        'or "Team" pages, press articles). Do not guess or hallucinate. If you '
        'cannot confidently identify a person, set person_name to '
        '"Not confidently identifiable" and explain why in the reason field.\n\n'
        'Do not return multiple people, do not list alternatives, and do not '
        'select individual contributors. Exclude recruiters, HR, '
        'engineers, designers, consultants, and former employees.\n\n'
        'IMPORTANT: Return your results as a JSON array. Each element must be '
        'an object with these exact keys:\n'
        '- "company_name": string (use the actual MSP company name, not the staffing agency)\n'
        '- "is_msp": boolean (true if the company is an IT MSP/managed IT services company)\n'
        '- "employee_count": integer or null\n'
        '- "company_website": string or null (the company\'s main website URL)\n'
        '- "person_name": string (or "Not confidently identifiable")\n'
        '- "title": string or null\n'
        '- "source_url": string or null (LinkedIn or other proof URL)\n'
        '- "confidence": "High" or "Medium"\n'
        '- "reason": string or null (only if person not found, or if is_msp is false)\n\n'
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
            companies: List of dicts, each with at least a "company" key (name).

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

        # Extract text — response.text can be None with Google Search grounding
        raw_text = None
        try:
            raw_text = response.text
        except (AttributeError, ValueError):
            pass

        if not raw_text:
            # Try extracting from candidates
            try:
                candidates = response.candidates or []
                for candidate in candidates:
                    content = getattr(candidate, "content", None)
                    if not content:
                        continue
                    parts = getattr(content, "parts", None) or []
                    texts = [p.text for p in parts if getattr(p, "text", None)]
                    if texts:
                        raw_text = "".join(texts)
                        break
            except (IndexError, AttributeError, TypeError):
                raw_text = ""

        if not raw_text:
            logger.warning("Gemini returned empty response for batch")
            return [
                DecisionMakerResult(
                    company_name=c["company"],
                    not_found_reason="Empty Gemini response",
                )
                for c in batch
            ]

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

                is_msp = entry.get("is_msp", False)
                person = entry.get("person_name", "")
                company_website = entry.get("company_website")

                # Parse employee count
                emp_count = entry.get("employee_count")
                if emp_count is not None:
                    try:
                        emp_count = int(emp_count)
                    except (ValueError, TypeError):
                        emp_count = None

                if not is_msp:
                    results_by_company[matched] = DecisionMakerResult(
                        company_name=matched,
                        is_verified_msp=False,
                        employee_count=emp_count,
                        company_website=company_website,
                        not_found_reason=entry.get("reason", "Not an IT MSP"),
                        raw_text=str(entry),
                    )
                elif person and "not confidently" in person.lower():
                    results_by_company[matched] = DecisionMakerResult(
                        company_name=matched,
                        is_verified_msp=True,
                        employee_count=emp_count,
                        company_website=company_website,
                        not_found_reason=entry.get("reason", person),
                        raw_text=str(entry),
                    )
                else:
                    results_by_company[matched] = DecisionMakerResult(
                        company_name=matched,
                        is_verified_msp=True,
                        person_name=person or None,
                        title=entry.get("title"),
                        source_url=entry.get("source_url"),
                        confidence=entry.get("confidence"),
                        employee_count=emp_count,
                        company_website=company_website,
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
        """Try to extract and parse a JSON array from the response.

        Handles cases where Gemini returns multiple JSON arrays by finding
        all valid arrays and returning the longest one.
        """
        # Strip markdown code fences if wrapping the entire response
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

        # Find all JSON arrays in the text (Gemini sometimes repeats them)
        best: Optional[List[Dict]] = None
        # Use non-greedy matching to find individual arrays
        for match in re.finditer(r"\[[\s\S]*?\](?=\s*```|\s*$|\s*\[)", text):
            try:
                parsed = json.loads(match.group())
                if isinstance(parsed, list) and (best is None or len(parsed) > len(best)):
                    best = parsed
            except json.JSONDecodeError:
                continue

        if best:
            return best

        # Final fallback: try each ```json ... ``` block individually
        for block_match in re.finditer(r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", text):
            try:
                parsed = json.loads(block_match.group(1))
                if isinstance(parsed, list) and (best is None or len(parsed) > len(best)):
                    best = parsed
            except json.JSONDecodeError:
                continue

        return best

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

            # Check for MSP status in the regex block
            is_msp = "is_msp" not in block.lower() or "true" in block.lower().split("is_msp")[-1][:20]

            results[company_name] = DecisionMakerResult(
                company_name=company_name,
                is_verified_msp=is_msp,
                person_name=person_match.group(1).strip() if person_match else None,
                title=title_match.group(1).strip() if title_match else None,
                source_url=url_match.group(1).strip() if url_match else None,
                confidence=conf_match.group(1).capitalize() if conf_match else None,
                raw_text=block,
            )

        return results
