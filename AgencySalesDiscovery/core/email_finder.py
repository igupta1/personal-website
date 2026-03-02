"""Apollo API email lookup for decision makers."""

import logging
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from .models import DecisionMakerResult, EmailLookupResult

logger = logging.getLogger(__name__)

APOLLO_BULK_MATCH_URL = "https://api.apollo.io/api/v1/people/bulk_match"


class ApolloEmailFinder:
    """
    Find emails for decision makers using Apollo's Bulk People Enrichment API.

    Batches lookups into groups of up to 10 to minimize API calls
    while consuming the same credits per match.
    """

    def __init__(self, api_key: str, batch_size: int = 10):
        self.api_key = api_key
        self.batch_size = min(batch_size, 10)  # Apollo max is 10

    async def find_emails(
        self,
        decision_makers: List[DecisionMakerResult],
    ) -> List[EmailLookupResult]:
        """
        Look up emails for decision makers via Apollo bulk enrichment.

        Args:
            decision_makers: DecisionMakerResult objects with person_name set.

        Returns:
            List of EmailLookupResult, one per input decision maker.
        """
        # Prepare lookup items (only those with a person name)
        lookup_items = []
        for dm in decision_makers:
            if not dm.person_name:
                continue
            first, last = self._split_name(dm.person_name)
            if not first:
                continue
            domain = self._extract_domain(dm.company_website) if dm.company_website else ""
            lookup_items.append({
                "company_name": dm.company_name,
                "person_name": dm.person_name,
                "first_name": first,
                "last_name": last,
                "domain": domain,
            })

        if not lookup_items:
            return []

        # Batch and process
        batches = [
            lookup_items[i : i + self.batch_size]
            for i in range(0, len(lookup_items), self.batch_size)
        ]

        logger.info(
            f"Looking up emails for {len(lookup_items)} decision makers "
            f"in {len(batches)} batch(es)"
        )

        all_results: List[EmailLookupResult] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for batch_idx, batch in enumerate(batches, 1):
                batch_names = [item["person_name"] for item in batch]
                logger.info(
                    f"Apollo email batch {batch_idx}/{len(batches)}: {batch_names}"
                )
                try:
                    batch_results = await self._process_batch(client, batch)
                    all_results.extend(batch_results)
                except Exception as e:
                    logger.error(
                        f"Failed to process Apollo batch {batch_idx}: {e}"
                    )
                    for item in batch:
                        all_results.append(
                            EmailLookupResult(
                                company_name=item["company_name"],
                                person_name=item["person_name"],
                                not_found_reason=f"API error: {e}",
                            )
                        )

        return all_results

    async def _process_batch(
        self, client: httpx.AsyncClient, batch: List[Dict]
    ) -> List[EmailLookupResult]:
        """Process a single batch via Apollo bulk_match."""
        details = []
        for item in batch:
            detail = {
                "first_name": item["first_name"],
                "last_name": item["last_name"],
            }
            if item.get("domain"):
                detail["domain"] = item["domain"]
                detail["organization_name"] = item["company_name"]
            else:
                detail["organization_name"] = item["company_name"]
            details.append(detail)

        payload = {
            "reveal_personal_emails": False,
            "details": details,
        }

        response = await client.post(
            APOLLO_BULK_MATCH_URL,
            json=payload,
            headers={
                "x-api-key": self.api_key,
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        data = response.json()

        logger.debug(f"Apollo response status: {data.get('status')}")
        logger.info(
            f"Apollo: {data.get('unique_enriched_records', 0)} enriched, "
            f"{data.get('missing_records', 0)} missing, "
            f"{data.get('credits_consumed', 0)} credits consumed"
        )

        # Parse matches back to our results
        matches = data.get("matches", [])
        results: List[EmailLookupResult] = []

        for i, item in enumerate(batch):
            if i < len(matches) and matches[i]:
                match = matches[i]
                email = match.get("email")
                linkedin = match.get("linkedin_url")
                apollo_title = match.get("title")

                if email:
                    results.append(
                        EmailLookupResult(
                            company_name=item["company_name"],
                            person_name=item["person_name"],
                            email=email,
                            linkedin_url=linkedin,
                            apollo_title=apollo_title,
                        )
                    )
                else:
                    results.append(
                        EmailLookupResult(
                            company_name=item["company_name"],
                            person_name=item["person_name"],
                            linkedin_url=linkedin,
                            apollo_title=apollo_title,
                            not_found_reason="Matched in Apollo but no email available",
                        )
                    )
            else:
                results.append(
                    EmailLookupResult(
                        company_name=item["company_name"],
                        person_name=item["person_name"],
                        not_found_reason="No match found in Apollo",
                    )
                )

        return results

    @staticmethod
    def _split_name(full_name: str) -> Tuple[str, str]:
        """Split a full name into (first_name, last_name)."""
        parts = full_name.strip().split()
        if len(parts) == 0:
            return ("", "")
        if len(parts) == 1:
            return (parts[0], "")
        return (parts[0], " ".join(parts[1:]))

    @staticmethod
    def _extract_domain(website: str) -> str:
        """Extract domain from a website URL."""
        if not website:
            return ""
        url = website if website.startswith("http") else f"https://{website}"
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace("www.", "")
            return domain
        except Exception:
            return ""
