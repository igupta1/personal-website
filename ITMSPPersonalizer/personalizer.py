"""LLM personalization using Gemini."""

import asyncio
import json
import logging
import re
from typing import Dict, List, Optional, Any

from google import genai
from google.genai import types

from .scraper import strip_em_dashes, normalize_domain, build_content_summary

logger = logging.getLogger(__name__)

PERSONALIZATION_PROMPT = """\
You are generating a personalized cold email subject line and opening line for an IT MSP prospect. The opener will be the first line of this email, followed immediately by this body:

"Most MSPs your size grow on referrals until growth flatlines. You can't control when they show up or who they bring.

I'm a software engineer who builds outbound systems. Two examples: web scrapers that find companies actively hiring for IT roles they can't fill and outreach engines that turn those leads into booked meetings.

5 qualified meetings in 30 days or I keep working until we hit it.

Worth a quick call?

Ishaan"

OBSERVATION HIERARCHY (pick the highest tier you can find in the website content):

Tier 1 (strongest): Named client or partner + a specific metric or result — uptime %, response time hit, cost savings, compliance achievement (SOC 2, HIPAA, ISO 27001).
  "Northpoint cut downtime by 60% for Stonewall Kitchen after the managed backup migration."

Tier 2: Named client without a metric, or a named certification/partnership (Microsoft Gold Partner, CompTIA MSP Trustmark, Datto Elite, Kaseya, ConnectWise partner).
  "noticed Peak is a Microsoft Gold Partner running O365 environments for law firms."

Tier 3: Specific case study result (unnamed client), vertical specialization with concrete depth, or specific tech stack ownership.
  "your ransomware recovery case study had a manufacturing client back online in under 4 hours."

Tier 4: Specific blog post, guide, or a unique service commitment (e.g., 15-minute response SLA, proprietary onboarding process).
  "read your post on why dental offices fail HIPAA audits."

Tier 5 (weakest, avoid if possible): General IT support description or generic managed services positioning.

Always aim for Tier 1 or 2. Only use Tier 4-5 if the website genuinely lacks case studies, named clients, certifications, and concrete results.

GOOD examples:

Example 1 (Tier 1, ultra-short subject):
Prospect: Dave at Northpoint Technology Solutions (website has case study: cut downtime 60% for Stonewall Kitchen, managed backup migration)
{{"subject": "stonewall kitchen", "opener": "Hey Dave, Northpoint cut downtime by 60% for Stonewall Kitchen after the managed backup migration."}}

Example 2 (Tier 2, named partnership):
Prospect: Chris at Peak IT Solutions (website: Microsoft Gold Partner, managing O365 for law firms)
{{"subject": "microsoft gold + law firms", "opener": "Hey Chris, noticed Peak is a Microsoft Gold Partner running O365 environments for law firms."}}

Example 3 (Tier 3, unnamed but specific):
Prospect: Sarah at ClearPath Managed Services (website has case study: ransomware recovery, manufacturing client back online in 4 hours)
{{"subject": "the ransomware recovery case study", "opener": "Hey Sarah, your ransomware recovery case study had a manufacturing client back online in under 4 hours."}}

Example 4 (Tier 4, content piece):
Prospect: Mark at Aligned Technology Solutions (website has blog post on why dental offices fail HIPAA audits)
{{"subject": "hipaa audits for dental offices", "opener": "Hey Mark, read your post on why dental offices fail HIPAA audits."}}

BAD examples (DO NOT generate anything like these):

BAD: {{"subject": "managed it services provider", "opener": "Hey John, TechPath is a trusted managed IT services provider for small businesses."}}
WHY: Subject is a service category. Opener restates what the company does. The prospect already knows what they sell.

BAD: {{"subject": "john's 20 years in it", "opener": "Hey John, noticed your 20 years of experience in the IT industry."}}
WHY: Prospect's name in subject signals mail merge. Opener references personal background, not company achievements.

Now generate for this prospect:
- Name: {first_name}
- Company: {company_name}
- Website content summary: {extracted_content}

Rules:
1. subject: Under 7 words, lowercase, an INCOMPLETE thought that creates a curiosity gap. The subject is a compressed version of the opener. The prospect must open to understand. GOOD: "stonewall kitchen" (what about it?). BAD: "managed it services provider" (complete, no reason to open). Ultra-short (1-3 words) is great — a client name alone, a certification alone. Do not end with "post". Do not default to starting with "the" or "your". Vary structure.
2. The observation must pass the DINNER PARTY TEST: would the prospect brag about this to a stranger? Cite ACHIEVEMENTS, not descriptions. What they DID, not what they ARE. "You cut downtime by 60% for Stonewall Kitchen" = YES. "You provide 24/7 IT support" = NO (that is a feature, not a brag). The reader should think "yeah, we are good at what we do" right before reading the email body.
3. opener: Exactly ONE sentence, under 25 words. Start with "Hey {{first_name}}," then the observation. No second sentence. No filler phrases like "was doing some digging on", "was looking through", "was checking out", "came across", or "took a look at".
4. Vary opener starts: "noticed...", "read your...", "[Company name] just...", "your [specific thing]...", or the observation itself. Do NOT default to "saw".
5. Named clients + specific metrics are strongest. Certifications and partnerships (Microsoft Gold, Datto Elite, CompTIA) are strong Tier 2 anchors. NEVER echo their tagline or homepage hero copy. NEVER reference their personal bio, LinkedIn, or career history — only company website content.
6. NEVER put the prospect's name in the subject line. Do NOT reference referrals or word-of-mouth (the email body covers this). Do NOT reference generic traits like years in business, city focus, team size, or job postings.
7. NEVER use "That's a [adjective] [noun]". NEVER use "That takes serious [anything]", "Clearly [verb]", "Not many [anything]", or any evaluative second sentence. State the observation and stop.

NEVER use these phrases (they are overused and detectable as AI-generated):
- "That takes serious execution" or any "That takes serious..." variant
- "Not many MSPs..." or any "Not many..." sentence starter
- "Clearly know..." or "Clearly understand..." or "Clearly" at the start of any clause
- "Not easy to pull off"
- "stood out"
- "specializes in"
- "is known for"
- "focuses on"
- "is dedicated to"
- "offers a range"
- "provides comprehensive"
- "trusted IT partner"
- "your IT needs"
- "technology solutions provider"
- "keeping businesses running"
- "peace of mind"
- "one-stop shop"
- "proactive approach"
- "seamless transition"

CRITICAL formatting rules:
- Subject must be pure lowercase ASCII. No em dashes, en dashes, curly quotes, or unicode.
- Opener must use only straight ASCII characters. No em dashes, en dashes, curly quotes, or unicode.
- Use commas or periods for separating clauses. Never use dashes of any kind.
- Never use: "impressive", "incredible", "amazing", "love", "fantastic", "groundbreaking", "game-changing", "revolutionary"
- Do not use exclamation marks.

Respond in JSON only, no markdown:
{{"subject": "...", "opener": "..."}}"""


def _sanitize_unicode(text: str) -> str:
    """Replace problematic unicode chars with ASCII equivalents."""
    text = strip_em_dashes(text)
    # Curly quotes -> straight quotes
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    # Ellipsis -> three dots
    text = text.replace("\u2026", "...")
    # Non-breaking space -> regular space
    text = text.replace("\u00a0", " ")
    # Zero-width chars
    for ch in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        text = text.replace(ch, "")
    return text


def _parse_json_response(raw_text: str) -> Optional[Dict[str, str]]:
    """Parse JSON from LLM response with fallbacks."""
    cleaned = raw_text.strip()
    # Strip markdown fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        cleaned = "\n".join(lines[1:-1]).strip()

    # Try direct parse
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict) and "subject" in parsed and "opener" in parsed:
            return parsed
    except json.JSONDecodeError:
        pass

    # Regex fallback
    subject_match = re.search(r'"subject"\s*:\s*"((?:[^"\\]|\\.)*)"', raw_text)
    opener_match = re.search(r'"opener"\s*:\s*"((?:[^"\\]|\\.)*)"', raw_text)
    if subject_match and opener_match:
        return {
            "subject": subject_match.group(1).replace('\\"', '"'),
            "opener": opener_match.group(1).replace('\\"', '"'),
        }

    return None


class Personalizer:
    """Generate personalized subject lines and openers using Gemini."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.5-pro",
    ):
        client_kwargs = {}
        if api_key:
            client_kwargs["api_key"] = api_key
        self.client = genai.Client(**client_kwargs)
        self.model = model

    async def personalize_one(
        self,
        first_name: str,
        company_name: str,
        content_summary: str,
        avoid_subject: str = "",
    ) -> Dict[str, Optional[str]]:
        """Generate a personalized subject + opener for one prospect.

        Returns dict with keys: subject, opener, error
        """
        if not content_summary or len(content_summary.strip()) < 50:
            return {"subject": None, "opener": None, "error": "insufficient_content"}

        prompt = PERSONALIZATION_PROMPT.format(
            first_name=first_name,
            company_name=company_name,
            extracted_content=content_summary,
        )

        if avoid_subject:
            prompt += (
                f'\n\nIMPORTANT: A colleague already used the subject "{avoid_subject}" '
                f"for someone else at this company. You MUST pick a completely different "
                f"observation from the website. Do not reference the same topic."
            )

        try:
            config = types.GenerateContentConfig(
                temperature=0.5,
            )
            response = await self.client.aio.models.generate_content(
                model=self.model,
                contents=prompt,
                config=config,
            )

            raw_text = response.text or ""
            parsed = _parse_json_response(raw_text)

            if not parsed:
                logger.warning(f"Failed to parse JSON for {company_name}: {raw_text[:200]}")
                return {"subject": None, "opener": None, "error": "json_parse_failed"}

            subject = _sanitize_unicode(parsed["subject"])
            opener = _sanitize_unicode(parsed["opener"])

            return {"subject": subject, "opener": opener, "error": None}

        except Exception as e:
            logger.error(f"LLM call failed for {company_name}: {e}")
            return {"subject": None, "opener": None, "error": str(e)}

    async def personalize_all(
        self,
        prospects: List[Dict[str, Any]],
        scrape_cache: Dict[str, Dict],
        concurrency: int = 15,
        on_complete=None,
    ) -> Dict[int, Dict[str, Optional[str]]]:
        """Personalize all prospects concurrently.

        Args:
            prospects: List of dicts with keys: row_index, first_name, company_name, website
            scrape_cache: Dict mapping normalized_domain -> scrape result
            concurrency: Max concurrent LLM calls
            on_complete: Optional callback(row_index, result) called after each completion

        Returns:
            Dict mapping row_index -> personalization result
        """
        semaphore = asyncio.Semaphore(concurrency)
        results: Dict[int, Dict] = {}
        completed = 0
        total = len(prospects)

        async def _process_one(prospect: Dict):
            nonlocal completed
            row_idx = prospect["row_index"]
            first_name = prospect["first_name"]
            company_name = prospect["company_name"]
            website = prospect.get("website", "")

            domain = normalize_domain(website) if website else ""
            scrape_result = scrape_cache.get(domain, {})
            content_summary = build_content_summary(scrape_result)

            avoid_subject = prospect.get("avoid_subject", "")

            async with semaphore:
                result = await self.personalize_one(first_name, company_name, content_summary, avoid_subject=avoid_subject)
                results[row_idx] = result

                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"  Personalized {completed}/{total} prospects")

                if on_complete:
                    on_complete(row_idx, result)

        await asyncio.gather(*[_process_one(p) for p in prospects])
        return results
