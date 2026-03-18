"""LLM personalization using Gemini Flash."""

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
You are generating a personalized cold email subject line and opening line for a prospect. The opener will be the first line of this email, followed immediately by this body:

"Here's the irony I keep seeing with agencies your size: you're great at filling pipelines for clients but your own pipeline runs on referrals and word-of-mouth. When those dry up, there's no system behind it.

We build done-for-you outbound systems for agencies. 5 qualified meetings in 30 days or we keep working on your behalf until we hit it.

Worth a quick call?

Ishaan"

OBSERVATION HIERARCHY (pick the highest tier you can find in the website content):

Tier 1 (strongest): Named third-party client or partner + a specific metric or result.
  "Verdant drove $2M in pipeline for CrowdStrike in one quarter."

Tier 2: Named third-party client or partner, without a metric.
  "noticed GrowthSpark is doing the Shopify Plus builds for Allbirds and Bombas."

Tier 3: Specific project, case study result (even unnamed client), or a named team hire.
  "you scaled a SaaS client's paid media from 50k to 200k a month."

Tier 4: Specific content piece (blog post, guide) or a truly unique service detail.
  "read your post on why agencies should stop discounting."

Tier 5 (weakest, avoid if possible): General service description or vertical focus.

Always aim for Tier 1 or 2. Only use Tier 4-5 if the website genuinely lacks case studies, named clients, and concrete results.

GOOD examples:

Example 1 (Tier 1, ultra-short subject):
Prospect: Anika at Verdant Agency (website has case study: drove $2M in pipeline for CrowdStrike)
{{"subject": "crowdstrike", "opener": "Hey Anika, Verdant drove $2M in pipeline for CrowdStrike in one quarter."}}

Example 2 (Tier 2):
Prospect: Sarah at GrowthSpark (website mentions Shopify Plus partnership, built stores for Allbirds and Bombas)
{{"subject": "the allbirds + bombas builds", "opener": "Hey Sarah, noticed GrowthSpark is doing the Shopify Plus builds for Allbirds and Bombas."}}

Example 3 (Tier 3):
Prospect: James at Bright Pixel Media (website has case study: scaled a SaaS client's paid media from $50k to $200k/mo)
{{"subject": "that saas paid media case study", "opener": "Hey James, you scaled a SaaS client's paid media from 50k to 200k a month."}}

Example 4 (Tier 3, named hire):
Prospect: Marcus at Trellis Digital (website shows they hired Alex Chen as Head of Strategy, previously at Deloitte Digital)
{{"subject": "alex chen joining trellis", "opener": "Hey Marcus, noticed Trellis just brought on Alex Chen from Deloitte Digital as Head of Strategy."}}

Example 5 (Tier 4, ultra-short subject):
Prospect: Priya at Launchpad Studios (website has a blog post about why agencies should stop discounting)
{{"subject": "stop discounting", "opener": "Hey Priya, read your post on why agencies should stop discounting."}}

Example 6 (Tier 1):
Prospect: Carlos at Peak Digital (website mentions Google Premier Partner status, managing $5M+ in annual ad spend across 40+ accounts)
{{"subject": "peak digital's google premier status", "opener": "Hey Carlos, Peak Digital is managing over $5M in annual ad spend as a Google Premier Partner."}}

Example 7 (Tier 2, ultra-short subject):
Prospect: Susan at Halo Creative (website has Nike 'Made to Play' campaign case study)
{{"subject": "the nike work", "opener": "Hey Susan, your Nike 'Made to Play' case study is strong."}}

BAD examples (DO NOT generate anything like these):

BAD: {{"subject": "partner program strategies", "opener": "Hey Jennifer, Fresh Marketing specializes in channel and partner program strategies."}}
WHY: Subject reads like a database category, not a human observation. Opener restates what the company does. The prospect already knows their own services.

BAD: {{"subject": "smart marketing systems 5x roi guarantee", "opener": "Hey Mike, your team builds customized strategies that drive measurable growth, not vanity metrics."}}
WHY: Subject is a complete thought with no curiosity gap, no reason to open. Opener echoes their own marketing copy back at them. Like reading someone's business card aloud.

BAD: {{"subject": "solangelee's personal branding genius", "opener": "Hey Solangelee, noticed your leadership experience at L'Oreal and Estee Lauder."}}
WHY: Prospect's name in subject signals mail merge. Opener references LinkedIn/bio, not website work. Company websites feel public; LinkedIn profiles feel personal and invasive.

BAD: {{"subject": "your ai and humans content post", "opener": "Hey Tom, Find Rentals features 15 vacation rental managers in the Great Smoky Mountains."}}
WHY: "post" is dead weight at the end of the subject. Opener describes how their platform works, not something they achieved. Features are not accomplishments.

Now generate for this prospect:
- Name: {first_name}
- Company: {company_name}
- Website content summary: {extracted_content}

Rules:
1. subject: Under 7 words, lowercase, an INCOMPLETE thought that creates a curiosity gap. The subject is a compressed version of the opener. The prospect must open to understand. GOOD: "crowdstrike" (what about it?). BAD: "smart marketing systems 5x roi guarantee" (complete, no reason to open). Ultra-short (1-3 words) is great. Do not end with "post". Do not default to starting with "the" or "your". Vary structure.
2. The observation must pass the DINNER PARTY TEST: would the prospect brag about this to a stranger? Cite ACHIEVEMENTS, not descriptions. What they DID, not what they ARE. "You scaled WSJ subscriptions to 100,000" = YES. "You specialize in channel partner strategies" = NO. If the opener could describe dozens of agencies, it is too generic. The reader should think "yeah, we are good at what we do" right before reading the email body's challenge.
3. opener: Exactly ONE sentence, under 25 words. Start with "Hey {{first_name}}," then the observation. No second sentence. No filler phrases like "was doing some digging on", "was looking through", "was checking out", "came across", or "took a look at".
4. Vary opener starts: "noticed...", "read your...", "[Company name] just...", "your [specific thing]...", or the observation itself. Do NOT default to "saw".
5. Named third parties (clients, partners, publications) are the strongest observations. When you name a client, you prove research. Specific numbers prove depth. NEVER echo their marketing copy or tagline. NEVER reference their personal bio, LinkedIn, or career history, only company website content.
6. NEVER put the prospect's name in the subject line. Do NOT reference referrals or word-of-mouth (the email body covers this). Do NOT reference generic traits like years of experience, decades of expertise, city focus, or job postings.
7. NEVER use the pattern "That's a [adjective] [noun]". This is an AI writing tell.

NEVER use these phrases (they are overused and detectable as AI-generated):
- "That takes serious execution" or any "That takes serious..." variant
- "Not many agencies..." or any "Not many..." sentence starter
- "Clearly know..." or "Clearly understand..." or "Clearly" at the start of any clause
- "Not easy to pull off"
- "stood out"
- "specializes in"
- "is known for"
- "focuses on"
- "is dedicated to"
- "offers a range"
- "provides comprehensive"

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
    """Generate personalized subject lines and openers using Gemini Flash."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.5-flash",
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
