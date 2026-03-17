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

Here are examples of good subject + opener pairs. Notice how some are one sentence, and the ones with two sentences connect them naturally instead of ending with a judgment:

Example 1:
Prospect: Sarah at GrowthSpark (website mentions ecommerce focus, Shopify Plus partnerships, DTC brands)
{{"subject": "the ecommerce focus on your site", "opener": "Hey Sarah, noticed GrowthSpark is going deep on Shopify Plus and DTC brands."}}

Example 2:
Prospect: James at Bright Pixel Media (website has case study about scaling a SaaS client's paid media from $50k to $200k/mo)
{{"subject": "that saas paid media case study", "opener": "Hey James, saw you scaled a SaaS client's paid media from 50k to 200k a month."}}

Example 3:
Prospect: Lauren at Cedar Creative (website mentions they recently launched a new content marketing service, blog post about B2B content strategy)
{{"subject": "the new content marketing service", "opener": "Hey Lauren, noticed Cedar Creative just added content marketing to the mix."}}

Example 4:
Prospect: Dave at Ridgeline Digital (website mentions healthcare and fintech as verticals, HIPAA-compliant marketing)
{{"subject": "the hipaa marketing angle", "opener": "Hey Dave, not many agencies touch HIPAA-compliant marketing so Ridgeline stood out."}}

Example 5:
Prospect: Priya at Launchpad Studios (website has a blog post about why agencies should stop discounting)
{{"subject": "your stop discounting blog post", "opener": "Hey Priya, read your post on why agencies should stop discounting. Been thinking about that a lot lately."}}

Example 6:
Prospect: Marcus at Trellis Digital (website shows they just hired a new Head of Strategy)
{{"subject": "the new head of strategy", "opener": "Hey Marcus, noticed Trellis just brought on a new Head of Strategy."}}

Now generate for this prospect:
- Name: {first_name}
- Company: {company_name}
- Website content summary: {extracted_content}

Rules:
1. subject: Under 7 words, lowercase, no punctuation except question marks. Reference something HIGHLY SPECIFIC from their site: a named client, a specific case study, a concrete service with details, a named blog post, a specific vertical, a recent hire by name. Generic observations like "your growth focus" or "your marketing services" are not acceptable.
2. opener: 1-2 sentences, under 25 words. Start with "Hey {first_name}," then go directly into the observation. The observation itself proves you did research, so do NOT use filler phrases like "was doing some digging on", "was looking through", "was checking out", "came across", or "took a look at". Just state what you noticed.
3. The subject and opener MUST reference the same specific observation.
4. Vary your sentence structure. After "Hey {first_name}," you can start with: the company name doing something, a verb like "noticed/saw/read", a reference to the specific thing ("the [thing] on your site"), or the observation itself. Do NOT repeat the same pattern.
5. The opener must naturally lead into the email body that follows it (which starts with "Here's the irony I keep seeing...").
6. The opener can be one sentence or two. One sentence is often better. If you write a second sentence, it must connect to the first, not evaluate it. NEVER end with a two-word judgment like "Good question.", "Smart move.", "Timely topic.", "Solid results.", "Good niche.", "Cool initiative.", or any "[Adjective] [noun]." pattern. Instead, either stop after one sentence, or add a brief connected thought like "been thinking about that too" or "we work with a lot of similar brands". The second sentence should feel like a thought trailing off, not a grade.
7. NEVER use the pattern "That's a [adjective] [noun]". This is the single biggest tell that the email was written by AI.

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

        try:
            config = types.GenerateContentConfig(
                temperature=0.4,
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

            async with semaphore:
                result = await self.personalize_one(first_name, company_name, content_summary)
                results[row_idx] = result

                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"  Personalized {completed}/{total} prospects")

                if on_complete:
                    on_complete(row_idx, result)

        await asyncio.gather(*[_process_one(p) for p in prospects])
        return results
