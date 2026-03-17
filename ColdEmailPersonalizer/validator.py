"""Deliverability validation for generated subject lines and openers."""

import re
from typing import List, Tuple

from .scraper import strip_em_dashes

# Words that trigger spam filters or sound sycophantic
BANNED_WORDS = {
    "impressive", "incredible", "amazing", "love", "fantastic",
    "revolutionary", "groundbreaking", "game-changing", "unbelievable",
    "guaranteed",
}

# Spam trigger words for subjects (high-signal)
SPAM_TRIGGER_WORDS = {
    "free", "urgent", "act now", "limited time",
    "buy now", "order now", "click here", "subscribe", "congratulations",
    "winner", "won", "cash", "earn money", "risk-free", "no obligation",
    "special offer", "exclusive deal", "hurry", "expires",
}

# Filler phrases that should trigger a hard reject + retry
BANNED_PHRASES = [
    "was doing some digging",
    "was looking through",
    "was checking out",
    "took a look at",
    "came across your",
    "was browsing",
    "was exploring",
    "that's a ",
]

# Characters that cause encoding issues across email clients
_CURLY_QUOTES = {"\u201c", "\u201d", "\u2018", "\u2019"}
_PROBLEM_UNICODE = {"\u2014", "\u2013", "\u2026", "\u00a0", "\u200b", "\u200c", "\u200d", "\ufeff"}


def sanitize(subject: str, opener: str) -> Tuple[str, str]:
    """Auto-fix minor issues. Returns (fixed_subject, fixed_opener)."""
    # Strip em/en dashes
    subject = strip_em_dashes(subject)
    opener = strip_em_dashes(opener)

    # Replace curly quotes with straight
    for ch in ("\u201c", "\u201d"):
        subject = subject.replace(ch, '"')
        opener = opener.replace(ch, '"')
    for ch in ("\u2018", "\u2019"):
        subject = subject.replace(ch, "'")
        opener = opener.replace(ch, "'")

    # Ellipsis char -> dots
    subject = subject.replace("\u2026", "...")
    opener = opener.replace("\u2026", "...")

    # Non-breaking space
    subject = subject.replace("\u00a0", " ")
    opener = opener.replace("\u00a0", " ")

    # Zero-width chars
    for ch in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        subject = subject.replace(ch, "")
        opener = opener.replace(ch, "")

    # Lowercase subject
    subject = subject.lower()

    # Strip trailing period from subject
    subject = subject.rstrip(".")

    # Collapse multiple spaces
    subject = re.sub(r" {2,}", " ", subject).strip()
    opener = re.sub(r" {2,}", " ", opener).strip()

    return subject, opener


def validate(subject: str, opener: str, first_name: str) -> Tuple[bool, List[str]]:
    """Validate a subject + opener pair for deliverability.

    Returns (is_valid, list_of_issues).
    is_valid=False means a hard reject (should trigger retry).
    """
    issues = []

    # --- Hard rejects ---

    # Word count checks
    subject_words = len(subject.split())
    if subject_words > 7:
        issues.append(f"subject_too_long: {subject_words} words (max 7)")

    opener_words = len(opener.split())
    if opener_words > 25:
        issues.append(f"opener_too_long: {opener_words} words (max 25)")

    # Em dash / en dash
    if "\u2014" in subject or "\u2014" in opener:
        issues.append("em_dash_present")
    if "\u2013" in subject or "\u2013" in opener:
        issues.append("en_dash_present")

    # Double/triple ASCII dashes (but not single hyphens)
    if "--" in subject or "--" in opener:
        issues.append("double_dash_present")

    # Curly/smart quotes
    all_text = subject + opener
    for ch in _CURLY_QUOTES:
        if ch in all_text:
            issues.append("curly_quotes_present")
            break

    # Problem unicode
    for ch in _PROBLEM_UNICODE:
        if ch in all_text:
            issues.append(f"problem_unicode: U+{ord(ch):04X}")
            break

    # Banned words (case-insensitive)
    combined_lower = all_text.lower()
    for word in BANNED_WORDS:
        if word in combined_lower:
            issues.append(f"banned_word: {word}")

    # Banned filler phrases (case-insensitive, checked against opener only)
    opener_lower = opener.lower()
    for phrase in BANNED_PHRASES:
        if phrase in opener_lower:
            issues.append(f"banned_phrase: {phrase}")

    # Exclamation marks in subject
    if "!" in subject:
        issues.append("exclamation_in_subject")

    # ALL CAPS words (6+ consecutive uppercase, allows short acronyms like HIPAA, IGTD, UGC)
    if re.search(r"\b[A-Z]{6,}\b", subject + " " + opener):
        issues.append("all_caps_word")

    # Subject not lowercase
    if subject != subject.lower():
        issues.append("subject_not_lowercase")

    # Subject punctuation - allow ?, $, +, %, commas (in numbers like 5,000), apostrophes
    subject_cleaned = re.sub(r"[?$+%,']", "", subject)
    if re.search(r"[!@#^&*()=\[\]{};:\"\\|<>/~`]", subject_cleaned):
        issues.append("subject_bad_punctuation")

    # Unfilled merge tags
    if re.search(r"\{\{.*?\}\}|\{[a-zA-Z_]+\}|\[\[.*?\]\]", all_text):
        issues.append("unfilled_merge_tag")

    # Opener must start with "Hey {first_name}"
    expected_start = f"Hey {first_name}"
    if not opener.startswith(expected_start):
        issues.append(f"opener_missing_greeting: expected '{expected_start}...'")

    # Reject short trailing judgments (1-3 words ending the opener after a period)
    # Catches "Smart move.", "Good niche.", "Timely topic.", "Solid results.", etc.
    if re.search(r"\.\s+[A-Z][a-z]*(\s+[a-z]+){0,2}\.\s*$", opener):
        issues.append("trailing_judgment")

    # --- Soft warnings (appended but don't cause hard reject) ---
    warnings = []

    if len(subject) > 50:
        warnings.append(f"subject_char_length: {len(subject)} chars (>50)")

    subject_lower = subject.lower()
    for trigger in SPAM_TRIGGER_WORDS:
        if trigger in subject_lower:
            warnings.append(f"spam_trigger: {trigger}")

    # Hard reject = any issue in issues list
    is_valid = len(issues) == 0

    # Include warnings in the returned list but they don't affect validity
    all_issues = [f"REJECT: {i}" for i in issues] + [f"WARN: {w}" for w in warnings]

    return is_valid, all_issues
