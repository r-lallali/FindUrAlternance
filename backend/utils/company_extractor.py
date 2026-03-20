"""
NLP-based company name extractor using spaCy NER.

Used to recover the real employer name from offer descriptions
when the company field is generic ("Entreprise confidentielle",
cabinet de recrutement, programme d'insertion, etc.).
"""

from __future__ import annotations
import re
from typing import Optional

# Lazy-loaded spaCy model to avoid slow import at startup
_nlp = None

def _get_nlp():
    global _nlp
    if _nlp is None:
        import spacy
        try:
            _nlp = spacy.load("fr_core_news_md")
        except OSError:
            # Model not downloaded — skip NLP silently
            _nlp = False
    return _nlp if _nlp is not False else None


# ─── Blacklists ───────────────────────────────────────────────────────────────

# Generic HR/tech terms that spaCy wrongly tags as ORG
_BLACKLIST_ORG = {
    # Generics
    "it", "b2b", "b2c", "saas", "erp", "crm", "rh", "hr", "ia", "ai",
    "kpi", "etl", "api", "sql", "bi", "etl", "devops", "ci", "cd",
    "agile", "scrum", "kanban", "lean", "cloud",
    # Institutions / programmes
    "pôle emploi", "france travail", "cpam", "caf", "urssaf", "ursaf",
    "cse", "cge", "cfa", "cci", "bpifrance", "apec", "opco",
    "engagement jeunes", "talents handicap", "groupe talents handicap",
    "handi-cv", "handicv", "1jeune1solution",
    # Generic company descriptions
    "groupe", "entreprise", "société", "cabinet", "agence", "association",
    "filiale", "holding", "pme", "tpe", "eti", "startup",
    # Common false positives in job ads
    "client", "clients", "partenaire", "recruteur",
    "microsoft", "google", "aws", "azure", "linkedin",  # brands often cited in tech stacks
    "rh mania", "jobmania", "cadre emploi", "cadremploi", "figaro classifieds",
    "nous", "vous", "notre client", "notre entreprise", "l'entreprise", "la société",
    "le groupe", "notre société", "notre groupe", "notre cabinet", "le cabinet",
}

# Context patterns that strongly indicate the following ORG is the real employer
_EMPLOYER_PATTERNS = [
    r"notre client[e]?,?[\s:\-]+(?P<org>[A-ZÀÂÉÈÊÔÙÛÎ][A-Za-zÀ-ÿ\s\-&\.]{1,50})(?:[,\s\.!:]|$)",
    r"pour le compte de[\s:\-]+(?P<org>[A-ZÀÂÉÈÊÔÙÛÎ][A-Za-zÀ-ÿ\s\-&\.]{1,50})(?:[,\s\.!:]|$)",
    r"(?:rejoignez|intégrez)\s+(?:la société|le groupe|l[''']entreprise|la structure)?[\s:\-]+(?P<org>[A-ZÀÂÉÈÊÔÙÛÎ][A-Za-zÀ-ÿ\s\-&\.]{1,50})(?:[,\s\.!:]|$)",
    r"(?:société|entreprise|groupe|cabinet)[\s:\-]+(?P<org>[A-ZÀÂÉÈÊÔÙÛÎ][A-Za-zÀ-ÿ\s\-&\.]{1,50})\s+(?:recrute|recherche|propose)",
    r"(?:recrute|recherche) pour\s+(?:son client|l[''']un de ses clients|le compte de son client)?[\s:\-]+(?P<org>[A-ZÀÂÉÈÊÔÙÛÎ][A-Za-zÀ-ÿ\s\-&\.]{1,50})(?:[,\s\.!:]|$)",
    r"(?:au sein de|chez)[\s:\-]+(?P<org>[A-ZÀÂÉÈÊÔÙÛÎ][A-Za-zÀ-ÿ\s\-&\.]{1,50})(?:[,\s\.!:]|$)",
]
_EMPLOYER_RE = [re.compile(p, re.IGNORECASE) for p in _EMPLOYER_PATTERNS]


def _clean_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text).strip()


def _is_blacklisted(name: str) -> bool:
    return name.lower().strip() in _BLACKLIST_ORG or len(name.strip()) < 2


def extract_company_from_description(description: str, current_company: str = "") -> Optional[str]:
    """
    Try to extract the real employer name from a job description.

    Returns the extracted company name, or None if nothing confident was found.
    The caller should only replace `current_company` if this returns a non-None value.
    """
    if not description:
        return None

    text = _clean_html(description)
    # Truncate to first 1500 chars — company name almost always appears near the top
    text = text[:1500]

    # ── Pass 1: regex patterns with explicit employer context (high confidence) ──
    for pattern in _EMPLOYER_RE:
        m = pattern.search(text)
        if m:
            candidate = m.group("org").strip().rstrip(".,;:")
            if not _is_blacklisted(candidate) and candidate.lower() != current_company.lower():
                return candidate

    # ── Pass 2: spaCy NER ────────────────────────────────────────────────────────
    nlp = _get_nlp()
    if nlp is None:
        return None

    doc = nlp(text)
    # Count occurrences of each ORG entity
    freq: dict[str, int] = {}
    for ent in doc.ents:
        if ent.label_ == "ORG":
            name = ent.text.strip()
            if _is_blacklisted(name):
                continue
            if name.lower() == current_company.lower():
                continue
            freq[name] = freq.get(name, 0) + 1

    if not freq:
        return None

    # Pick most frequent ORG entity; require at least 2 occurrences for confidence
    best = max(freq, key=lambda k: freq[k])
    if freq[best] >= 2:
        return best

    return None
