"""
Text normalization helpers for wine data matching.

Core functions used across the entire pipeline for consistent
accent-stripping, slug generation, and name normalization.
"""

import re
import unicodedata


def normalize(s: str | None) -> str:
    """
    Normalize a string for matching: strip accents, lowercase, collapse whitespace,
    remove non-alphanumeric characters.

    >>> normalize("Château Léoville-Las Cases")
    'chateau leoville las cases'
    >>> normalize("Müller-Thurgau")
    'muller thurgau'
    >>> normalize(None)
    ''
    """
    if not s:
        return ""
    # NFD decomposition then strip combining marks (accents)
    nfkd = unicodedata.normalize("NFD", s)
    stripped = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    # Lowercase, replace non-alphanumeric with space, collapse whitespace
    lower = stripped.lower()
    alphanum = re.sub(r"[^a-z0-9 ]", " ", lower)
    return re.sub(r"\s+", " ", alphanum).strip()


def slugify(s: str | None) -> str:
    """
    Generate a URL-safe slug from a string.

    >>> slugify("Château d'Yquem")
    'chateau-dyquem'
    >>> slugify("López de Heredia")
    'lopez-de-heredia'
    """
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFD", s)
    stripped = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    lower = stripped.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", lower)
    return slug.strip("-")


def normalize_producer(name: str | None) -> str:
    """
    Normalize a producer name for matching. Strips common prefixes
    (Domaine, Château, Bodega, etc.) and suffixes (Winery, Vineyards, etc.).

    >>> normalize_producer("Domaine de la Romanée-Conti")
    'romanee conti'
    >>> normalize_producer("Ridge Vineyards")
    'ridge'
    """
    if not name:
        return ""
    n = normalize(name)
    # Strip common prefixes
    prefixes = [
        "domaine de la ", "domaine du ", "domaine de ", "domaine des ", "domaine ",
        "chateau ", "château ", "clos ", "mas ", "cave ", "caves ",
        "maison ", "famille ",
        "bodega ", "bodegas ", "vina ", "viña ",
        "tenuta ", "azienda agricola ", "azienda vinicola ", "azienda ",
        "cantina ", "fattoria ", "podere ", "cascina ",
        "weingut ", "schloss ",
        "quinta da ", "quinta de ", "quinta do ", "quinta dos ", "quinta ",
        "herdade ", "casa ",
        "the ",
    ]
    for p in prefixes:
        if n.startswith(p):
            n = n[len(p):]
            break
    # Strip common suffixes
    suffixes = [
        " winery", " vineyards", " vineyard", " wines", " wine",
        " estate", " cellars", " cellar",
        " & fils", " et fils", " pere et fils", " pere & fils",
        " & son", " & sons", " brothers", " family",
    ]
    for s in suffixes:
        if n.endswith(s):
            n = n[:-len(s)]
            break
    return n.strip()


def normalize_wine_name(name: str | None) -> str:
    """
    Normalize a wine name for matching. Strips vintage years,
    bottle sizes, and common noise words.

    >>> normalize_wine_name("Tignanello 2019 750ml")
    'tignanello'
    """
    if not name:
        return ""
    n = normalize(name)
    # Strip vintage year
    n = re.sub(r"\b(19|20)\d{2}\b", "", n)
    # Strip bottle sizes
    n = re.sub(r"\b\d+\s*ml\b", "", n)
    n = re.sub(r"\b(375|750|1500|3000)\b", "", n)
    n = re.sub(r"\b\d+(\.\d+)?\s*l\b", "", n)
    # Strip common noise
    noise = [" gift box", " gift set", " magnum", " half bottle", " jeroboam",
             " double magnum", " imperial", " methuselah", " balthazar"]
    for word in noise:
        n = n.replace(word, "")
    return re.sub(r"\s+", " ", n).strip()


def parse_vintage(text: str | None) -> str | None:
    """
    Extract a 4-digit vintage year from text.

    >>> parse_vintage("Opus One 2018 Napa Valley")
    '2018'
    >>> parse_vintage("NV Champagne Brut")
    """
    if not text:
        return None
    match = re.search(r"\b(19|20)\d{2}\b", text)
    return match.group(0) if match else None
