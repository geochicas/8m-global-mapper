import re
from urllib.parse import urlparse

EVENT_VERBS = [
    # ES/PT
    "convocatoria", "marcha", "manifestación", "manifestacion", "huelga",
    "concentración", "concentracion", "paro", "plantón", "planton",
    "asamblea", "encuentro", "taller", "seminario", "charla", "conversatorio",
    "foro", "jornada", "ciclo", "actividad", "acto",
    "inscripción", "inscripcion", "regístrate", "registrate",
    # EN
    "march", "rally", "protest", "demonstration", "strike", "walkout",
    "event", "workshop", "seminar", "talk", "conference", "panel",
    "register", "join us",
    # FR/IT/DE
    "manifestation", "grève", "greve", "atelier", "conférence",
    "sciopero", "manifestazione", "incontro",
    "streik", "demo", "kundgebung"
]

IWD_KEYWORDS = [
    "8m", "8 marzo", "8 de marzo", "8 mars", "8 march",
    "international women's day", "international womens day",
    "dia internacional da mulher", "día internacional de la mujer",
    "journee internationale des droits des femmes",
    # algunas variantes comunes
    "womens day", "día de la mujer", "dia da mulher", "journee internationale de la femme"
]

DATE_HINTS = [
    r"\b20\d{2}-\d{2}-\d{2}\b",
    r"\b\d{1,2}[\/\.-]\d{1,2}[\/\.-]20\d{2}\b",
    r"\b(8|08)\s*(de)?\s*marzo\b",
    r"\b(8|08)\s*mars\b",
    r"\b(8|08)\s*march\b",
]

TIME_HINTS = [
    r"\b([01]?\d|2[0-3]):([0-5]\d)\b",
    r"\b([01]?\d|2[0-3])\s?h\b",
    r"\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s?(am|pm)\b",
]

LOCATION_HINTS = [
    "dirección", "direccion", "lugar", "punto de encuentro", "ubicación", "ubicacion",
    "address", "location", "venue", "meet at",
    "lieu", "adresse",
    "indirizzo", "luogo",
    "ort", "treffpunkt"
]

BAD_PAGE_HINTS = [
    "cookie", "privacy", "terms", "sitemap", "newsletter", "subscribe",
    "accessibility", "impressum"
]

def _contains_any(text: str, needles: list[str]) -> bool:
    t = (text or "").lower()
    return any(n.lower() in t for n in needles)

def _count_matches_regex(text: str, patterns: list[str]) -> int:
    t = text or ""
    c = 0
    for p in patterns:
        if re.search(p, t, flags=re.IGNORECASE):
            c += 1
    return c

def score_page(url: str, title: str, text: str) -> tuple[int, dict]:
    """
    Score alto = más probable convocatoria 8M real.
    Devuelve (score, signals) para debug.
    """
    title_l = (title or "").lower()
    text_l = (text or "").lower()
    url_l = (url or "").lower()

    score = 0
    signals = {}

    # Señal dura: si no hay 8M/IWD en URL/TITLE/TEXT → casi seguro NO es 8M
    has_iwd = (
        _contains_any(title_l, IWD_KEYWORDS)
        or _contains_any(text_l, IWD_KEYWORDS)
        or _contains_any(url_l, ["8m", "iwd", "womens-day", "women-s-day", "dia-da-mulher", "dia-internacional", "8-marzo", "8-mars"])
    )
    signals["has_iwd"] = has_iwd
    if not has_iwd:
        # Esto corta CNT 1 Mayo, concentraciones mensuales, etc.
        return -100, signals

    score += 8  # base fuerte por ser 8M/IWD

    verbs = sum(1 for v in EVENT_VERBS if v in title_l) + sum(1 for v in EVENT_VERBS if v in text_l)
    signals["event_verbs"] = verbs
    score += min(verbs, 6)

    date_hits = _count_matches_regex(text, DATE_HINTS) + _count_matches_regex(title, DATE_HINTS)
    time_hits = _count_matches_regex(text, TIME_HINTS)
    signals["date_hits"] = date_hits
    signals["time_hits"] = time_hits
    score += 4 if date_hits > 0 else 0
    score += 2 if time_hits > 0 else 0

    loc = _contains_any(text_l, LOCATION_HINTS)
    signals["location_hints"] = loc
    score += 2 if loc else 0

    url_bonus = 0
    for k in [
        "/event", "/events", "/agenda", "/calendar", "/calendario",
        "/activities", "/activity", "/actividad", "/actividades",
        "/convocatoria", "/marcha", "/manifest", "/huelga", "/paro",
        "/inscripcion", "/register"
    ]:
        if k in url_l:
            url_bonus = 2
            break
    signals["url_bonus"] = url_bonus
    score += url_bonus

    bad = sum(1 for b in BAD_PAGE_HINTS if (b in title_l or b in text_l or b in url_l))
    signals["bad_hints"] = bad
    score -= min(bad * 2, 6)

    return score, signals

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""
