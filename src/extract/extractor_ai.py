import re
from typing import Any, Dict, Optional

from dateparser.search import search_dates


# =========================
# 8M ANCHORS (obligatorio: al menos uno)
# =========================
ANCHORS_8M = [
    r"\b8\s*(de\s*)?(marzo|mar|mars|march)\b",
    r"\b8m\b",
    r"\biwd\b",
    r"\binternational\s+women['’]?s\s+day\b",
    r"\bd[ií]a\s+internacional\s+de\s+la\s+mujer\b",
    r"\bdia\s+internacional\s+da\s+mulher\b",
    r"\bjourn[ée]e\s+internationale\s+des\s+droits\s+des\s+femmes\b",
]
ANCHORS_8M_RE = re.compile("|".join(ANCHORS_8M), flags=re.IGNORECASE)


# =========================
# ACTIVITY / EVENT SIGNALS (queremos actividad, no “tema”)
# =========================
EVENT_WORDS = [
    "evento", "actividad", "agenda", "programa", "calendario",
    "convoca", "convocatoria", "participa", "acompáñanos", "acompananos",
    "inscripción", "inscripcion", "registro", "regístrate", "registrate",
    "entrada", "cupos", "aforo", "gratuito", "gratis",
    "taller", "seminario", "conferencia", "conversatorio", "charla",
    "encuentro", "mesa", "panel", "cineforo", "proyección", "proyeccion",
    "marcha", "manifestación", "manifestacion", "movilización", "movilizacion",
    "concentración", "concentracion",
    "horario", "hora", "fecha",
    "lugar", "ubicación", "ubicacion", "dirección", "direccion",
    "plaza", "parque", "auditorio", "sala",
]
EVENT_WORDS_LOWER = set(w.lower() for w in EVENT_WORDS)


# =========================
# BOILERPLATE / NAV noise patterns
# =========================
BOILERPLATE_PATTERNS = [
    r"utili[sz]em\s+galetes.*?(configuraci[oó]n|configuraci[oó])",
    r"cookie(s)?",
    r"pol[ií]tica\s+de\s+privacidad",
    r"aviso\s+legal",
    r"t[eé]rminos?\s+y\s+condiciones",
    r"configuraci[oó]n\s+de\s+las\s+cookies",
    r"accept(ar|ar)?\s+cookies",
    r"rechaz(ar|ar)?\s+cookies",
]

NAV_HEAVY_WORDS = [
    "inicio", "home", "menú", "menu", "categorías", "categorias", "secciones",
    "buscar", "search", "contacto", "about", "mapa del sitio", "sitemap",
    "entradas", "archivo", "archivos", "etiquetas", "tags",
    "política", "privacidad", "cookies", "suscrib", "newsletter",
]


# Horas tipo 5pm, 17:30, 17h, 5:30 pm
HOUR_PATTERNS = [
    r"\b([01]?\d|2[0-3]):([0-5]\d)\b",                 # 17:30
    r"\b([01]?\d|2[0-3])\s?h\b",                       # 17h
    r"\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s?(am|pm)\b",  # 5pm / 5:30 pm
]


def clean_text(s: str, max_len: int = 600) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s[:max_len]


def extract_hour(text: str) -> str:
    txt = (text or "").lower()

    m = re.search(HOUR_PATTERNS[2], txt)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2) or 0)
        ampm = m.group(3)
        if ampm == "pm" and hour != 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0
        return f"{hour:02d}:{minute:02d}"

    m = re.search(HOUR_PATTERNS[0], txt)
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"

    m = re.search(HOUR_PATTERNS[1], txt)
    if m:
        return f"{int(m.group(1)):02d}:00"

    return ""


def _strip_absurd_years(chunk: str) -> str:
    # elimina años absurdos (3000+) que rompen dateparser
    return re.sub(r"\b(3[0-9]{3}|[4-9][0-9]{3,})\b", " ", chunk)


def strip_boilerplate(text: str) -> str:
    t = text or ""
    # recorta banners típicos
    for pat in BOILERPLATE_PATTERNS:
        t = re.sub(pat, " ", t, flags=re.IGNORECASE | re.DOTALL)
    # si hay una sección enorme de cookies/privacidad, la tumba
    t = re.sub(r"(cookies|privacidad|privacy|galetes).{0,2000}", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def has_8m_anchor(title: str, text: str) -> bool:
    blob = (title + "\n" + (text or "")).strip()
    return bool(ANCHORS_8M_RE.search(blob))


def looks_like_index_or_nav_page(title: str, text: str, url: str) -> bool:
    """
    Heurística: páginas índice/home (mucho menú, muchas secciones, poco contenido continuo).
    No bloquea universidades: solo baja ruido de navegación.
    """
    t = (text or "").lower()
    # si el texto tiene muchísimas palabras “de menú”
    nav_hits = sum(1 for w in NAV_HEAVY_WORDS if w in t)
    # muchas “palabras cortas” repetidas (típico de listas de menú)
    token_count = len(re.findall(r"\b\w+\b", t))
    unique_tokens = len(set(re.findall(r"\b\w+\b", t)))
    uniqueness = (unique_tokens / token_count) if token_count else 0.0

    # URLs tipo homepage o secciones
    path = ""
    try:
        from urllib.parse import urlparse
        path = (urlparse(url).path or "").strip("/")
    except Exception:
        path = ""

    homepage_like = (path == "" or path in ["es", "en", "pt", "fr"])
    very_nav = nav_hits >= 6
    low_uniqueness = (token_count >= 120 and uniqueness < 0.35)

    # título genérico tipo “Mujer | Ayuntamiento …”
    generic_title = len(title.strip()) <= 40 and any(x in title.lower() for x in ["ayuntamiento", "universidad", "facultad", "department", "departamento"])

    return (homepage_like and very_nav) or low_uniqueness or (generic_title and very_nav)


def extract_event_date_contextual(text: str, title: str = "") -> str:
    blob = (title + "\n" + (text or "")).strip()
    if not blob:
        return ""

    chunk = _strip_absurd_years(blob[:1600])

    settings = {
        "PREFER_DATES_FROM": "future",
        "STRICT_PARSING": False,
        "RETURN_AS_TIMEZONE_AWARE": False,
    }

    try:
        hits = search_dates(
            chunk,
            languages=["es", "en", "pt", "fr", "ca"],
            settings=settings,
        )
    except Exception:
        return ""

    if not hits:
        return ""

    for _, dt in hits:
        try:
            if dt and 1900 <= dt.year <= 2100:
                return dt.date().isoformat()
        except Exception:
            continue

    return ""


def count_activity_words(blob_lower: str) -> int:
    hits = 0
    for w in EVENT_WORDS_LOWER:
        if w in blob_lower:
            hits += 1
    return hits


def has_date_hint(blob_lower: str) -> bool:
    # hint “ligero” (sin parsear): 2026-03-08, 8 marzo, 13 de marzo, etc.
    return bool(re.search(r"\b(\d{4}-\d{2}-\d{2}|\d{1,2}\s*(de)?\s*(marzo|mar|mars|march))\b", blob_lower))


def has_location_hint(blob_lower: str) -> bool:
    return any(w in blob_lower for w in ["plaza", "parque", "auditorio", "sala", "punto de encuentro", "dirección", "direccion", "ubicación", "ubicacion", "lugar"])


def relevance_score(title: str, raw_text: str, url: str) -> int:
    """
    Score explicable, pero sin sesgo por institución:
    - requiere ancla 8M
    - requiere “señales de actividad” (fecha/hora/lugar/registro o vocabulario de evento)
    - tumba páginas índice/cookies
    """
    title = title or ""
    text = strip_boilerplate(raw_text or "")
    blob = (title + "\n" + text).strip()
    blob_lower = blob.lower()

    if not has_8m_anchor(title, text):
        return 0

    # si es página índice/cookies/nav, fuera
    if looks_like_index_or_nav_page(title, text, url):
        return 0

    # contenido mínimo útil
    if len(text) < 120:
        return 0

    score = 6  # ancla 8M

    aw = count_activity_words(blob_lower)
    if aw >= 2:
        score += 3
    elif aw == 1:
        score += 1

    if extract_hour(blob_lower):
        score += 2

    if has_location_hint(blob_lower):
        score += 2

    if has_date_hint(blob_lower):
        score += 2

    # bonus si ancla 8M aparece cerca de “actividad”
    if re.search(r"(8m|8\s*(de\s*)?(marzo|mar|mars|march)).{0,140}(evento|actividad|agenda|programa|convoc|taller|seminario|conferencia|marcha|manifest|inscrip|registro)", blob_lower):
        score += 2

    # si no hay ninguna “señal dura” (fecha/hora/lugar), probablemente es artículo temático
    hard_signals = 0
    if extract_hour(blob_lower): hard_signals += 1
    if has_date_hint(blob_lower): hard_signals += 1
    if has_location_hint(blob_lower): hard_signals += 1
    if any(x in blob_lower for x in ["inscripción", "inscripcion", "registro", "regístrate", "registrate"]): hard_signals += 1

    if hard_signals == 0:
        score -= 4

    return score


def extract_event_fields(parsed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = parsed.get("url", "") or ""
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""

    score = relevance_score(title, text, url)

    # Umbral: 8 funciona bien para: ancla(6) + (1–2 señales)
    # Si sigue sucio, subilo a 9.
    if score < 8:
        return None

    imgs = parsed.get("images") or []
    imagen = imgs[0] if imgs else ""

    # fecha/hora (del texto ya limpiado)
    cleaned = strip_boilerplate(text)
    fecha = extract_event_date_contextual(cleaned, title)
    hora = extract_hour(cleaned)

    convocatoria = clean_text(title, 180)
    if not convocatoria:
        lines = [l.strip() for l in (cleaned or "").split("\n") if len(l.strip()) > 10]
        convocatoria = clean_text(lines[0] if lines else "", 180)

    out = {
        "colectiva": "",
        "convocatoria": convocatoria,
        "descripcion": clean_text(cleaned, 280),
        "fecha": fecha,
        "hora": hora,
        "pais": "",
        "ciudad": "",
        "localizacion_exacta": "",
        "direccion": "",
        "lat": "",
        "lon": "",
        "imagen": imagen,
        "cta_url": url,
        "sitio_web_colectiva": "",
        "trans_incluyente": "",
        "confianza_extraccion": "media",
        "precision_ubicacion": "",
        "score_relevancia": score,
    }
    return out
