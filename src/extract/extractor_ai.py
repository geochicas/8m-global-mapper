import re
from typing import Any, Dict, Optional

from dateparser.search import search_dates


TRIGGERS = [
    "8m", "8 marzo", "8 de marzo", "8 mars", "8 march", "8 mar",
    "women", "womens", "woman", "women’s", "women's",
    "mujer", "mujeres", "femin", "feminista",
    "international women's day", "international womens day", "iwd",
    "dia internacional da mulher", "dia internacional de la mujer",
    "journée internationale des droits des femmes",
    "journee internationale des droits des femmes",
    "huelga feminista", "paro de mujeres", "paro internacional de mujeres",
    "marcha", "manifestación", "manifestacion",
    "movilización", "movilizacion",
]


HOUR_PATTERNS = [
    r"\b([01]?\d|2[0-3]):([0-5]\d)\b",
    r"\b([01]?\d|2[0-3])\s?h\b",
    r"\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s?(am|pm)\b",
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


def looks_like_event(text: str, title: str = "") -> bool:
    blob = (title + "\n" + (text or "")).lower()
    return any(t in blob for t in TRIGGERS)


def _strip_absurd_years(chunk: str) -> str:
    return re.sub(r"\b(3[0-9]{3}|[4-9][0-9]{3,})\b", " ", chunk)


def extract_event_date_contextual(text: str, title: str = "") -> str:
    blob = (title + "\n" + (text or "")).strip()
    if not blob:
        return ""

    chunk = blob[:1500]
    chunk = _strip_absurd_years(chunk)

    # 🔥 PRIORIDAD: detectar explícitamente 8 de marzo
    explicit_8m = re.search(
        r"(8\s*(de)?\s*(marzo|mar|mars|march))",
        chunk.lower()
    )
    if explicit_8m:
        # Asumimos año actual si no está claro
        import datetime
        year = datetime.datetime.now().year
        return f"{year}-03-08"

    settings = {
        "PREFER_DATES_FROM": "future",
        "STRICT_PARSING": False,
        "RETURN_AS_TIMEZONE_AWARE": False,
    }

    try:
        hits = search_dates(
            chunk,
            languages=["es", "en", "pt", "fr"],
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


def extract_event_fields(parsed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = parsed.get("url", "")
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""

    if not looks_like_event(text, title):
        return None

    imgs = parsed.get("images") or []
    imagen = imgs[0] if imgs else ""

    fecha = extract_event_date_contextual(text, title)
    hora = extract_hour(text)

    convocatoria = clean_text(title, 180)
    if not convocatoria:
        lines = [l.strip() for l in (text or "").split("\n") if len(l.strip()) > 10]
        convocatoria = clean_text(lines[0] if lines else "", 180)

    out = {
        "colectiva": "",
        "convocatoria": convocatoria,
        "descripcion": clean_text(text, 280),
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
    }
    return out
