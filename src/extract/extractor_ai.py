import re
from dateparser.search import search_dates

TRIGGERS = [
    "8m", "8 marzo", "8 mars", "8 march",
    "women", "womens", "woman",
    "mujer", "mujeres",
    "femin", "international women's day",
    "dia internacional da mulher",
    "journée internationale des droits des femmes"
]

HOUR_PATTERNS = [
    r"\b([01]?\d|2[0-3]):([0-5]\d)\b",
    r"\b([01]?\d|2[0-3])\s?h\b",
    r"\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s?(am|pm)\b",
]

def clean_text(s, max_len=280):
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s[:max_len]

def extract_hour(text):
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

def _parse_iso_date(s):
    s = (s or "").strip()
    if not s:
        return ""
    # soporta 2025-03-08, 2025-03-08T10:00:00Z
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else ""

def extract_event_date(parsed):
    # 1) schema.org Event (JSON-LD)
    for ev in (parsed.get("jsonld_events") or []):
        d = _parse_iso_date(ev.get("startDate", ""))
        if d:
            return d

    # 2) texto visible (multilingüe)
    blob = (parsed.get("title","") + "\n" + parsed.get("text","")).strip()
    if not blob:
        return ""

    settings = {
        "DATE_ORDER": "DMY",
        "PREFER_DAY_OF_MONTH": "first",
        "STRICT_PARSING": False,
    }

    hits = search_dates(
        blob,
        languages=["es", "en", "pt", "fr"],
        settings=settings
    )

    if not hits:
        return ""

    # devolvemos la primera fecha encontrada
    for frag, dt in hits:
        if dt:
            return dt.strftime("%Y-%m-%d")

    return ""

def extract_event_fields(parsed):
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""
    blob = (title + "\n" + text).lower()

    if not any(t in blob for t in TRIGGERS):
        return None

    desc = clean_text(text, max_len=280)
    img = parsed.get("images", [""])[0] if parsed.get("images") else ""

    fecha = extract_event_date(parsed)
    hora = extract_hour(title + "\n" + text)

    return {
        "colectiva": "",
        "convocatoria": clean_text(title, max_len=180),
        "descripcion": desc,
        "fecha": fecha,
        "hora": hora,
        "pais": "",
        "ciudad": "",
        "localizacion_exacta": "",
        "direccion": "",
        "lat": "",
        "lon": "",
        "imagen": img,
        "cta_url": parsed.get("url", ""),
        "sitio_web_colectiva": "",
        "trans_incluyente": "",
        "fuente_url": parsed.get("url", ""),
        "fuente_tipo": "web",
        "confianza_extraccion": "baja",
        "precision_ubicacion": ""
    }
