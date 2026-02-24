import re
from dateutil import parser as dateutil_parser

import dateparser
from dateparser.search import search_dates

TRIGGERS = [
    "8m", "8 marzo", "8 mars", "8 march",
    "women", "womens", "woman",
    "mujer", "mujeres",
    "femin", "international women's day",
    "dia internacional da mulher",
    "journée internationale des droits des femmes"
]

# Horas tipo 5pm, 17:30, 17h, 5:30 pm
HOUR_PATTERNS = [
    r"\b([01]?\d|2[0-3]):([0-5]\d)\b",                 # 17:30
    r"\b([01]?\d|2[0-3])\s?h\b",                       # 17h
    r"\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s?(am|pm)\b",  # 5pm / 5:30 pm
]

def clean_text(s, max_len=280):
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s[:max_len]

def extract_hour(text):
    txt = (text or "").lower()

    # am/pm
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

    # HH:MM
    m = re.search(HOUR_PATTERNS[0], txt)
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"

    # HHh
    m = re.search(HOUR_PATTERNS[1], txt)
    if m:
        return f"{int(m.group(1)):02d}:00"

    return ""

def extract_date_multilang(text):
    """
    Busca fechas dentro de texto en múltiples idiomas usando dateparser.
    Devuelve YYYY-MM-DD o "".
    """
    txt = (text or "").strip()
    if not txt:
        return ""

    # settings: preferimos day-first (8/3/2025), y buscamos fechas futuras cercanas a marzo
    settings = {
        "PREFER_DAY_OF_MONTH": "first",
        "DATE_ORDER": "DMY",
        "RETURN_AS_TIMEZONE_AWARE": False,
        "STRICT_PARSING": False,
    }

    # search_dates devuelve lista de (fragmento_encontrado, datetime)
    try:
        hits = search_dates(
            txt,
            languages=["es", "en", "pt", "fr"],
            settings=settings
        )
    except Exception:
        hits = None

    if hits:
        # Elegimos la primera fecha razonable
        for frag, dt in hits:
            if dt:
                return dt.strftime("%Y-%m-%d")

    # fallback: intentos simples por si search_dates falla
    # iso-like
    m = re.search(r"\b\d{4}-\d{2}-\d{2}\b", txt)
    if m:
        return m.group(0)

    # último fallback: dateutil (en inglés suele ayudar)
    try:
        dt = dateutil_parser.parse(txt, fuzzy=True, dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def guess_location_line(text):
    txt = text or ""
    patterns = [
        r"(?:\ben\b|\blugar\b|\bubicaci[oó]n\b)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^\n\.]{3,120})",
        r"(?:\bat\b|\bvenue\b|\blocation\b)\s*[:\-]?\s*([A-Z][^\n\.]{3,120})",
    ]
    for p in patterns:
        m = re.search(p, txt, flags=re.IGNORECASE)
        if m:
            return clean_text(m.group(1), max_len=120)
    return ""

def extract_event_fields(parsed):
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""
    blob = (title + "\n" + text).lower()

    if not any(t in blob for t in TRIGGERS):
        return None

    desc = clean_text(text, max_len=280)
    img = parsed.get("images", [""])[0] if parsed.get("images") else ""

    # ✅ NUEVO: fecha multilingüe robusta
    fecha = extract_date_multilang(title + "\n" + text)

    hora = extract_hour(title + "\n" + text)
    localizacion = guess_location_line(title + "\n" + text)

    return {
        "colectiva": "",
        "convocatoria": clean_text(title, max_len=180),
        "descripcion": desc,
        "fecha": fecha,
        "hora": hora,
        "pais": "",
        "ciudad": "",
        "localizacion_exacta": localizacion,
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
    }# src/extract/extractor_ai.py
import re
from dateutil import parser as date_parser

TRIGGERS = [
    "8m", "8 marzo", "8 mars", "8 march",
    "women", "womens", "woman",
    "mujer", "mujeres",
    "femin", "international women's day",
    "dia internacional da mulher",
    "journée internationale des droits des femmes"
]

# Horas tipo 5pm, 17:30, 17h, 5:30 pm
HOUR_PATTERNS = [
    r"\b([01]?\d|2[0-3]):([0-5]\d)\b",           # 17:30
    r"\b([01]?\d|2[0-3])\s?h\b",                 # 17h
    r"\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s?(am|pm)\b",  # 5pm / 5:30 pm
]

# Fechas comunes (simple)
DATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}\b",                    # 2026-03-08
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",              # 08/03/2026
    r"\b\d{1,2}-\d{1,2}-\d{2,4}\b",              # 08-03-2026
]

MONTH_WORDS = [
    "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","setiembre","octubre","noviembre","diciembre",
    "january","february","march","april","may","june","july","august","september","october","november","december",
    "janvier","février","fevrier","mars","avril","mai","juin","juillet","août","aout","septembre","octobre","novembre","décembre","decembre",
    "março","marco"
]

def clean_text(s, max_len=280):
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s[:max_len]

def extract_hour(text):
    txt = (text or "").lower()

    # am/pm
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

    # HH:MM
    m = re.search(HOUR_PATTERNS[0], txt)
    if m:
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"

    # HHh
    m = re.search(HOUR_PATTERNS[1], txt)
    if m:
        return f"{int(m.group(1)):02d}:00"

    return ""

def extract_date(text):
    txt = text or ""

    # formatos numéricos
    for pat in DATE_PATTERNS:
        m = re.search(pat, txt)
        if m:
            raw = m.group(0)
            try:
                dt = date_parser.parse(raw, dayfirst=True, fuzzy=True)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass

    # intentos con mes en palabras (por ejemplo "8 de marzo 2026")
    low = txt.lower()
    if any(month in low for month in MONTH_WORDS):
        # buscar una frase de fecha "candidate"
        # tomamos una ventana de texto alrededor de "marzo"/"march"/etc.
        for month in MONTH_WORDS:
            idx = low.find(month)
            if idx != -1:
                start = max(0, idx - 20)
                end = min(len(txt), idx + 30)
                chunk = txt[start:end]
                try:
                    dt = date_parser.parse(chunk, dayfirst=True, fuzzy=True)
                    return dt.strftime("%Y-%m-%d")
                except Exception:
                    continue

    return ""

def guess_location_line(text):
    """
    Heurística simple:
    Busca frases con "en", "at", "venue", "lugar".
    """
    txt = text or ""
    patterns = [
        r"(?:\ben\b|\blugar\b|\bubicaci[oó]n\b)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^\n\.]{3,120})",
        r"(?:\bat\b|\bvenue\b|\blocation\b)\s*[:\-]?\s*([A-Z][^\n\.]{3,120})",
    ]
    for p in patterns:
        m = re.search(p, txt, flags=re.IGNORECASE)
        if m:
            return clean_text(m.group(1), max_len=120)
    return ""

def extract_event_fields(parsed):
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""
    blob = (title + "\n" + text).lower()

    if not any(t in blob for t in TRIGGERS):
        return None

    desc = clean_text(text, max_len=280)
    img = parsed.get("images", [""])[0] if parsed.get("images") else ""

    fecha = extract_date(title + "\n" + text)
    hora = extract_hour(title + "\n" + text)
    localizacion = guess_location_line(title + "\n" + text)

    return {
        "colectiva": "",
        "convocatoria": clean_text(title, max_len=180),
        "descripcion": desc,
        "fecha": fecha,
        "hora": hora,
        "pais": "",
        "ciudad": "",
        "localizacion_exacta": localizacion,
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
