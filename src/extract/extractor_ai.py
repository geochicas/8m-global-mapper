import re
from datetime import datetime
from dateparser.search import search_dates

TRIGGERS = [
    "8m", "8 marzo", "8 de marzo", "8 mars", "8 march",
    "women", "mujer", "mujeres", "femin", "feminista",
    "international women's day",
    "dia internacional da mulher",
    "journée internationale des droits des femmes"
]

EVENT_CONTEXT_KEYWORDS = [
    "marcha", "manifestación", "manifestacion",
    "concentración", "concentracion",
    "convoca", "convocatoria",
    "protesta", "movilización", "movilizacion",
    "plaza", "parque", "avenida",
    "punto de encuentro", "salida",
    "a las", "hora", "horas", "h", "pm", "am"
]

HOUR_PATTERNS = [
    r"\b([01]?\d|2[0-3]):([0-5]\d)\b",
    r"\b([01]?\d|2[0-3])\s?h\b",
    r"\b(1[0-2]|0?[1-9])(?::([0-5]\d))?\s?(am|pm)\b",
]

CITIES_CACHE = None

def load_cities(path="config/cities.txt"):
    global CITIES_CACHE
    if CITIES_CACHE is not None:
        return CITIES_CACHE
    cities = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                c = line.strip()
                if c:
                    cities.append(c)
    except FileNotFoundError:
        cities = []
    # ordenar por longitud desc para matchear "San José" antes que "José"
    cities.sort(key=lambda x: len(x), reverse=True)
    CITIES_CACHE = cities
    return CITIES_CACHE

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

def valid_year(dt):
    return 2000 <= dt.year <= 2100

def score_date(dt, frag, context):
    score = 0
    today = datetime.utcnow().date()

    # preferimos marzo y el día 8
    if dt.month == 3:
        score += 6
    if dt.day == 8:
        score += 4

    # penalizar "hoy" y placeholders
    if dt.date() == today:
        score -= 4
    if dt.month == 1 and dt.day == 1:
        score -= 3

    txt = (frag + " " + context).lower()

    if "8m" in txt or "8 marzo" in txt or "8 de marzo" in txt:
        score += 3
    if "marcha" in txt or "manifest" in txt or "concentr" in txt:
        score += 2

    return score

def extract_event_date_contextual(text, title=""):
    blob = ((title or "") + "\n" + (text or "")).strip()
    if not blob:
        return ""

    lines = [ln.strip() for ln in blob.split("\n") if ln.strip()]
    chunks = []

    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(k in low for k in EVENT_CONTEXT_KEYWORDS):
            start = max(0, i - 2)
            end = min(len(lines), i + 3)
            chunks.append(" ".join(lines[start:end]))

    if not chunks:
        chunks = [blob]

    settings = {
        "DATE_ORDER": "DMY",
        "PREFER_DAY_OF_MONTH": "first",
        "STRICT_PARSING": False,
    }

    best = None  # (score, dt)

    for chunk in chunks:
        hits = search_dates(chunk, languages=["es", "en", "pt", "fr"], settings=settings)
        if not hits:
            continue

        for frag, dt in hits:
            if not dt or not valid_year(dt):
                continue
            sc = score_date(dt, frag, chunk)
            if best is None or sc > best[0]:
                best = (sc, dt)

    return best[1].strftime("%Y-%m-%d") if best else ""

def detect_city(title, text):
    cities = load_cities()
    if not cities:
        return ""
    blob = (title or "") + "\n" + (text or "")
    # búsqueda case-insensitive por palabra completa aproximada
    for c in cities:
        # escapar y permitir acentos como vienen; usamos límite de palabra flexible
        pattern = r"(?i)(?<!\w)" + re.escape(c) + r"(?!\w)"
        if re.search(pattern, blob):
            return c
    return ""

def extract_event_fields(parsed):
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""
    blob = (title + "\n" + text).lower()

    if not any(t in blob for t in TRIGGERS):
        return None

    desc = clean_text(text, max_len=280)
    img = parsed.get("images", [""])[0] if parsed.get("images") else ""

    fecha = extract_event_date_contextual(text, title)
    hora = extract_hour(title + "\n" + text)

    ciudad = detect_city(title, text)

    # localizacion_exacta: por ahora al menos la ciudad (luego mejoramos venue/dirección)
    localizacion_exacta = ciudad if ciudad else ""

    return {
        "colectiva": "",
        "convocatoria": clean_text(title, max_len=180),
        "descripcion": desc,
        "fecha": fecha,
        "hora": hora,
        "pais": "",
        "ciudad": ciudad,
        "localizacion_exacta": localizacion_exacta,
        "direccion": "",
        "lat": "",
        "lon": "",
        "imagen": img,
        "cta_url": parsed.get("url", ""),
        "sitio_web_colectiva": "",
        "trans_incluyente": "",
        "fuente_url": parsed.get("url", ""),
        "fuente_tipo": "web",
        "confianza_extraccion": "media",
        "precision_ubicacion": ""
    }
