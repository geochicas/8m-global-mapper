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
# EVENT INTENT / ACTIVITY SIGNALS
# (necesitamos al menos 2 señales)
# =========================
EVENT_WORDS = [
    "evento", "actividad", "agenda", "programa", "calendario",
    "convoca", "convocatoria", "participa", "acompáñanos", "acompananos",
    "inscripción", "inscripcion", "registro", "regístrate", "registrate",
    "entrada", "cupos", "aforo", "gratuito", "gratis",
    "taller", "seminario", "conferencia", "conversatorio", "charla",
    "encuentro", "mesa", "panel", "cineforo", "proyección", "proyeccion",
    "marcha", "manifestación", "manifestacion", "movilización", "movilizacion",
    "concentración", "concentracion", "parque", "plaza", "auditorio", "sala",
    "lugar", "ubicación", "ubicacion", "dirección", "direccion",
]

EVENT_WORDS_LOWER = set(w.lower() for w in EVENT_WORDS)


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


def has_8m_anchor(title: str, text: str) -> bool:
    blob = (title + "\n" + (text or "")).strip()
    return bool(ANCHORS_8M_RE.search(blob))


def count_activity_signals(title: str, text: str) -> int:
    """
    Cuenta señales de que es una ACTIVIDAD (no solo contenido temático):
    - menciona palabras típicas de agenda/registro/lugar/actividad
    - tiene hora
    - tiene fecha
    """
    blob = (title + "\n" + (text or "")).lower()

    # 1) palabras
    word_hits = 0
    # contamos por presencia (no frecuencia) para evitar inflar por repetición
    for w in EVENT_WORDS_LOWER:
        if w in blob:
            word_hits += 1

    # 2) hora
    hour_hit = 1 if extract_hour(blob) else 0

    # 3) fecha (light): presencia de patrones típicos (sin parsear aún)
    date_hint = 1 if re.search(r"\b(\d{1,2}\s*(de)?\s*(marzo|mar|mars|march)|\d{4}-\d{2}-\d{2})\b", blob) else 0

    # Señales: al menos 2 entre (palabras, hora, date_hint)
    signals = 0
    if word_hits >= 2:
        signals += 1
    if hour_hit:
        signals += 1
    if date_hint:
        signals += 1

    return signals


def proximity_bonus(title: str, text: str) -> int:
    """
    Bonus si el ancla 8M aparece cerca de palabras de evento (típico de convocatorias).
    """
    blob = (title + "\n" + (text or "")).lower()
    # ventana de 120 caracteres
    if re.search(r"(8m|8\s*(de\s*)?(marzo|mar|mars|march)).{0,120}(evento|actividad|agenda|programa|convoc|taller|seminario|conferencia|marcha|manifest|inscrip|registro)", blob):
        return 2
    return 0


def relevance_score(title: str, text: str) -> int:
    """
    Score explicable:
    - +6 si hay ancla 8M
    - +3 si hay 2+ señales de actividad
    - +2 si hay proximidad ancla-evento
    - +1 si hay hora
    - +1 si hay fecha parseable
    """
    score = 0

    anchor = has_8m_anchor(title, text)
    if anchor:
        score += 6
    else:
        return 0  # sin ancla 8M NO es evento 8M (evita ruido masivo)

    signals = count_activity_signals(title, text)
    score += signals * 3

    score += proximity_bonus(title, text)

    if extract_hour(text):
        score += 1

    if extract_event_date_contextual(text, title):
        score += 1

    return score


def extract_event_fields(parsed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = parsed.get("url", "") or ""
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""

    score = relevance_score(title, text)

    # Umbral recomendado: 9.
    # - requiere ancla (6) + actividad (>=3) y algo más.
    if score < 9:
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
        "score_relevancia": score,   # 👈 útil para auditar "por qué entró"
    }
    return out
