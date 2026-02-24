# src/extract/extractor_ai.py
import re

TRIGGERS = [
    "8m", "8 marzo", "8 mars", "8 march",
    "women", "womens", "woman",
    "mujer", "mujeres",
    "femin", "international women's day",
    "dia internacional da mulher",
    "journée internationale des droits des femmes"
]

def clean_text(s, max_len=280):
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s[:max_len]

def extract_event_fields(parsed):
    title = parsed.get("title", "") or ""
    text = parsed.get("text", "") or ""
    blob = (title + "\n" + text).lower()

    # Filtro base
    if not any(t in blob for t in TRIGGERS):
        return None

    # Descripción breve
    desc = clean_text(text, max_len=280)

    # Primer imagen si existe
    img = ""
    if parsed.get("images"):
        img = parsed["images"][0]

    return {
        "colectiva": "",
        "convocatoria": clean_text(title, max_len=180),
        "descripcion": desc,
        "fecha": "",
        "hora": "",
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
