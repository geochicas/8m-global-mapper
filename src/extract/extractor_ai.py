# src/extract/extractor_ai.py
def extract_event_fields(parsed):
    text_blob = (parsed.get("title", "") + "\n" + parsed.get("text", "")).lower()

    # Filtro muy básico (luego lo mejoramos)
    triggers = ["8m", "8 marzo", "women", "mujer", "femin", "international women's day"]
    if not any(t in text_blob for t in triggers):
        return None

    # Registro mínimo
    return {
        "colectiva": "",
        "convocatoria": parsed.get("title", "")[:180],
        "descripcion": (parsed.get("text", "")[:280]).strip(),
        "fecha": "",
        "hora": "",
        "pais": "",
        "ciudad": "",
        "localizacion_exacta": "",
        "direccion": "",
        "lat": "",
        "lon": "",
        "imagen": parsed.get("images", [""])[0] if parsed.get("images") else "",
        "cta_url": parsed.get("url", ""),
        "sitio_web_colectiva": "",
        "trans_incluyente": "",
        "fuente_url": parsed.get("url", ""),
        "fuente_tipo": "web",
        "confianza_extraccion": "baja",
        "precision_ubicacion": ""
    }
