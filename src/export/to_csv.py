# src/export/to_csv.py
import csv
import os

COLUMNS = [
    "colectiva", "convocatoria", "descripcion", "fecha", "hora", "pais", "ciudad",
    "localizacion_exacta", "direccion", "lat", "lon", "imagen", "cta_url",
    "sitio_web_colectiva", "trans_incluyente", "fuente_url", "fuente_tipo",
    "confianza_extraccion", "precision_ubicacion"
]

def _format_image_for_umap(value):
    v = (value or "").strip()
    if not v:
        return ""

    # si ya viene envuelto, lo deja igual
    if v.startswith("{{") and v.endswith("}}"):
        return v

    # si es un nombre local de imagen, envuelve para uMap
    if v.lower().endswith((".jpg", ".jpeg", ".png")) and not v.startswith(("http://", "https://")):
        return "{{" + v + "}}"

    # si es URL pública, también puede envolver
    if v.startswith(("http://", "https://")) and v.lower().endswith((".jpg", ".jpeg", ".png")):
        return "{{" + v + "}}"

    return v

def export_csv(records, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for r in records:
            row = {k: r.get(k, "") for k in COLUMNS}
            row["imagen"] = _format_image_for_umap(row.get("imagen", ""))
            writer.writerow(row)
