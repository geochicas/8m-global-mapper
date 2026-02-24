# src/export/to_csv.py
import csv
import os

COLUMNS = [
    "colectiva", "convocatoria", "descripcion", "fecha", "hora", "pais", "ciudad",
    "localizacion_exacta", "direccion", "lat", "lon", "imagen", "cta_url",
    "sitio_web_colectiva", "trans_incluyente", "fuente_url", "fuente_tipo",
    "confianza_extraccion", "precision_ubicacion"
]

def export_csv(records, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k, "") for k in COLUMNS})
