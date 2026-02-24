import os
import csv
from datetime import datetime

try:
    import yaml
except ImportError:
    yaml = None


EXPORT_PATH = "data/exports/mapa_8m_master.csv"

COLUMNS = [
    "colectiva", "convocatoria", "descripcion", "fecha", "hora", "pais", "ciudad",
    "localizacion_exacta", "direccion", "lat", "lon", "imagen", "cta_url",
    "sitio_web_colectiva", "trans_incluyente", "fuente_url", "fuente_tipo",
    "confianza_extraccion", "precision_ubicacion"
]


def load_yaml(path):
    if yaml is None:
        raise RuntimeError("Falta PyYAML. Instalá dependencias con: pip install -r requirements.txt")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dirs():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/exports", exist_ok=True)
    os.makedirs("data/images", exist_ok=True)


def build_seed_records(keywords_cfg, sources_cfg):
    """
    MVP: genera registros semilla desde fuentes configuradas.
    Todavía no scrapea contenido real; deja la estructura andando.
    """
    records = []
    now = datetime.utcnow().isoformat()

    for src in sources_cfg.get("sources", []):
        records.append({
            "colectiva": "",
            "convocatoria": "",
            "descripcion": f"Fuente semilla configurada para búsqueda automatizada ({src.get('name','')}).",
            "fecha": "",
            "hora": "",
            "pais": "",
            "ciudad": "",
            "localizacion_exacta": "",
            "direccion": "",
            "lat": "",
            "lon": "",
            "imagen": "",
            "cta_url": "",
            "sitio_web_colectiva": "",
            "trans_incluyente": "",
            "fuente_url": src.get("url", ""),
            "fuente_tipo": src.get("type", "web"),
            "confianza_extraccion": "baja",
            "precision_ubicacion": ""
        })

    # Guardar snapshot de keywords usadas (útil para trazabilidad)
    with open("data/processed/keywords_snapshot.txt", "w", encoding="utf-8") as f:
        f.write(f"Generado: {now}\n\n")
        for lang, kws in keywords_cfg.get("languages", {}).items():
            f.write(f"[{lang}]\n")
            for kw in kws:
                f.write(f"- {kw}\n")
            f.write("\n")

    return records


def export_csv(records, out_path):
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k, "") for k in COLUMNS})


def main():
    ensure_dirs()

    keywords_cfg = load_yaml("config/keywords.yml")
    sources_cfg = load_yaml("config/sources.yml")

    records = build_seed_records(keywords_cfg, sources_cfg)
    export_csv(records, EXPORT_PATH)

    print("✅ MVP inicial ejecutado")
    print(f"📄 CSV generado: {EXPORT_PATH}")
    print(f"🧠 Fuentes configuradas: {len(sources_cfg.get('sources', []))}")
    print(f"🔎 Idiomas con keywords: {len(keywords_cfg.get('languages', {}))}")


if __name__ == "__main__":
    main()
