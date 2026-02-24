# main.py
import os
from src.collect.web_search import search_seed_urls
from src.collect.web_fetch import fetch_page
from src.parse.html_parser import parse_page
from src.extract.extractor_ai import extract_event_fields
from src.export.to_csv import export_csv

EXPORT_PATH = "data/exports/mapa_8m_master.csv"

def ensure_dirs():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/exports", exist_ok=True)
    os.makedirs("data/images", exist_ok=True)

def main():
    ensure_dirs()

    urls = search_seed_urls("config/sources.yml")
    print(f"🔎 URLs semilla: {len(urls)}")

    records = []
    for url in urls:
        try:
            html = fetch_page(url)
            parsed = parse_page(url, html)
            event = extract_event_fields(parsed)
            if event:
                records.append(event)
                print(f"✅ Posible actividad detectada: {url}")
            else:
                print(f"— Sin coincidencias 8M: {url}")
        except Exception as e:
            print(f"[WARN] Error en {url}: {e}")

    export_csv(records, EXPORT_PATH)
    print(f"\n📄 CSV generado: {EXPORT_PATH}")
    print(f"🧾 Registros exportados: {len(records)}")

if __name__ == "__main__":
    main()
