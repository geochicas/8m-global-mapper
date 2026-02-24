# main.py
import os
from src.collect.web_search import load_sources_and_keywords
from src.collect.discover_links import discover_candidate_links
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

def dedupe_keep_order(items):
    seen = set()
    out = []
    for i in items:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out

def main():
    ensure_dirs()

    seed_urls, keywords = load_sources_and_keywords(
        "config/sources.yml",
        "config/keywords.yml"
    )

    print(f"🌐 Fuentes semilla: {len(seed_urls)}")
    print(f"🔎 Keywords cargadas: {len(keywords)}")

    # 1) Descubrir links candidatos desde fuentes semilla
    candidate_links = []
    for seed in seed_urls:
        try:
            links = discover_candidate_links(seed, keywords, max_links=30)
            print(f"🔗 {seed} -> {len(links)} links candidatos")
            candidate_links.extend(links)
        except Exception as e:
            print(f"[WARN] No se pudieron descubrir links en {seed}: {e}")

    candidate_links = dedupe_keep_order(candidate_links)
    print(f"\n🧭 Total links candidatos únicos: {len(candidate_links)}")

    # 2) Procesar páginas candidatas
    records = []
    for url in candidate_links:
        try:
            html = fetch_page(url)
            parsed = parse_page(url, html)
            event = extract_event_fields(parsed)
            if event:
                records.append(event)
                print(f"✅ Detectado: {url}")
            else:
                print(f"— Sin match: {url}")
        except Exception as e:
            print(f"[WARN] Error procesando {url}: {e}")

    # 3) Exportar
    export_csv(records, EXPORT_PATH)
    print(f"\n📄 CSV generado: {EXPORT_PATH}")
    print(f"🧾 Registros exportados: {len(records)}")

if __name__ == "__main__":
    main()
