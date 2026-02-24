# src/collect/web_search.py
import yaml

def search_seed_urls(sources_config_path="config/sources.yml"):
    with open(sources_config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    urls = []
    for src in cfg.get("sources", []):
        url = src.get("url", "").strip()
        if url:
            urls.append(url)

    # quitar duplicados conservando orden
    seen = set()
    unique_urls = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    return unique_urls
