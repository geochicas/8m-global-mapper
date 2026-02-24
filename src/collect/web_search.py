# src/collect/web_search.py
import yaml

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_sources_and_keywords(
    sources_config_path="config/sources.yml",
    keywords_config_path="config/keywords.yml"
):
    sources_cfg = load_yaml(sources_config_path)
    keywords_cfg = load_yaml(keywords_config_path)

    seed_urls = []
    for src in sources_cfg.get("sources", []):
        url = (src.get("url") or "").strip()
        if url:
            seed_urls.append(url)

    # Flatten keywords (idiomas + términos)
    keywords = []
    for _, kws in keywords_cfg.get("languages", {}).items():
        keywords.extend(kws or [])
    keywords.extend(keywords_cfg.get("event_terms", []) or [])

    # dedupe conservando orden
    def unique_keep_order(items):
        seen = set()
        out = []
        for i in items:
            if i not in seen:
                seen.add(i)
                out.append(i)
        return out

    return unique_keep_order(seed_urls), unique_keep_order(keywords)
