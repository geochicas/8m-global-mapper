# main.py — 8m-global-mapper
# Mantiene el pipeline existente:
# - merge de sources (base/generated/feminist)
# - priority URLs desde master CSV
# - crawl por seeds (same-domain)
# - parse + extract + score + reglas + fecha mínima
# - geocode + cache
# - descarga de imagen + filtro anti-logo/footer
# - export master + umap + sin_coord
# - archivos para GitHub Pages (Actions)
#
# Cambio clave aquí:
# - sources.yml puede ser ANIDADO (región/tema/urls/social/hashtags)
# - hashtags se incorporan como keywords extra (para scoring/matching)
# - social no se scrapea por defecto (ENABLE_SOCIAL_SEEDS=false)

from __future__ import annotations

import csv
import os
import re
import sys
import time
import hashlib
from datetime import date, datetime

import yaml

# --- módulos del repo ---
from src.collect.web_fetch import make_session, fetch_url
from src.collect.discover_links import extract_links, same_domain
from src.parse.html_parser import parse_page
from src.extract.extractor_ai import extract_event_fields
from src.geocode.geocoder import geocode_event, load_geocode_cache, save_geocode_cache
from src.media.image_processor import download_and_process_image
from src.export.to_csv import export_master_csv, export_umap_csv, export_sin_coord_csv

from src.collect.sources_loader import load_sources, should_include_social_seeds


# =========================
# Paths / Config
# =========================
BASE_SOURCES_YML = "config/sources.yml"
GENERATED_SOURCES_YML = "config/sources.generated.yml"
FEMINIST_SOURCES_YML = "config/sources.feminist.yml"

KEYWORDS_YML = "config/keywords.yml"
CITIES_TXT = "config/cities.txt"
DOMAIN_RULES_YML = "config/domain_rules.yml"

MASTER_CSV_PATH = "data/exports/mapa_8m_global_master.csv"

EXPORT_MASTER = "data/exports/mapa_8m_global_master.csv"
EXPORT_UMAP = "data/exports/mapa_8m_global_umap.csv"
EXPORT_SIN_COORD = "data/exports/mapa_8m_global_sin_coord.csv"

IMAGES_DIR = "data/images"
GEOCODE_CACHE_PATH = "data/processed/geocode_cache.json"

# =========================
# Tunables
# =========================
FAST_MODE = os.environ.get("FAST_MODE", "true").strip().lower() in ("1", "true", "yes", "y", "on")

MAX_SEEDS = int(os.environ.get("MAX_SEEDS", "220"))            # cuántos seeds usar
MAX_PRIORITY = int(os.environ.get("MAX_PRIORITY", "750"))      # cuántas priority urls
MAX_TOTAL_CANDIDATES = int(os.environ.get("MAX_TOTAL_CANDIDATES", "3000"))

MAX_PAGES_PER_SEED = int(os.environ.get("MAX_PAGES_PER_SEED", "60" if FAST_MODE else "120"))

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))
SLEEP_EVERY = int(os.environ.get("SLEEP_EVERY", "0"))  # 0 = no sleep

THRESHOLD_EXTRACT = int(os.environ.get("THRESHOLD_EXTRACT", "6"))
THRESHOLD_EXPORT_UMAP = int(os.environ.get("THRESHOLD_EXPORT_UMAP", "10"))

MIN_EVENT_DATE = date.fromisoformat(os.environ.get("MIN_EVENT_DATE", "2025-01-01"))


# =========================
# Utils
# =========================
def ensure_dirs():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/exports", exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs("config", exist_ok=True)


def file_exists(p: str) -> bool:
    return os.path.exists(p) and os.path.getsize(p) > 0


def load_yaml(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def dedupe_urls(urls: list[str]) -> list[str]:
    seen = set()
    out = []
    for u in urls:
        u = (u or "").strip()
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def normalize(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_csv_urls(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    urls = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            u = (row.get("fuente_url") or row.get("cta_url") or row.get("convocatoria") or "").strip()
            if u.startswith("http"):
                urls.append(u)
    return dedupe_urls(urls)


# =========================
# Domain rules (allow/deny)
# =========================
def load_domain_rules() -> dict:
    y = load_yaml(DOMAIN_RULES_YML)
    return y if isinstance(y, dict) else {}


def url_allowed_by_rules(rules: dict, url: str, seed: str) -> bool:
    if not rules:
        return True
    u = url.lower()

    deny = rules.get("deny_contains", []) if isinstance(rules.get("deny_contains"), list) else []
    for pat in deny:
        if isinstance(pat, str) and pat.lower() in u:
            return False

    allow = rules.get("allow_contains", []) if isinstance(rules.get("allow_contains"), list) else []
    if allow:
        for pat in allow:
            if isinstance(pat, str) and pat.lower() in u:
                return True
        # si hay allow list y no matchea nada, bloquea
        return False

    return True


# =========================
# Keywords (base + hashtags)
# =========================
def read_keywords() -> list[str]:
    y = load_yaml(KEYWORDS_YML)
    out = []
    if isinstance(y, list):
        out.extend([str(x).strip() for x in y if str(x).strip()])
    elif isinstance(y, dict):
        for v in y.values():
            if isinstance(v, list):
                out.extend([str(x).strip() for x in v if str(x).strip()])
    return [k for k in out if k]


def read_keywords_count() -> int:
    return len(read_keywords())


def merge_keywords_with_hashtags(keywords: list[str], hashtags: list[str]) -> list[str]:
    # hashtags como tokens “buscables”; también agrega versiones sin '#'
    extra = []
    for h in hashtags or []:
        h = (h or "").strip()
        if not h:
            continue
        extra.append(h)
        if h.startswith("#") and len(h) > 1:
            extra.append(h[1:])
    return dedupe_urls([k for k in (keywords + extra) if k and k.strip()])


# =========================
# City list
# =========================
def load_cities(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    cities = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            cities.append(s)
    cities.sort(key=lambda x: len(x), reverse=True)
    return cities


def detect_city(text: str, cities: list[str]) -> str | None:
    if not text or not cities:
        return None
    t = " " + normalize(text).lower() + " "
    for city in cities:
        c = city.lower()
        if re.search(rf"(?<!\w){re.escape(c)}(?!\w)", t):
            return city
    return None


# =========================
# Sources: merge base/generated/feminist + nested YAML support
# =========================
def generate_sources_from_base_and_master_csv(
    base_sources_yml: str,
    master_csv_path: str,
    out_generated_yml: str,
    max_priority: int = 2200,
):
    base_bundle = load_sources(base_sources_yml)
    seeds = base_bundle.seeds_urls[:MAX_SEEDS]

    priority_urls = read_csv_urls(master_csv_path)[:max_priority]
    payload = {"seeds": seeds, "priority_urls": priority_urls}

    os.makedirs(os.path.dirname(out_generated_yml), exist_ok=True)
    with open(out_generated_yml, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False, allow_unicode=True)


def select_sources_file() -> str:
    if (not file_exists(GENERATED_SOURCES_YML)) and os.path.exists(MASTER_CSV_PATH):
        print(f"🧩 Generando {GENERATED_SOURCES_YML} desde {MASTER_CSV_PATH} + {BASE_SOURCES_YML}")
        generate_sources_from_base_and_master_csv(
            base_sources_yml=BASE_SOURCES_YML,
            master_csv_path=MASTER_CSV_PATH,
            out_generated_yml=GENERATED_SOURCES_YML,
            max_priority=2200,
        )
    return GENERATED_SOURCES_YML if file_exists(GENERATED_SOURCES_YML) else BASE_SOURCES_YML


def read_sources_merged() -> tuple[list[str], list[str], list[str]]:
    """
    Merge de:
      - sources.yml (anidado OK)
      - sources.generated.yml (si existe)
      - sources.feminist.yml (si existe)
    Devuelve: (seeds, priority_urls, hashtags)
    """
    seeds_all: list[str] = []
    priority_all: list[str] = []
    hashtags_all: list[str] = []

    paths = [BASE_SOURCES_YML, select_sources_file(), FEMINIST_SOURCES_YML]
    for p in paths:
        if not file_exists(p):
            continue
        b = load_sources(p)
        seeds_all.extend(b.seeds_urls)
        priority_all.extend(b.priority_urls)
        hashtags_all.extend(b.hashtags)

        # opcional: si habilitan social seeds explícitamente
        if should_include_social_seeds():
            seeds_all.extend(b.social_urls)

    return dedupe_urls(seeds_all), dedupe_urls(priority_all), dedupe_urls(hashtags_all)


# =========================
# MAIN
# =========================
def main():
    ensure_dirs()
    session = make_session(timeout=REQUEST_TIMEOUT)
    cities = load_cities(CITIES_TXT)
    rules = load_domain_rules()

    seeds, priority, hashtags = read_sources_merged()
    base_keywords = read_keywords()
    keywords = merge_keywords_with_hashtags(base_keywords, hashtags)

    print(f"🌐 Seeds: {min(len(seeds), MAX_SEEDS)}")
    print(f"🎯 Priority URLs: {min(len(priority), MAX_PRIORITY)}")
    print(f"🔎 Keywords: {len(keywords)} (base={len(base_keywords)} + hashtags={len(hashtags)})")
    print(f"⚡ FAST_MODE: {FAST_MODE}")
    print(f"🧠 Score thresholds: extract>={THRESHOLD_EXTRACT} | umap>={THRESHOLD_EXPORT_UMAP}")
    print(f"🗓️  MIN_EVENT_DATE: {MIN_EVENT_DATE.isoformat()}")
    if not should_include_social_seeds():
        print("🧷 Social seeds: OFF (ENABLE_SOCIAL_SEEDS=false)")

    candidates: list[str] = []
    seen = set()

    # priority primero
    for u in priority[:MAX_PRIORITY]:
        if u not in seen and url_allowed_by_rules(rules, u, ""):
            seen.add(u)
            candidates.append(u)
        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break

    # crawl por seeds (misma domain)
    for seed in seeds[:MAX_SEEDS]:
        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break

        if not url_allowed_by_rules(rules, seed, ""):
            continue

        html = fetch_url(session, seed, use_cache=True)
        if not html:
            print(f"[WARN] Seed sin respuesta: {seed}")
            continue

        links = extract_links(seed, html)
        picked = 0

        for link in links:
            if len(candidates) >= MAX_TOTAL_CANDIDATES:
                break
            if link in seen:
                continue
            if not same_domain(seed, link):
                continue
            if not url_allowed_by_rules(rules, link, ""):
                continue
            seen.add(link)
            candidates.append(link)
            picked += 1
            if picked >= MAX_PAGES_PER_SEED:
                break

        if picked:
            print(f"🔗 {seed} -> candidatos: {picked}")

    print(f"🔎 Candidates total: {len(candidates)}")

    records: list[dict] = []
    started = time.time()

    geocode_cache = load_geocode_cache(GEOCODE_CACHE_PATH)

    # contadores
    n_events = 0
    n_geocoded = 0
    n_imgs = 0
    n_old_skip = 0
    n_low_score = 0
    n_rules_skip = 0

    # procesar
    for i, url in enumerate(candidates, start=1):
        if SLEEP_EVERY and (i % SLEEP_EVERY == 0):
            time.sleep(1)

        html = fetch_url(session, url, use_cache=True)
        if not html:
            continue

        parsed = parse_page(url, html)
        if not parsed:
            continue

        # extracción
        ev = extract_event_fields(parsed)
        if not ev:
            continue

        # scoring básico: usamos el score que ya produce extractor_ai (si existe),
        # y además reforzamos con keywords/hashtags.
        score = int(ev.get("score_relevancia") or 0)

        text_blob = " ".join([
            str(ev.get("colectiva") or ""),
            str(ev.get("convocatoria") or ""),
            str(ev.get("descripcion") or ""),
            str(ev.get("direccion") or ""),
            str(parsed.get("title") or ""),
            str(parsed.get("text") or ""),
        ]).lower()

        # bonus por keywords (incluye hashtags)
        bonus = 0
        for k in keywords:
            kk = k.lower()
            if kk and kk in text_blob:
                bonus += 1
        score += min(8, bonus)  # cap

        ev["score_relevancia"] = score
        ev["fuente_url"] = ev.get("fuente_url") or url
        ev["cta_url"] = (ev.get("cta_url") or ev.get("convocatoria") or url).split("#")[0]  # evita links vacíos por fragments

        # fecha mínima
        f = ev.get("fecha") or ""
        try:
            if f:
                d = date.fromisoformat(f)
                if d < MIN_EVENT_DATE:
                    n_old_skip += 1
                    continue
        except Exception:
            pass

        # reglas / thresholds
        if score < THRESHOLD_EXTRACT:
            n_low_score += 1
            continue
        if not url_allowed_by_rules(rules, url, ""):
            n_rules_skip += 1
            continue

        # ciudad
        if not ev.get("ciudad"):
            c = detect_city(text_blob, cities)
            if c:
                ev["ciudad"] = c

        # geocode
        geo = geocode_event(ev, geocode_cache=geocode_cache)
        if geo and geo.get("lat") and geo.get("lon"):
            ev["lat"] = geo["lat"]
            ev["lon"] = geo["lon"]
            n_geocoded += 1

        # imagen (solo si hay url candidata)
        img_url = ev.get("imagen") or ev.get("actividad_url_imagen") or ""
        if img_url and isinstance(img_url, str) and img_url.startswith("http"):
            out = download_and_process_image(img_url, out_dir=IMAGES_DIR)
            if out and out.get("public_url"):
                ev["imagen"] = out["public_url"]  # debe ser URL publicada en Pages
                n_imgs += 1

        records.append(ev)
        n_events += 1

        if i % 100 == 0 or i == len(candidates):
            elapsed = time.time() - started
            print(f"⏳ {i}/{len(candidates)} | eventos:{n_events} | geocoded:{n_geocoded} | imgs:{n_imgs} | old_skip:{n_old_skip} | low_score:{n_low_score} | {elapsed:.1f}s")

    save_geocode_cache(GEOCODE_CACHE_PATH, geocode_cache)

    # export
    export_master_csv(EXPORT_MASTER, records)
    export_umap_csv(EXPORT_UMAP, records, min_score=THRESHOLD_EXPORT_UMAP)
    export_sin_coord_csv(EXPORT_SIN_COORD, records, min_score=THRESHOLD_EXPORT_UMAP)

    total_time = time.time() - started
    print("")
    print(f"📄 CSV master:    {EXPORT_MASTER}")
    print(f"📄 CSV uMap:      {EXPORT_UMAP}")
    print(f"📄 CSV sin coord: {EXPORT_SIN_COORD}")
    print(f"🧾 Eventos master:        {len(records)}")
    print(f"🗑️  Filtrados por fecha:  {n_old_skip}")
    print(f"🧠 Skipped low score:     {n_low_score}")
    print(f"🧱 Skipped by rules:      {n_rules_skip}")
    print(f"🖼️ imágenes descargadas:  {n_imgs}")
    print(f"⏱️  Tiempo total: {total_time:.1f}s")


if __name__ == "__main__":
    main()
