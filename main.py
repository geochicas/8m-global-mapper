import csv
import os
import re
import time
from urllib.parse import urljoin, urlparse

import requests
import yaml

from src.parse.html_parser import parse_page
from src.extract.extractor_ai import extract_event_fields
from src.geocode.geocoder import Geocoder
from src.media.image_processor import download_and_convert_to_jpg


# =========================
# CONFIG
# =========================
SOURCES_YML = "config/sources.yml"
KEYWORDS_YML = "config/keywords.yml"

EXPORT_MASTER = "data/exports/mapa_8m_global_master.csv"

CACHE_DIR = "data/raw/html_cache"
IMAGES_DIR = "data/images"

MAX_SEEDS = 50
MAX_PAGES_PER_SEED = 80
MAX_TOTAL_CANDIDATES = 2000

FETCH_TIMEOUT = 20
DELAY_BETWEEN_REQUESTS = 0.2

USER_AGENT = "geochicas-8m-global-mapper/1.0 (public observatory)"


# =========================
# UTIL
# =========================
def ensure_dirs():
    os.makedirs(os.path.dirname(EXPORT_MASTER), exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)


def load_yaml(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def normalize(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def safe_filename_from_url(url: str) -> str:
    import hashlib
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return f"{h}.html"


def read_keywords() -> list[str]:
    """
    Acepta keywords.yml en varios formatos comunes:
      - lista directa: ["8M", "8 de marzo"]
      - dict con keys conocidas: {keywords:[...]} o {terms:[...]}
      - dict con listas por idioma: {es:[...], en:[...]}
      - dict con cualquier lista (fallback)
    """
    y = load_yaml(KEYWORDS_YML)

    # Caso 1: lista directa
    if isinstance(y, list):
        return [str(x).strip() for x in y if str(x).strip()]

    # Caso 2: dict
    if isinstance(y, dict):
        # prioridades conocidas
        for k in ["keywords", "terms", "list", "palabras_clave", "tags"]:
            if k in y and isinstance(y[k], list):
                return [str(x).strip() for x in y[k] if str(x).strip()]

        # si hay listas por idioma o por categoría, juntarlas
        collected = []
        for _, v in y.items():
            if isinstance(v, list):
                collected.extend([str(x).strip() for x in v if str(x).strip()])
        # de-dupe
        out = []
        seen = set()
        for k in collected:
            if k.lower() not in seen:
                seen.add(k.lower())
                out.append(k)
        return out

    return []


def read_seeds() -> list[str]:
    """
    Acepta sources.yml en formatos:
      - {seeds:[...]}
      - {sources:[ {name:..., url:...}, ... ]} o {sources:[...strings...]}
      - lista directa
    """
    y = load_yaml(SOURCES_YML)
    seeds = []

    if isinstance(y, dict):
        if "seeds" in y and isinstance(y["seeds"], list):
            seeds = y["seeds"]
        elif "sources" in y and isinstance(y["sources"], list):
            for it in y["sources"]:
                if isinstance(it, str):
                    seeds.append(it)
                elif isinstance(it, dict) and it.get("url"):
                    seeds.append(it["url"])
    elif isinstance(y, list):
        seeds = y

    seeds = [str(s).strip() for s in seeds if str(s).strip()]

    out = []
    seen = set()
    for s in seeds:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def fetch_url(url: str, use_cache: bool = True) -> str | None:
    cache_path = os.path.join(CACHE_DIR, safe_filename_from_url(url))

    if use_cache and os.path.exists(cache_path) and os.path.getsize(cache_path) > 50:
        try:
            with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception:
            pass

    try:
        r = requests.get(
            url,
            timeout=FETCH_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            allow_redirects=True,
        )
        r.raise_for_status()
        html = r.text
    except Exception:
        return None

    if use_cache:
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

    time.sleep(DELAY_BETWEEN_REQUESTS)
    return html


def extract_links(base_url: str, html: str) -> list[str]:
    links = set()
    for m in re.finditer(r'href=["\'](.*?)["\']', html, flags=re.IGNORECASE):
        href = (m.group(1) or "").strip()
        if not href:
            continue
        if href.startswith("#"):
            continue
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        full = urljoin(base_url, href)
        if full.startswith(("http://", "https://")):
            links.add(full)
    return list(links)


def same_domain(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc == urlparse(b).netloc
    except Exception:
        return False


def looks_relevant(url: str, keywords: list[str]) -> bool:
    if not keywords:
        return True  # si no hay keywords, no filtrar por URL
    u = url.lower()
    return any(k.lower() in u for k in keywords)


def build_geocode_query(ev: dict) -> str:
    parts = []
    for key in ["direccion", "localizacion_exacta", "ciudad", "pais"]:
        v = normalize(ev.get(key, ""))
        if v:
            parts.append(v)
    cleaned = []
    for p in parts:
        if not cleaned or cleaned[-1].lower() != p.lower():
            cleaned.append(p)
    return ", ".join(cleaned)


def master_columns() -> list[str]:
    return [
        "colectiva",
        "convocatoria",
        "descripcion",
        "fecha",
        "hora",
        "pais",
        "ciudad",
        "localizacion_exacta",
        "direccion",
        "lat",
        "lon",
        "imagen",              # {{hash.jpg}} después de descargar
        "imagen_archivo",      # hash.jpg (trazabilidad)
        "cta_url",
        "sitio_web_colectiva",
        "trans_incluyente",
        "fuente_url",
        "fuente_tipo",
        "confianza_extraccion",
        "precision_ubicacion",
    ]


# =========================
# MAIN
# =========================
def main():
    ensure_dirs()

    seeds = read_seeds()[:MAX_SEEDS]
    keywords = read_keywords()

    print(f"🌐 Seeds: {len(seeds)}")
    print(f"🔎 Keywords: {len(keywords)}")

    candidates = []
    seen = set()

    for seed in seeds:
        html = fetch_url(seed, use_cache=True)
        if not html:
            print(f"[WARN] Seed sin respuesta: {seed}")
            continue

        links = extract_links(seed, html)

        picked = []
        for link in links:
            if link in seen:
                continue
            if not same_domain(seed, link):
                continue
            if not looks_relevant(link, keywords):
                continue
            seen.add(link)
            picked.append(link)
            if len(picked) >= MAX_PAGES_PER_SEED:
                break

        print(f"🔗 {seed} -> candidatos: {len(picked)}")
        candidates.extend(picked)

        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break

    geocoder = Geocoder()
    records = []

    for i, url in enumerate(candidates, start=1):
        html = fetch_url(url, use_cache=True)
        if not html:
            continue

        parsed = parse_page(url, html)
        ev = extract_event_fields(parsed)
        if not ev:
            continue

        ev["fuente_url"] = url
        ev["fuente_tipo"] = "web"
        ev["confianza_extraccion"] = ev.get("confianza_extraccion") or "media"

        # Imagen: bajar+convertir y guardar como {{hash.jpg}}
        img_url = normalize(ev.get("imagen", ""))
        if img_url and not (img_url.startswith("{{") and img_url.endswith("}}")):
            res = download_and_convert_to_jpg(img_url, out_dir=IMAGES_DIR)
            if res:
                filename, template = res
                ev["imagen"] = template
                ev["imagen_archivo"] = filename
            else:
                ev["imagen_archivo"] = ""
        else:
            ev["imagen_archivo"] = ""

        # Geocode si falta lat/lon
        lat = normalize(ev.get("lat", ""))
        lon = normalize(ev.get("lon", ""))
        if not lat or not lon:
            q = build_geocode_query(ev)
            if q:
                geo = geocoder.geocode(q, country_code=(ev.get("pais_iso2") or "").strip())
                if geo:
                    ev["lat"] = geo.lat
                    ev["lon"] = geo.lon
                    ev["precision_ubicacion"] = geo.precision

        records.append(ev)

        if i % 50 == 0:
            print(f"✅ procesadas {i}/{len(candidates)} | eventos: {len(records)}")

    geocoder.close()

    cols = master_columns()
    with open(EXPORT_MASTER, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in records:
            for c in cols:
                if c not in r:
                    r[c] = ""
            w.writerow(r)

    print(f"\n📄 CSV master: {EXPORT_MASTER}")
    print(f"🧾 Eventos exportados: {len(records)}")


if __name__ == "__main__":
    main()
