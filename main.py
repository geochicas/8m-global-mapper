import csv
import os
import re
import time
from urllib.parse import urljoin, urlparse

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.parse.html_parser import parse_page
from src.extract.extractor_ai import extract_event_fields
from src.geocode.geocoder import Geocoder
from src.media.image_processor import download_and_convert_to_jpg


SOURCES_YML = "config/sources.generated.yml" if os.path.exists("config/sources.generated.yml") else "config/sources.yml"
KEYWORDS_YML = "config/keywords.yml"

EXPORT_MASTER = "data/exports/mapa_8m_global_master.csv"

CACHE_DIR = "data/raw/html_cache"
IMAGES_DIR = "data/images"

MAX_TOTAL_CANDIDATES = 1200
MAX_PRIORITY = 600
MAX_SEEDS = 150
MAX_PAGES_PER_SEED = 30

TIMEOUT = (5, 12)  # (connect, read)
DELAY_BETWEEN_REQUESTS = 0.05
USER_AGENT = "geochicas-8m-global-mapper/1.0 (public observatory)"


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


def make_session():
    s = requests.Session()
    retries = Retry(
        total=1,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def is_html_content_type(ct: str) -> bool:
    ct = (ct or "").lower()
    return ("text/html" in ct) or ("application/xhtml" in ct) or ct == ""


def fetch_url(session, url: str, use_cache: bool = True) -> str | None:
    cache_path = os.path.join(CACHE_DIR, safe_filename_from_url(url))

    if use_cache and os.path.exists(cache_path) and os.path.getsize(cache_path) > 50:
        try:
            with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception:
            pass

    try:
        r = session.get(
            url,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            allow_redirects=True,
        )
    except Exception:
        return None

    ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
    if not is_html_content_type(ct):
        return None

    html = r.text or ""
    if not html:
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


def read_sources():
    y = load_yaml(SOURCES_YML)
    seeds = []
    priority = []
    if isinstance(y, dict):
        seeds = y.get("seeds") or []
        priority = y.get("priority_urls") or []
    elif isinstance(y, list):
        seeds = y

    def dedupe(lst):
        out, seen = [], set()
        for s in lst:
            s = str(s).strip()
            if not s:
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out

    return dedupe(seeds), dedupe(priority)


def read_keywords_count():
    y = load_yaml(KEYWORDS_YML)
    if isinstance(y, list):
        return len([x for x in y if str(x).strip()])
    if isinstance(y, dict):
        total = 0
        for v in y.values():
            if isinstance(v, list):
                total += len([x for x in v if str(x).strip()])
        return total
    return 0


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
        "imagen",
        "imagen_archivo",
        "cta_url",
        "sitio_web_colectiva",
        "trans_incluyente",
        "fuente_url",
        "fuente_tipo",
        "confianza_extraccion",
        "precision_ubicacion",
    ]


def main():
    ensure_dirs()
    session = make_session()

    seeds, priority = read_sources()
    kw_count = read_keywords_count()

    print(f"🌐 Seeds: {min(len(seeds), MAX_SEEDS)}")
    print(f"🔎 Keywords: {kw_count}")
    print(f"🎯 Priority URLs: {min(len(priority), MAX_PRIORITY)}")

    candidates = []
    seen = set()

    for u in priority[:MAX_PRIORITY]:
        if u not in seen:
            seen.add(u)
            candidates.append(u)
        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break

    for seed in seeds[:MAX_SEEDS]:
        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break

        html = fetch_url(session, seed, use_cache=True)
        if not html:
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
            seen.add(link)
            candidates.append(link)
            picked += 1
            if picked >= MAX_PAGES_PER_SEED:
                break

        print(f"🔗 {seed} -> candidatos (seed crawl): {picked}")

    print(f"🔎 Candidates total: {len(candidates)}")

    geocoder = Geocoder()
    records = []

    for i, url in enumerate(candidates, start=1):
        html = fetch_url(session, url, use_cache=True)
        if not html:
            continue

        parsed = parse_page(url, html)

        # ✅ anti-crash: jamás tumbar el pipeline por una página
        try:
            ev = extract_event_fields(parsed)
        except Exception:
            ev = None

        if not ev:
            continue

        ev["fuente_url"] = url
        ev["fuente_tipo"] = "web"
        ev["confianza_extraccion"] = ev.get("confianza_extraccion") or "media"

        img_url = normalize(ev.get("imagen", ""))
        if img_url and not (img_url.startswith("{{") and img_url.endswith("}}")):
            res = download_and_convert_to_jpg(img_url, out_dir=IMAGES_DIR)
            if res:
                filename, template = res
                ev["imagen"] = template
                ev["imagen_archivo"] = filename
            else:
                ev["imagen_archivo"] = ""

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

        if i % 25 == 0:
            print(f"✅ procesadas {i}/{len(candidates)} | eventos: {len(records)}")

    geocoder.close()

    cols = master_columns()
    with open(EXPORT_MASTER, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in records:
            for c in cols:
                r.setdefault(c, "")
            w.writerow(r)

    print(f"\n📄 CSV master: {EXPORT_MASTER}")
    print(f"🧾 Eventos exportados: {len(records)}")


if __name__ == "__main__":
    main()
