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


# =========================
# PIPELINE PROFILE
# =========================
FAST_MODE = True  # mantenelo True: estable

# =========================
# PATHS / CONFIG FILES
# =========================
BASE_SOURCES_YML = "config/sources.yml"
GENERATED_SOURCES_YML = "config/sources.generated.yml"
KEYWORDS_YML = "config/keywords.yml"

MASTER_CSV_PATH = "data/raw/convocatorias_2019_2025.csv"

EXPORT_MASTER = "data/exports/mapa_8m_global_master.csv"
EXPORT_UMAP = "data/exports/mapa_8m_global_umap.csv"

CACHE_DIR = "data/raw/html_cache"
IMAGES_DIR = "data/images"

# Para popup HTML (si usás {{archivo.jpg}} en el futuro)
PUBLIC_BASE_URL = "https://geochicas.github.io/8m-global-mapper"

# =========================
# SOURCES GENERATION
# =========================
GENERATE_SOURCES_IF_MISSING = True
REFRESH_SOURCES_EVERY_RUN = False
MAX_PRIORITY_URLS_FROM_CSV = 1200

# =========================
# RUNTIME LIMITS
# =========================
MAX_TOTAL_CANDIDATES = 400 if FAST_MODE else 2500
MAX_PRIORITY = 600 if FAST_MODE else 1200
MAX_SEEDS = 120 if FAST_MODE else 150
MAX_PAGES_PER_SEED = 30 if FAST_MODE else 60

TIMEOUT = (6, 14)  # (connect, read)
DELAY_BETWEEN_REQUESTS = 0.03 if FAST_MODE else 0.06
USER_AGENT = "geochicas-8m-global-mapper/1.0 (public observatory)"

MAX_SECONDS_PER_URL = 18 if FAST_MODE else 35

# =========================
# GEOCODING (Nominatim)
# =========================
GEOCODING_ENABLED = True  # ✅ esto es lo que te faltaba
GEOCODE_CACHE_PATH = "data/processed/geocode_cache.csv"
GEOCODE_MAX_PER_RUN = 220 if FAST_MODE else 900  # para no pasarse en Actions
GEOCODE_DELAY_SECONDS = 1.1  # rate limit respetuoso
NOMINATIM_ENDPOINT = "https://nominatim.openstreetmap.org/search"

# =========================
# UTIL
# =========================
def ensure_dirs():
    os.makedirs(os.path.dirname(EXPORT_MASTER), exist_ok=True)
    os.makedirs(os.path.dirname(GEOCODE_CACHE_PATH), exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs("config", exist_ok=True)


def load_yaml(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def file_exists(p: str) -> bool:
    return os.path.exists(p) and os.path.getsize(p) > 10


def dedupe_urls(urls: list[str]) -> list[str]:
    out, seen = [], set()
    for u in urls:
        u = (u or "").strip()
        if not u:
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def read_csv_urls(path: str) -> list[str]:
    """
    Lee URLs desde un CSV histórico.
    Busca columnas típicas: fuente_url, cta_url, convocatoria_url, actividad_url_convocatoria, url, link.
    Soporta CSV con delimitador ',' o ';'.
    """
    if not os.path.exists(path):
        return []

    candidates_cols = [
        "fuente_url",
        "cta_url",
        "convocatoria_url",
        "actividad_url_convocatoria",
        "actividad_url",
        "url",
        "link",
        "convocatoria",
    ]

    urls: list[str] = []

    def _parse(delimiter=","):
        nonlocal urls
        with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if not reader.fieldnames:
                return
            fieldnames = [c.strip() for c in reader.fieldnames]
            cols = [c for c in candidates_cols if c in fieldnames]
            if not cols:
                return
            for row in reader:
                for c in cols:
                    u = (row.get(c) or "").strip()
                    if u.startswith(("http://", "https://")):
                        urls.append(u)

    _parse(",")
    if not urls:
        _parse(";")

    return dedupe_urls(urls)


def generate_sources_from_base_and_master_csv(
    base_sources_yml: str,
    master_csv_path: str,
    out_generated_yml: str,
    max_priority: int = 1200,
):
    base = load_yaml(base_sources_yml) or {}

    seeds: list[str] = []
    if isinstance(base, dict):
        seeds = base.get("seeds") or []
    elif isinstance(base, list):
        seeds = base

    seeds = [str(s).strip() for s in seeds if str(s).strip()]
    priority_urls = read_csv_urls(master_csv_path)[:max_priority]

    payload = {"seeds": seeds, "priority_urls": priority_urls}

    os.makedirs(os.path.dirname(out_generated_yml), exist_ok=True)
    with open(out_generated_yml, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False, allow_unicode=True)


def select_sources_file() -> str:
    should_generate = (
        REFRESH_SOURCES_EVERY_RUN
        or (GENERATE_SOURCES_IF_MISSING and not file_exists(GENERATED_SOURCES_YML))
    )

    if should_generate and os.path.exists(MASTER_CSV_PATH):
        print(f"🧩 Generando {GENERATED_SOURCES_YML} desde {MASTER_CSV_PATH} + {BASE_SOURCES_YML}")
        generate_sources_from_base_and_master_csv(
            base_sources_yml=BASE_SOURCES_YML,
            master_csv_path=MASTER_CSV_PATH,
            out_generated_yml=GENERATED_SOURCES_YML,
            max_priority=MAX_PRIORITY_URLS_FROM_CSV,
        )

    if file_exists(GENERATED_SOURCES_YML):
        return GENERATED_SOURCES_YML

    return BASE_SOURCES_YML


def read_sources_from_path(path: str):
    y = load_yaml(path)
    seeds: list[str] = []
    priority: list[str] = []

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


def read_keywords_count() -> int:
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


def safe_filename_from_url(url: str) -> str:
    import hashlib
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return f"{h}.html"


def make_session():
    s = requests.Session()
    retries = Retry(
        total=1 if FAST_MODE else 2,
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
        "score_relevancia",
        "popup_html",
    ]


def umap_columns() -> list[str]:
    return [
        "colectiva",
        "convocatoria",
        "direccion",
        "fecha",
        "hora",
        "lat",
        "lon",
        "imagen",
        "cta_url",
        "popup_html",
        "fuente_url",
        "score_relevancia",
    ]


def export_csv(path: str, rows: list[dict], columns: list[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            for c in columns:
                r.setdefault(c, "")
            w.writerow(r)


def make_umap_popup_html(ev: dict, public_base_url: str = "") -> str:
    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    colectiva = (ev.get("colectiva") or "").strip()
    convocatoria_titulo = (ev.get("convocatoria") or "").strip()
    titulo = colectiva or convocatoria_titulo or "Convocatoria 8M"

    convocatoria_url = (ev.get("cta_url") or ev.get("fuente_url") or "").strip()

    direccion = (ev.get("direccion") or ev.get("localizacion_exacta") or "").strip()
    fecha = (ev.get("fecha") or "").strip()
    hora = (ev.get("hora") or "").strip()

    imagen = (ev.get("imagen") or "").strip()
    imagen_archivo = (ev.get("imagen_archivo") or "").strip()

    fecha_hora = " - ".join([x for x in [fecha, hora] if x])

    if imagen.startswith("{{") and imagen.endswith("}}") and public_base_url:
        fn = imagen.strip("{} ").strip()
        imagen = f"{public_base_url.rstrip('/')}/images/{fn}"
    elif (not imagen) and imagen_archivo and public_base_url:
        imagen = f"{public_base_url.rstrip('/')}/images/{imagen_archivo}"

    parts = []

    if convocatoria_url:
        parts.append(
            f'<h3 style="margin:0 0 8px 0;">'
            f'<a href="{convocatoria_url}" target="_blank" rel="noopener noreferrer">'
            f'{esc(titulo)}</a></h3>'
        )
    else:
        parts.append(f'<h3 style="margin:0 0 8px 0;">{esc(titulo)}</h3>')

    if direccion:
        parts.append(f'<p style="margin:0 0 10px 0;">{esc(direccion)}</p>')

    if fecha_hora:
        parts.append(f'<p style="margin:0 0 14px 0;">{esc(fecha_hora)}</p>')

    if imagen:
        parts.append(
            '<div style="margin:0 0 12px 0;">'
            f'<img src="{imagen}" style="max-width:100%; height:auto; border-radius:4px;" />'
            '</div>'
        )

    if convocatoria_url:
        parts.append(
            f'<p style="margin:0;">'
            f'<a href="{convocatoria_url}" target="_blank" rel="noopener noreferrer">'
            f'Accede a la convocatoria</a></p>'
        )

    return "\n".join(parts).strip()


# =========================
# uMap strict helpers
# =========================
def _to_float(s: str):
    s = (s or "").strip()
    if not s:
        return None
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    s = re.sub(r"\s+", "", s)
    try:
        return float(s)
    except Exception:
        return None


def _valid_latlon(lat, lon) -> bool:
    if lat is None or lon is None:
        return False
    return (-90.0 <= lat <= 90.0) and (-180.0 <= lon <= 180.0)


# =========================
# GEOCODING CACHE
# =========================
def load_geocode_cache(path: str) -> dict[str, tuple[str, str]]:
    """
    cache: query -> (lat, lon) en strings
    """
    cache: dict[str, tuple[str, str]] = {}
    if not os.path.exists(path):
        return cache
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                q = (row.get("query") or "").strip()
                lat = (row.get("lat") or "").strip()
                lon = (row.get("lon") or "").strip()
                if q and lat and lon:
                    cache[q] = (lat, lon)
    except Exception:
        return cache
    return cache


def save_geocode_cache(path: str, cache: dict[str, tuple[str, str]]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["query", "lat", "lon"])
        w.writeheader()
        for q, (lat, lon) in cache.items():
            w.writerow({"query": q, "lat": lat, "lon": lon})


def build_geocode_query(ev: dict) -> str:
    """
    Construye una query razonable con lo que haya:
    - direccion / localizacion_exacta
    - ciudad
    - pais (código o nombre)
    """
    direccion = normalize(ev.get("direccion", "")) or normalize(ev.get("localizacion_exacta", ""))
    ciudad = normalize(ev.get("ciudad", ""))
    pais = normalize(ev.get("pais", ""))

    parts = []
    if direccion:
        parts.append(direccion)
    if ciudad and ciudad.lower() not in (direccion or "").lower():
        parts.append(ciudad)
    if pais:
        parts.append(pais)

    q = ", ".join([p for p in parts if p])
    return q.strip()


def geocode_nominatim(session: requests.Session, query: str) -> tuple[str, str] | None:
    if not query:
        return None

    params = {
        "q": query,
        "format": "json",
        "limit": 1,
    }
    try:
        r = session.get(
            NOMINATIM_ENDPOINT,
            params=params,
            timeout=(6, 14),
            headers={"User-Agent": USER_AGENT},
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not data:
            return None
        lat = str(data[0].get("lat", "")).strip()
        lon = str(data[0].get("lon", "")).strip()
        if lat and lon:
            return lat, lon
    except Exception:
        return None
    return None


# =========================
# MAIN
# =========================
def main():
    ensure_dirs()
    session = make_session()

    sources_path = select_sources_file()
    seeds, priority = read_sources_from_path(sources_path)
    kw_count = read_keywords_count()

    print(f"📌 Sources: {sources_path}")
    print(f"🌐 Seeds: {min(len(seeds), MAX_SEEDS)}")
    print(f"🎯 Priority URLs: {min(len(priority), MAX_PRIORITY)}")
    print(f"🔎 Keywords: {kw_count}")
    print(f"⚡ FAST_MODE: {FAST_MODE}")
    print(f"🗺️  Geocoding: {GEOCODING_ENABLED} | max/run={GEOCODE_MAX_PER_RUN}")

    candidates: list[str] = []
    seen = set()

    # priority primero
    for u in priority[:MAX_PRIORITY]:
        if u not in seen:
            seen.add(u)
            candidates.append(u)
        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break

    # seed crawl (mismo dominio)
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

        if picked:
            print(f"🔗 {seed} -> candidatos (seed crawl): {picked}")

    print(f"🔎 Candidates total: {len(candidates)}")

    records: list[dict] = []
    n_fetch_ok = 0
    n_events = 0
    started = time.time()

    # geocode cache
    geocode_cache = load_geocode_cache(GEOCODE_CACHE_PATH)
    geocoded_now = 0

    for i, url in enumerate(candidates, start=1):
        t0 = time.time()

        if i % 25 == 0:
            elapsed = time.time() - started
            print(f"⏳ {i}/{len(candidates)} | eventos: {n_events} | fetch_ok:{n_fetch_ok} | geocoded:{geocoded_now} | {elapsed:.1f}s")

        html = fetch_url(session, url, use_cache=True)
        if html is None:
            continue
        n_fetch_ok += 1

        if (time.time() - t0) > MAX_SECONDS_PER_URL:
            continue

        parsed = parse_page(url, html)

        try:
            ev = extract_event_fields(parsed)
        except Exception:
            ev = None

        if not ev:
            continue

        ev["fuente_url"] = url
        ev["fuente_tipo"] = "web"
        ev["confianza_extraccion"] = ev.get("confianza_extraccion") or "media"
        ev["imagen_archivo"] = ev.get("imagen_archivo", "")

        # =========================
        # GEOCODING (si falta lat/lon)
        # =========================
        if GEOCODING_ENABLED and geocoded_now < GEOCODE_MAX_PER_RUN:
            lat0 = _to_float(str(ev.get("lat", "")))
            lon0 = _to_float(str(ev.get("lon", "")))

            if not _valid_latlon(lat0, lon0):
                q = build_geocode_query(ev)
                if q:
                    if q in geocode_cache:
                        ev["lat"], ev["lon"] = geocode_cache[q]
                    else:
                        res = geocode_nominatim(session, q)
                        if res:
                            lat_s, lon_s = res
                            # validar
                            lat1 = _to_float(lat_s)
                            lon1 = _to_float(lon_s)
                            if _valid_latlon(lat1, lon1):
                                ev["lat"] = lat_s
                                ev["lon"] = lon_s
                                geocode_cache[q] = (lat_s, lon_s)
                                geocoded_now += 1
                        time.sleep(GEOCODE_DELAY_SECONDS)

        # popup HTML
        ev["popup_html"] = make_umap_popup_html(ev, public_base_url=PUBLIC_BASE_URL)

        records.append(ev)
        n_events += 1

    # Guardar cache al final (muy importante para Actions)
    if GEOCODING_ENABLED:
        save_geocode_cache(GEOCODE_CACHE_PATH, geocode_cache)
        print(f"🧠 Geocode cache guardado: {GEOCODE_CACHE_PATH} | entradas: {len(geocode_cache)}")

    # Export master (con lo que se pudo geocodificar)
    export_csv(EXPORT_MASTER, records, master_columns())

    # Export uMap STRICT (solo lat/lon válidos)
    umap_rows = []
    invalid = 0
    for r in records:
        lat = _to_float(str(r.get("lat", "")))
        lon = _to_float(str(r.get("lon", "")))

        if not _valid_latlon(lat, lon):
            invalid += 1
            continue

        r2 = dict(r)
        r2["lat"] = f"{lat:.6f}"
        r2["lon"] = f"{lon:.6f}"
        umap_rows.append(r2)

    print(f"🧹 uMap strict: {len(umap_rows)} filas OK | {invalid} descartadas por lat/lon inválidos")
    export_csv(EXPORT_UMAP, umap_rows, umap_columns())

    elapsed_total = time.time() - started
    print(f"\n📄 CSV master: {EXPORT_MASTER}")
    print(f"📄 CSV uMap:   {EXPORT_UMAP}")
    print(f"🧾 Eventos exportados (master): {len(records)}")
    print(f"🧾 Eventos exportados (uMap):   {len(umap_rows)}")
    print(f"⏱️  Tiempo total: {elapsed_total:.1f}s")


if __name__ == "__main__":
    main()
