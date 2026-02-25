# ==========================================
# PIPELINE PROFILE
# ==========================================
FAST_MODE = True  # Mantener en True para estabilidad diaria

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


# =========================
# CONFIG
# =========================
SOURCES_YML = "config/sources.generated.yml" if os.path.exists("config/sources.generated.yml") else "config/sources.yml"
KEYWORDS_YML = "config/keywords.yml"

EXPORT_MASTER = "data/exports/mapa_8m_global_master.csv"
EXPORT_UMAP = "data/exports/mapa_8m_global_umap.csv"

CACHE_DIR = "data/raw/html_cache"
IMAGES_DIR = "data/images"

# ⚡ Modo rápido para iterar sin quedarse pegado
FAST_MODE = True

# Cobertura (FAST_MODE reduce automáticamente)
MAX_TOTAL_CANDIDATES = 400 if FAST_MODE else 2500
MAX_PRIORITY = 400 if FAST_MODE else 1200
MAX_SEEDS = 80 if FAST_MODE else 150
MAX_PAGES_PER_SEED = 30 if FAST_MODE else 60

# Networking
TIMEOUT = (5, 12)  # (connect, read)
DELAY_BETWEEN_REQUESTS = 0.03 if FAST_MODE else 0.06
USER_AGENT = "geochicas-8m-global-mapper/1.0 (public observatory)"

# Public base para GitHub Pages (ajustá si el repo cambia)
PUBLIC_BASE_URL = "https://geochicas.github.io/8m-global-mapper"

# Watchdog: si una URL tarda demasiado “en total”, la saltamos
MAX_SECONDS_PER_URL = 18 if FAST_MODE else 35


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

    # Si imagen = {{archivo.jpg}} => URL pública en Pages
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
# MAIN
# =========================
def main():
    ensure_dirs()
    session = make_session()

    seeds, priority = read_sources()
    kw_count = read_keywords_count()

    print(f"🌐 Seeds: {min(len(seeds), MAX_SEEDS)}")
    print(f"🔎 Keywords: {kw_count}")
    print(f"🎯 Priority URLs: {min(len(priority), MAX_PRIORITY)}")
    print(f"⚡ FAST_MODE: {FAST_MODE}")

    candidates = []
    seen = set()

    # 1) priority primero
    for u in priority[:MAX_PRIORITY]:
        if u not in seen:
            seen.add(u)
            candidates.append(u)
        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break

    # 2) seed crawl
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

    # Geocoder solo si no estamos en FAST_MODE
    geocoder = None if FAST_MODE else Geocoder()

    records = []
    # métricas para ver “dónde se va el tiempo”
    n_fetch_ok = 0
    n_html_ok = 0
    n_events = 0
    n_img_ok = 0
    n_geocode_ok = 0
    started = time.time()

    for i, url in enumerate(candidates, start=1):
        t0 = time.time()

        if i % 25 == 0:
            elapsed = time.time() - started
            print(f"⏳ procesando {i}/{len(candidates)} | eventos: {n_events} | fetch_ok:{n_fetch_ok} img_ok:{n_img_ok} geo_ok:{n_geocode_ok} | {elapsed:.1f}s")

        html = fetch_url(session, url, use_cache=True)
        if html is None:
            continue
        n_fetch_ok += 1

        # watchdog: si solo el fetch ya tardó demasiado, salta pronto
        if (time.time() - t0) > MAX_SECONDS_PER_URL:
            continue

        parsed = parse_page(url, html)
        n_html_ok += 1

        # extractor: nunca tumbar el pipeline
        try:
            ev = extract_event_fields(parsed)
        except Exception:
            ev = None

        if not ev:
            continue

        # completa campos de trazabilidad
        ev["fuente_url"] = url
        ev["fuente_tipo"] = "web"
        ev["confianza_extraccion"] = ev.get("confianza_extraccion") or "media"

        # imagen: opcional (FAST_MODE lo apaga)
        if not FAST_MODE:
            img_url = normalize(ev.get("imagen", ""))
            if img_url and not (img_url.startswith("{{") and img_url.endswith("}}")):
                res = download_and_convert_to_jpg(img_url, out_dir=IMAGES_DIR)
                if res:
                    filename, template = res
                    ev["imagen"] = template
                    ev["imagen_archivo"] = filename
                    n_img_ok += 1
                else:
                    ev["imagen_archivo"] = ""
        else:
            # en modo rápido, no baja imágenes
            ev["imagen_archivo"] = ev.get("imagen_archivo", "")

        # geocode: opcional (FAST_MODE lo apaga)
        if (not FAST_MODE) and geocoder:
            lat = normalize(ev.get("lat", ""))
            lon = normalize(ev.get("lon", ""))
            if (not lat or not lon) and (ev.get("ciudad") or ev.get("direccion") or ev.get("localizacion_exacta")):
                q = build_geocode_query(ev)
                if q:
                    try:
                        geo = geocoder.geocode(q, country_code=(ev.get("pais_iso2") or "").strip())
                    except Exception:
                        geo = None
                    if geo:
                        ev["lat"] = geo.lat
                        ev["lon"] = geo.lon
                        ev["precision_ubicacion"] = geo.precision
                        n_geocode_ok += 1

        # popup HTML para uMap (usa cta_url como “Accede a la convocatoria”)
        ev["popup_html"] = make_umap_popup_html(ev, public_base_url=PUBLIC_BASE_URL)

        records.append(ev)
        n_events += 1

        # watchdog general: si esta URL se fue demasiado, cortar pronto
        if (time.time() - t0) > MAX_SECONDS_PER_URL:
            continue

    if geocoder:
        geocoder.close()

    # Export master
    export_csv(EXPORT_MASTER, records, master_columns())

    # Export umap (más “clean” si filtrás por score)
    umap_rows = records  # o: [r for r in records if int(r.get("score_relevancia", 0) or 0) >= 9]
    export_csv(EXPORT_UMAP, umap_rows, umap_columns())

    elapsed_total = time.time() - started
    print(f"\n📄 CSV master: {EXPORT_MASTER}")
    print(f"📄 CSV uMap:   {EXPORT_UMAP}")
    print(f"🧾 Eventos exportados: {len(records)}")
    print(f"⏱️  Tiempo total: {elapsed_total:.1f}s")


if __name__ == "__main__":
    main()
