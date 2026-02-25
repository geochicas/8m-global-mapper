import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.parse.html_parser import parse_page
from src.extract.extractor_ai import TRIGGERS, extract_event_fields

SOURCES_YML = "config/sources.generated.yml"

MAX_URLS = 50           # subilo a 100 cuando esté estable
TIMEOUT = (5, 10)       # (connect, read) en segundos
UA = "geochicas-8m-global-mapper/1.0 debug-fast"

def make_session():
    s = requests.Session()
    retries = Retry(
        total=1,              # 1 reintento máximo
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def load_priority_urls():
    with open(SOURCES_YML, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f) or {}
    urls = y.get("priority_urls") or []
    # de-dupe manteniendo orden
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= MAX_URLS:
            break
    return out

def is_html_content_type(ct: str) -> bool:
    ct = (ct or "").lower()
    return ("text/html" in ct) or ("application/xhtml" in ct) or ct == ""

def main():
    urls = load_priority_urls()
    print(f"🔎 Debug fast: probando {len(urls)} URLs\n")

    s = make_session()

    ok_fetch = 0
    ok_html = 0
    trig_hits = 0
    ev_hits = 0

    for i, url in enumerate(urls, 1):
        try:
            r = s.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT, allow_redirects=True)
            ok_fetch += 1
        except Exception as e:
            print(f"[{i:02d}] ❌ timeout/error | {url}")
            continue

        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        status = r.status_code

        if not is_html_content_type(ct):
            print(f"[{i:02d}] ⚠️ skip non-html ({ct}) status={status} | {url}")
            continue

        html = (r.text or "")
        ok_html += 1

        parsed = parse_page(url, html)
        blob = ((parsed.get("title") or "") + "\n" + (parsed.get("text") or "")).lower()

        trig = any(t in blob for t in TRIGGERS)
        if trig:
            trig_hits += 1

        ev = extract_event_fields(parsed)
        if ev:
            ev_hits += 1

        print(f"[{i:02d}] status={status} html={len(html)} trig={trig} ev={bool(ev)} | {parsed.get('title','')[:90]}")

    print("\n====== RESUMEN DEBUG FAST ======")
    print("Fetch OK:", ok_fetch)
    print("HTML OK:", ok_html)
    print("TRIGGERS match:", trig_hits)
    print("Eventos extraídos:", ev_hits)
    print("================================\n")

if __name__ == "__main__":
    main()
