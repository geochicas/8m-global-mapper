# src/collect/web_fetch.py
# Networking + cache HTML
# Compat con main.py:
#   - make_session(timeout=...)
#   - fetch_url(session, url, use_cache=True) -> str

from __future__ import annotations

import os
import time
import hashlib
from typing import Optional

import requests


CACHE_DIR = "data/cache/html"

DEFAULT_HEADERS = {
    "User-Agent": os.environ.get(
        "USER_AGENT",
        "Mozilla/5.0 (compatible; 8M-Mapper/0.1; +https://github.com/geochicas/atlas-8m-dashboard)",
    )
}


def make_session(timeout: int = 20) -> requests.Session:
    """
    Crea una requests.Session con headers y timeout por defecto.
    main.py la llama como make_session(timeout=REQUEST_TIMEOUT)
    """
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    # guardamos timeout en el objeto para que fetch_url lo use si no le pasan otro
    setattr(s, "request_timeout", int(timeout) if timeout else 20)
    return s


def _cache_path_for_url(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8", errors="ignore")).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.html")


def fetch_url(
    session: Optional[requests.Session],
    url: str,
    use_cache: bool = True,
    timeout: Optional[int] = None,
) -> str:
    """
    Descarga HTML. Si use_cache=True, lee/escribe en data/cache/html.
    Devuelve "" si falla (para que el pipeline siga).
    """
    url = (url or "").strip()
    if not url:
        return ""

    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = _cache_path_for_url(url)

    if use_cache and os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        try:
            with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception:
            pass

    s = session or requests.Session()
    # si no es Session creada por make_session, aseguramos headers
    if not getattr(s, "headers", None):
        s.headers = DEFAULT_HEADERS.copy()
    else:
        # no pisamos headers existentes, solo ponemos UA si falta
        if "User-Agent" not in s.headers:
            s.headers.update(DEFAULT_HEADERS)

    req_timeout = timeout
    if req_timeout is None:
        req_timeout = int(getattr(s, "request_timeout", 20))

    try:
        r = s.get(url, timeout=req_timeout, allow_redirects=True)
        r.raise_for_status()
        html = r.text or ""
    except Exception:
        return ""

    if use_cache and html:
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

    return html


# Backwards compat (por si algo viejo aún llama fetch_page)
def fetch_page(url: str, timeout: int = 20) -> str:
    return fetch_url(None, url, use_cache=False, timeout=timeout)
