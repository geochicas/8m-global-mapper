# src/collect/discover_links.py
# Compat con main.py:
#   from src.collect.discover_links import extract_links, same_domain
#
# Responsabilidad:
# - extraer links de una página (a href) y normalizarlos
# - decidir si dos URLs son del mismo dominio (netloc)

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse, urldefrag


_SKIP_SCHEMES = ("mailto:", "tel:", "javascript:", "data:")
_RE_HREF = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)


def _norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    u, _ = urldefrag(u)  # quita #fragment
    return u.strip()


def same_domain(seed_url: str, candidate_url: str) -> bool:
    try:
        a = urlparse(seed_url)
        b = urlparse(candidate_url)
        if not a.netloc or not b.netloc:
            return False
        # normaliza www.
        an = a.netloc.lower().lstrip("www.")
        bn = b.netloc.lower().lstrip("www.")
        return an == bn
    except Exception:
        return False


def extract_links(base_url: str, html: str) -> list[str]:
    """
    Extrae links del HTML y devuelve una lista de URLs absolutas normalizadas.
    - Solo toma <a href="...">
    - Convierte relativos con urljoin(base_url, href)
    - Filtra esquemas no-web (mailto/tel/js/data)
    """
    base_url = (base_url or "").strip()
    html = html or ""
    if not base_url or not html:
        return []

    found: list[str] = []

    # 1) intento simple con regex (rápido, sin deps)
    for m in _RE_HREF.finditer(html):
        href = (m.group(1) or "").strip()
        if not href:
            continue
        low = href.lower()
        if low.startswith(_SKIP_SCHEMES):
            continue
        if low.startswith("#"):
            continue

        abs_u = urljoin(base_url, href)
        abs_u = _norm_url(abs_u)
        if abs_u.startswith("http://") or abs_u.startswith("https://"):
            found.append(abs_u)

    # dedupe preservando orden
    seen = set()
    out: list[str] = []
    for u in found:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    return out
