# src/collect/discover_links.py
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; 8M-Mapper/0.2; +https://github.com/Geochicas/8m-global-mapper)"
}

def normalize_url(base_url, href):
    if not href:
        return None
    full = urljoin(base_url, href)
    parsed = urlparse(full)

    if parsed.scheme not in ("http", "https"):
        return None

    # quitar fragmentos (#...)
    clean = parsed._replace(fragment="").geturl()
    return clean

def same_domain(url_a, url_b):
    return urlparse(url_a).netloc.replace("www.", "") == urlparse(url_b).netloc.replace("www.", "")

def fetch_html(url, timeout=20):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def discover_candidate_links(seed_url, keywords, max_links=40):
    """
    Busca links internos potencialmente relevantes a partir de una URL semilla.
    Filtra por keywords en URL o texto del enlace.
    """
    html = fetch_html(seed_url)
    soup = BeautifulSoup(html, "lxml")

    found = []
    seen = set()

    for a in soup.find_all("a"):
        href = a.get("href")
        text = a.get_text(" ", strip=True) or ""
        full = normalize_url(seed_url, href)
        if not full:
            continue

        # solo links del mismo dominio
        if not same_domain(seed_url, full):
            continue

        # evita cosas poco útiles
        if any(x in full.lower() for x in ["/tag/", "/category/", "/author/", "/wp-content/", ".jpg", ".png", ".pdf"]):
            continue

        haystack = (full + " " + text).lower()
        if keywords and not any(k.lower() in haystack for k in keywords):
            continue

        if full not in seen:
            seen.add(full)
            found.append(full)

        if len(found) >= max_links:
            break

    return found
