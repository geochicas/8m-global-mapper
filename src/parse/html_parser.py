from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from urllib.parse import urljoin
import json
import re
import warnings


def _get_meta(soup, key, attr="property"):
    tag = soup.find("meta", attrs={attr: key})
    if tag and tag.get("content"):
        return tag["content"].strip()
    return ""


def _extract_jsonld_events(soup):
    events = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if not isinstance(item, dict):
                continue

            graph = item.get("@graph")
            if isinstance(graph, list):
                candidates.extend([x for x in graph if isinstance(x, dict)])

            t = item.get("@type") or ""
            if isinstance(t, list):
                t = ",".join(t)

            if "Event" in str(t):
                events.append(
                    {
                        "startDate": item.get("startDate", "") or "",
                        "endDate": item.get("endDate", "") or "",
                        "name": item.get("name", "") or "",
                        "image": item.get("image", "") or "",
                    }
                )
    return events


def _looks_like_logo(url: str) -> bool:
    u = (url or "").lower()
    bad = [
        "logo", "brand", "icon", "favicon", "sprite",
        "footer", "header", "navbar", "nav", "menu",
        "badge", "avatar", "profile", "placeholder",
        "tracking", "pixel", "spacer",
    ]
    return any(b in u for b in bad)


def _score_image(url: str) -> int:
    """Score simple por heurística de URL (sin descargar)."""
    u = (url or "").lower()
    score = 0

    # Muy bueno: imágenes destacadas típicas
    good = [
        "og:image", "twitter", "featured", "feature", "hero", "banner",
        "cover", "post", "article", "media", "uploads", "wp-content",
        "flyer", "afiche", "cartel", "poster", "convoc", "8m", "marzo"
    ]
    if any(g in u for g in good):
        score += 6

    # Malo: logos/icons
    if _looks_like_logo(u):
        score -= 8

    # Penalizar svgs (muchos logos)
    if u.endswith(".svg"):
        score -= 4

    # Premiar jpg/png sobre webp/avif (uMap friendly)
    if u.endswith(".jpg") or u.endswith(".jpeg") or u.endswith(".png"):
        score += 2
    if u.endswith(".webp") or u.endswith(".avif"):
        score -= 1

    return score


def parse_page(url, html):
    # Evitar warning cuando el HTML en realidad es RSS/XML
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

    txt = (html or "").lstrip()
    is_xml = txt.startswith("<?xml") or "<rss" in txt[:500].lower() or "<feed" in txt[:500].lower()

    if is_xml:
        soup = BeautifulSoup(html, "xml")
    else:
        soup = BeautifulSoup(html, "lxml")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Texto: paragraphs + headings (a veces el contenido está en h1/h2)
    texts = []
    for tag in soup.find_all(["h1", "h2", "h3", "p", "li"]):
        t = tag.get_text(" ", strip=True)
        if t and len(t) >= 20:
            texts.append(t)
    text = "\n".join(texts)

    meta = {
        "og_title": _get_meta(soup, "og:title"),
        "og_description": _get_meta(soup, "og:description"),
        "og_image": _get_meta(soup, "og:image"),
        "twitter_image": _get_meta(soup, "twitter:image", attr="name"),
        "article_published_time": _get_meta(soup, "article:published_time"),
        "article_modified_time": _get_meta(soup, "article:modified_time"),
        "og_updated_time": _get_meta(soup, "og:updated_time"),
        "twitter_title": _get_meta(soup, "twitter:title", attr="name"),
        "twitter_description": _get_meta(soup, "twitter:description", attr="name"),
    }

    jsonld_events = _extract_jsonld_events(soup)

    # 1) candidatos “destacados” primero (meta + JSON-LD image)
    candidates = []

    for k in ["og_image", "twitter_image"]:
        if meta.get(k):
            candidates.append(urljoin(url, meta[k]))

    for ev in jsonld_events:
        img = ev.get("image")
        if isinstance(img, str) and img.strip():
            candidates.append(urljoin(url, img.strip()))
        elif isinstance(img, list) and img:
            candidates.append(urljoin(url, str(img[0]).strip()))

    # 2) luego imágenes en el body
    for img in soup.find_all("img"):
        src = img.get("src") or ""
        if not src:
            continue
        if src.startswith("data:"):
            continue
        full = urljoin(url, src)
        candidates.append(full)

    # de-dupe manteniendo orden
    seen = set()
    uniq = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            uniq.append(c)

    # 3) filtrar logos “obvios” y ordenar por score
    filtered = []
    for u in uniq:
        if _looks_like_logo(u):
            continue
        filtered.append(u)

    filtered.sort(key=_score_image, reverse=True)

    return {
        "url": url,
        "title": title,
        "text": text,
        "images": filtered,  # ya ordenadas “mejor primero”
        "meta": meta,
        "jsonld_events": jsonld_events,
    }
