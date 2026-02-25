from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json

def _get_meta(soup, key, attr="property"):
    tag = soup.find("meta", attrs={attr: key})
    if tag and tag.get("content"):
        return tag["content"].strip()
    return ""

def _extract_jsonld_events(soup):
    """Extrae startDate/endDate de schema.org (JSON-LD), si existe."""
    events = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue

        # puede ser dict o lista
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if not isinstance(item, dict):
                continue

            # a veces viene con @graph
            graph = item.get("@graph")
            if isinstance(graph, list):
                candidates.extend([x for x in graph if isinstance(x, dict)])

            t = (item.get("@type") or "")
            if isinstance(t, list):
                t = ",".join(t)

            if "Event" in str(t):
                events.append({
                    "startDate": item.get("startDate", "") or "",
                    "endDate": item.get("endDate", "") or "",
                    "name": item.get("name", "") or ""
                })
    return events

def parse_page(url, html):
    soup = BeautifulSoup(html, "lxml")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = "\n".join([p for p in paragraphs if p])

    images = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        full = urljoin(url, src)
        if full not in images:
            images.append(full)

    meta = {
        "og_title": _get_meta(soup, "og:title"),
        "og_description": _get_meta(soup, "og:description"),
        "article_published_time": _get_meta(soup, "article:published_time"),
        "article_modified_time": _get_meta(soup, "article:modified_time"),
        "og_updated_time": _get_meta(soup, "og:updated_time"),
        "twitter_title": _get_meta(soup, "twitter:title", attr="name"),
        "twitter_description": _get_meta(soup, "twitter:description", attr="name"),
    }

    jsonld_events = _extract_jsonld_events(soup)

    return {
        "url": url,
        "title": title,
        "text": text,
        "images": images,
        "meta": meta,
        "jsonld_events": jsonld_events,
    }
