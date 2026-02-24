# src/parse/html_parser.py
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def parse_page(url, html):
    soup = BeautifulSoup(html, "lxml")

    # Título
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Texto base (simple por ahora)
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = "\n".join([p for p in paragraphs if p])

    # Imágenes
    images = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        full = urljoin(url, src)
        if full not in images:
            images.append(full)

    # Metadata básica
    site_name = ""
    og_site = soup.find("meta", property="og:site_name")
    if og_site and og_site.get("content"):
        site_name = og_site["content"].strip()

    return {
        "url": url,
        "title": title,
        "text": text,
        "images": images,
        "site_name": site_name,
        "published_at": ""
    }
