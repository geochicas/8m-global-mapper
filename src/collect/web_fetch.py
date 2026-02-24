# src/collect/web_fetch.py
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; 8M-Mapper/0.1; +https://github.com/Geochicas/8m-global-mapper)"
}

def fetch_page(url, timeout=20):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text
