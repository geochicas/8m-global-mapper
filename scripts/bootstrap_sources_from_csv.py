import csv
import os
from collections import Counter
from urllib.parse import urlparse

import yaml

INPUT_CSV = "data/raw/convocatorias_2019_2025.csv"
OUTPUT_YML = "config/sources.generated.yml"

# Columnas candidatas de URL (ajustadas a tu CSV)

URL_COLUMNS_CANDIDATES = [
    "fuente_url",
    "cta_url",
    "actividad_url_imagen",
]

def normalize_domain(url):
    if not url:
        return None
    url = str(url).strip()
    if not url:
        return None

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower().strip()
        if host.startswith("www."):
            host = host[4:]
        if not host:
            return None
        return host
    except Exception:
        return None

def detect_delimiter(file_path):
    """Intenta detectar si el CSV usa coma o punto y coma."""
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        return dialect.delimiter
    except Exception:
        # fallback razonable
        return ";" if sample.count(";") > sample.count(",") else ","

def main():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(
            f"No encontré {INPUT_CSV}. Copiá ahí tu CSV histórico (2019-2025)."
        )

    delimiter = detect_delimiter(INPUT_CSV)
    print(f"Separador detectado: {repr(delimiter)}")

    domain_counter = Counter()

    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        headers = reader.fieldnames or []
        print("Columnas detectadas:", headers)

        valid_cols = [c for c in URL_COLUMNS_CANDIDATES if c in headers]
        if not valid_cols:
            raise ValueError(
                "No encontré columnas de URL útiles. "
                "Agregá en URL_COLUMNS_CANDIDATES los nombres reales de tu CSV."
            )

        print("Columnas URL que se usarán:", valid_cols)

        for row in reader:
            for col in valid_cols:
                raw_value = row.get(col, "")
                if not raw_value:
                    continue

                # por si vinieran varias URLs en la misma celda
                possible_urls = [u.strip() for u in str(raw_value).split() if u.strip()]
                for u in possible_urls:
                    domain = normalize_domain(u)
                    if domain:
                        domain_counter[domain] += 1

    top_domains = domain_counter.most_common()

    sources = []
    for domain, count in top_domains:
        sources.append({
            "name": f"{domain} ({count})",
            "type": "web",
            "url": f"https://{domain}"
        })

    out = {"sources": sources}

    os.makedirs(os.path.dirname(OUTPUT_YML), exist_ok=True)
    with open(OUTPUT_YML, "w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, allow_unicode=True, sort_keys=False)

    print(f"✅ Generado: {OUTPUT_YML}")
    print(f"🌐 Dominios únicos: {len(sources)}")
    print("Top 20 dominios:")
    for d, c in top_domains[:20]:
        print(f" - {d}: {c}")

if __name__ == "__main__":
    main()
