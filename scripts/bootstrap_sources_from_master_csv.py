import csv
import os
from urllib.parse import urlparse
import yaml

INPUT_CSV = "data/raw/convocatorias_2019_2025.csv"
OUTPUT_YML = "config/sources.generated.yml"

def detect_delimiter(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as f:
        sample = f.read(4000)
    return ";" if sample.count(";") > sample.count(",") else ","

def base_site(url):
    try:
        p = urlparse(url)
        if not p.scheme or not p.netloc:
            return ""
        return f"{p.scheme}://{p.netloc}"
    except:
        return ""

def is_http(u):
    return str(u or "").startswith("http")

def main():
    if not os.path.exists(INPUT_CSV):
        print("❌ No existe:", INPUT_CSV)
        return

    delim = detect_delimiter(INPUT_CSV)

    seeds = {}
    priority = []

    with open(INPUT_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=delim)
        print("Columnas detectadas:", reader.fieldnames)

        for row in reader:
            fuente = row.get("fuente_url", "")
            cta = row.get("cta_url", "")

            for u in (fuente, cta):
                if is_http(u):
                    priority.append(u)

            for u in (fuente, cta):
                if is_http(u):
                    b = base_site(u)
                    if b:
                        seeds[b] = True

    seeds_list = sorted(list(seeds.keys()))
    priority = list(dict.fromkeys(priority))

    out = {
        "seeds": seeds_list,
        "priority_urls": priority,
    }

    os.makedirs("config", exist_ok=True)
    with open(OUTPUT_YML, "w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, allow_unicode=True)

    print("\n✅ sources.generated.yml creado")
    print("Seeds:", len(seeds_list))
    print("Priority URLs:", len(priority))


if __name__ == "__main__":
    main()
