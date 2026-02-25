import csv
import os

INPUT_CSV = "data/raw/convocatorias_2019_2025.csv"
OUTPUT_TXT = "config/cities.txt"

def detect_delimiter(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as f:
        sample = f.read(4096)
    return ";" if sample.count(";") > sample.count(",") else ","

def norm(s):
    return str(s or "").strip()

def main():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"No encontré {INPUT_CSV}")

    delim = detect_delimiter(INPUT_CSV)
    cities = set()

    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        for row in reader:
            c = norm(row.get("ciudad"))
            if c and c.lower() not in ("nan", "none", "null"):
                cities.add(c)

    cities_sorted = sorted(cities, key=lambda x: (x.lower(), x))

    os.makedirs(os.path.dirname(OUTPUT_TXT), exist_ok=True)
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        for c in cities_sorted:
            f.write(c + "\n")

    print(f"✅ Generado {OUTPUT_TXT} con {len(cities_sorted)} ciudades")

if __name__ == "__main__":
    main()
