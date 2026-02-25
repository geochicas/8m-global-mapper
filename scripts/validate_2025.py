import sys
import os
import csv
import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from src.parse.html_parser import parse_page
from src.extract.extractor_ai import extract_event_fields

INPUT_CSV = "data/raw/convocatorias_2019_2025.csv"
MAX_ROWS = 100
TIMEOUT_SECONDS = 12

def detect_delimiter(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as f:
        sample = f.read(4096)
    return ";" if sample.count(";") > sample.count(",") else ","

def fetch(url):
    try:
        r = requests.get(
            url,
            timeout=TIMEOUT_SECONDS,
            headers={"User-Agent": "Mozilla/5.0 (8m-global-mapper validate)"}
        )
        r.raise_for_status()
        return r.text
    except Exception:
        return None

def norm(s):
    return str(s or "").strip()

def norm_date(s):
    s = norm(s)
    if s.lower() in ("", "nan", "none", "null"):
        return ""
    return s  # YYYY-MM-DD

def md(date_str):
    # devuelve MM-DD
    if not date_str or len(date_str) < 10:
        return ""
    return date_str[5:10]

def norm_time(s):
    s = norm(s)
    if s.lower() in ("", "nan", "none", "null"):
        return ""
    return s[:5] if len(s) >= 5 else s

def similar(a, b):
    a = norm(a).lower()
    b = norm(b).lower()
    if not a or not b:
        return False
    return a in b or b in a

def main():
    if not os.path.exists(INPUT_CSV):
        print("❌ No encuentro el CSV en:", INPUT_CSV)
        return

    delim = detect_delimiter(INPUT_CSV)

    total = 0
    urls_ok = 0
    detected = 0

    fecha_found = 0
    hora_found = 0

    fecha_ok_exact = 0
    fecha_ok_md = 0
    hora_ok = 0

    ciudad_ok = 0
    imagen_ok = 0

    top_fechas = {}

    print(f"\n🔎 Validación (MAX_ROWS={MAX_ROWS}, timeout={TIMEOUT_SECONDS}s)\n")

    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=delim)

        for row in reader:
            total += 1
            if total > MAX_ROWS:
                break

            url = row.get("fuente_url") or row.get("cta_url")
            if not url:
                print(f"[{total}] — sin URL")
                continue

            html = fetch(url)
            if not html:
                print(f"[{total}] ⚠️ timeout/error")
                continue

            urls_ok += 1

            parsed = parse_page(url, html)
            event = extract_event_fields(parsed)

            if not event:
                print(f"[{total}] — sin match evento")
                continue

            detected += 1

            real_fecha = norm_date(row.get("actividad_fecha"))
            real_hora = norm_time(row.get("actividad_hora"))
            real_ciudad = norm(row.get("ciudad"))

            got_fecha = norm_date(event.get("fecha"))
            got_hora = norm_time(event.get("hora"))
            got_ciudad = norm(event.get("ciudad") or event.get("localizacion_exacta"))

            if got_fecha:
                fecha_found += 1
                top_fechas[got_fecha] = top_fechas.get(got_fecha, 0) + 1

            if got_hora:
                hora_found += 1

            if real_fecha and got_fecha and real_fecha == got_fecha:
                fecha_ok_exact += 1

            # match por mes-día aunque el año cambie
            if real_fecha and got_fecha and md(real_fecha) and md(real_fecha) == md(got_fecha):
                fecha_ok_md += 1

            if real_hora and got_hora and real_hora == got_hora:
                hora_ok += 1

            if real_ciudad and got_ciudad and similar(real_ciudad, got_ciudad):
                ciudad_ok += 1

            if event.get("imagen"):
                imagen_ok += 1

            print(f"[{total}] ✅ evento | fecha={got_fecha or '-'} hora={got_hora or '-'} ciudad={got_ciudad or '-'}")

    print("\n====== REPORTE VALIDACIÓN (MUESTRA) ======")
    print("Filas procesadas:", total if total <= MAX_ROWS else MAX_ROWS)
    print("URLs accesibles:", urls_ok)
    print("Detectados como evento:", detected)
    print("Fecha encontrada (cualquiera):", fecha_found)
    print("Hora encontrada (cualquiera):", hora_found)
    print("Fecha correcta (exacta):", fecha_ok_exact)
    print("Fecha correcta (mes-día):", fecha_ok_md)
    print("Hora correcta:", hora_ok)
    print("Ciudad detectada:", ciudad_ok)
    print("Imagen detectada:", imagen_ok)
    print("Top 10 fechas encontradas:")
    for k, v in sorted(top_fechas.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f" - {k}: {v}")
    print("=========================================\n")

if __name__ == "__main__":
    main()
