# src/export/to_csv.py
from __future__ import annotations

import csv
import os
from typing import Iterable, List, Dict, Optional


def _ensure_parent_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _normalize_rows(rows: Optional[Iterable[dict]]) -> List[Dict]:
    if not rows:
        return []
    out: List[Dict] = []
    for r in rows:
        if isinstance(r, dict):
            out.append(r)
    return out


def _infer_columns(rows: List[Dict]) -> List[str]:
    """
    Fallback: si no nos pasan columnas, inferimos un set ordenado
    (orden alfabético para reproducibilidad).
    """
    keys = set()
    for r in rows:
        keys.update([k for k in r.keys() if isinstance(k, str) and k.strip()])
    return sorted(keys)


def export_csv(path: str, rows: Iterable[dict], columns: Optional[List[str]] = None) -> str:
    """
    Export genérico a CSV.

    Compat:
      - export_csv(path, rows) -> columnas inferidas
      - export_csv(path, rows, columns) -> columnas explícitas

    Retorna: path (útil para logs/tests).
    """
    rows_n = _normalize_rows(rows)
    cols = [c for c in (columns or []) if isinstance(c, str) and c.strip()]
    if not cols:
        cols = _infer_columns(rows_n)

    _ensure_parent_dir(path)

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows_n:
            # garantiza que existan todas las columnas (evita blanks raros en algunos viewers)
            for c in cols:
                r.setdefault(c, "")
            w.writerow(r)

    return path


# =========================
# Wrappers que espera main.py
# =========================

MASTER_COLUMNS = [
    "colectiva",
    "convocatoria",
    "descripcion",
    "fecha",
    "hora",
    "pais",
    "ciudad",
    "localizacion_exacta",
    "direccion",
    "lat",
    "lon",
    "imagen",
    "cta_url",
    "fuente_url",
    "sitio_web_colectiva",
    "trans_incluyente",
    "confianza_extraccion",
    "precision_ubicacion",
    "score_relevancia",
]

UMAP_COLUMNS = [
    "colectiva",
    "convocatoria",
    "descripcion",
    "fecha",
    "hora",
    "pais",
    "ciudad",
    "localizacion_exacta",
    "direccion",
    "lat",
    "lon",
    "imagen",
    "cta_url",
    "fuente_url",
    "score_relevancia",
]

SIN_COORD_COLUMNS = [
    "colectiva",
    "convocatoria",
    "descripcion",
    "fecha",
    "hora",
    "pais",
    "ciudad",
    "localizacion_exacta",
    "direccion",
    "imagen",
    "cta_url",
    "fuente_url",
    "score_relevancia",
]


def _score_ok(r: dict, min_score: int) -> bool:
    try:
        return int(r.get("score_relevancia") or 0) >= int(min_score)
    except Exception:
        return False


def export_master_csv(path: str, rows: List[dict]) -> str:
    return export_csv(path, rows, MASTER_COLUMNS)


def _abs_pages_url(path_or_url: str) -> str:
    """
    Convierte rutas relativas (images/xxx.jpg) a URL absoluta de GitHub Pages.
    Si ya es http(s), la deja igual.
    """
    u = (path_or_url or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    u = u.lstrip("/")  # por si acaso
    return f"https://geochicas.github.io/8m-global-mapper/{u}"


def _build_umap_description(r: dict) -> str:
    """
    Formato uMap wiki (lo que tú estás usando):
      ## Título
      YYYY-MM-DD - HH:MM
      {{https://.../images/...jpg}}
      [[https://...|Accede a la convocatoria]]
    """
    title = (r.get("convocatoria") or "").strip()
    fecha = (r.get("fecha") or "").strip()
    hora = (r.get("hora") or "").strip()
    ciudad = (r.get("ciudad") or "").strip()
    pais = (r.get("pais") or "").strip()

    when = ""
    if fecha and hora:
        when = f"{fecha} - {hora}"
    elif fecha:
        when = fecha
    elif hora:
        when = hora

    # imagen: aceptar "images/..." o URL ya absoluta
    img = (r.get("imagen") or "").strip()
    img_abs = _abs_pages_url(img) if img else ""

    # link: preferimos cta_url, si no fuente_url
    link = (r.get("cta_url") or r.get("fuente_url") or "").strip()

    lines = []
    if title:
        lines.append(f"## {title}")
    # ubicación en 1 línea (suave, sin inventar)
    place_bits = " · ".join([x for x in [ciudad, pais] if x])
    if place_bits:
        lines.append(place_bits)
    if when:
        lines.append(when)
    if img_abs:
        lines.append(f"{{{{{img_abs}}}}}")  # {{...}} literal
    if link:
        lines.append(f"[[{link}|Accede a la convocatoria]]")

    return "\n".join(lines).strip()


def export_umap_csv(path: str, rows: List[dict], min_score: int = 10) -> str:
    """
    Export uMap-friendly:
      - name: título del punto
      - description: popup en wiki uMap (con {{img}} y [[link|text]])
      - lat/lon: coordenadas
    """
    rows_n = _normalize_rows(rows)
    out = []

    for r in rows_n:
        if not _score_ok(r, min_score):
            continue
        if not (r.get("lat") and r.get("lon")):
            continue

        item = {
            "name": (r.get("convocatoria") or "").strip(),
            "description": _build_umap_description(r),
            "lat": r.get("lat", ""),
            "lon": r.get("lon", ""),
        }
        out.append(item)

    # columnas explícitas para estabilidad
    return export_csv(path, out, ["name", "description", "lat", "lon"])


def export_sin_coord_csv(path: str, rows: List[dict], min_score: int = 10) -> str:
    rows_n = _normalize_rows(rows)
    filtered = []
    for r in rows_n:
        if not _score_ok(r, min_score):
            continue
        # “sin coord”: no tiene lat/lon
        if r.get("lat") and r.get("lon"):
            continue
        filtered.append(r)
    return export_csv(path, filtered, SIN_COORD_COLUMNS)


