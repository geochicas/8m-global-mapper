# src/export/to_csv.py
import csv
import os

COLUMNS = [
    "colectiva", "convocatoria", "descripcion", "fecha", "hora", "pais", "ciudad",
    "localizacion_exacta", "direccion", "lat", "lon", "imagen", "cta_url",
    "sitio_web_colectiva", "trans_incluyente", "fuente_url", "fuente_tipo",
    "confianza_extraccion", "precision_ubicacion"
]

def _format_image_for_umap(value):
    v = (value or "").strip()
    if not v:
        return ""

    # si ya viene envuelto, lo deja igual
    if v.startswith("{{") and v.endswith("}}"):
        return v

    # si es un nombre local de imagen, envuelve para uMap
    if v.lower().endswith((".jpg", ".jpeg", ".png")) and not v.startswith(("http://", "https://")):
        return "{{" + v + "}}"

    # si es URL pública, también puede envolver
    if v.startswith(("http://", "https://")) and v.lower().endswith((".jpg", ".jpeg", ".png")):
        return "{{" + v + "}}"

    return v

def export_csv(records, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for r in records:
            row = {k: r.get(k, "") for k in COLUMNS}
            row["imagen"] = _format_image_for_umap(row.get("imagen", ""))
            writer.writerow(row)

# ======================================================
# COMPAT: main.py espera export_master_csv / export_umap_csv / export_sin_coord_csv
# ======================================================

def export_master_csv(path: str, rows: list[dict]):
    """
    Wrapper compat. Intenta llamar a la función real existente.
    """
    for fn_name in ["export_master", "write_master_csv", "export_csv_master", "export_csv"]:
        fn = globals().get(fn_name)
        if callable(fn):
            try:
                return fn(path, rows)
            except TypeError:
                pass
    # fallback: si el módulo ya tiene un export_csv genérico con (path, rows, columns)
    fn = globals().get("export_csv")
    if callable(fn):
        # intenta detectar columnas por union de llaves
        cols = []
        seen = set()
        for r in rows or []:
            for k in (r or {}).keys():
                if k not in seen:
                    seen.add(k)
                    cols.append(k)
        return fn(path, rows, cols)
    raise ImportError("No se encontró función base para export_master_csv en src/export/to_csv.py")


def export_umap_csv(path: str, rows: list[dict], min_score: int = 10):
    """
    Wrapper compat. Intenta llamar a la función real existente.
    """
    for fn_name in ["export_umap", "write_umap_csv", "export_csv_umap"]:
        fn = globals().get(fn_name)
        if callable(fn):
            try:
                return fn(path, rows, min_score=min_score)
            except TypeError:
                try:
                    return fn(path, rows, min_score)
                except TypeError:
                    return fn(path, rows)
    raise ImportError("No se encontró función base para export_umap_csv en src/export/to_csv.py")


def export_sin_coord_csv(path: str, rows: list[dict], min_score: int = 10):
    """
    Wrapper compat. Intenta llamar a la función real existente.
    """
    for fn_name in ["export_sin_coord", "write_sin_coord_csv", "export_csv_sin_coord", "export_without_coords"]:
        fn = globals().get(fn_name)
        if callable(fn):
            try:
                return fn(path, rows, min_score=min_score)
            except TypeError:
                try:
                    return fn(path, rows, min_score)
                except TypeError:
                    return fn(path, rows)
    raise ImportError("No se encontró función base para export_sin_coord_csv en src/export/to_csv.py")
