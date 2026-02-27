# src/media/image_processor.py
# Descarga y guarda imágenes localmente.
# Wrapper compat: download_and_process_image(url, out_dir=...) -> {"public_url": "...", "local_path": "..."}
#
# Nota: en GitHub Pages, "public_url" idealmente debería mapear al path publicado.
# Aquí dejamos "public_url" como ruta relativa estándar para que tu pipeline
# la pueda reescribir si ya tienes esa lógica en Actions/Pages.

from __future__ import annotations

import os
import re
import hashlib
from urllib.parse import urlparse

import requests


def _safe_filename_from_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return "image"

    parsed = urlparse(u)
    name = os.path.basename(parsed.path) or "image"

    # limpia query strings “pegadas” (por si acaso)
    name = name.split("?")[0].split("#")[0].strip() or "image"

    # extensión
    if not re.search(r"\.(jpg|jpeg|png|webp|gif)$", name, flags=re.IGNORECASE):
        name += ".jpg"

    # evita cosas raras
    name = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
    return name


def _hash(url: str) -> str:
    return hashlib.sha1((url or "").encode("utf-8", errors="ignore")).hexdigest()[:16]


def _download_bytes(url: str, timeout: int = 25) -> bytes | None:
    headers = {
        "User-Agent": os.environ.get(
            "USER_AGENT",
            "geochicas-8m-global-mapper/1.0 (+https://github.com/geochicas/8m-global-mapper)",
        )
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout, stream=True)
        r.raise_for_status()
        return r.content
    except Exception:
        return None


def download_and_process_image(url: str, out_dir: str = "data/images") -> dict | None:
    """
    Compat con main.py.
    - descarga
    - guarda en out_dir
    - retorna dict con public_url y local_path

    No hace “procesado” pesado para no romper deps en Actions.
    """
    url = (url or "").strip()
    if not url.startswith("http"):
        return None

    os.makedirs(out_dir, exist_ok=True)

    # nombre estable por hash para evitar duplicados
    base_name = _safe_filename_from_url(url)
    stem, ext = os.path.splitext(base_name)
    fname = f"{stem}_{_hash(url)}{ext}"
    local_path = os.path.join(out_dir, fname)

    # si ya existe, devolvemos
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return {
            "public_url": _public_url_for_local_path(local_path),
            "local_path": local_path,
            "source_url": url,
        }

    b = _download_bytes(url)
    if not b:
        return None

    try:
        with open(local_path, "wb") as f:
            f.write(b)
    except Exception:
        return None

    return {
        "public_url": _public_url_for_local_path(local_path),
        "local_path": local_path,
        "source_url": url,
    }


def _public_url_for_local_path(local_path: str) -> str:
    """
    Convierte data/images/xxx.jpg -> images/xxx.jpg
    para que funcione en GitHub Pages.
    """
    lp = local_path.replace("\\", "/")

    if "data/images/" in lp:
        return lp.split("data/images/")[-1].join(["images/", ""])

    # fallback seguro
    filename = os.path.basename(lp)
    return f"images/{filename}"
