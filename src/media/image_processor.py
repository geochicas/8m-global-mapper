import os
import re
import hashlib
from io import BytesIO

import requests
from PIL import Image


BLOCKLIST_HINTS = [
    "logo", "icon", "sprite", "favicon", "apple-touch-icon", "site-icon",
    "header", "footer", "navbar", "menu", "brand", "badge", "avatar"
]

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def is_probably_ui_asset(url: str) -> bool:
    s = (url or "").lower()
    return any(h in s for h in BLOCKLIST_HINTS)

def _safe_ext_from_content_type(ct: str) -> str | None:
    ct = (ct or "").lower()
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    return None

def _safe_ext_from_url(url: str) -> str | None:
    u = (url or "").lower()
    if u.endswith(".jpg") or u.endswith(".jpeg"):
        return ".jpg"
    if u.endswith(".png"):
        return ".png"
    if u.endswith(".webp"):
        return ".webp"
    return None

def download_image_filtered(
    session: requests.Session,
    image_url: str,
    out_dir: str,
    user_agent: str,
    timeout=(7, 25),
    min_bytes=40_000,
    max_bytes=6_000_000,
    min_width=500,
    min_height=350,
    max_banner_ratio=3.0,
    min_ratio=0.45,
    max_ratio=2.4,
) -> str | None:
    """
    Descarga imagen y filtra banners/headers/logos usando:
    - tamaño (bytes)
    - dimensiones (min_width/min_height)
    - proporción (ratio ancho/alto)
    Devuelve filename (hash.ext) o None.
    """
    if not image_url or not image_url.startswith(("http://", "https://")):
        return None

    # Evitar assets UI desde URL (logo/header/footer)
    if is_probably_ui_asset(image_url):
        # ojo: a veces el poster tiene "header", pero en general esto reduce mucho basura
        return None

    os.makedirs(out_dir, exist_ok=True)

    h = sha1(image_url)

    # cache local si existe
    for ext in [".jpg", ".png", ".webp"]:
        fp = os.path.join(out_dir, f"{h}{ext}")
        if os.path.exists(fp) and os.path.getsize(fp) >= min_bytes:
            return f"{h}{ext}"

    try:
        r = session.get(
            image_url,
            timeout=timeout,
            headers={"User-Agent": user_agent},
            stream=True,
            allow_redirects=True,
        )
    except Exception:
        return None

    if r.status_code != 200:
        return None

    ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
    if "image" not in ct:
        return None

    ext = _safe_ext_from_url(image_url) or _safe_ext_from_content_type(ct)
    if ext not in [".jpg", ".png", ".webp"]:
        return None

    data = b""
    total = 0
    try:
        for chunk in r.iter_content(chunk_size=16384):
            if not chunk:
                break
            data += chunk
            total += len(chunk)
            if total > max_bytes:
                return None
    except Exception:
        return None

    if total < min_bytes:
        return None

    # Validar con Pillow: dimensiones + ratio
    try:
        im = Image.open(BytesIO(data))
        im.load()
        w, h_px = im.size
    except Exception:
        return None

    # Tamaño mínimo en pixels (mata logos/cosas chicas)
    if w < min_width or h_px < min_height:
        return None

    ratio = (w / max(h_px, 1))
    # Muy horizontal => header/banner
    if ratio > max_banner_ratio:
        return None
    # Muy estrecha => iconos verticales raros
    if ratio < min_ratio:
        return None
    # Tope de “no poster”
    if ratio > max_ratio:
        return None

    filename = f"{sha1(image_url)}{ext}"
    fp = os.path.join(out_dir, filename)
    try:
        with open(fp, "wb") as f:
            f.write(data)
    except Exception:
        return None

    return filename
# ======================================================
# COMPAT: main.py espera download_and_process_image(...)
# ======================================================

def download_and_process_image(img_url: str, out_dir: str = "data/images"):
    """
    Wrapper de compatibilidad para Actions/main.py.
    Intenta usar la función 'real' existente en este módulo.
    Debe devolver dict con al menos: {"public_url": "..."} cuando funcione.
    """
    if not img_url:
        return None

    # 1) Si ya existe alguna función “principal” en este módulo, úsala.
    #    (Mantiene el comportamiento anterior sin reescribir pipeline.)
    for fn_name in [
        "download_and_process",            # nombre típico
        "process_and_publish_image",       # nombre típico
        "download_process_image",          # nombre típico
        "process_image",                   # genérico
    ]:
        fn = globals().get(fn_name)
        if callable(fn):
            try:
                # probamos firmas comunes
                try:
                    return fn(img_url, out_dir=out_dir)
                except TypeError:
                    return fn(img_url, out_dir)
            except Exception:
                return None

    # 2) Fallback ultra simple: descargar sin “procesado”.
    #    Esto evita que el job muera; si no hay public_url, simplemente no setea imagen en main.
    try:
        import os
        import hashlib
        import requests

        os.makedirs(out_dir, exist_ok=True)
        ext = ".jpg"
        h = hashlib.sha1(img_url.encode("utf-8")).hexdigest()[:16]
        path = os.path.join(out_dir, f"{h}{ext}")

        r = requests.get(img_url, timeout=20, headers={"User-Agent": os.environ.get("USER_AGENT", "geochicas-8m-global-mapper/1.0")})
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)

        # sin saber tu lógica de GitHub Pages aquí, devolvemos solo local_path
        return {"local_path": path}
    except Exception:
        return None
