# src/media/image_processor.py
import os
import re
import unicodedata
from io import BytesIO

import requests
from PIL import Image

IMG_DIR = "data/images"

def slugify(text, max_len=80):
    text = str(text or "").strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:max_len] if text else "imagen"

def _to_jpg(img, out_path):
    if img.mode in ("RGBA", "LA", "P"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode != "RGBA":
            img = img.convert("RGBA")
        bg.paste(img, mask=img.split()[-1])
        img = bg
    else:
        img = img.convert("RGB")

    img.save(out_path, format="JPEG", quality=90, optimize=True)

def process_image(event, index=0):
    os.makedirs(IMG_DIR, exist_ok=True)

    img_url = (event.get("imagen") or "").strip()
    if not img_url.startswith(("http://", "https://")):
        return event

    base = "-".join([
        slugify(event.get("pais", ""), 20),
        slugify(event.get("ciudad", ""), 20),
        slugify(event.get("convocatoria", ""), 30),
        str(index)
    ]).strip("-")

    if not base:
        base = f"evento-{index}"

    filename = f"{base}.jpg"
    out_path = os.path.join(IMG_DIR, filename)

    # Evita descargar de nuevo si ya existe
    if os.path.exists(out_path):
        event["imagen"] = filename
        return event

    try:
        r = requests.get(img_url, timeout=20)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content))
        _to_jpg(img, out_path)
        event["imagen"] = filename
    except Exception:
        # Deja la URL original si falla
        pass

    return event
