import hashlib
import os
from io import BytesIO
from typing import Optional, Tuple

import requests
from PIL import Image


DEFAULT_IMAGES_DIR = "data/images"

# Umbrales anti-logo: ajustables
MIN_WIDTH = 420
MIN_HEIGHT = 420
MIN_AREA = 420 * 420


def _sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


def download_and_convert_to_jpg(
    image_url: str,
    out_dir: str = DEFAULT_IMAGES_DIR,
    timeout_seconds: int = 25,
    user_agent: str = "geochicas-8m-global-mapper/1.0 (media)",
) -> Optional[Tuple[str, str]]:
    """
    Descarga image_url, convierte a JPG (RGB), y guarda como <sha1>.jpg en out_dir.
    Rechaza imágenes pequeñas (logos/icons).
    Retorna (filename, template) => ("<sha1>.jpg", "{{<sha1>.jpg}}")
    """
    if not image_url:
        return None

    os.makedirs(out_dir, exist_ok=True)

    name = _sha1(image_url) + ".jpg"
    out_path = os.path.join(out_dir, name)

    # Si ya existe, no re-descargar
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return name, f"{{{{{name}}}}}"

    try:
        r = requests.get(
            image_url,
            timeout=timeout_seconds,
            headers={"User-Agent": user_agent},
        )
        r.raise_for_status()
        content = r.content
    except Exception:
        return None

    try:
        img = Image.open(BytesIO(content))
        w, h = img.size

        # filtro anti-logo
        if (w < MIN_WIDTH) or (h < MIN_HEIGHT) or (w * h < MIN_AREA):
            return None

        img = img.convert("RGB")
        img.save(out_path, format="JPEG", quality=85, optimize=True)
    except Exception:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        return None

    return name, f"{{{{{name}}}}}"
