# src/geocode/geocoder.py
import time
from geopy.geocoders import Nominatim

GEOCODER = Nominatim(user_agent="8m-global-mapper-geochicas")

def _safe_geocode(query, timeout=10):
    if not query:
        return None
    try:
        return GEOCODER.geocode(query, timeout=timeout)
    except Exception:
        return None

def geocode_event(event, sleep_seconds=1.1):
    """
    Intenta geocodificar en orden:
    1) direccion
    2) localizacion_exacta
    3) ciudad + pais
    """
    if event.get("lat") and event.get("lon"):
        return event

    direccion = (event.get("direccion") or "").strip()
    localizacion = (event.get("localizacion_exacta") or "").strip()
    ciudad = (event.get("ciudad") or "").strip()
    pais = (event.get("pais") or "").strip()

    candidates = []
    if direccion:
        candidates.append((direccion, "exacta"))
    if localizacion:
        candidates.append((localizacion, "aproximada"))
    if ciudad or pais:
        q = ", ".join([x for x in [ciudad, pais] if x])
        candidates.append((q, "ciudad"))

    for query, precision in candidates:
        loc = _safe_geocode(query)
        time.sleep(sleep_seconds)  # respetar límites de Nominatim público
        if loc:
            event["lat"] = str(loc.latitude)
            event["lon"] = str(loc.longitude)
            event["precision_ubicacion"] = precision
            return event

    event["precision_ubicacion"] = event.get("precision_ubicacion") or "sin_ubicar"
    return event
