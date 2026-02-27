# src/geocode/geocoder.py
from __future__ import annotations

import os
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests

DEFAULT_DB_PATH = "data/processed/geocode_cache.sqlite"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


@dataclass
class GeocodeResult:
    lat: str
    lon: str
    display_name: str
    confidence: str
    precision: str


class Geocoder:

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        user_agent: str = "geochicas-8m-global-mapper/1.0",
        min_delay_seconds: float = 1.1,
        timeout_seconds: int = 20,
    ):
        self.db_path = db_path
        self.user_agent = user_agent
        self.min_delay_seconds = min_delay_seconds
        self.timeout_seconds = timeout_seconds
        self._last_call_ts = 0.0

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS geocode_cache (
                query TEXT PRIMARY KEY,
                lat TEXT,
                lon TEXT,
                display_name TEXT,
                confidence TEXT,
                precision TEXT
            )
            """
        )
        self.conn.commit()

    def _norm_query(self, q: str) -> str:
        return re.sub(r"\s+", " ", (q or "").strip()).lower()

    def _get_cached(self, q_norm: str) -> Optional[GeocodeResult]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT lat, lon, display_name, confidence, precision FROM geocode_cache WHERE query = ?",
            (q_norm,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return GeocodeResult(*row)

    def _set_cache(self, q_norm: str, res: GeocodeResult):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO geocode_cache(query, lat, lon, display_name, confidence, precision)
            VALUES(?,?,?,?,?,?)
            """,
            (q_norm, res.lat, res.lon, res.display_name, res.confidence, res.precision),
        )
        self.conn.commit()

    def _rate_limit(self):
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_delay_seconds:
            time.sleep(self.min_delay_seconds - elapsed)
        self._last_call_ts = time.time()

    def geocode(self, query: str) -> Optional[GeocodeResult]:
        q_norm = self._norm_query(query)
        if not q_norm:
            return None

        cached = self._get_cached(q_norm)
        if cached:
            return cached

        self._rate_limit()

        try:
            r = requests.get(
                NOMINATIM_URL,
                params={"q": query, "format": "jsonv2", "limit": 1},
                timeout=self.timeout_seconds,
                headers={"User-Agent": self.user_agent},
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            return None

        if not data:
            return None

        hit = data[0]
        res = GeocodeResult(
            lat=str(hit.get("lat", "")),
            lon=str(hit.get("lon", "")),
            display_name=str(hit.get("display_name", "")),
            confidence="media",
            precision="exacta",
        )

        self._set_cache(q_norm, res)
        return res


# =========================
# Compat layer para main.py
# =========================

_GEOCODER: Optional[Geocoder] = None


def _get_geocoder() -> Geocoder:
    global _GEOCODER
    if _GEOCODER is None:
        _GEOCODER = Geocoder()
    return _GEOCODER


def load_geocode_cache(path: str) -> Dict[str, Any]:
    return {}


def save_geocode_cache(path: str, cache: Dict[str, Any]) -> None:
    return None


def geocode_event(ev: Dict[str, Any], geocode_cache=None) -> Optional[Dict[str, str]]:
    if ev.get("lat") and ev.get("lon"):
        return {
            "lat": ev["lat"],
            "lon": ev["lon"],
            "display_name": "",
            "confidence": "alta",
            "precision": ev.get("precision_ubicacion", ""),
        }

    ciudad = (ev.get("ciudad") or "").strip()
    pais = (ev.get("pais") or "").strip()

    if not ciudad and not pais:
        return None

    query = ", ".join([x for x in [ciudad, pais] if x])

    g = _get_geocoder()
    res = g.geocode(query)
    if not res:
        return None

    return {
        "lat": res.lat,
        "lon": res.lon,
        "display_name": res.display_name,
        "confidence": res.confidence,
        "precision": res.precision,
    }
