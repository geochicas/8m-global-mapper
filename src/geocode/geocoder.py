import os
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

import requests

DEFAULT_DB_PATH = "data/processed/geocode_cache.sqlite"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


@dataclass
class GeocodeResult:
    lat: str
    lon: str
    display_name: str
    confidence: str  # "alta" | "media" | "baja"
    precision: str   # "exacta" | "ciudad" | "pais" | ""


class Geocoder:
    """
    Geocoder con cache SQLite + rate limit.
    Usa Nominatim (OpenStreetMap) con un User-Agent identificable.
    """

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        user_agent: str = "geochicas-8m-global-mapper/1.0 (contact: geochicas)",
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

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

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
                precision TEXT,
                ts INTEGER
            )
            """
        )
        self.conn.commit()

    @staticmethod
    def _norm_query(q: str) -> str:
        q = (q or "").strip()
        q = re.sub(r"\s+", " ", q)
        return q.lower()

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
            INSERT OR REPLACE INTO geocode_cache(query, lat, lon, display_name, confidence, precision, ts)
            VALUES(?,?,?,?,?,?, strftime('%s','now'))
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

    def geocode(self, query: str, country_code: str = "") -> Optional[GeocodeResult]:
        """
        query: texto tipo "San José, Costa Rica" o "Plaza de Mayo, Buenos Aires"
        country_code: opcional (ISO2) para acotar (ej. "cr", "ar")
        """
        q_norm = self._norm_query(query)
        if not q_norm:
            return None

        cached = self._get_cached(q_norm)
        if cached:
            return cached

        self._rate_limit()

        params = {
            "q": query,
            "format": "jsonv2",
            "limit": 1,
        }
        if country_code:
            params["countrycodes"] = country_code.lower()

        try:
            r = requests.get(
                NOMINATIM_URL,
                params=params,
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
        lat = str(hit.get("lat", "")).strip()
        lon = str(hit.get("lon", "")).strip()
        display = str(hit.get("display_name", "")).strip()

        # heurística simple de precisión
        typ = (hit.get("type") or "").lower()
        cls = (hit.get("class") or "").lower()

        precision = ""
        confidence = "baja"

        if cls == "place" and typ in ("city", "town", "village"):
            precision = "ciudad"
            confidence = "media"
        elif cls == "boundary" and typ in ("administrative",):
            precision = "pais"
            confidence = "baja"
        else:
            precision = "exacta"
            confidence = "media"

        res = GeocodeResult(lat=lat, lon=lon, display_name=display, confidence=confidence, precision=precision)
        self._set_cache(q_norm, res)
        return res
