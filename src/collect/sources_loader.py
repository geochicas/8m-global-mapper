# src/collect/sources_loader.py
# Lee sources.yml (anidado) y devuelve:
# - seeds_urls (scrapeables)
# - social_urls (guardadas para futuro; bloqueadas sin APIs)
# - hashtags (para scoring/matching)
# - priority_urls (si existen)
#
# Regla: por defecto NO usamos social_urls como seeds (ENABLE_SOCIAL_SEEDS=false).

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os

import yaml


@dataclass
class SourcesBundle:
    seeds_urls: list[str]
    social_urls: list[str]
    hashtags: list[str]
    priority_urls: list[str]


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _is_url(s: str) -> bool:
    s = (s or "").strip().lower()
    return s.startswith("http://") or s.startswith("https://")


def _collect_from_node(node: Any, seeds: list[str], social: list[str], hashtags: list[str], priority: list[str]) -> None:
    """
    Acepta estructuras tipo:
      - lista de strings
      - dict anidado con llaves arbitrarias
      - dict con llaves especiales: urls, social, hashtags, priority_urls
    """
    if node is None:
        return

    # lista
    if isinstance(node, list):
        for item in node:
            _collect_from_node(item, seeds, social, hashtags, priority)
        return

    # string
    if isinstance(node, str):
        s = node.strip()
        if not s:
            return
        if _is_url(s):
            seeds.append(s)
        elif s.startswith("#"):
            hashtags.append(s)
        return

    # dict
    if isinstance(node, dict):
        # llaves directas
        if "priority_urls" in node and isinstance(node["priority_urls"], list):
            for u in node["priority_urls"]:
                if isinstance(u, str) and _is_url(u):
                    priority.append(u.strip())

        if "urls" in node and isinstance(node["urls"], list):
            for u in node["urls"]:
                if isinstance(u, str) and _is_url(u):
                    seeds.append(u.strip())

        if "social" in node and isinstance(node["social"], list):
            for u in node["social"]:
                if isinstance(u, str) and _is_url(u):
                    social.append(u.strip())

        if "hashtags" in node:
            # hashtags puede venir como lista o string
            h = node["hashtags"]
            if isinstance(h, list):
                for t in h:
                    if isinstance(t, str):
                        t = t.strip()
                        if t:
                            hashtags.append(t if t.startswith("#") else f"#{t}")
            elif isinstance(h, str):
                t = h.strip()
                if t:
                    hashtags.append(t if t.startswith("#") else f"#{t}")

        # recursión sobre todas las llaves (para estructura libre)
        for _, v in node.items():
            _collect_from_node(v, seeds, social, hashtags, priority)
        return


def load_sources(path: str) -> SourcesBundle:
    if not os.path.exists(path):
        return SourcesBundle([], [], [], [])

    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)

    seeds: list[str] = []
    social: list[str] = []
    hashtags: list[str] = []
    priority: list[str] = []

    _collect_from_node(y, seeds, social, hashtags, priority)

    # Nota: seeds incluye TODO lo que parezca URL. Filtramos:
    # - Sacamos social_urls de seeds (si se colaron) para evitar 403/locks.
    social_set = set([s.lower() for s in social])
    seeds_filtered = []
    for u in seeds:
        if u.lower() in social_set:
            continue
        # heurística simple: si es dominio típico social, lo tratamos como social
        if any(d in u.lower() for d in ["instagram.com", "twitter.com", "x.com", "facebook.com", "fb.me", "t.co"]):
            social.append(u)
            continue
        seeds_filtered.append(u)

    return SourcesBundle(
        seeds_urls=_dedupe(seeds_filtered),
        social_urls=_dedupe(social),
        hashtags=_dedupe(hashtags),
        priority_urls=_dedupe(priority),
    )


def should_include_social_seeds() -> bool:
    v = os.environ.get("ENABLE_SOCIAL_SEEDS", "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")
