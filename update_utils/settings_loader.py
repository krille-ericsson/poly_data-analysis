from __future__ import annotations

from dataclasses import dataclass
from typing import List, Set
import yaml


@dataclass
class PipelineSettings:
    markets: List[str]
    timeframes: List[str]

    @property
    def market_set(self) -> Set[str]:
        return {m.lower() for m in self.markets}

    @property
    def timeframe_set(self) -> Set[str]:
        return {t.lower() for t in self.timeframes}


def load_settings(path: str = "settings.yaml") -> PipelineSettings:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    filters = data.get("filters", {})
    markets = [str(x).lower() for x in filters.get("markets", ["btc", "eth", "xrp", "sol"])]
    timeframes = [str(x).lower() for x in filters.get("timeframes", ["5m", "15m"])]

    return PipelineSettings(markets=markets, timeframes=timeframes)


def slug_matches(slug: str, markets: Set[str], timeframes: Set[str]) -> bool:
    if not slug:
        return False
    s = slug.lower()
    return any(f"{m}-updown-" in s for m in markets) and any(f"-{tf}-" in s for tf in timeframes)
