"""Location values as pando reads them (src/core/geo.cpp): "lat,lon", "lat lon", "lat;lon",
"geo:lat,lon[,alt][;…]" (latitude first) and WKT "POINT(lon lat)". None for anything else."""

from __future__ import annotations

import re
from typing import Optional

_NUM = r"[-+]?\d+(?:\.\d+)?"


def parse_geo(text: str) -> Optional[tuple[float, float]]:
    s = (text or "").strip()
    m = re.fullmatch(rf"(?i)point\s*\(\s*({_NUM})[\s,;]+({_NUM})\s*\)", s)
    if m:
        lat, lon = float(m.group(2)), float(m.group(1))
    else:
        if s.lower().startswith("geo:"):
            s = s[4:].split(";")[0]
            s = ",".join(s.split(",")[:2])            # without the altitude
        m = re.fullmatch(rf"({_NUM})\s*[,; \t]\s*({_NUM})", s)
        if not m:
            return None
        lat, lon = float(m.group(1)), float(m.group(2))
    return (lat, lon) if -90 <= lat <= 90 and -180 <= lon <= 180 else None
