"""An SVG preview of a variable's map: Voronoi cells coloured by the dominant variant (paler where it is
less dominant), a point per place sized by its hits, the place names and a legend. For a quick look;
the GeoJSON layers are for GIS."""

from __future__ import annotations

import math
from html import escape

from pando_geo.counts import PlaceCounts
from pando_geo.voronoi import LocalPlane, voronoi_cells

PALETTE = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1", "#ff9da7", "#9c755f"]
OTHER = "#bab0ac"


def svg(places: list[PlaceCounts], variants: list[str], title: str, width: int = 900) -> str:
    points = [(pc.lat, pc.lon) for pc in places]
    plane = LocalPlane(points)
    cells = voronoi_cells(points)
    allxy = [plane.xy(lat, lon) for ring in cells for lat, lon in ring] or [plane.xy(*p) for p in points]
    x0, x1 = min(p[0] for p in allxy), max(p[0] for p in allxy)
    y0, y1 = min(p[1] for p in allxy), max(p[1] for p in allxy)
    legend_h = 28 + 18 * (len(variants) + 1)
    label_room = 110   # place names to the right of the easternmost points
    scale = (width - 20 - label_room) / (x1 - x0)
    height = int((y1 - y0) * scale) + 40 + legend_h
    tx = lambda x, y: (10 + (x - x0) * scale, 30 + (y1 - y) * scale)
    colour = {v: PALETTE[i % len(PALETTE)] for i, v in enumerate(variants)}
    colour["other"] = OTHER
    maxn = max(pc.n for pc in places)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
           'font-family="Helvetica, Arial, sans-serif">',
           f'<rect width="{width}" height="{height}" fill="#fff"/>',
           f'<text x="10" y="20" font-size="15" font-weight="600">{escape(title)}</text>']
    for pc, ring in zip(places, cells):
        if not ring:
            continue
        d = " ".join(f"{a:.1f},{b:.1f}" for a, b in (tx(*plane.xy(lat, lon)) for lat, lon in ring))
        dom = pc.dominant()
        opacity = 0.25 + 0.65 * max(0.0, (pc.share(dom) - 0.3) / 0.7)
        out.append(f'<polygon points="{d}" fill="{colour.get(dom, OTHER)}" fill-opacity="{opacity:.2f}" stroke="#fff" stroke-width="1.5">'
                   f'<title>{escape(pc.place)}: {escape(dom)} {pc.share(dom):.0%} of {pc.n}</title></polygon>')
    for pc in places:
        x, y = tx(*plane.xy(pc.lat, pc.lon))
        r = 2.5 + 7 * math.sqrt(pc.n / maxn)
        out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{colour.get(pc.dominant(), OTHER)}" stroke="#222" stroke-width="0.8"/>')
        out.append(f'<text x="{x + r + 2:.1f}" y="{y + 4:.1f}" font-size="11" fill="#222" paint-order="stroke" stroke="#fff" stroke-width="3">{escape(pc.place)}</text>')
    ly = height - legend_h + 10
    out.append(f'<text x="10" y="{ly + 4}" font-size="12">Dominant variant (paler: less dominant; point size: hits)</text>')
    for i, v in enumerate(variants + ["other"]):
        y = ly + 18 * (i + 1)
        out.append(f'<rect x="10" y="{y - 10}" width="14" height="12" fill="{colour[v]}"/><text x="30" y="{y}" font-size="12">{escape(v)}</text>')
    out.append("</svg>")
    return "\n".join(out)
