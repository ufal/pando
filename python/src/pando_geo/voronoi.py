"""Voronoi (Thiessen) cells of places: the area closer to each place than to any other, clipped to a
box around them. Computed in a local plane (km east / north of the points' centre), so cells are not
stretched by latitude; returned as (lat, lon) rings. Places at the same coordinates share a cell.
Clipping to a country border is a later step (it needs a border polygon and polygon intersection)."""

from __future__ import annotations

import math

_KM_PER_DEG_LAT = 110.574


class LocalPlane:
    """Equirectangular projection around a centre: good enough over a few hundred km."""

    def __init__(self, points: list[tuple[float, float]]):
        self.lat0 = sum(p[0] for p in points) / len(points)
        self.lon0 = sum(p[1] for p in points) / len(points)
        self.kx = 111.320 * math.cos(math.radians(self.lat0))

    def xy(self, lat: float, lon: float) -> tuple[float, float]:
        return ((lon - self.lon0) * self.kx, (lat - self.lat0) * _KM_PER_DEG_LAT)

    def latlon(self, x: float, y: float) -> tuple[float, float]:
        return (self.lat0 + y / _KM_PER_DEG_LAT, self.lon0 + x / self.kx)


def _clip(poly: list[tuple[float, float]], a: tuple[float, float], b: tuple[float, float]) -> list[tuple[float, float]]:
    """The part of convex `poly` closer to point a than to point b (Sutherland–Hodgman, one half-plane)."""
    nx, ny = b[0] - a[0], b[1] - a[1]
    mx, my = (a[0] + b[0]) / 2, (a[1] + b[1]) / 2
    side = lambda p: (p[0] - mx) * nx + (p[1] - my) * ny     # < 0: a's side
    out = []
    for i, p in enumerate(poly):
        q = poly[(i + 1) % len(poly)]
        sp, sq = side(p), side(q)
        if sp <= 0:
            out.append(p)
        if (sp < 0 < sq) or (sq < 0 < sp):
            t = sp / (sp - sq)
            out.append((p[0] + t * (q[0] - p[0]), p[1] + t * (q[1] - p[1])))
    return out


def voronoi_cells(points: list[tuple[float, float]], margin_km: float = 15.0) -> list[list[tuple[float, float]]]:
    """For each (lat, lon) point its cell, a closed ring of (lat, lon), clipped to the points' bounding
    box widened by `margin_km`."""
    if not points:
        return []
    plane = LocalPlane(points)
    xy = [plane.xy(*p) for p in points]
    x0, x1 = min(p[0] for p in xy) - margin_km, max(p[0] for p in xy) + margin_km
    y0, y1 = min(p[1] for p in xy) - margin_km, max(p[1] for p in xy) + margin_km
    box = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
    cells = []
    for i, a in enumerate(xy):
        poly = box
        for j, b in enumerate(xy):
            if i != j and (abs(a[0] - b[0]) > 1e-9 or abs(a[1] - b[1]) > 1e-9):
                poly = _clip(poly, a, b)
                if not poly:
                    break
        ring = [plane.latlon(*p) for p in poly]
        cells.append(ring + ring[:1] if ring else [])
    return cells
