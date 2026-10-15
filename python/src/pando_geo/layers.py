"""GIS layers of a variable as GeoJSON (EPSG:4326): points per place and their Voronoi cells, with the
counts, shares and dominant variant of each place as properties."""

from __future__ import annotations

import json

from pando_geo.counts import PlaceCounts
from pando_geo.voronoi import voronoi_cells


def properties(pc: PlaceCounts, variants: list[str]) -> dict:
    props = {"place": pc.place, "n": pc.n, "dominant": pc.dominant(),
             "dominant_share": round(pc.share(pc.dominant()), 3)}
    for v in variants:
        props[f"count_{v}"] = pc.counts.get(v, 0)
        props[f"share_{v}"] = round(pc.share(v), 3)
    return props


def point_layer(places: list[PlaceCounts], variants: list[str], name: str) -> dict:
    return {"type": "FeatureCollection", "name": f"{name}_points", "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [round(pc.lon, 6), round(pc.lat, 6)]},
         "properties": properties(pc, variants)} for pc in places]}


def voronoi_layer(places: list[PlaceCounts], variants: list[str], name: str) -> dict:
    cells = voronoi_cells([(pc.lat, pc.lon) for pc in places])
    return {"type": "FeatureCollection", "name": f"{name}_voronoi", "features": [
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[round(lon, 6), round(lat, 6)] for lat, lon in ring]]},
         "properties": properties(pc, variants)} for pc, ring in zip(places, cells) if ring]}


def write(layer: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(layer, f, ensure_ascii=False)
