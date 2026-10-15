"""A web map of a variable: its Voronoi cells and points on OpenStreetMap (Leaflet), one HTML file that
opens in any browser (it loads Leaflet and the map tiles from the internet). Cells are coloured by
the dominant variant (paler where it is less dominant); a click shows a place's counts."""

from __future__ import annotations

import json
from html import escape

from pando_geo.preview import OTHER, PALETTE

LEAFLET = "https://unpkg.com/leaflet@1.9.4/dist/leaflet"


def html(points: dict, cells: dict, variants: list[str], title: str, description: str = "") -> str:
    colours = {v: PALETTE[i % len(PALETTE)] for i, v in enumerate(variants)}
    colours["other"] = OTHER
    data = json.dumps({"points": points, "cells": cells, "variants": variants + ["other"], "colours": colours},
                      ensure_ascii=False).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title>
<link rel="stylesheet" href="{LEAFLET}.css">
<script src="{LEAFLET}.js"></script>
<style>
  html, body {{ margin: 0; height: 100%; font-family: Helvetica, Arial, sans-serif; background: #fff; color: #222; }}
  #map {{ position: absolute; inset: 0; }}
  .box {{ background: #fff; padding: 5px 8px; border-radius: 4px; box-shadow: 0 1px 4px rgba(0,0,0,.3); font-size: 12px; line-height: 1.45; max-width: 45vw; }}
  .box summary {{ font-weight: 600; cursor: pointer; }}
  .box p {{ margin: 2px 0 4px; color: #555; }}
  .sw {{ display: inline-block; width: 12px; height: 12px; margin-right: 6px; vertical-align: -1px; }}
  table {{ border-collapse: collapse; }} td {{ padding: 0 6px 0 0; }} td.n {{ text-align: right; }}
</style></head>
<body><div id="map"></div>
<script>
const D = {data};
const map = L.map('map');
L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 18, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
}}).addTo(map);
const colour = (v) => D.colours[v] || D.colours.other;
const share = (p) => p.dominant_share || 0;
const counts = (p) => '<b>' + p.place + '</b> (' + p.n + ' hits)<table>' + D.variants.map((v) =>
  '<tr><td><span class="sw" style="background:' + colour(v) + '"></span>' + v + '</td><td class="n">' +
  (p['count_' + v] || 0) + '</td><td class="n">' + Math.round(100 * (p['share_' + v] || 0)) + '%</td></tr>').join('') + '</table>';
const cells = L.geoJSON(D.cells, {{
  style: (f) => ({{ color: '#fff', weight: 1.5, fillColor: colour(f.properties.dominant),
                   fillOpacity: 0.15 + 0.55 * Math.max(0, (share(f.properties) - 0.3) / 0.7) }}),
  onEachFeature: (f, layer) => layer.bindPopup(counts(f.properties))
}}).addTo(map);
const maxn = Math.max(...D.points.features.map((f) => f.properties.n));
L.geoJSON(D.points, {{
  pointToLayer: (f, ll) => L.circleMarker(ll, {{ radius: 3 + 8 * Math.sqrt(f.properties.n / maxn), color: '#222', weight: 1,
                                                 fillColor: colour(f.properties.dominant), fillOpacity: 0.95 }}),
  onEachFeature: (f, layer) => layer.bindPopup(counts(f.properties)).bindTooltip(f.properties.place)
}}).addTo(map);
map.fitBounds(cells.getBounds(), {{ padding: [10, 10] }});
const legend = L.control({{ position: 'bottomleft' }});
legend.onAdd = () => {{
  const d = L.DomUtil.create('div', 'box');
  d.innerHTML = '<details open><summary>{escape(title)}</summary><p>{escape(description)}</p>' +
    D.variants.map((v) => '<span class="sw" style="background:' + colour(v) + '"></span>' + v).join('<br>') +
    '<p>colour: the dominant variant, paler where it is less dominant; click a place for its counts</p></details>';
  if (innerWidth < 600) d.querySelector('details').removeAttribute('open');
  L.DomEvent.disableClickPropagation(d);
  return d;
}};
legend.addTo(map);
</script></body></html>
"""
