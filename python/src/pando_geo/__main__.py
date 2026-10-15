"""python -m pando_geo ATLAS.toml [--out DIR] [--variable NAME …]

For each variable of the atlas: counts its variants per place with pando, and writes to DIR
NAME_points.geojson, NAME_voronoi.geojson, NAME.svg (a preview) and NAME.html (a web map on
OpenStreetMap); prints a table per place."""

from __future__ import annotations

import argparse
import os

from pando_cql import Corpus
from pando_geo import layers, preview, webmap
from pando_geo.atlas import load_atlas
from pando_geo.counts import count_variable, place_locations


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(prog="python -m pando_geo", description=__doc__.split("\n\n")[0])
    ap.add_argument("atlas")
    ap.add_argument("--out", default="maps")
    ap.add_argument("--variable", action="append", help="only these variables (repeatable)")
    a = ap.parse_args(argv)
    atlas = load_atlas(a.atlas)
    os.makedirs(a.out, exist_ok=True)
    with Corpus.open(atlas.index, pando_bin=atlas.pando_bin) as corpus:
        locations = place_locations(corpus, atlas)
        print(f"{len(locations)} places with coordinates")
        for var in atlas.variables:
            if a.variable and var.name not in a.variable:
                continue
            places = [pc for pc in count_variable(corpus, atlas, var, locations) if pc.n >= atlas.min_n]
            variants = list(var.variants) or sorted({v for pc in places for v in pc.counts})
            if not places:
                print(f"\n{var.name}: no place with at least {atlas.min_n} hits")
                continue
            pts, cells = layers.point_layer(places, variants, var.name), layers.voronoi_layer(places, variants, var.name)
            for kind, layer in (("points", pts), ("voronoi", cells)):
                layers.write(layer, os.path.join(a.out, f"{var.name}_{kind}.geojson"))
            with open(os.path.join(a.out, f"{var.name}.html"), "w", encoding="utf-8") as f:
                f.write(webmap.html(pts, cells, variants, var.name, var.description or var.query))
            with open(os.path.join(a.out, f"{var.name}.svg"), "w", encoding="utf-8") as f:
                f.write(preview.svg(places, variants, f"{var.name}: {var.description or var.query}"))
            print(f"\n{var.name}: {len(places)} places (at least {atlas.min_n} hits), {sum(pc.n for pc in places):,} hits")
            print(f"  {'place':24s}{'n':>6s}  " + "".join(f"{v:>8s}" for v in variants + ['other']))
            for pc in sorted(places, key=lambda p: (p.dominant(), -p.share(p.dominant()))):
                print(f"  {pc.place[:23]:24s}{pc.n:6d}  " + "".join(f"{pc.share(v):8.0%}" for v in variants + ['other']))
    print(f"\nLayers and previews in {os.path.abspath(a.out)}")


if __name__ == "__main__":
    main()
