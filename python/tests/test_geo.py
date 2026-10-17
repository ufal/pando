"""pando_geo: location values (as pando reads them), Voronoi cells, variant mapping, the atlas file."""

import math

import pytest

from pando_geo.atlas import Variable, load_atlas
from pando_geo.counts import PlaceCounts
from pando_geo.layers import point_layer, voronoi_layer
from pando_geo.location import parse_geo
from pando_geo.voronoi import LocalPlane, voronoi_cells


@pytest.mark.parametrize("text, want", [
    ("50.087,14.421", (50.087, 14.421)), ("50.087 14.421", (50.087, 14.421)), ("50.087;14.421", (50.087, 14.421)),
    ("geo:48.2082,16.3738;u=10", (48.2082, 16.3738)), ("geo:48.2,16.3,200", (48.2, 16.3)),
    ("POINT(13.405 52.52)", (52.52, 13.405)), ("51.5074 -0.1278", (51.5074, -0.1278)),
    ("somewhere", None), ("95,10", None), ("", None),
])
def test_parse_geo(text, want):
    assert parse_geo(text) == want


def _inside(ring, p):
    """Point in polygon (ray casting) on (lat, lon) rings."""
    lat, lon = p
    inside = False
    for (a_lat, a_lon), (b_lat, b_lon) in zip(ring, ring[1:]):
        if (a_lat > lat) != (b_lat > lat):
            x = a_lon + (lat - a_lat) * (b_lon - a_lon) / (b_lat - a_lat)
            if lon < x:
                inside = not inside
    return inside


SWISS = [(47.558, 7.588), (47.374, 8.541), (46.948, 7.452), (46.957, 8.366), (47.099, 9.063), (46.324, 7.988)]


def test_each_place_lies_in_its_own_cell():
    cells = voronoi_cells(SWISS)
    assert len(cells) == len(SWISS)
    for i, p in enumerate(SWISS):
        assert _inside(cells[i], p)
        assert not any(_inside(cells[j], p) for j in range(len(SWISS)) if j != i)


def test_cells_tile_the_box():
    plane = LocalPlane(SWISS)
    area = lambda ring: abs(sum(a[0] * b[1] - b[0] * a[1] for a, b in zip(ring, ring[1:]))) / 2
    cells = [[plane.xy(*p) for p in ring] for ring in voronoi_cells(SWISS, margin_km=10)]
    xy = [plane.xy(*p) for p in SWISS]
    w = max(p[0] for p in xy) - min(p[0] for p in xy) + 20
    h = max(p[1] for p in xy) - min(p[1] for p in xy) + 20
    assert math.isclose(sum(area(c) for c in cells), w * h, rel_tol=1e-6)


def test_places_at_one_location_share_a_cell():
    cells = voronoi_cells([(47.0, 8.0), (47.0, 8.0), (47.5, 8.5)])
    assert cells[0] == cells[1]


def test_variant_of_takes_the_first_full_match():
    v = Variable("nicht", "[]", variants={"nöd": "nö+d", "nid": "n[iì]+d"})
    assert [v.variant_of(x) for x in ("nöd", "nööd", "nìd", "nid", "nidd", "")] == ["nöd", "nöd", "nid", "nid", None, None]


def test_layers_put_counts_and_shares_on_each_feature():
    places = [PlaceCounts("Zürich", 47.374, 8.541, {"nöd": 8, "nid": 2}), PlaceCounts("Bern", 46.948, 7.452, {"nid": 9, "other": 1})]
    pts = point_layer(places, ["nöd", "nid"], "nicht")
    f = pts["features"][0]
    assert f["geometry"]["coordinates"] == [8.541, 47.374]          # GeoJSON: longitude first
    want = {"place": "Zürich", "n": 10, "dominant": "nöd", "share_nöd": 0.8, "count_nid": 2}
    assert {k: f["properties"][k] for k in want} == want
    vor = voronoi_layer(places, ["nöd", "nid"], "nicht")
    assert vor["features"][1]["properties"]["dominant"] == "nid"
    ring = vor["features"][1]["geometry"]["coordinates"][0]
    assert ring[0] == ring[-1] and len(ring) >= 4


def test_load_atlas(tmp_path):
    p = tmp_path / "a.toml"
    p.write_text('[corpus]\nindex = "~/x/pando"\nmin_n = 5\n[[variable]]\nname = "nicht"\nquery = \'[nform="nicht"]\'\n'
                 '[variable.variants]\n"nöd" = "nö+d"\n', encoding="utf-8")
    a = load_atlas(str(p))
    assert a.index.endswith("/x/pando") and not a.index.startswith("~") and a.min_n == 5
    assert a.variables[0].variants == {"nöd": "nö+d"}
    p.write_text('[corpus]\nindex = "x"\n[[variable]]\nname = "bad name"\nquery = "[]"\n')
    with pytest.raises(ValueError, match="letters, digits"):
        load_atlas(str(p))


def test_webmap_is_one_page_with_the_layers():
    from pando_geo.webmap import html
    places = [PlaceCounts("Zürich", 47.374, 8.541, {"nöd": 8, "nid": 2}), PlaceCounts("Bern", 46.948, 7.452, {"nid": 9})]
    page = html(point_layer(places, ["nöd", "nid"], "nicht"), voronoi_layer(places, ["nöd", "nid"], "nicht"),
                ["nöd", "nid"], "nicht", "Swiss German 'nicht' </script>")
    assert page.startswith("<!doctype html>") and "leaflet@1.9.4" in page and "tile.openstreetmap.org" in page
    assert '"place": "Zürich"' in page and "</script>" not in page.split("const D =")[1].split("\n")[0]
