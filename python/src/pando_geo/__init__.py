"""pando_geo: geolinguistic map layers from a pando corpus (on pando_cql).

An atlas definition (TOML) names variables, each a query with competing variants; pando_geo counts
them per place with pando and writes GIS layers (GeoJSON, EPSG:4326) — points per place and Voronoi
cells — plus an SVG preview. No dependencies beyond pando_cql and the standard library.

    python -m pando_geo atlas.toml --out maps/
"""

from pando_geo.atlas import Atlas, Variable, load_atlas
from pando_geo.counts import PlaceCounts, count_variable, place_locations
from pando_geo.voronoi import voronoi_cells

__all__ = ["Atlas", "Variable", "load_atlas", "PlaceCounts", "count_variable", "place_locations", "voronoi_cells"]
__version__ = "0.1.0"
