"""Counting with pando: each place's coordinates, and a variable's variants per place."""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field

from pando_cql import Corpus
from pando_geo.location import parse_geo
from pando_geo.atlas import Atlas, Variable


@dataclass
class PlaceCounts:
    """A variable at one place: its hits, and how many of them are each variant ("other": none)."""
    place: str
    lat: float
    lon: float
    counts: dict[str, int] = dc_field(default_factory=dict)

    @property
    def n(self) -> int:
        return sum(self.counts.values())

    def share(self, variant: str) -> float:
        return self.counts.get(variant, 0) / self.n if self.n else 0.0

    def dominant(self) -> str:
        return max(self.counts, key=lambda v: (self.counts[v], v != "other")) if self.counts else ""


def place_locations(corpus: Corpus, atlas: Atlas) -> dict[str, tuple[float, float]]:
    """place → (lat, lon), from the places' texts; places without coordinates are left out."""
    out = {}
    for row in corpus.run(f"[]; count by {atlas.place}, {atlas.geo}"):
        p = parse_geo(row.get(atlas.geo) or "")
        if p and row.get(atlas.place) not in (None, "", "_"):
            out.setdefault(row[atlas.place], p)
    return out


def count_variable(corpus: Corpus, atlas: Atlas, var: Variable,
                   locations: dict[str, tuple[float, float]]) -> list[PlaceCounts]:
    """The variable's variants per place (every place with a location), in place order."""
    per: dict[str, PlaceCounts] = {}
    q = var.query.strip().rstrip(";")
    for row in corpus.run(f"{q}; count by {atlas.place}, {var.field}"):
        place = row.get(atlas.place)
        if place not in locations:
            continue
        variant = var.variant_of(row.get(var.field) or "") or ("other" if var.variants else (row.get(var.field) or ""))
        pc = per.setdefault(place, PlaceCounts(place, *locations[place]))
        pc.counts[variant] = pc.counts.get(variant, 0) + int(row["freq"])
    return [per[p] for p in sorted(per)]
