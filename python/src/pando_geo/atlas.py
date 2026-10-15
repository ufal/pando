"""The atlas definition: which corpus, where places and their coordinates are, which variables.

    [corpus]
    index = "/path/to/project/pando"
    place = "text_place"        # the attribute that names a place (a region attribute)
    geo = "text_geo"            # its coordinates ("lat lon", "lat,lon", geo: URI or WKT POINT)
    min_n = 10                  # places with fewer hits of a variable are left out of its maps

    [[variable]]
    name = "nicht"
    query = '[nform="nicht"]'   # the hits that vary
    field = "form"              # their value that is the variant
    [variable.variants]         # variant → pattern for the field's values (a full match);
    "nöd" = "nö+d"             # (quote keys with letters beyond ASCII); values matching none: "other"
    nid = "ni+d|nì+d"
"""

from __future__ import annotations

import os
import re

try:
    import tomllib                     # Python 3.11+
except ImportError:                    # older Pythons: pip install tomli (the extra "geo")
    import tomli as tomllib
from dataclasses import dataclass, field as dc_field
from typing import Optional


@dataclass
class Variable:
    name: str
    query: str
    field: str = "form"
    variants: dict[str, str] = dc_field(default_factory=dict)
    description: str = ""

    def variant_of(self, value: str) -> Optional[str]:
        """The variant a field value belongs to (the first that matches), or None."""
        for name, pattern in self.variants.items():
            if re.fullmatch(pattern, value or ""):
                return name
        return None


@dataclass
class Atlas:
    index: str
    place: str = "text_place"
    geo: str = "text_geo"
    min_n: int = 10
    pando_bin: Optional[str] = None
    variables: list[Variable] = dc_field(default_factory=list)


def load_atlas(path: str) -> Atlas:
    with open(path, "rb") as f:
        d = tomllib.load(f)
    c = d.get("corpus", {})
    if "index" not in c:
        raise ValueError(f"{path}: [corpus] needs index = the pando index folder")
    variables = []
    for v in d.get("variable", []):
        if not v.get("name") or not v.get("query"):
            raise ValueError(f"{path}: every [[variable]] needs name and query")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", v["name"]):
            raise ValueError(f"{path}: variable name '{v['name']}': letters, digits and _ only (it names the files)")
        for vn, pat in v.get("variants", {}).items():
            re.compile(pat)
        variables.append(Variable(v["name"], v["query"], v.get("field", "form"), dict(v.get("variants", {})), v.get("description", "")))
    return Atlas(os.path.expanduser(c["index"]), c.get("place", "text_place"), c.get("geo", "text_geo"), int(c.get("min_n", 10)),
                 c.get("pando_bin"), variables)
