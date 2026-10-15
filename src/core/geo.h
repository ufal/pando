#pragma once
// Geolocations in attribute values (e.g. a text's place, `text_geo`), read the way date functions
// read dates: leniently, and a value that is not a location gives no value (the hit has none).
// Used by the query functions lat(), lon(), geodist() and geocell() (PANDO-CQL-Reference.md).

#include <optional>
#include <string>
#include <string_view>

namespace pando {

struct GeoPoint {
    double lat = 0;   // degrees north, -90 … 90
    double lon = 0;   // degrees east, -180 … 180
};

/// A location from an attribute value: "50.087,14.421", "50.087 14.421", "50.087;14.421",
/// "geo:50.087,14.421" (latitude first), or WKT "POINT(14.421 50.087)" (longitude first, as WKT
/// has it). nullopt for anything else, and for coordinates out of range.
std::optional<GeoPoint> parse_geo(std::string_view text);

/// Great-circle distance in kilometres (haversine, mean Earth radius 6371.0088 km).
double geo_distance_km(const GeoPoint& a, const GeoPoint& b);

/// The grid cell of `p` for cells of `step` degrees (0 < step ≤ 90), as its centre "lat,lon",
/// without trailing zeros: geocell(50.087,14.421 / 0.5) = "50.25,14.25".
std::string geo_cell(const GeoPoint& p, double step);

/// A number as the query functions show it: up to 6 decimals, no trailing zeros.
std::string geo_number(double v);

/// A number argument of a query function ("50.08", "-3.7"); nullopt when it is not one.
std::optional<double> geo_parse_number(std::string_view s);

}  // namespace pando
