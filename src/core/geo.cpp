#include "core/geo.h"

#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>

namespace pando {

namespace {

std::string_view trim(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
    return s;
}

bool in_range(const GeoPoint& p) {
    return std::isfinite(p.lat) && std::isfinite(p.lon) && p.lat >= -90 && p.lat <= 90 && p.lon >= -180 && p.lon <= 180;
}

// two numbers separated by a comma, a semicolon or white space
std::optional<std::pair<double, double>> two_numbers(std::string_view s) {
    s = trim(s);
    size_t sep = s.find_first_of(",; \t");
    if (sep == std::string_view::npos) return std::nullopt;
    auto a = geo_parse_number(s.substr(0, sep));
    std::string_view rest = trim(s.substr(sep));
    if (!rest.empty() && (rest.front() == ',' || rest.front() == ';')) rest = trim(rest.substr(1));
    auto b = geo_parse_number(rest);
    if (!a || !b) return std::nullopt;
    return std::make_pair(*a, *b);
}

bool starts_with_ci(std::string_view s, std::string_view prefix) {
    if (s.size() < prefix.size()) return false;
    for (size_t i = 0; i < prefix.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(s[i])) != prefix[i]) return false;
    return true;
}

}  // namespace

std::optional<double> geo_parse_number(std::string_view s) {
    s = trim(s);
    if (s.empty() || s.size() > 40) return std::nullopt;
    std::string buf(s);
    char* end = nullptr;
    double v = std::strtod(buf.c_str(), &end);
    if (end == buf.c_str() || *end != '\0' || !std::isfinite(v)) return std::nullopt;
    return v;
}

std::optional<GeoPoint> parse_geo(std::string_view text) {
    std::string_view s = trim(text);
    if (s.empty()) return std::nullopt;
    GeoPoint p;
    if (starts_with_ci(s, "point")) {            // WKT: POINT(lon lat)
        size_t open = s.find('('), close = s.rfind(')');
        if (open == std::string_view::npos || close == std::string_view::npos || close < open) return std::nullopt;
        auto xy = two_numbers(s.substr(open + 1, close - open - 1));
        if (!xy) return std::nullopt;
        p.lon = xy->first;
        p.lat = xy->second;
    } else {
        if (starts_with_ci(s, "geo:")) {          // geo URI (RFC 5870): lat,lon[,alt][;params]
            s = s.substr(4);
            size_t semi = s.find(';');
            if (semi != std::string_view::npos) s = s.substr(0, semi);
            size_t c1 = s.find(','), c2 = c1 == std::string_view::npos ? c1 : s.find(',', c1 + 1);
            if (c2 != std::string_view::npos) s = s.substr(0, c2);   // drop the altitude
        }
        auto ll = two_numbers(s);
        if (!ll) return std::nullopt;
        p.lat = ll->first;
        p.lon = ll->second;
    }
    if (!in_range(p)) return std::nullopt;
    return p;
}

double geo_distance_km(const GeoPoint& a, const GeoPoint& b) {
    constexpr double kEarthKm = 6371.0088;
    constexpr double kRad = 3.14159265358979323846 / 180.0;
    const double dlat = (b.lat - a.lat) * kRad, dlon = (b.lon - a.lon) * kRad;
    const double h = std::sin(dlat / 2) * std::sin(dlat / 2)
        + std::cos(a.lat * kRad) * std::cos(b.lat * kRad) * std::sin(dlon / 2) * std::sin(dlon / 2);
    return 2 * kEarthKm * std::asin(std::sqrt(std::min(1.0, h)));
}

std::string geo_number(double v) {
    if (std::fabs(v) < 5e-7) v = 0;   // no "-0"
    char buf[48];
    std::snprintf(buf, sizeof buf, "%.6f", v);
    std::string s(buf);
    while (!s.empty() && s.back() == '0') s.pop_back();
    if (!s.empty() && s.back() == '.') s.pop_back();
    return s;
}

std::string geo_cell(const GeoPoint& p, double step) {
    if (!(step > 0) || step > 90) return "";
    auto centre = [&](double v, double lo, double hi) {
        double c = (std::floor((v - lo) / step) + 0.5) * step + lo;
        return std::min(c, hi);
    };
    return geo_number(centre(p.lat, -90, 90)) + "," + geo_number(centre(p.lon, -180, 180));
}

}  // namespace pando
