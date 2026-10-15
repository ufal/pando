#include "query/geo_fields.h"

#include "core/geo.h"
#include <cmath>
#include <stdexcept>
#include <vector>

namespace pando {

std::optional<std::string> evaluate_geo_tabulate_field(const Corpus& corpus, const Match& m,
                                                       const NameIndexMap& name_map, const std::string& field) {
    const size_t open = field.find('(');
    if (open == std::string::npos || field.back() != ')') return std::nullopt;
    const std::string fn = field.substr(0, open);
    if (fn != "lat" && fn != "lon" && fn != "geodist" && fn != "geocell") return std::nullopt;

    std::vector<std::string> args;   // the parser writes them without spaces: geocell(text_geo,0.5)
    std::string inner = field.substr(open + 1, field.size() - open - 2);
    for (size_t start = 0; start <= inner.size();) {
        size_t comma = inner.find(',', start);
        if (comma == std::string::npos) comma = inner.size();
        args.push_back(inner.substr(start, comma - start));
        start = comma + 1;
    }
    auto location = [&](const std::string& spec) -> std::optional<GeoPoint> {
        if (geo_parse_number(spec))
            throw std::runtime_error("Tabulate field '" + field + "': a location (an attribute such as text_geo) is needed, not a number");
        return parse_geo(read_tabulate_field(corpus, m, name_map, spec));
    };
    auto number = [&](const std::string& s) {
        auto v = geo_parse_number(s);
        if (!v) throw std::runtime_error("Tabulate field '" + field + "': '" + s + "' is not a number");
        return *v;
    };

    if (fn == "lat" || fn == "lon") {
        if (args.size() != 1) throw std::runtime_error("Tabulate field '" + field + "': " + fn + "(x) takes one location");
        auto p = location(args[0]);
        return p ? geo_number(fn == "lat" ? p->lat : p->lon) : std::string();
    }
    if (fn == "geocell") {
        if (args.size() != 2) throw std::runtime_error("Tabulate field '" + field + "': geocell(x, step) takes a location and a cell size in degrees");
        const double step = number(args[1]);
        if (!(step > 0) || step > 90) throw std::runtime_error("Tabulate field '" + field + "': the cell size is in degrees, more than 0 and at most 90");
        auto p = location(args[0]);
        return p ? geo_cell(*p, step) : std::string();
    }
    // geodist: km to a point, or between two locations; to the metre
    if (args.size() != 2 && args.size() != 3)
        throw std::runtime_error("Tabulate field '" + field + "': geodist(x, y) or geodist(x, lat, lon)");
    auto a = location(args[0]);
    if (!a) return std::string();
    std::optional<GeoPoint> b = args.size() == 3 ? std::optional<GeoPoint>(GeoPoint{number(args[1]), number(args[2])}) : location(args[1]);
    if (!b) return std::string();
    return geo_number(std::round(geo_distance_km(*a, *b) * 1000.0) / 1000.0);
}

}  // namespace pando
