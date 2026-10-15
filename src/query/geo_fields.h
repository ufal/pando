#pragma once
// tabulate / count fields on geolocations: lat(x), lon(x), geodist(x, y), geodist(x, lat, lon),
// geocell(x, step) (core/geo.h for the values they read).

#include "query/executor.h"
#include <optional>
#include <string>

namespace pando {

/// The field's value when `field` is one of these functions ("geocell(text_geo,0.5)"), "" when the
/// hit's value is not a location; nullopt when `field` is something else.
std::optional<std::string> evaluate_geo_tabulate_field(const Corpus& corpus, const Match& m,
                                                       const NameIndexMap& name_map, const std::string& field);

}  // namespace pando
