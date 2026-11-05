#pragma once

// The on-disk layout of an index directory (corpus.info `layout_version=`).
//
// Bump kIndexLayoutVersion when a core file (lexicons, .dat, .rev, regions, dep.*) changes
// so that an older pando would misread it; raise kIndexLayoutOldest when this build can no
// longer read an older layout (the index must be rebuilt). Derived files that `pando-index
// --upgrade` can (re)build are not part of it: their staleness is checked per file
// (index_status_json_fields). An index without the line is layout 1 (every index before it
// was recorded).

namespace pando {

constexpr int kIndexLayoutVersion = 1;   // what this build writes
constexpr int kIndexLayoutOldest = 1;    // the oldest layout this build reads

}  // namespace pando
