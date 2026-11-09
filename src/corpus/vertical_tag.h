#pragma once

// One structure-tag line of a vertical file (CWB / Manatee / Korp VRT): <x a="v">, </x>, <x/>.
// Attribute values: double or single quotes; inside them Manatee's \" \' \\ and the XML
// entities &quot; &amp; &lt; &gt; &apos; (and &#N; / &#xH;) are decoded.

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace pando {

struct VerticalTag {
    enum class Kind { Open, Close, Empty };
    Kind kind = Kind::Open;
    std::string name;
    std::vector<std::pair<std::string, std::string>> attrs;
};

/// Parse a line that starts with '<' (not <!-- or <?). False when it is no tag (no name).
bool parse_vertical_tag(std::string_view line, VerticalTag& out);

/// A raw attribute value (between its quotes) with escapes and entities decoded.
std::string decode_vertical_value(std::string_view raw);

} // namespace pando
