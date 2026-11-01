#include "query/date_notes.h"

#include "core/json_utils.h"

#include <algorithm>
#include <map>
#include <set>
#include <unordered_set>

namespace pando {

namespace {

// The attribute a spec reads: text_date from match.text_date, a.text_date or text_date.
std::string attr_of(const std::string& spec) {
    const size_t dot = spec.find('.');
    return dot == std::string::npos ? spec : spec.substr(dot + 1);
}

void add_call(const GlobalFuncCall& fc, std::vector<DateUse>& out) {
    std::optional<DatePart> part;
    switch (fc.func) {
        case GlobalFunctionType::YEAR: part = DatePart::Year; break;
        case GlobalFunctionType::CENTURY: part = DatePart::Century; break;
        case GlobalFunctionType::DECADE: part = DatePart::Decade; break;
        case GlobalFunctionType::MONTH: part = DatePart::Month; break;
        case GlobalFunctionType::WEEK: part = DatePart::Week; break;
        case GlobalFunctionType::DAY: part = DatePart::Day; break;
        case GlobalFunctionType::EARLIEST: part = DatePart::Earliest; break;
        case GlobalFunctionType::LATEST: part = DatePart::Latest; break;
        default: break;
    }
    if (part && fc.args.size() == 1) out.push_back({fc.args[0], *part});
}

void add_query(const TokenQuery& q, std::vector<DateUse>& out) {
    for (const auto& f : q.global_function_filters) {
        add_call(f.lhs, out);
        if (f.has_rhs_func) add_call(f.rhs, out);
    }
}

// The distinct values of an attribute (a region attribute's lexicon, else its regions; a token
// attribute's lexicon); false when the corpus has no such attribute.
bool distinct_values(const Corpus& corpus, const std::string& attr, std::vector<std::string>& out) {
    constexpr size_t kMax = 1000000;
    if (corpus.has_attr(attr)) {
        const auto& lex = corpus.attr(attr).lexicon();
        for (LexiconId id = 0; id < lex.size() && out.size() < kMax; ++id) out.emplace_back(lex.get(id));
        return true;
    }
    RegionAttrParts parts;
    if (!split_region_attr_name(attr, parts) || !corpus.has_structure(parts.struct_name)) return false;
    const auto& sa = corpus.structure(parts.struct_name);
    auto key = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
    if (!key) return false;
    if (sa.has_region_value_reverse(*key)) {
        const LexiconId n = sa.region_attr_lex_size(*key);
        for (LexiconId id = 0; id < n && out.size() < kMax; ++id) out.emplace_back(sa.region_attr_lex_get(*key, id));
        return true;
    }
    std::unordered_set<std::string> seen;
    for (size_t i = 0; i < sa.region_count() && seen.size() < kMax; ++i) seen.emplace(sa.region_value(*key, i));
    out.assign(seen.begin(), seen.end());
    std::sort(out.begin(), out.end());
    return true;
}

std::string examples(const std::vector<std::string>& vals) {
    std::string s;
    for (size_t i = 0; i < vals.size() && i < 3; ++i) {
        std::string v = vals[i].size() > 40 ? vals[i].substr(0, 40) + "…" : vals[i];
        s += (i ? ", '" : "'") + v + "'";
    }
    return s;
}

const char* part_noun(DatePart p) {
    switch (p) {
        case DatePart::Year: return "a year";
        case DatePart::Century: return "a century";
        case DatePart::Decade: return "a decade";
        case DatePart::Month: return "a month";
        case DatePart::Week: return "a week";
        case DatePart::Day: return "a day";
        case DatePart::Earliest: return "an earliest year";
        case DatePart::Latest: return "a latest year";
        case DatePart::Precision: return "a precision";
    }
    return "a value";
}

}  // namespace

std::vector<DateUse> date_uses(const Program& prog) {
    std::vector<DateUse> out;
    for (const auto& st : prog) {
        if (st.has_query) add_query(st.query, out);
        if (st.is_parallel) add_query(st.target_query, out);
        if (!st.has_command) continue;
        for (const auto& f : st.command.fields) {
            const size_t open = f.find('(');
            if (open == std::string::npos || open == 0 || f.back() != ')') continue;
            if (auto part = date_part_of_function(std::string_view(f).substr(0, open)))
                out.push_back({f.substr(open + 1, f.size() - open - 2), *part});
        }
    }
    return out;
}

std::vector<std::string> date_value_notes(const Corpus& corpus, const std::vector<DateUse>& uses) {
    std::map<std::string, std::set<DatePart>> by_attr;   // in a fixed order: the notes are stable
    for (const auto& u : uses) by_attr[attr_of(u.spec)].insert(u.part);
    std::vector<std::string> notes;
    for (const auto& [attr, parts] : by_attr) {
        std::vector<std::string> vals;
        if (!distinct_values(corpus, attr, vals) || vals.empty()) continue;
        std::vector<std::string> legacy, none;
        std::map<DatePart, std::vector<std::string>> undetermined;
        for (const auto& v : vals) {
            const DateValue& d = parse_date_cached(v);
            if (!d.ok()) { none.push_back(v); continue; }
            if (d.source == DateSource::Legacy) legacy.push_back(v);
            for (DatePart p : parts)
                if (p != DatePart::Precision && !date_part_value(d, p)) undetermined[p].push_back(v);
        }
        const std::string of = " of " + std::to_string(vals.size()) + " values";
        if (!legacy.empty())
            notes.push_back(attr + ": " + std::to_string(legacy.size()) + of + " are not EDTF and were read as before ("
                            + examples(legacy) + "); converting them (flexiconv) states what they mean");
        if (!none.empty())
            notes.push_back(attr + ": " + std::to_string(none.size()) + of + " are no date and count as none ("
                            + examples(none) + ")");
        for (const auto& [p, list] : undetermined) {
            std::string why = p == DatePart::Earliest ? " (an open start)"
                              : p == DatePart::Latest ? " (an open end)"
                                                      : "; earliest() and latest() give their range";
            notes.push_back(std::string(date_part_function_name(p)) + "(" + attr + "): " + std::to_string(list.size())
                            + of + " do not give " + part_noun(p) + " (" + examples(list) + ")" + why);
        }
    }
    return notes;
}

std::string with_result_warnings(std::string json, const std::vector<std::string>& notes) {
    if (notes.empty()) return json;
    std::string items;
    for (size_t i = 0; i < notes.size(); ++i) items += (i ? ", " : "") + jstr(notes[i]);
    const size_t res = json.find("\"result\"");
    const size_t open = res == std::string::npos ? std::string::npos : json.find_first_not_of(" \t\r\n:", res + 8);
    if (open != std::string::npos && json[open] == '{') {
        // into result.warnings: the list the answer has, or a new one
        const size_t w = json.find("\"warnings\": [", open);
        if (w != std::string::npos) {
            const size_t lb = w + 13;
            const bool empty = json.compare(lb, 1, "]") == 0;
            json.insert(lb, items + (empty ? "" : ", "));
            return json;
        }
        const bool empty_obj = json.find_first_not_of(" \t\r\n", open + 1) != std::string::npos
                               && json[json.find_first_not_of(" \t\r\n", open + 1)] == '}';
        json.insert(open + 1, "\"warnings\": [" + items + "]" + (empty_obj ? "" : ", "));
        return json;
    }
    // a result that is no object (a number, a list): the answer's own warnings
    const size_t brace = json.find('{');
    if (brace != std::string::npos) json.insert(brace + 1, "\"warnings\": [" + items + "], ");
    return json;
}

}  // namespace pando
