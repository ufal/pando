#include "corpus/corefud_entity.h"

#include <vector>

namespace pando {

namespace {

std::vector<std::string_view> split_dash(std::string_view s) {
    std::vector<std::string_view> out;
    size_t a = 0;
    while (true) {
        const size_t b = s.find('-', a);
        out.push_back(s.substr(a, b == std::string_view::npos ? std::string_view::npos : b - a));
        if (b == std::string_view::npos) return out;
        a = b + 1;
    }
}

}  // namespace

void CorefEntityReader::set_format(std::string_view global_entity) {
    const auto fields = split_dash(global_entity);
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i] == "eid") eid_field_ = static_cast<int>(i);
        if (fields[i] == "head") head_field_ = static_cast<int>(i);
    }
}

std::string CorefEntityReader::on_token(CorpusPos pos, std::string_view misc) {
    // the Entity= value inside MISC (fields separated by '|')
    std::string_view ent;
    for (size_t a = 0; a < misc.size();) {
        size_t b = misc.find('|', a);
        if (b == std::string_view::npos) b = misc.size();
        const std::string_view f = misc.substr(a, b - a);
        if (f.rfind("Entity=", 0) == 0) ent = f.substr(7);
        a = b + 1;
    }
    if (!ent.empty()) {
        seen_ = true;
        // each '(' opens a mention: its spec runs to the next '(' or ')'
        for (size_t i = 0; i < ent.size(); ++i) {
            if (ent[i] != '(') continue;
            size_t j = i + 1;
            while (j < ent.size() && ent[j] != '(' && ent[j] != ')') ++j;
            const auto fields = split_dash(ent.substr(i + 1, j - i - 1));
            if (static_cast<int>(fields.size()) <= eid_field_) continue;
            std::string_view eid = fields[static_cast<size_t>(eid_field_)];
            if (const size_t br = eid.find('['); br != std::string_view::npos) eid = eid.substr(0, br);   // e5[1/2]
            int head = 1;
            if (static_cast<int>(fields.size()) > head_field_) {
                int h = 0;
                bool ok = !fields[static_cast<size_t>(head_field_)].empty();
                for (char c : fields[static_cast<size_t>(head_field_)]) {
                    if (c < '0' || c > '9') { ok = false; break; }
                    h = h * 10 + (c - '0');
                }
                if (ok && h > 0) head = h;
            }
            if (eid.empty()) continue;
            std::string& ids = heads_[pos + head - 1];
            ids += (ids.empty() ? "" : "|") + std::string(eid);
            ++mentions_;
        }
    }
    std::string out;
    auto it = heads_.begin();
    while (it != heads_.end() && it->first <= pos) {   // a head before pos was never reached (malformed): dropped
        if (it->first == pos) out = std::move(it->second);
        it = heads_.erase(it);
    }
    return out;
}

}  // namespace pando
