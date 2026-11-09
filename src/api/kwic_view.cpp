// KonText's KWIC lines over a pando index: manatee-open's KWICLines (concord/concget.cc) and its
// contexts (concord/concctx.cc), step by step. See kwic_view.h.
#include "api/kwic_view.h"

#include "core/json_utils.h"
#include "index/structural_attr.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <list>
#include <memory>
#include <optional>
#include <sstream>
#include <vector>

namespace pando {

KwicViewSpec parse_kwic_view(const std::string& raw) {
    KwicViewSpec s;
    if (raw.empty() || raw[0] != '{') return s;
    s.on = true;
    auto field = [&](const char* key, std::string& into) {
        if (json_value_pos(raw, key) != std::string::npos) into = json_extract_str(raw, key);
    };
    field("left", s.left);
    field("right", s.right);
    field("attrs", s.attrs);
    field("ctxattrs", s.ctxattrs);
    field("structs", s.structs);
    field("refs", s.refs);
    s.maxctx = static_cast<int>(json_extract_num(raw, "maxctx", 0));
    return s;
}

namespace {

std::vector<std::string> split_commas(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream in(s);
    std::string item;
    while (std::getline(in, item, ',')) out.push_back(item);
    return out;
}

size_t utf8_len(std::string_view s) {
    size_t n = 0;
    for (unsigned char c : s)
        if ((c & 0xC0) != 0x80) ++n;
    return n;
}

// ---- a structure as Manatee's `ranges` (begin inclusive, end exclusive) ----
struct Rng {
    const StructuralAttr* sa;
    int64_t size() const { return static_cast<int64_t>(sa->region_count()); }
    CorpusPos beg_at(int64_t n) const { return sa->get(static_cast<size_t>(n)).start; }
    CorpusPos end_at(int64_t n) const { return sa->get(static_cast<size_t>(n)).end + 1; }
    int64_t num_at_pos(CorpusPos p) const { return sa->find_region(p); }
    // the first region whose (exclusive) end is past `pos` (Manatee's num_next_pos)
    int64_t num_next_pos(CorpusPos pos) const {
        const int64_t in = sa->find_region(pos);
        if (in >= 0) return in;
        size_t lo = 0, hi = sa->region_count();
        const Region* r = sa->region_data();
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (r[mid].start <= pos) lo = mid + 1;
            else hi = mid;
        }
        return static_cast<int64_t>(lo);
    }
};

// ---- contexts (concctx.cc) ----
struct Ctx {
    virtual ~Ctx() = default;
    int chars = 0;   // `#`: a context in characters
    virtual CorpusPos get(CorpusPos kwbeg, CorpusPos kwend) const = 0;
};

struct Fun {
    virtual ~Fun() = default;
    virtual CorpusPos operator()(CorpusPos pos, int64_t size) const = 0;
};

struct AddPos : Fun {
    int64_t delta;
    explicit AddPos(int64_t d) : delta(d) {}
    CorpusPos operator()(CorpusPos pos, int64_t) const override { return pos + delta; }
};

struct StructBeg : Fun {
    Rng rng; int64_t delta;
    StructBeg(Rng r, int64_t d) : rng(r), delta(d) {}
    CorpusPos operator()(CorpusPos pos, int64_t size) const override {
        int64_t n = rng.num_at_pos(pos);
        if (n == -1) return pos - 15;
        CorpusPos p = rng.beg_at(std::min(std::max<int64_t>(0, n + delta), rng.size() - 1));
        if (p == pos && size == -1) {
            n = rng.num_at_pos(pos - 1);
            if (n == -1) return pos - 15;
            return rng.beg_at(std::min(std::max<int64_t>(0, n + delta), rng.size() - 1));
        }
        return p;
    }
};

struct StructEnd : Fun {
    Rng rng; int64_t delta;
    StructEnd(Rng r, int64_t d) : rng(r), delta(d) {}
    CorpusPos operator()(CorpusPos pos, int64_t size) const override {
        int64_t n = rng.num_at_pos(pos);
        if (n == -1) return pos + 15;
        CorpusPos p = rng.end_at(std::min(std::max<int64_t>(0, n + delta), rng.size() - 1)) - 1;
        if (p == pos && size == -1) {
            n = rng.num_at_pos(pos + 1);
            if (n == -1) return pos + 15;
            return rng.end_at(std::min(std::max<int64_t>(0, n + delta), rng.size() - 1)) - 1;
        }
        return p;
    }
};

// simple_context (no labels in pando's hits here: collnum is read as 0)
struct Simple : Ctx {
    bool begin;
    std::unique_ptr<Fun> fun;
    Simple(bool b, Fun* f, int ch = 0) : begin(b), fun(f) { chars = ch; }
    CorpusPos get(CorpusPos kwbeg, CorpusPos kwend) const override {
        const CorpusPos beg = kwbeg;
        const CorpusPos end = std::max<CorpusPos>(0, kwend - 1);
        const int64_t size = end - beg;
        return begin ? (*fun)(beg, size) : (*fun)(end, size);
    }
};

struct MinMax : Ctx {
    std::unique_ptr<Ctx> a, b;
    bool is_max;
    MinMax(Ctx* x, Ctx* y, bool mx) : a(x), b(y), is_max(mx) {}
    CorpusPos get(CorpusPos kwbeg, CorpusPos kwend) const override {
        const CorpusPos x = a->get(kwbeg, kwend), y = b->get(kwbeg, kwend);
        return is_max ? std::max(x, y) : std::min(x, y);
    }
};

std::unique_ptr<Ctx> prepare_context(const Corpus& corpus, const std::string& ctxstr, bool toleft, int maxctx) {
    const char* s = ctxstr.c_str();
    if (s[0] == 'a') return std::make_unique<Simple>(toleft, new AddPos(0));   // aligned segment: no parallel here
    long num = std::atol(s);
    if (std::strchr(s, '#') != nullptr) {
        if (num == 0) return std::make_unique<Simple>(toleft, new AddPos(0));
        if (num < 0) num = -num;
        long npos = num / 2 + 1;
        if (maxctx && npos > maxctx) npos = maxctx;
        return std::make_unique<Simple>(toleft, new AddPos(toleft ? -npos : npos), static_cast<int>(num));
    }
    bool fromdir = toleft;
    const char* p;
    if ((p = std::strchr(s, '<')) != nullptr) fromdir = true;            // a label: read as the hit
    else if ((p = std::strchr(s, '>')) != nullptr) fromdir = false;
    if ((p = std::strchr(s, ':')) != nullptr) {
        std::string name;
        for (++p; std::isalpha(static_cast<unsigned char>(*p)); ++p) name += *p;
        if (!corpus.has_structure(name))
            return std::make_unique<Simple>(toleft, new AddPos(toleft ? -3 : 3));
        if (num == 0) return std::make_unique<Simple>(fromdir, new AddPos(0));
        const bool numgt0 = num > 0;
        if (num < 0) num++;
        if (num > 0) num--;
        Rng rng{&corpus.structure(name)};
        Ctx* c = toleft ? static_cast<Ctx*>(new Simple(fromdir, new StructBeg(rng, num)))
                        : static_cast<Ctx*>(new Simple(fromdir, new StructEnd(rng, num)));
        if (!maxctx) return std::unique_ptr<Ctx>(c);
        if (numgt0) return std::make_unique<MinMax>(c, new Simple(fromdir, new AddPos(maxctx)), false);
        return std::make_unique<MinMax>(c, new Simple(fromdir, new AddPos(-maxctx)), true);
    }
    if (maxctx) {
        if (num > maxctx) {
            num = maxctx;
            if (toleft) num++;
        } else if (num <= -maxctx) {
            num = -maxctx - 1;
            if (toleft) num++;
        }
    }
    return std::make_unique<Simple>(fromdir, new AddPos(num));
}

// ---- attributes, structures, refs (concget.cc) ----
const PositionalAttr* find_attr(const Corpus& corpus, const std::string& name) {
    if (corpus.has_attr(name)) return &corpus.attr(name);
    if (name == "word" && corpus.has_attr("form")) return &corpus.attr("form");   // pando's word
    return nullptr;
}

std::vector<const PositionalAttr*> split_attributes(const Corpus& corpus, const std::string& list) {
    std::vector<const PositionalAttr*> out;
    for (const std::string& a : split_commas(list)) {
        if (a.empty()) continue;
        if (const PositionalAttr* pa = find_attr(corpus, a)) out.push_back(pa);   // ignore_nondef
    }
    return out;
}

std::string region_value(const StructuralAttr& sa, const std::string& sname, const std::string& attr, int64_t n) {
    auto key = resolve_region_attr_key(sa, sname, attr);
    if (!key) return "";
    return std::string(sa.region_value(*key, static_cast<size_t>(n)));
}

enum class Ev { BEG, END, BKW, EKW, STRUCT, BTAG, ETAG, TEXT };

struct PosEvent {
    CorpusPos pos;
    int sub;
    Ev type;
    std::string value;
};

struct OutStruct {
    std::string name;
    const StructuralAttr* sa;
    std::vector<std::string> attrs;
    void add_attr(const std::string& a) {
        if (std::find(attrs.begin(), attrs.end(), a) == attrs.end()) attrs.push_back(a);
    }
    void add_all() {
        attrs.clear();
        for (const std::string& a : sa->region_attr_names()) attrs.push_back(a);
    }
    std::string begstr(int64_t num) const {
        std::string r = "<" + name;
        for (const std::string& a : attrs) r += " " + a + "=" + region_value(*sa, name, a, num);
        return r;
    }
    // DISPLAYTAG on, no DISPLAYCLASS / DISPLAYBEGIN / DISPLAYEND, not nested (see kwic_view.h)
    void add_struct_events(int priority, CorpusPos fpos, CorpusPos tpos, std::vector<PosEvent>& events) const {
        Rng rng{sa};
        for (int64_t num = rng.num_next_pos(fpos); num < rng.size(); ++num) {
            const CorpusPos b = rng.beg_at(num), e = rng.end_at(num);
            const int nest = 0;
            if (b > tpos) break;
            if (b >= fpos) events.push_back({b, priority + nest, Ev::STRUCT, begstr(num) + (b == e ? "/>" : ">")});
            if (e <= tpos && e >= b && e > fpos && e > b)
                events.push_back({e, -priority - nest, Ev::STRUCT, "</" + name + ">"});
        }
    }
};

std::vector<OutStruct> split_structures(const Corpus& corpus, const std::string& list) {
    std::vector<OutStruct> out;
    for (std::string item : split_commas(list)) {
        if (item.empty() || item[0] == '-') continue;
        const size_t dot = item.find('.');
        const std::string attr = dot == std::string::npos ? "" : item.substr(dot + 1);
        if (dot != std::string::npos) item.erase(dot);
        if (!corpus.has_structure(item)) continue;   // ignore_nondef
        auto it = std::find_if(out.begin(), out.end(), [&](const OutStruct& o) { return o.name == item; });
        if (it == out.end()) {
            out.push_back({item, &corpus.structure(item), {}});
            it = out.end() - 1;
        }
        if (dot != std::string::npos) {
            if (attr == "*") it->add_all();
            else if (resolve_region_attr_key(*it->sa, item, attr)) it->add_attr(attr);
        }
    }
    return out;
}

// One ref (OutRef): "" when it gives nothing (ORdummy, or no region at the position).
std::string ref_at(const Corpus& corpus, const std::string& item_in, CorpusPos pos) {
    std::string item = item_in;
    if (item == "#") return "#" + std::to_string(pos);
    const size_t dot = item.find('.');
    if (dot == std::string::npos) {
        if (!corpus.has_structure(item)) return "";
        const int64_t n = corpus.structure(item).find_region(pos);
        return n == -1 ? "" : item + "#" + std::to_string(n);
    }
    std::string attr = item.substr(dot + 1);
    item.erase(dot);
    const bool value_only = !item.empty() && item[0] == '=';
    if (value_only) item.erase(0, 1);
    if (!corpus.has_structure(item)) return "";
    const StructuralAttr& sa = corpus.structure(item);
    if (attr != "*" && !resolve_region_attr_key(sa, item, attr)) return "";
    const int64_t n = sa.find_region(pos);
    if (n == -1) return "";
    if (attr == "*") {
        std::string r = "<" + item;
        for (const std::string& a : sa.region_attr_names()) r += " " + a + "=" + region_value(sa, item, a, n);
        return r + ">";
    }
    return (value_only ? "" : item + "." + attr + "=") + region_value(sa, item, attr, n);
}

// tags_set
struct Tags {
    std::list<std::string> l;
    void insert(const std::string& x) { l.push_back(x); }
    void remove(const std::string& x) {
        auto it = std::find(l.begin(), l.end(), x);
        if (it != l.end()) l.erase(it);
    }
    std::string join() const {
        if (l.empty()) return "{}";
        if (l.size() == 1) return l.front();
        std::string r = "{";
        for (const auto& t : l) r += t + ' ';
        r.back() = '}';
        return r;
    }
};

void get_corp_text(const std::vector<const PositionalAttr*>& attrs, const std::string& currtags, CorpusPos fpos,
                   CorpusPos tpos, std::vector<std::string>& strs, std::vector<std::string>& tags) {
    if (fpos >= tpos || attrs.empty()) return;
    for (CorpusPos p = fpos; p < tpos; ++p) {
        strs.emplace_back(attrs[0]->value_at(p));
        tags.push_back(currtags);
        if (attrs.size() > 1) {
            std::string as;
            for (size_t i = 1; i < attrs.size(); ++i) {
                as += '\x1F';   // the attribute delimiter of KonText's Manatee (ÚČNK's manatee-open; '/' upstream)
                as += attrs[i]->value_at(p);
            }
            strs.push_back(std::move(as));
            tags.emplace_back("attr");
        }
        strs.emplace_back(" ");
        tags.push_back(currtags);
    }
    strs.pop_back();
    tags.pop_back();
}

void fill_segment(std::vector<std::string>& partstr, std::vector<std::string>& parttag, std::vector<std::string>& output) {
    if (!parttag.empty()) {
        std::string currtag = parttag[0];
        output.push_back(partstr[0]);
        for (size_t i = 1; i < partstr.size(); ++i) {
            if (currtag != parttag[i]) {
                output.push_back(currtag);
                output.push_back(partstr[i]);
                currtag = parttag[i];
            } else {
                output.back() += partstr[i];
            }
        }
        output.push_back(currtag);
    }
    parttag.clear();
    partstr.clear();
}

void list_json(std::ostream& out, const std::vector<std::string>& v) {
    out << '[';
    for (size_t i = 0; i < v.size(); ++i) out << (i ? "," : "") << jstr(v[i]);
    out << ']';
}

}  // namespace

void write_kwic_view(std::ostream& out, const Corpus& corpus, const KwicViewSpec& spec, CorpusPos kwbeg,
                     CorpusPos kwend) {
    const CorpusPos csize = corpus.size();
    auto leftctx = prepare_context(corpus, spec.left, true, spec.maxctx);
    auto rightctx = prepare_context(corpus, spec.right, false, spec.maxctx);
    const auto kwicattrs = split_attributes(corpus, spec.attrs);
    const auto ctxattrs = spec.ctxattrs.empty() ? kwicattrs : split_attributes(corpus, spec.ctxattrs);
    const auto structs = split_structures(corpus, spec.structs);

    CorpusPos ctxbeg = std::min(std::max<CorpusPos>(0, leftctx->get(kwbeg, kwend)), csize);
    CorpusPos ctxend = std::min(std::max<CorpusPos>(0, rightctx->get(kwbeg, kwend) + 1), csize);

    std::vector<std::string> outleft, outkwic, outright, outrefs;
    if (ctxbeg < ctxend && !ctxattrs.empty()) {
        // characters: Manatee's reduce_ctxbeg / reduce_ctxend (word lengths, spaces not counted)
        if (leftctx->chars) {
            std::vector<size_t> lens;
            for (CorpusPos p = ctxbeg; p < kwbeg; ++p) lens.push_back(utf8_len(ctxattrs[0]->value_at(p)));
            CorpusPos nb = ctxbeg;
            size_t clen = 0;
            for (int64_t i = static_cast<int64_t>(lens.size()) - 1; i >= 0; --i)
                if ((clen += lens[static_cast<size_t>(i)]) > static_cast<size_t>(leftctx->chars)) {
                    nb = ctxbeg + i + 1;
                    break;
                }
            ctxbeg = nb;
        }
        if (rightctx->chars) {
            size_t clen = 0;
            for (CorpusPos p = kwend; p < ctxend; ++p) {
                clen += utf8_len(ctxattrs[0]->value_at(p));
                if (clen > static_cast<size_t>(rightctx->chars)) {
                    ctxend = p;
                    break;
                }
            }
        }

        std::vector<PosEvent> events;
        events.push_back({ctxbeg, 0, Ev::BEG, ""});
        events.push_back({ctxend, 0, Ev::END, ""});
        events.push_back({kwbeg, 1000, Ev::BKW, ""});
        events.push_back({kwend, kwbeg == kwend ? 2000 : -1000, Ev::EKW, ""});
        int prior = 1;
        for (const OutStruct& st : structs) st.add_struct_events(prior++ * 15, ctxbeg, ctxend, events);
        std::stable_sort(events.begin(), events.end(), [](const PosEvent& x, const PosEvent& y) {
            return x.pos < y.pos || (x.pos == y.pos && x.sub < y.sub);
        });

        for (const std::string& r : split_commas(spec.refs))
            if (!r.empty() && r[0] != '-') outrefs.push_back(ref_at(corpus, r, kwbeg));

        std::vector<std::string> partstr, parttag;
        Tags tags;
        const std::vector<const PositionalAttr*>* currattrs = &ctxattrs;
        bool inside = false, addspace = false;
        CorpusPos a1 = events[0].pos;
        for (const PosEvent& e : events) {
            const CorpusPos a2 = e.pos;
            if (a1 < a2 && inside) {
                if (addspace) {
                    partstr.emplace_back(" ");
                    parttag.push_back(tags.join());
                }
                addspace = true;
                get_corp_text(*currattrs, tags.join(), a1, a2, partstr, parttag);
            }
            a1 = a2;
            switch (e.type) {
                case Ev::BEG: inside = true; break;
                case Ev::END: fill_segment(partstr, parttag, outright); break;
                case Ev::BKW:
                    currattrs = &kwicattrs;
                    tags.insert("col0");
                    tags.insert("coll");
                    fill_segment(partstr, parttag, outleft);
                    break;
                case Ev::EKW:
                    fill_segment(partstr, parttag, outkwic);
                    currattrs = &ctxattrs;
                    tags.remove("col0");
                    tags.remove("coll");
                    break;
                case Ev::STRUCT:
                    partstr.push_back(e.value);
                    parttag.emplace_back("strc");
                    addspace = false;
                    break;
                case Ev::BTAG: tags.insert(e.value); break;
                case Ev::ETAG: tags.remove(e.value); break;
                case Ev::TEXT:
                    partstr.push_back(e.value);
                    parttag.push_back(tags.join());
                    addspace = false;
                    break;
            }
            if (e.type == Ev::END) break;
        }
    }
    out << ", \"kwic_view\": {\"pos\": " << kwbeg << ", \"kwiclen\": " << (kwend - kwbeg) << ", \"ctxbeg\": " << ctxbeg
        << ", \"ctxend\": " << ctxend << ", \"linegroup\": 0, \"refs\": ";
    list_json(out, outrefs);
    out << ", \"left\": ";
    list_json(out, outleft);
    out << ", \"kwic\": ";
    list_json(out, outkwic);
    out << ", \"right\": ";
    list_json(out, outright);
    out << "}";
}

}  // namespace pando
