#include "api/dcoll_totals.h"

#include "corpus/corpus.h"
#include "index/dcoll_totals_file.h"

#include <cstdint>
#include <unordered_map>
#include <unordered_set>

namespace pando {

bool parse_dcoll_baseline(std::string_view s, DcollBaseline& out) {
    if (s == "relation") { out = DcollBaseline::Relation; return true; }
    if (s == "corpus")   { out = DcollBaseline::Corpus;   return true; }
    return false;
}

const char* dcoll_baseline_name(DcollBaseline b) {
    return b == DcollBaseline::Corpus ? "corpus" : "relation";
}

namespace {

// a token's ancestors are followed at most this far (a malformed tree with a cycle stops there)
constexpr int kMaxDepth = 4096;

class Tally {
public:
    explicit Tally(const PositionalAttr& pa) : pa_(pa), out_(std::make_shared<PairTotals>()) {
        out_->by_id.assign(pa.lexicon().size(), 0);
    }
    // as DcollCounter::count: a position without a valid id is not counted
    void add(CorpusPos p, uint64_t times = 1) {
        if (!times) return;
        const LexiconId id = pa_.id_at(p);
        if (id < 0 || static_cast<size_t>(id) >= out_->by_id.size()) return;
        out_->by_id[static_cast<size_t>(id)] += times;
        out_->sum += times;
    }
    std::shared_ptr<const PairTotals> done() { return out_; }
private:
    const PositionalAttr& pa_;
    std::shared_ptr<PairTotals> out_;
};

// ── the basic tree ──────────────────────────────────────────────────────

// Heads in corpus order (and up a token's own sentence): head_from keeps its place in the sentences, so an index
// without head offsets (dep.head_rel) needs no sentence search per token.
struct BasicTree {
    const DependencyIndex& deps;
    CorpusPos n;
    mutable int64_t hint = -1;
    explicit BasicTree(const Corpus& c) : deps(c.deps()), n(c.size()) {}
    CorpusPos head(CorpusPos c) const { return deps.head_from(c, hint); }
};

// all children: every token with a head is the child of one
std::shared_ptr<const PairTotals> basic_children(const Corpus& corpus, const PositionalAttr& pa) {
    const BasicTree t(corpus);
    Tally out(pa);
    for (CorpusPos c = 0; c < t.n; ++c)
        if (t.head(c) != NO_HEAD) out.add(c);
    return out.done();
}

// the head: a token is the head once per child
std::shared_ptr<const PairTotals> basic_heads(const Corpus& corpus, const PositionalAttr& pa) {
    const BasicTree t(corpus);
    Tally out(pa);
    for (CorpusPos c = 0; c < t.n; ++c) {
        const CorpusPos h = t.head(c);
        if (h != NO_HEAD && h != c) out.add(h);
    }
    return out.done();
}

// children with one deprel
std::shared_ptr<const PairTotals> basic_deprel(const Corpus& corpus, const PositionalAttr& pa, const std::string& name) {
    Tally out(pa);
    if (!corpus.has_attr("deprel")) return out.done();
    const PositionalAttr& deprel = corpus.attr("deprel");
    const LexiconId id = deprel.lexicon().lookup(name);
    if (id < 0) return out.done();
    // one pass in corpus order (as for all children) beats reading the deprel's positions
    const BasicTree t(corpus);
    for (CorpusPos c = 0; c < t.n; ++c)
        if (deprel.id_at(c) == id && t.head(c) != NO_HEAD) out.add(c);
    return out.done();
}

// descendants: a token is the descendant of each of its ancestors
std::shared_ptr<const PairTotals> basic_descendants(const Corpus& corpus, const PositionalAttr& pa) {
    Tally out(pa);
    for_each_ancestor_count(corpus.deps(), corpus.size(), [&](CorpusPos c, uint32_t k) { out.add(c, k); });
    return out.done();
}

// stored totals (index/dcoll_totals_file.h): a view into the file
struct StoredDense final : PairCounts {
    std::shared_ptr<const DcollTotalsFile> file;
    const uint64_t* by_id;
    uint64_t sum;
    StoredDense(std::shared_ptr<const DcollTotalsFile> f, const uint64_t* v, uint64_t t)
        : file(std::move(f)), by_id(v), sum(t) {}
    uint64_t of(LexiconId id) const override {
        return id >= 0 && static_cast<size_t>(id) < file->values() ? by_id[id] : 0;
    }
    uint64_t total() const override { return sum; }
};
struct StoredDeprel final : PairCounts {
    std::shared_ptr<const DcollTotalsFile> file;
    LexiconId deprel;
    StoredDeprel(std::shared_ptr<const DcollTotalsFile> f, LexiconId d) : file(std::move(f)), deprel(d) {}
    uint64_t of(LexiconId id) const override { return file->deprel_count(deprel, id); }
    uint64_t total() const override { return file->deprel_total(deprel); }
};

// ── a relation layer (a token may have several heads) ───────────────────

// all children: once per distinct head (as DcollCounter counts a node's children once each)
std::shared_ptr<const PairTotals> layer_children(const Corpus& corpus, const RelationLayer& L, const PositionalAttr& pa) {
    Tally out(pa);
    const CorpusPos n = corpus.size();
    for (CorpusPos c = 0; c < n; ++c) {
        CorpusPos last = NO_HEAD;
        L.for_each_head(c, [&](CorpusPos h, std::string_view) {
            if (h != NO_HEAD && h != last) out.add(c);
            last = h;   // edges come sorted by head
            return true;
        });
    }
    return out.done();
}

// the head: once per distinct (dependent, head)
std::shared_ptr<const PairTotals> layer_heads(const Corpus& corpus, const RelationLayer& L, const PositionalAttr& pa) {
    Tally out(pa);
    const CorpusPos n = corpus.size();
    for (CorpusPos c = 0; c < n; ++c) {
        CorpusPos last = NO_HEAD;
        L.for_each_head(c, [&](CorpusPos h, std::string_view) {
            if (h != NO_HEAD && h != c && h != last) out.add(h);
            last = h;
            return true;
        });
    }
    return out.done();
}

// children with one label: once per edge with that label
std::shared_ptr<const PairTotals> layer_label(const Corpus& corpus, const RelationLayer& L, const PositionalAttr& pa,
                                              const std::string& name) {
    Tally out(pa);
    const CorpusPos n = corpus.size();
    for (CorpusPos c = 0; c < n; ++c)
        L.for_each_head(c, [&](CorpusPos h, std::string_view label) {
            if (h != NO_HEAD && label == name) out.add(c);
            return true;
        });
    return out.done();
}

// descendants: once per distinct ancestor
std::shared_ptr<const PairTotals> layer_descendants(const Corpus& corpus, const RelationLayer& L, const PositionalAttr& pa) {
    Tally out(pa);
    const CorpusPos n = corpus.size();
    std::unordered_set<CorpusPos> seen;
    std::vector<CorpusPos> todo;
    for (CorpusPos c = 0; c < n; ++c) {
        seen.clear();
        seen.insert(c);
        todo.assign(1, c);
        uint64_t k = 0;
        while (!todo.empty() && k < kMaxDepth) {
            const CorpusPos from = todo.back();
            todo.pop_back();
            L.for_each_head(from, [&](CorpusPos h, std::string_view) {
                if (h != NO_HEAD && seen.insert(h).second) {
                    ++k;
                    todo.push_back(h);
                }
                return true;
            });
        }
        out.add(c, k);
    }
    return out.done();
}

}  // namespace

DcollTotals DcollTotals::corpus(const PositionalAttr& pa, size_t corpus_size) {
    DcollTotals t;
    t.baseline_ = DcollBaseline::Corpus;
    t.pa_ = &pa;
    t.total_ = corpus_size;
    return t;
}

DcollTotals DcollTotals::relation(const Corpus& corpus, const std::string& attr, const DcollPairKinds& kinds) {
    DcollTotals t;
    t.baseline_ = DcollBaseline::Relation;
    const PositionalAttr& pa = corpus.attr(attr);
    t.pa_ = &pa;
    const RelationLayer* L = kinds.layer;
    if (!corpus.has_deps() && !L) return t;
    // the basic tree: from the index when pando-index stored the totals (checked once per attribute)
    std::shared_ptr<const DcollTotalsFile> file;
    if (!L) {
        auto holder = pa.cache().get<std::shared_ptr<const DcollTotalsFile>>("dcoll-totals-file", [&] {
            return std::make_shared<const std::shared_ptr<const DcollTotalsFile>>(DcollTotalsFile::open(corpus, attr));
        });
        if (holder) file = *holder;
    }
    if (file) {
        t.stored_ = true;
        const PositionalAttr* deprel = corpus.has_attr("deprel") ? &corpus.attr("deprel") : nullptr;
        if (kinds.head) t.parts_.push_back(std::make_shared<StoredDense>(file, file->heads(), file->heads_total()));
        if (kinds.descendants)
            t.parts_.push_back(std::make_shared<StoredDense>(file, file->descendants(), file->descendants_total()));
        if (kinds.children)
            t.parts_.push_back(std::make_shared<StoredDense>(file, file->children(), file->children_total()));
        for (const auto& d : kinds.deprels)
            t.parts_.push_back(std::make_shared<StoredDeprel>(file, deprel ? deprel->lexicon().lookup(d) : -1));
    } else {
        const std::string prefix = "dcoll-totals:" + (L ? "via=" + kinds.layer_name + ":" : std::string());
        auto part = [&](const std::string& kind, auto build) {
            t.parts_.push_back(pa.cache().get<PairTotals>(prefix + kind, build));
        };
        if (kinds.head)
            part("head", [&] { return L ? layer_heads(corpus, *L, pa) : basic_heads(corpus, pa); });
        if (kinds.descendants)
            part("descendants", [&] { return L ? layer_descendants(corpus, *L, pa) : basic_descendants(corpus, pa); });
        if (kinds.children)
            part("children", [&] { return L ? layer_children(corpus, *L, pa) : basic_children(corpus, pa); });
        for (const auto& d : kinds.deprels)
            part("deprel=" + d, [&] { return L ? layer_label(corpus, *L, pa, d) : basic_deprel(corpus, pa, d); });
    }
    for (const auto& p : t.parts_)
        if (p) t.total_ += p->total();
    return t;
}

uint64_t DcollTotals::of(LexiconId id) const {
    if (id < 0) return 0;
    if (baseline_ == DcollBaseline::Corpus) return pa_ ? pa_->count_of_id(id) : 0;
    uint64_t sum = 0;
    for (const auto& p : parts_)
        if (p) sum += p->of(id);
    return sum;
}

}  // namespace pando
