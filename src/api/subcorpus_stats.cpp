#include "api/subcorpus_stats.h"

#include "corpus/corpus.h"
#include "index/positional_attr.h"

namespace pando {

std::string SubcorpusStats::cache_key() const {
    std::string k;
    if (inside) k += "in=" + inside->key();
    if (use && use->sc) k += ";use=" + use->sc->key();
    return k;
}

uint64_t stats_corpus_size(const Corpus& corpus, const SubcorpusStats* s) {
    if (s && s->use && s->use->sc) return s->use->sc->set().tokens();
    return static_cast<uint64_t>(corpus.size());
}

const PositionSet* stats_inside(const SubcorpusStats* s) {
    return s && s->inside ? &s->inside->set() : nullptr;
}

uint64_t stats_span_tokens(const SubcorpusStats* s, int64_t a, int64_t b) {
    if (b < a) return 0;
    if (s && s->use && s->use->sc) return s->use->sc->set().count_in(a, b);
    return static_cast<uint64_t>(b - a + 1);
}

StatsFreqs::StatsFreqs(const Corpus& corpus, const SubcorpusStats* s, const std::string& attr_name)
    : pa_(corpus.attr(attr_name)) {
    if (s && s->use && s->use->sc) counts_ = s->use->sc->id_counts(attr_name, pa_);
}

uint64_t StatsFreqs::of(int32_t id) const {
    if (!counts_) return pa_.count_of_id(id);
    return id >= 0 && static_cast<size_t>(id) < counts_->size() ? (*counts_)[static_cast<size_t>(id)] : 0;
}

} // namespace pando

#include "api/coll_counts.h"

namespace pando {

std::shared_ptr<const PairCounts> stats_dcoll_counts(const Corpus& corpus, const SubcorpusStats* s,
                                                     const std::string& attr, DcollBaseline baseline,
                                                     const std::vector<std::string>& relations,
                                                     const std::string& layer) {
    if (!s || !s->use || !s->use->sc) return nullptr;
    const ResolvedSubcorpus& sc = *s->use->sc;
    const PositionalAttr& pa = corpus.attr(attr);
    std::string key = "dcoll/" + std::string(dcoll_baseline_name(baseline)) + "/" + attr + "/" + layer;
    for (const auto& r : relations) key += "," + r;
    auto v = sc.memo(key, [&]() -> std::shared_ptr<const void> {
        auto t = std::make_shared<PairTotals>();
        if (baseline == DcollBaseline::Corpus) {
            auto c = sc.id_counts(attr, pa);
            t->by_id = *c;
            t->sum = sc.set().tokens();
            return t;
        }
        t->by_id.assign(static_cast<size_t>(std::max<int64_t>(pa.lexicon().size(), 0)), 0);
        // every token of the subcorpus a node, in batches (the counter buffers its nodes)
        constexpr size_t kBatch = size_t{1} << 20;
        auto run = [&](const std::vector<CorpusPos>& nodes) {
            DcollCounter dc(corpus, pa, relations, layer);
            dc.set_inside(&sc.set());
            for (CorpusPos p : nodes) dc.add_node(p);
            const CollCounts cc = dc.finish();
            for (const auto& [id, n] : cc.items)
                if (id >= 0 && static_cast<size_t>(id) < t->by_id.size()) t->by_id[static_cast<size_t>(id)] += n;
            t->sum += cc.total;
        };
        std::vector<CorpusPos> batch;
        batch.reserve(kBatch);
        sc.set().for_each_run([&](int64_t a, int64_t b) {
            for (int64_t p = a; p <= b; ++p) {
                batch.push_back(p);
                if (batch.size() == kBatch) { run(batch); batch.clear(); }
            }
        });
        if (!batch.empty()) run(batch);
        return t;
    });
    return std::static_pointer_cast<const PairCounts>(std::static_pointer_cast<const PairTotals>(v));
}

} // namespace pando
