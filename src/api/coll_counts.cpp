#include "api/coll_counts.h"

#include <algorithm>
#include <unordered_map>

namespace pando {

// ── coll ────────────────────────────────────────────────────────────────

CollCounter::CollCounter(const PositionalAttr& pa, int left, int right, size_t corpus_size)
    : pa_(pa),
      left_(static_cast<CorpusPos>(std::max(left, 0))),
      right_(static_cast<CorpusPos>(std::max(right, 0))),
      n_(static_cast<CorpusPos>(corpus_size)),
      empty_id_(pa.lexicon().lookup("")),
      counts_(pa.lexicon().size(), 0) {}

inline void CollCounter::count(CorpusPos p) {
    // parallel tiers often leave lemma (etc.) empty: one huge "" bucket would hide
    // the real collocates
    const LexiconId id = pa_.id_at(p);
    if (id == empty_id_ || id < 0 || static_cast<size_t>(id) >= counts_.size()) return;
    ++counts_[static_cast<size_t>(id)];
    ++total_;
}

void CollCounter::add_envelope(CorpusPos first, CorpusPos last, const std::vector<CorpusPos>* excl) {
    if (first < 0 || last < first) return;
    auto excluded = [&](CorpusPos p) {
        return excl && std::binary_search(excl->begin(), excl->end(), p);
    };
    const CorpusPos lo = first > left_ ? first - left_ : 0;
    for (CorpusPos p = lo; p < first; ++p)
        if (!excluded(p)) count(p);
    const CorpusPos hi = std::min(last + right_ + 1, n_);
    for (CorpusPos p = last + 1; p < hi; ++p)
        if (!excluded(p)) count(p);
}

void CollCounter::add_hub(CorpusPos hub, const std::vector<CorpusPos>& excl) {
    if (hub < 0) return;
    auto excluded = [&](CorpusPos p) {
        for (CorpusPos e : excl)
            if (e == p) return true;
        return false;
    };
    const CorpusPos lo = hub > left_ ? hub - left_ : 0;
    for (CorpusPos p = lo; p < hub; ++p)
        if (!excluded(p)) count(p);
    const CorpusPos hi = std::min(hub + right_ + 1, n_);
    for (CorpusPos p = hub + 1; p < hi; ++p)
        if (!excluded(p)) count(p);
}

static CollCounts collect(const std::vector<uint32_t>& counts, size_t total) {
    CollCounts out;
    out.total = total;
    for (size_t id = 0; id < counts.size(); ++id)
        if (counts[id]) out.items.emplace_back(static_cast<LexiconId>(id), counts[id]);
    return out;
}

CollCounts CollCounter::finish() const { return collect(counts_, total_); }

// ── dcoll ───────────────────────────────────────────────────────────────

DcollCounter::DcollCounter(const Corpus& corpus, const PositionalAttr& pa,
                           const std::vector<std::string>& relations)
    : corpus_(corpus), pa_(pa), counts_(pa.lexicon().size(), 0) {
    const PositionalAttr* deprel = corpus.has_attr("deprel") ? &corpus.attr("deprel") : nullptr;
    if (relations.empty()) want_all_children_ = true;
    for (const auto& rel : relations) {
        if (rel == "head") want_head_ = true;
        else if (rel == "descendants") want_descendants_ = true;
        else if (rel == "children") want_all_children_ = true;
        else {
            deprel_filter_ = true;
            if (deprel) {
                LexiconId id = deprel->lexicon().lookup(rel);
                if (id >= 0 && std::find(deprel_ids_.begin(), deprel_ids_.end(), id) == deprel_ids_.end())
                    deprel_ids_.push_back(id);
            }
        }
    }
}

inline void DcollCounter::count(CorpusPos p, uint32_t times) {
    const LexiconId id = pa_.id_at(p);
    if (id < 0 || static_cast<size_t>(id) >= counts_.size()) return;
    counts_[static_cast<size_t>(id)] += times;
    total_ += times;
}

CollCounts DcollCounter::finish() {
    if (!corpus_.has_deps() || nodes_.empty()) return collect(counts_, total_);
    const auto& deps = corpus_.deps();
    const CorpusPos n = corpus_.size();

    if (want_head_)
        for (CorpusPos node : nodes_) {
            CorpusPos h = deps.head(node);
            if (h != NO_HEAD && h != node) count(h);
        }
    if (want_descendants_)
        for (CorpusPos node : nodes_)
            for (CorpusPos rp : deps.subtree(node))
                if (rp != node) count(rp);

    const bool want_children = want_all_children_ || deprel_filter_;
    if (!want_children) return collect(counts_, total_);

    // candidate children: every token (all children) or the tokens with one of the
    // deprels; their number decides between the two ways of finding children
    const PositionalAttr* deprel = corpus_.has_attr("deprel") ? &corpus_.attr("deprel") : nullptr;
    size_t filtered_candidates = 0;
    if (deprel)
        for (LexiconId id : deprel_ids_) filtered_candidates += deprel->count_of_id(id);
    const size_t candidates = (want_all_children_ ? static_cast<size_t>(n) : 0) + filtered_candidates;

    // per node, a sentence's children map is built once per sentence visit
    // (~ a binary search + a sentence scan); from the child side it is one head
    // lookup per candidate
    const bool from_children = nodes_.size() * 32 > candidates;

    if (!from_children) {
        std::vector<char> wanted_rel;
        if (deprel && !deprel_ids_.empty()) {
            wanted_rel.assign(deprel->lexicon().size(), 0);
            for (LexiconId id : deprel_ids_) wanted_rel[static_cast<size_t>(id)] = 1;
        }
        for (CorpusPos node : nodes_) {
            for (CorpusPos rp : deps.children(node)) {
                if (want_all_children_) count(rp);
                if (!wanted_rel.empty()) {
                    LexiconId d = deprel->id_at(rp);
                    if (d >= 0 && static_cast<size_t>(d) < wanted_rel.size() && wanted_rel[static_cast<size_t>(d)])
                        count(rp);
                }
            }
        }
        return collect(counts_, total_);
    }

    // node set as a bitmap; a node that is the node of several hits counts that often
    std::sort(nodes_.begin(), nodes_.end());
    std::vector<uint64_t> bits(static_cast<size_t>(n) / 64 + 1, 0);
    std::unordered_map<CorpusPos, uint32_t> repeated;
    for (size_t i = 0; i < nodes_.size();) {
        size_t j = i;
        while (j < nodes_.size() && nodes_[j] == nodes_[i]) ++j;
        const CorpusPos p = nodes_[i];
        if (p >= 0 && p < n) {
            bits[static_cast<size_t>(p) >> 6] |= uint64_t{1} << (p & 63);
            if (j - i > 1) repeated[p] = static_cast<uint32_t>(j - i);
        }
        i = j;
    }
    auto times_of = [&](CorpusPos h) -> uint32_t {
        if (h < 0 || h >= n) return 0;
        if (!(bits[static_cast<size_t>(h) >> 6] >> (h & 63) & 1)) return 0;
        if (repeated.empty()) return 1;
        auto it = repeated.find(h);
        return it == repeated.end() ? 1 : it->second;
    };
    const int16_t* rel = deps.head_rel_data();
    auto head_of = [&](CorpusPos c) -> CorpusPos {
        if (rel) {
            int16_t d = rel[c];
            return d ? c + d : NO_HEAD;
        }
        return deps.head(c);
    };
    if (want_all_children_) {
        for (CorpusPos c = 0; c < n; ++c) {
            CorpusPos h = head_of(c);
            if (h == NO_HEAD) continue;
            if (uint32_t k = times_of(h)) count(c, k);
        }
    }
    if (deprel)
        for (LexiconId id : deprel_ids_)
            deprel->for_each_position_id(id, [&](CorpusPos c) {
                CorpusPos h = head_of(c);
                if (h != NO_HEAD)
                    if (uint32_t k = times_of(h)) count(c, k);
                return true;
            });
    return collect(counts_, total_);
}

// ── sinks ───────────────────────────────────────────────────────────────

// as Match::first_pos / last_pos
static CorpusPos first_of(const CorpusPos* starts, size_t n) {
    CorpusPos mn = NO_HEAD;
    for (size_t i = 0; i < n; ++i)
        if (starts[i] != NO_HEAD && (mn == NO_HEAD || starts[i] < mn)) mn = starts[i];
    return mn == NO_HEAD ? 0 : mn;
}
static CorpusPos last_of(const CorpusPos* ends, size_t n) {
    CorpusPos mx = 0;
    for (size_t i = 0; i < n; ++i)
        if (ends[i] != NO_HEAD && ends[i] > mx) mx = ends[i];
    return mx;
}

void CollHitSink::hit(const CorpusPos* starts, const CorpusPos* ends, size_t n) {
    if (hub_ == -2) return;
    if (hub_ < 0) {
        c_.add_envelope(first_of(starts, n), last_of(ends, n));
        return;
    }
    if (static_cast<size_t>(hub_) >= n || starts[hub_] == NO_HEAD) return;
    excl_.clear();
    for (size_t i = 0; i < n; ++i)
        if (starts[i] != NO_HEAD)
            for (CorpusPos p = starts[i]; p <= ends[i]; ++p) excl_.push_back(p);
    c_.add_hub(starts[hub_], excl_);
}

void DcollHitSink::hit(const CorpusPos* starts, const CorpusPos* ends, size_t n) {
    (void)ends;
    CorpusPos node = first_of(starts, n);
    if (anchor_ >= 0 && static_cast<size_t>(anchor_) < n && starts[anchor_] != NO_HEAD) node = starts[anchor_];
    c_.add_node(node);
}

} // namespace pando
