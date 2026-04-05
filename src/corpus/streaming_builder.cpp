#include "corpus/streaming_builder.h"
#include <future>
#include <thread>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <unordered_set>
#include <unordered_map>

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

namespace manatree {
namespace fs = std::filesystem;

static constexpr size_t IO_CHUNK = 4 * 1024 * 1024; // 4M elements per chunk

namespace {

// RG-REG-1: immediate parent = smallest-span region that strictly contains this region.
bool strictly_contains_region(const Region& outer, const Region& inner) {
    if (outer.start > outer.end || inner.start > inner.end) return false;
    if (outer.start > inner.start || inner.end > outer.end) return false;
    return outer.start < inner.start || inner.end < outer.end;
}

std::vector<int32_t> compute_parent_region_ids(const std::vector<Region>& r,
                                               size_t& ambiguous_count) {
    size_t n = r.size();
    std::vector<int32_t> parent(n, -1);
    ambiguous_count = 0;
    for (size_t i = 0; i < n; ++i) {
        if (r[i].start > r[i].end) continue;
        int64_t best_span = std::numeric_limits<int64_t>::max();
        std::vector<size_t> best_js;
        for (size_t j = 0; j < n; ++j) {
            if (i == j) continue;
            if (!strictly_contains_region(r[j], r[i])) continue;
            int64_t span = static_cast<int64_t>(r[j].end) - static_cast<int64_t>(r[j].start);
            if (span < 0) continue;
            if (span < best_span) {
                best_span = span;
                best_js = {j};
            } else if (span == best_span)
                best_js.push_back(j);
        }
        if (best_js.size() == 1)
            parent[i] = static_cast<int32_t>(best_js[0]);
        else if (best_js.size() > 1)
            ++ambiguous_count;
    }
    return parent;
}

}  // namespace

// Reverse index for region attribute values: same basename as .val → adds .lex, .rev, .rev.idx
// (value lexicon + sorted region indices per distinct value). Used for fast :: filters and counts.
static void build_region_attr_reverse_index(const std::string& base) {
    std::string val_path = base + ".val";
    std::string idx_path = base + ".val.idx";
    MmapFile v = MmapFile::open(val_path, false);
    MmapFile i = MmapFile::open(idx_path, false);
    if (!v.valid() || !i.valid()) return;

    const auto* off = i.as<int64_t>();
    size_t n_idx = i.count<int64_t>();
    if (n_idx < 2) return;
    const size_t n_regions = n_idx - 1;
    const char* vbase = static_cast<const char*>(v.data());

    std::vector<std::string> region_vals;
    region_vals.reserve(n_regions);
    for (size_t ri = 0; ri < n_regions; ++ri) {
        int64_t a = off[ri];
        int64_t b = off[ri + 1];
        if (b <= a) continue;
        region_vals.emplace_back(vbase + a, static_cast<size_t>(b - a - 1));
    }
    if (region_vals.empty()) return;

    std::vector<std::string> sorted = region_vals;
    std::sort(sorted.begin(), sorted.end());
    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());

    std::vector<int64_t> lex_offsets;
    write_strings(base + ".lex", sorted, lex_offsets);
    write_vec(base + ".lex.idx", lex_offsets);

    std::unordered_map<std::string, LexiconId> to_id;
    to_id.reserve(sorted.size() * 2);
    for (LexiconId id = 0; id < static_cast<LexiconId>(sorted.size()); ++id)
        to_id[sorted[static_cast<size_t>(id)]] = id;

    const LexiconId nlex = static_cast<LexiconId>(sorted.size());
    std::vector<int64_t> cnt(static_cast<size_t>(nlex), 0);
    for (const auto& s : region_vals)
        ++cnt[static_cast<size_t>(to_id.at(s))];

    std::vector<int64_t> rev_idx(static_cast<size_t>(nlex) + 1);
    rev_idx[0] = 0;
    for (LexiconId id = 0; id < nlex; ++id)
        rev_idx[static_cast<size_t>(id) + 1] =
            rev_idx[static_cast<size_t>(id)] + cnt[static_cast<size_t>(id)];

    int64_t total = rev_idx[static_cast<size_t>(nlex)];
    std::vector<int64_t> cur = rev_idx;
    std::vector<int64_t> rev_flat(static_cast<size_t>(total));
    for (size_t ri = 0; ri < region_vals.size(); ++ri) {
        LexiconId id = to_id.at(region_vals[ri]);
        int64_t slot = cur[static_cast<size_t>(id)]++;
        rev_flat[static_cast<size_t>(slot)] = static_cast<int64_t>(ri);
    }
    for (LexiconId id = 0; id < nlex; ++id) {
        int64_t lo = rev_idx[static_cast<size_t>(id)];
        int64_t hi = rev_idx[static_cast<size_t>(id) + 1];
        std::sort(rev_flat.begin() + lo, rev_flat.begin() + hi);
    }

    write_file(base + ".rev", rev_flat.data(), rev_flat.size() * sizeof(int64_t));
    write_vec(base + ".rev.idx", rev_idx);
}

// ── AttrState ───────────────────────────────────────────────────────────

int32_t StreamingBuilder::AttrState::get_or_assign(const std::string& value) {
    auto [it, inserted] = str_to_id.try_emplace(
        value, static_cast<int32_t>(id_to_str.size()));
    if (inserted)
        id_to_str.push_back(value);
    return it->second;
}

int32_t StreamingBuilder::AttrState::get_placeholder() {
    if (placeholder_id < 0)
        placeholder_id = get_or_assign("_");
    return placeholder_id;
}

StreamingBuilder::AttrState::~AttrState() {
    if (dat_file) fclose(dat_file);
}

// ── StreamingBuilder lifecycle ──────────────────────────────────────────

StreamingBuilder::StreamingBuilder(const std::string& output_dir)
    : output_dir_(output_dir) {
    fs::create_directories(output_dir);
    sent_rgn_file_ = fopen((output_dir + "/s.rgn").c_str(), "wb");
    if (!sent_rgn_file_)
        throw std::runtime_error("Cannot create " + output_dir + "/s.rgn");
}

StreamingBuilder::~StreamingBuilder() {
    if (dep_head_file_)      fclose(dep_head_file_);
    if (dep_euler_in_file_)  fclose(dep_euler_in_file_);
    if (dep_euler_out_file_) fclose(dep_euler_out_file_);
    if (sent_rgn_file_)      fclose(sent_rgn_file_);
}

// ── Attribute management ────────────────────────────────────────────────

void StreamingBuilder::ensure_attr(const std::string& name) {
    if (attr_set_.count(name)) return;
    attr_set_.insert(name);
    attr_order_.push_back(name);

    auto state = std::make_unique<AttrState>();
    std::string path = output_dir_ + "/" + name + ".dat";
    state->dat_file = fopen(path.c_str(), "wb");
    if (!state->dat_file)
        throw std::runtime_error("Cannot create " + path);

    // Pre-size hash map: large for form/lemma, small for categorical attrs
    bool large_vocab = (name == "form" || name == "lemma");
    state->str_to_id.reserve(large_vocab ? 500000 : 256);
    state->id_to_str.reserve(large_vocab ? 500000 : 256);

    attrs_[name] = std::move(state);
}

void StreamingBuilder::backfill_attr(AttrState& state) {
    if (state.written >= corpus_size_) return;
    int32_t placeholder = state.get_or_assign("_");
    CorpusPos gap = corpus_size_ - state.written;

    std::vector<int32_t> fill(std::min(static_cast<size_t>(gap), IO_CHUNK),
                              placeholder);
    CorpusPos remaining = gap;
    while (remaining > 0) {
        size_t batch = std::min(static_cast<size_t>(remaining), fill.size());
        fwrite(fill.data(), sizeof(int32_t), batch, state.dat_file);
        remaining -= static_cast<CorpusPos>(batch);
    }
    state.written = corpus_size_;
}

// ── Dependency file management ──────────────────────────────────────────

void StreamingBuilder::open_dep_files() {
    dep_head_file_      = fopen((output_dir_ + "/dep.head").c_str(), "wb");
    dep_euler_in_file_  = fopen((output_dir_ + "/dep.euler_in").c_str(), "wb");
    dep_euler_out_file_ = fopen((output_dir_ + "/dep.euler_out").c_str(), "wb");

    if (!dep_head_file_ || !dep_euler_in_file_ || !dep_euler_out_file_)
        throw std::runtime_error("Cannot create dep files in " + output_dir_);

    // Backfill positions before the first sentence with dep info
    if (dep_written_ < sent_start_) {
        int16_t neg1 = -1, zero = 0;
        for (CorpusPos i = dep_written_; i < sent_start_; ++i) {
            fwrite(&neg1, sizeof(int16_t), 1, dep_head_file_);
            fwrite(&zero, sizeof(int16_t), 1, dep_euler_in_file_);
            fwrite(&zero, sizeof(int16_t), 1, dep_euler_out_file_);
        }
        dep_written_ = sent_start_;
    }
}

// ── add_token ───────────────────────────────────────────────────────────

void StreamingBuilder::add_token(
        const std::unordered_map<std::string, std::string>& attrs,
        int sentence_head_id) {
    if (finalized_)
        throw std::logic_error("add_token called after finalize");

    for (const auto& [name, value] : attrs)
        ensure_attr(name);

    // Write values for attributes present in this token
    for (const auto& [name, value] : attrs) {
        auto& state = *attrs_[name];
        backfill_attr(state);
        int32_t id = state.get_or_assign(value);
        fwrite(&id, sizeof(int32_t), 1, state.dat_file);
        ++state.written;
    }

    // Write placeholder "_" for attributes NOT in this token (fast path)
    for (auto& [name, state] : attrs_) {
        if (state->written > corpus_size_) continue;  // already written above
        backfill_attr(*state);
        int32_t id = state->get_placeholder();
        fwrite(&id, sizeof(int32_t), 1, state->dat_file);
        ++state->written;
    }

    sent_buf_.push_back({sentence_head_id});
    ++corpus_size_;
}

// ── end_sentence ────────────────────────────────────────────────────────

void StreamingBuilder::end_sentence(
        const std::vector<std::pair<std::string, std::string>>& sent_region_attrs) {
    if (sent_buf_.empty()) return;

    int sent_len = static_cast<int>(sent_buf_.size());
    CorpusPos sent_end = corpus_size_ - 1;

    // Write sentence region
    Region rgn{sent_start_, sent_end};
    fwrite(&rgn, sizeof(Region), 1, sent_rgn_file_);
    sentence_region_attr_rows_.push_back(sent_region_attrs);

    // Check if any token has dependency info
    bool any_deps = false;
    for (const auto& t : sent_buf_)
        if (t.sentence_head_id >= 0) { any_deps = true; break; }

    if (any_deps) {
        has_deps_ = true;
        if (!dep_head_file_) open_dep_files();

        // Convert 1-based head IDs to sentence-local int16
        std::vector<int16_t> heads(sent_len);
        for (int i = 0; i < sent_len; ++i) {
            int h = sent_buf_[i].sentence_head_id;
            heads[i] = (h == 0) ? int16_t(-1) : static_cast<int16_t>(h - 1);
        }

        // Build children lists for Euler tour
        std::vector<std::vector<int16_t>> children(sent_len);
        for (int i = 0; i < sent_len; ++i) {
            if (heads[i] >= 0)
                children[static_cast<size_t>(heads[i])].push_back(
                    static_cast<int16_t>(i));
        }
        for (auto& ch : children)
            std::sort(ch.begin(), ch.end());

        // Iterative DFS for Euler tour timestamps
        std::vector<int16_t> euler_in(sent_len, 0);
        std::vector<int16_t> euler_out(sent_len, 0);
        int16_t clock = 0;

        struct DfsFrame { int16_t node; size_t child_idx; };
        std::vector<DfsFrame> stack;

        for (int i = 0; i < sent_len; ++i) {
            if (heads[i] != -1) continue;
            euler_in[i] = clock++;
            stack.push_back({static_cast<int16_t>(i), 0});

            while (!stack.empty()) {
                auto& frame = stack.back();
                auto& ch = children[static_cast<size_t>(frame.node)];
                if (frame.child_idx < ch.size()) {
                    int16_t child = ch[frame.child_idx++];
                    euler_in[child] = clock++;
                    stack.push_back({child, 0});
                } else {
                    euler_out[frame.node] = clock++;
                    stack.pop_back();
                }
            }
        }

        fwrite(heads.data(), sizeof(int16_t), sent_len, dep_head_file_);
        fwrite(euler_in.data(), sizeof(int16_t), sent_len, dep_euler_in_file_);
        fwrite(euler_out.data(), sizeof(int16_t), sent_len, dep_euler_out_file_);
        dep_written_ += sent_len;

    } else if (dep_head_file_) {
        // Dep files exist but this sentence has no deps — write placeholders
        int16_t neg1 = -1, zero = 0;
        for (int i = 0; i < sent_len; ++i) {
            fwrite(&neg1, sizeof(int16_t), 1, dep_head_file_);
            fwrite(&zero, sizeof(int16_t), 1, dep_euler_in_file_);
            fwrite(&zero, sizeof(int16_t), 1, dep_euler_out_file_);
        }
        dep_written_ += sent_len;
    }

    sent_buf_.clear();
    sent_start_ = corpus_size_;
}

// ── add_region ──────────────────────────────────────────────────────────

void StreamingBuilder::add_region(const std::string& type,
                                  CorpusPos start, CorpusPos end) {
    struct_set_.insert(type);
    detect_structure_mode(type, start, end);
    regions_[type].push_back({start, end});
}

// SM-ROAD-6: Check the new region against existing regions of the same type
// to detect nesting, overlap, and zero-width spans incrementally.
void StreamingBuilder::detect_structure_mode(const std::string& type,
                                             CorpusPos start, CorpusPos end) {
    auto& det = struct_mode_detected_[type];

    // Zero-width check
    if (start > end) {
        det.zerowidth_seen = true;
        ++det.zerowidth_count;
        return;  // zero-width regions don't nest/overlap in the normal sense
    }

    // Check against recent regions of the same type for nesting/overlap.
    // Only look at the last few (regions arrive roughly in order).
    const auto& existing = regions_[type];
    if (existing.empty()) return;

    // Scan backwards from the end (at most 8 entries to bound cost)
    size_t scan_start = existing.size() > 8 ? existing.size() - 8 : 0;
    for (size_t i = scan_start; i < existing.size(); ++i) {
        const Region& prev = existing[i];
        if (prev.start > prev.end) continue;  // skip zero-width
        // Check if new region is fully inside prev, or prev is fully inside new
        bool new_inside_prev = (start >= prev.start && end <= prev.end && !(start == prev.start && end == prev.end));
        bool prev_inside_new = (prev.start >= start && prev.end <= end && !(start == prev.start && end == prev.end));
        if (new_inside_prev || prev_inside_new) {
            if (!det.nesting_seen) det.nesting_seen = true;
            ++det.nesting_count;
            continue;
        }
        // Check for crossing overlap: they intersect but neither contains the other
        bool intersects = (start <= prev.end && end >= prev.start);
        if (intersects && !new_inside_prev && !prev_inside_new
            && !(start == prev.start && end == prev.end)) {
            if (!det.overlap_seen) det.overlap_seen = true;
            ++det.overlap_count;
        }
    }
}

void StreamingBuilder::add_region(const std::string& type,
                                  CorpusPos start, CorpusPos end,
                                  const std::string& value) {
    add_region(type, start, end);
    region_values_[type].push_back(value);
}

void StreamingBuilder::add_region(const std::string& type,
                                  CorpusPos start, CorpusPos end,
                                  const std::vector<std::pair<std::string, std::string>>& attrs) {
    add_region(type, start, end);
    const size_t n = regions_[type].size();
    if (n == 0) return;

    auto& type_map = region_attr_values_[type];
    std::unordered_set<std::string> present;
    present.reserve(attrs.size());
    for (const auto& [k, v] : attrs)
        present.insert(k);

    // Each region row must contribute one value per attribute column. Sparse attrs (e.g. # doc_info on
    // only some documents) otherwise leave shorter vectors than regions_[type].size().
    for (const auto& [k, v] : attrs) {
        auto& vec = type_map[k];
        while (vec.size() < n - 1)
            vec.push_back("_");
        if (vec.size() == n - 1)
            vec.push_back(v);
        else
            vec.back() = v;
    }
    for (auto& [k, vec] : type_map) {
        if (present.count(k)) continue;
        while (vec.size() < n)
            vec.push_back("_");
    }
}

// ── finalize ────────────────────────────────────────────────────────────

void StreamingBuilder::finalize() {
    if (finalized_) return;

    // Auto-close last sentence
    if (!sent_buf_.empty())
        end_sentence();

    // Close streaming files
    for (auto& [name, state] : attrs_) {
        backfill_attr(*state);
        if (state->dat_file) { fclose(state->dat_file); state->dat_file = nullptr; }
    }
    if (dep_head_file_)      { fclose(dep_head_file_);      dep_head_file_ = nullptr; }
    if (dep_euler_in_file_)  { fclose(dep_euler_in_file_);  dep_euler_in_file_ = nullptr; }
    if (dep_euler_out_file_) { fclose(dep_euler_out_file_); dep_euler_out_file_ = nullptr; }
    if (sent_rgn_file_)      { fclose(sent_rgn_file_);      sent_rgn_file_ = nullptr; }

    // Sort attribute names for deterministic output
    std::vector<std::string> sorted_attrs(attr_order_.begin(), attr_order_.end());
    std::sort(sorted_attrs.begin(), sorted_attrs.end());

    std::cerr << "Finalizing " << sorted_attrs.size() << " attributes, "
              << corpus_size_ << " tokens ...\n";

    // Parallel finalization: each attribute is independent (ROADMAP 3a).
    std::vector<std::future<void>> futures;
    futures.reserve(sorted_attrs.size());
    for (const auto& name : sorted_attrs) {
        std::string name_copy = name;
        AttrState* state = attrs_[name].get();
        futures.push_back(std::async(std::launch::async, [this, name_copy, state]() {
            finalize_attribute(name_copy, *state);
        }));
    }
    for (auto& f : futures)
        f.get();
    for (const auto& name : sorted_attrs) {
        std::cerr << "  " << name << " (" << attrs_[name]->id_to_str.size()
                  << " types) ...\n";
    }

    // Collect region_attrs for corpus.info (#8)
    std::vector<std::string> region_attrs_list;

    // Sentence structural attributes (s_<name>.val), parallel to s.rgn
    {
        FILE* probe = fopen((output_dir_ + "/s.rgn").c_str(), "rb");
        if (probe) {
            fseek(probe, 0, SEEK_END);
            long sz = ftell(probe);
            fclose(probe);
            size_t n_sents = (sz > 0) ? static_cast<size_t>(sz) / sizeof(Region) : 0;
            if (n_sents != sentence_region_attr_rows_.size())
                throw std::runtime_error("s.rgn / sentence_region_attr_rows_ size mismatch");
        }
        if (!sentence_region_attr_rows_.empty()) {
            std::unordered_set<std::string> sent_attr_keys;
            for (const auto& row : sentence_region_attr_rows_)
                for (const auto& kv : row)
                    sent_attr_keys.insert(kv.first);
            for (const auto& key : sent_attr_keys) {
                std::vector<std::string> values;
                values.reserve(sentence_region_attr_rows_.size());
                for (const auto& row : sentence_region_attr_rows_) {
                    std::string val = "_";
                    for (const auto& kv : row)
                        if (kv.first == key) {
                            val = kv.second;
                            break;
                        }
                    values.push_back(std::move(val));
                }
                std::string attr_base = output_dir_ + "/s_" + key;
                std::vector<int64_t> offsets;
                write_strings(attr_base + ".val", values, offsets);
                write_vec(attr_base + ".val.idx", offsets);
                region_attrs_list.push_back("s_" + key);
            }
        }
    }

    // SM-ROAD-2: Sort nested-type regions by (start, -end) so that
    // find_innermost_region() can find the most deeply nested region
    // containing a position via binary search + backwards scan.
    // Same nested_set is used for RG-REG-1 parent ids (.par).
    std::unordered_set<std::string> nested_sort_set(declared_nested_);
    for (const auto& [stype, det] : struct_mode_detected_)
        if (det.nesting_seen) nested_sort_set.insert(stype);

    for (auto& [type, regions] : regions_) {
        if (nested_sort_set.count(type) == 0) continue;
        size_t n = regions.size();
        if (n < 2) continue;

        // Build permutation index.
        // Note: structured bindings can't be captured in C++17 lambdas,
        // so alias 'regions' through a local pointer.
        auto* rgn_ptr = &regions;
        std::vector<size_t> perm(n);
        std::iota(perm.begin(), perm.end(), size_t(0));
        std::sort(perm.begin(), perm.end(), [rgn_ptr](size_t a, size_t b) {
            const auto& rgns = *rgn_ptr;
            if (rgns[a].start != rgns[b].start)
                return rgns[a].start < rgns[b].start;
            return rgns[a].end > rgns[b].end;  // -end: larger end first
        });

        // Apply permutation to regions
        std::vector<Region> sorted_rgn(n);
        for (size_t i = 0; i < n; ++i) sorted_rgn[i] = regions[perm[i]];
        regions = std::move(sorted_rgn);

        // Apply same permutation to parallel attribute value vectors
        auto avit = region_attr_values_.find(type);
        if (avit != region_attr_values_.end()) {
            for (auto& [attr_name, values] : avit->second) {
                if (values.size() != n) continue;
                std::vector<std::string> sorted_vals(n);
                for (size_t i = 0; i < n; ++i) sorted_vals[i] = std::move(values[perm[i]]);
                values = std::move(sorted_vals);
            }
        }
        auto vit = region_values_.find(type);
        if (vit != region_values_.end() && vit->second.size() == n) {
            std::vector<std::string> sorted_vals(n);
            for (size_t i = 0; i < n; ++i) sorted_vals[i] = std::move(vit->second[perm[i]]);
            vit->second = std::move(sorted_vals);
        }

        std::cerr << "  sorted " << n << " nested " << type
                  << " regions by (start, -end)\n";
    }

    // Write non-sentence structural regions (and optional values)
    for (const auto& [type, regions] : regions_) {
        std::string base = output_dir_ + "/" + type;
        write_file(base + ".rgn", regions.data(), regions.size() * sizeof(Region));

        // RG-REG-1: parent region id per row (nested structural types only)
        if (nested_sort_set.count(type) != 0 && !regions.empty()) {
            size_t amb = 0;
            std::vector<int32_t> par = compute_parent_region_ids(regions, amb);
            write_vec(base + ".par", par);
            if (amb > 0)
                std::cerr << "Warning: structure '" << type << "': " << amb
                          << " region(s) with ambiguous parent (parent set to -1)\n";
            std::cerr << "  wrote " << regions.size() << " parent ids for nested " << type << "\n";
        }

        auto avit = region_attr_values_.find(type);
        if (avit != region_attr_values_.end() && !avit->second.empty()) {
            for (const auto& [attr_name, values] : avit->second) {
                if (values.size() != regions.size())
                    throw std::runtime_error("Region attr " + type + "_" + attr_name + " size mismatch");
                std::string attr_base = output_dir_ + "/" + type + "_" + attr_name;
                std::vector<int64_t> offsets;
                write_strings(attr_base + ".val", values, offsets);
                write_vec(attr_base + ".val.idx", offsets);
                region_attrs_list.push_back(type + "_" + attr_name);
            }
        } else {
            auto vit = region_values_.find(type);
            if (vit != region_values_.end() && !vit->second.empty()) {
                std::vector<int64_t> offsets;
                write_strings(base + ".val", vit->second, offsets);
                write_vec(base + ".val.idx", offsets);
            }
        }
    }

    for (const auto& ra : region_attrs_list) {
        std::cerr << "  region reverse index " << ra << " ...\n";
        build_region_attr_reverse_index(output_dir_ + "/" + ra);
    }

    // Write corpus.info
    {
        std::ofstream info(output_dir_ + "/corpus.info");
        if (!info) throw std::runtime_error("Cannot create corpus.info");
        info << "size=" << corpus_size_ << "\n";
        info << "positional=";
        for (size_t i = 0; i < sorted_attrs.size(); ++i) {
            if (i > 0) info << ",";
            info << sorted_attrs[i];
        }
        info << "\n";

        std::vector<std::string> structs;
        structs.push_back("s");
        for (const auto& s : struct_set_)
            if (s != "s") structs.push_back(s);
        std::sort(structs.begin(), structs.end());

        info << "structural=";
        for (size_t i = 0; i < structs.size(); ++i) {
            if (i > 0) info << ",";
            info << structs[i];
        }
        info << "\n";
        if (!region_attrs_list.empty()) {
            info << "region_attrs=";
            for (size_t i = 0; i < region_attrs_list.size(); ++i) {
                if (i > 0) info << ",";
                info << region_attrs_list[i];
            }
            info << "\n";
        }
        if (!default_within_.empty())
            info << "default_within=" << default_within_ << "\n";

        // SM-ROAD-1 + SM-ROAD-6: Merge declared (from header) and detected structure modes.
        std::unordered_set<std::string> nested_set(declared_nested_);
        std::unordered_set<std::string> overlap_set(declared_overlapping_);
        std::unordered_set<std::string> zerowidth_set(declared_zerowidth_);
        for (const auto& [stype, det] : struct_mode_detected_) {
            if (det.nesting_seen)  nested_set.insert(stype);
            if (det.overlap_seen)  overlap_set.insert(stype);
            if (det.zerowidth_seen) zerowidth_set.insert(stype);
        }
        std::vector<std::string> nested_list(nested_set.begin(), nested_set.end());
        std::vector<std::string> overlap_list(overlap_set.begin(), overlap_set.end());
        std::vector<std::string> zerowidth_list(zerowidth_set.begin(), zerowidth_set.end());
        std::sort(nested_list.begin(), nested_list.end());
        std::sort(overlap_list.begin(), overlap_list.end());
        std::sort(zerowidth_list.begin(), zerowidth_list.end());

        if (!nested_list.empty()) {
            info << "nested=";
            for (size_t i = 0; i < nested_list.size(); ++i) {
                if (i > 0) info << ",";
                info << nested_list[i];
            }
            info << "\n";
        }
        if (!overlap_list.empty()) {
            info << "overlapping=";
            for (size_t i = 0; i < overlap_list.size(); ++i) {
                if (i > 0) info << ",";
                info << overlap_list[i];
            }
            info << "\n";
        }
        if (!zerowidth_list.empty()) {
            info << "zerowidth=";
            for (size_t i = 0; i < zerowidth_list.size(); ++i) {
                if (i > 0) info << ",";
                info << zerowidth_list[i];
            }
            info << "\n";
        }
        if (!declared_multivalue_.empty()) {
            std::vector<std::string> mv_list(declared_multivalue_.begin(), declared_multivalue_.end());
            std::sort(mv_list.begin(), mv_list.end());
            info << "multivalue=";
            for (size_t i = 0; i < mv_list.size(); ++i) {
                if (i > 0) info << ",";
                info << mv_list[i];
            }
            info << "\n";
        }
    }

    // SM-ROAD-6: Warn when non-flat geometry is detected but the corpus header did not
    // declare it (intentional corpora list nested=/overlapping=/zerowidth= and stay quiet).
    for (const auto& [stype, det] : struct_mode_detected_) {
        if (det.nesting_seen && declared_nested_.count(stype) == 0)
            std::cerr << "Warning: structure '" << stype << "' has nested regions ("
                      << det.nesting_count << " nesting pairs). "
                      << "Add nested=" << stype << " to the corpus header so metadata matches the data "
                      << "(index still sorts spans and writes .par when nesting is detected).\n";
        if (det.overlap_seen && declared_overlapping_.count(stype) == 0)
            std::cerr << "Warning: structure '" << stype << "' has crossing/overlapping regions ("
                      << det.overlap_count << " overlap pairs). "
                      << "Add overlapping=" << stype << " to the input header if you want it declared "
                      << "up front; the built corpus.info will still list overlapping=" << stype
                      << " so queries use overlap-aware paths.\n";
        if (det.zerowidth_seen && declared_zerowidth_.count(stype) == 0)
            std::cerr << "Warning: structure '" << stype << "' has zero-width regions ("
                      << det.zerowidth_count << " detected). "
                      << "Add zerowidth=" << stype << " to the corpus header; gap attachment remains "
                      << "incomplete (SM-ROAD-4).\n";
    }

    finalized_ = true;
    std::cerr << "Done.\n";
}

// ── finalize_attribute: sort lexicon, remap .dat, build .rev ────────────

void StreamingBuilder::finalize_attribute(const std::string& name,
                                          AttrState& state) {
    std::string base = output_dir_ + "/" + name;
    int32_t lex_size = static_cast<int32_t>(state.id_to_str.size());

    // Choose optimal .dat width based on vocabulary size
    int dat_width = 4;
    if (lex_size < 256) dat_width = 1;
    else if (lex_size < 65536) dat_width = 2;

    // Choose optimal .rev width based on corpus size
    int rev_width = 8;
    if (corpus_size_ < 32768) rev_width = 2;
    else if (corpus_size_ < INT32_MAX) rev_width = 4;

    std::cerr << "    dat_width=" << dat_width << " rev_width=" << rev_width << "\n";

    // 1. Sort lexicon: compute permutation old_index → sorted position
    std::vector<int32_t> sorted_idx(lex_size);
    std::iota(sorted_idx.begin(), sorted_idx.end(), 0);
    std::sort(sorted_idx.begin(), sorted_idx.end(),
              [&](int32_t a, int32_t b) {
                  return state.id_to_str[a] < state.id_to_str[b];
              });

    // Build old→new remap: remap[old_id] = new_id
    std::vector<int32_t> remap(lex_size);
    for (int32_t new_id = 0; new_id < lex_size; ++new_id)
        remap[sorted_idx[new_id]] = new_id;

    // 2. Write .lex and .lex.idx (sorted strings)
    std::vector<std::string> sorted_strings(lex_size);
    for (int32_t i = 0; i < lex_size; ++i)
        sorted_strings[i] = state.id_to_str[sorted_idx[i]];

    std::vector<int64_t> lex_offsets;
    write_strings(base + ".lex", sorted_strings, lex_offsets);
    write_vec(base + ".lex.idx", lex_offsets);

    // Free lexicon memory — no longer needed
    state.str_to_id.clear();
    state.id_to_str.clear();
    sorted_strings.clear();
    sorted_strings.shrink_to_fit();

    // 3. Remap .dat (temp int32 IDs → sorted IDs, compacted to target width)
    remap_dat(base + ".dat", remap, dat_width);

    // 4. Build reverse index from the remapped .dat
    build_reverse_index(base, lex_size, rev_width);

    // 5. RG-5f: Build MV (multivalue) component reverse index if this attr is MV
    if (declared_multivalue_.count(name))
        build_mv_reverse_index(base, lex_size, rev_width);
}

// ── remap_dat: read temp IDs, apply remap, write final IDs ─────────────

void StreamingBuilder::remap_dat(const std::string& dat_path,
                                 const std::vector<int32_t>& remap,
                                 int target_width) {
    std::string tmp_path = dat_path + ".tmp";
    FILE* in  = fopen(dat_path.c_str(), "rb");
    FILE* out = fopen(tmp_path.c_str(), "wb");
    if (!in || !out) {
        if (in)  fclose(in);
        if (out) fclose(out);
        throw std::runtime_error("Cannot remap " + dat_path);
    }

    std::vector<int32_t> buf(IO_CHUNK);
    size_t nread;
    while ((nread = fread(buf.data(), sizeof(int32_t), IO_CHUNK, in)) > 0) {
        for (size_t i = 0; i < nread; ++i)
            buf[i] = remap[static_cast<size_t>(buf[i])];

        if (target_width == 1) {
            std::vector<uint8_t> narrow(nread);
            for (size_t i = 0; i < nread; ++i)
                narrow[i] = static_cast<uint8_t>(buf[i]);
            fwrite(narrow.data(), 1, nread, out);
        } else if (target_width == 2) {
            std::vector<uint16_t> narrow(nread);
            for (size_t i = 0; i < nread; ++i)
                narrow[i] = static_cast<uint16_t>(buf[i]);
            fwrite(narrow.data(), 2, nread, out);
        } else {
            fwrite(buf.data(), sizeof(int32_t), nread, out);
        }
    }

    fclose(in);
    fclose(out);
    fs::rename(tmp_path, dat_path);
}

// ── build_reverse_index: two-pass (count + fill via mmap) ──────────────

void StreamingBuilder::build_reverse_index(const std::string& base,
                                           int32_t lex_size,
                                           int rev_width) {
    std::string dat_path     = base + ".dat";
    std::string rev_path     = base + ".rev";
    std::string rev_idx_path = base + ".rev.idx";

    // Determine .dat element width so we can read the compacted file
    int dat_width = 4;
    {
        FILE* probe = fopen(dat_path.c_str(), "rb");
        if (!probe) throw std::runtime_error("Cannot open " + dat_path);
        fseek(probe, 0, SEEK_END);
        long fsize = ftell(probe);
        fclose(probe);
        if (corpus_size_ > 0)
            dat_width = static_cast<int>(static_cast<size_t>(fsize) /
                                         static_cast<size_t>(corpus_size_));
    }

    FILE* dat = fopen(dat_path.c_str(), "rb");
    if (!dat) throw std::runtime_error("Cannot open " + dat_path);

    // Pass 1: count positions per lex ID (reads dat with correct width)
    std::vector<int64_t> cnt(static_cast<size_t>(lex_size), 0);
    size_t nread;

    if (dat_width == 1) {
        std::vector<uint8_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 1, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) ++cnt[buf[i]];
    } else if (dat_width == 2) {
        std::vector<uint16_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 2, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) ++cnt[buf[i]];
    } else {
        std::vector<int32_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), sizeof(int32_t), IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i)
                ++cnt[static_cast<size_t>(buf[i])];
    }

    // Compute prefix sums → rev.idx
    std::vector<int64_t> rev_idx(static_cast<size_t>(lex_size) + 1);
    rev_idx[0] = 0;
    for (int32_t i = 0; i < lex_size; ++i)
        rev_idx[static_cast<size_t>(i) + 1] =
            rev_idx[static_cast<size_t>(i)] + cnt[static_cast<size_t>(i)];

    write_vec(rev_idx_path, rev_idx);
    cnt.clear();

    int64_t total_pos = rev_idx[static_cast<size_t>(lex_size)];
    if (total_pos == 0) {
        fclose(dat);
        write_file(rev_path, nullptr, 0);
        return;
    }

    // Pass 2: fill .rev via writable mmap (at chosen rev_width)
    size_t rev_bytes = static_cast<size_t>(total_pos) * static_cast<size_t>(rev_width);
    int fd = ::open(rev_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("Cannot create " + rev_path);
    if (ftruncate(fd, static_cast<off_t>(rev_bytes)) != 0) {
        ::close(fd);
        throw std::runtime_error("Cannot set size of " + rev_path);
    }

    void* rev_raw = mmap(nullptr, rev_bytes, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
    if (rev_raw == MAP_FAILED) {
        ::close(fd);
        throw std::runtime_error("Cannot mmap " + rev_path + " for writing");
    }

    // Cursors: where to write the next position for each lex ID
    std::vector<int64_t> cursor(rev_idx.begin(),
                                rev_idx.begin() + lex_size);

    // Helper to write a position at cursor[id] with the right width
    auto write_pos = [&](size_t id, CorpusPos pos) {
        int64_t idx = cursor[id]++;
        switch (rev_width) {
            case 2:
                static_cast<int16_t*>(rev_raw)[idx] = static_cast<int16_t>(pos);
                break;
            case 4:
                static_cast<int32_t*>(rev_raw)[idx] = static_cast<int32_t>(pos);
                break;
            default:
                static_cast<int64_t*>(rev_raw)[idx] = static_cast<int64_t>(pos);
                break;
        }
    };

    rewind(dat);
    CorpusPos pos = 0;

    if (dat_width == 1) {
        std::vector<uint8_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 1, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) write_pos(buf[i], pos++);
    } else if (dat_width == 2) {
        std::vector<uint16_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 2, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) write_pos(buf[i], pos++);
    } else {
        std::vector<int32_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), sizeof(int32_t), IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i)
                write_pos(static_cast<size_t>(buf[i]), pos++);
    }

    msync(rev_raw, rev_bytes, MS_SYNC);
    munmap(rev_raw, rev_bytes);
    ::close(fd);
    fclose(dat);
}

// ── build_mv_reverse_index: component-level reverse index for multivalue attrs ──
// For an attribute like wsd with compound values "artist|writer", this builds:
//   .mv.lex      — sorted unique component strings ("artist", "writer", ...)
//   .mv.lex.idx  — byte offsets into .mv.lex
//   .mv.rev      — sorted positions for each component
//   .mv.rev.idx  — int64[mv_lex_size+1] offsets into .mv.rev
//
// Algorithm:
//   1. Read the existing sorted .lex to find compound entries containing '|'.
//      For each compound, split on '|' → map component string to a set of
//      original lex IDs whose values contain that component.
//   2. Build the MV component lexicon (sorted unique components).
//   3. Two-pass over .dat (like build_reverse_index):
//      Pass 1 — count: for each position, look up its lex ID; if that ID has
//               components, increment counts for each component's MV lex ID.
//      Pass 2 — fill: same scan, write positions into the right .mv.rev slots.

void StreamingBuilder::build_mv_reverse_index(const std::string& base,
                                               int32_t lex_size,
                                               int rev_width) {
    // 1. Read the existing .lex to find pipe-containing entries
    MmapFile lex_data = MmapFile::open(base + ".lex", false);
    MmapFile lex_idx  = MmapFile::open(base + ".lex.idx", false);
    if (!lex_data.valid() || !lex_idx.valid()) return;

    const int64_t* loff = lex_idx.as<int64_t>();
    const char* lbase = static_cast<const char*>(lex_data.data());

    // Collect unique component strings and map: orig_lex_id → list of component strings
    std::unordered_map<std::string, int32_t> comp_to_mvid;  // filled after sorting
    std::unordered_set<std::string> comp_set;

    // For each orig lex id, which MV component IDs does it map to?
    // (filled after we know the sorted MV lex)
    // First pass: just collect component strings and which orig IDs have them
    std::vector<std::vector<std::string>> orig_to_comps(static_cast<size_t>(lex_size));

    for (int32_t id = 0; id < lex_size; ++id) {
        int64_t a = loff[id];
        int64_t b = loff[id + 1];
        std::string_view sv(lbase + a, static_cast<size_t>(b - a));
        // Remove trailing NUL if present
        if (!sv.empty() && sv.back() == '\0') sv.remove_suffix(1);

        // Check for pipe
        if (sv.find('|') == std::string_view::npos) continue;

        // Split on '|'
        size_t start = 0;
        while (start < sv.size()) {
            size_t p = sv.find('|', start);
            if (p == std::string_view::npos) p = sv.size();
            std::string comp(sv.substr(start, p - start));
            if (!comp.empty()) {
                comp_set.insert(comp);
                orig_to_comps[static_cast<size_t>(id)].push_back(std::move(comp));
            }
            start = p + 1;
        }
    }

    if (comp_set.empty()) return;  // no compounds found

    // Also add non-compound entries as components of themselves — positions with
    // a single value "artist" (no pipe) should still be findable via the MV index.
    // We only need to do this for entries that ALSO appear as components of compounds.
    for (int32_t id = 0; id < lex_size; ++id) {
        if (!orig_to_comps[static_cast<size_t>(id)].empty()) continue;  // already split
        int64_t a = loff[id];
        int64_t b = loff[id + 1];
        std::string_view sv(lbase + a, static_cast<size_t>(b - a));
        if (!sv.empty() && sv.back() == '\0') sv.remove_suffix(1);
        std::string s(sv);
        if (comp_set.count(s)) {
            // This single value also appears as a component in some compound
            orig_to_comps[static_cast<size_t>(id)].push_back(std::move(s));
        }
    }

    // 2. Build sorted MV component lexicon
    std::vector<std::string> mv_sorted(comp_set.begin(), comp_set.end());
    std::sort(mv_sorted.begin(), mv_sorted.end());
    for (int32_t i = 0; i < static_cast<int32_t>(mv_sorted.size()); ++i)
        comp_to_mvid[mv_sorted[static_cast<size_t>(i)]] = i;

    int32_t mv_lex_size = static_cast<int32_t>(mv_sorted.size());

    // Write .mv.lex and .mv.lex.idx
    std::vector<int64_t> mv_lex_offsets;
    write_strings(base + ".mv.lex", mv_sorted, mv_lex_offsets);
    write_vec(base + ".mv.lex.idx", mv_lex_offsets);

    // Build orig_id → mv_ids mapping (using int32_t mv lex IDs)
    std::vector<std::vector<int32_t>> orig_to_mvids(static_cast<size_t>(lex_size));
    for (int32_t id = 0; id < lex_size; ++id) {
        for (const auto& comp : orig_to_comps[static_cast<size_t>(id)])
            orig_to_mvids[static_cast<size_t>(id)].push_back(comp_to_mvid[comp]);
    }

    // Free memory we no longer need
    orig_to_comps.clear();
    orig_to_comps.shrink_to_fit();
    comp_set.clear();
    comp_to_mvid.clear();

    // 3. Two-pass over .dat to build .mv.rev
    std::string dat_path     = base + ".dat";
    std::string rev_path     = base + ".mv.rev";
    std::string rev_idx_path = base + ".mv.rev.idx";

    // Determine .dat element width
    int dat_width = 4;
    {
        FILE* probe = fopen(dat_path.c_str(), "rb");
        if (!probe) throw std::runtime_error("Cannot open " + dat_path);
        fseek(probe, 0, SEEK_END);
        long fsize = ftell(probe);
        fclose(probe);
        if (corpus_size_ > 0)
            dat_width = static_cast<int>(static_cast<size_t>(fsize) /
                                         static_cast<size_t>(corpus_size_));
    }

    FILE* dat = fopen(dat_path.c_str(), "rb");
    if (!dat) throw std::runtime_error("Cannot open " + dat_path);

    // Pass 1: count positions per MV component ID
    std::vector<int64_t> cnt(static_cast<size_t>(mv_lex_size), 0);
    size_t nread;

    auto count_id = [&](size_t orig_id) {
        for (int32_t mvid : orig_to_mvids[orig_id])
            ++cnt[static_cast<size_t>(mvid)];
    };

    if (dat_width == 1) {
        std::vector<uint8_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 1, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) count_id(buf[i]);
    } else if (dat_width == 2) {
        std::vector<uint16_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 2, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) count_id(buf[i]);
    } else {
        std::vector<int32_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), sizeof(int32_t), IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) count_id(static_cast<size_t>(buf[i]));
    }

    // Compute prefix sums → .mv.rev.idx
    std::vector<int64_t> mv_rev_idx(static_cast<size_t>(mv_lex_size) + 1);
    mv_rev_idx[0] = 0;
    for (int32_t i = 0; i < mv_lex_size; ++i)
        mv_rev_idx[static_cast<size_t>(i) + 1] =
            mv_rev_idx[static_cast<size_t>(i)] + cnt[static_cast<size_t>(i)];

    write_vec(rev_idx_path, mv_rev_idx);
    cnt.clear();

    int64_t total_pos = mv_rev_idx[static_cast<size_t>(mv_lex_size)];
    if (total_pos == 0) {
        fclose(dat);
        write_file(rev_path, nullptr, 0);
        return;
    }

    // Pass 2: fill .mv.rev via writable mmap
    size_t rev_bytes = static_cast<size_t>(total_pos) * static_cast<size_t>(rev_width);
    int fd = ::open(rev_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("Cannot create " + rev_path);
    if (ftruncate(fd, static_cast<off_t>(rev_bytes)) != 0) {
        ::close(fd);
        throw std::runtime_error("Cannot set size of " + rev_path);
    }

    void* rev_raw = mmap(nullptr, rev_bytes, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
    if (rev_raw == MAP_FAILED) {
        ::close(fd);
        throw std::runtime_error("Cannot mmap " + rev_path + " for writing");
    }

    // Cursors: where to write the next position for each MV lex ID
    std::vector<int64_t> cursor(mv_rev_idx.begin(),
                                mv_rev_idx.begin() + mv_lex_size);

    auto write_pos = [&](int32_t mvid, CorpusPos pos) {
        int64_t idx = cursor[static_cast<size_t>(mvid)]++;
        switch (rev_width) {
            case 2:
                static_cast<int16_t*>(rev_raw)[idx] = static_cast<int16_t>(pos);
                break;
            case 4:
                static_cast<int32_t*>(rev_raw)[idx] = static_cast<int32_t>(pos);
                break;
            default:
                static_cast<int64_t*>(rev_raw)[idx] = static_cast<int64_t>(pos);
                break;
        }
    };

    rewind(dat);
    CorpusPos pos = 0;

    auto fill_id = [&](size_t orig_id) {
        for (int32_t mvid : orig_to_mvids[orig_id])
            write_pos(mvid, pos);
        ++pos;
    };

    if (dat_width == 1) {
        std::vector<uint8_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 1, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) fill_id(buf[i]);
    } else if (dat_width == 2) {
        std::vector<uint16_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 2, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) fill_id(buf[i]);
    } else {
        std::vector<int32_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), sizeof(int32_t), IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) fill_id(static_cast<size_t>(buf[i]));
    }

    msync(rev_raw, rev_bytes, MS_SYNC);
    munmap(rev_raw, rev_bytes);
    ::close(fd);
    fclose(dat);

    std::cerr << "    mv_reverse: " << mv_lex_size << " components, "
              << total_pos << " entries\n";
}

} // namespace manatree
