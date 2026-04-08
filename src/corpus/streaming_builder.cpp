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

namespace {

void merge_mv_gid(std::string& cell, const std::string& gid) {
    if (gid.empty()) return;
    if (cell.empty() || cell == "_") {
        cell = gid;
        return;
    }
    size_t start = 0;
    while (start <= cell.size()) {
        size_t p = cell.find('|', start);
        size_t end = (p == std::string::npos) ? cell.size() : p;
        if (end > start && cell.compare(start, end - start, gid) == 0) return;
        if (p == std::string::npos) break;
        start = p + 1;
    }
    cell += '|';
    cell += gid;
}

} // namespace

// ── StreamingBuilder lifecycle ──────────────────────────────────────────

StreamingBuilder::StreamingBuilder(const std::string& output_dir, bool overlay_standoff_only)
    : output_dir_(output_dir), overlay_standoff_only_(overlay_standoff_only) {
    fs::create_directories(output_dir);
    if (!overlay_standoff_only_) {
        sent_rgn_file_ = fopen((output_dir + "/s.rgn").c_str(), "wb");
        if (!sent_rgn_file_)
            throw std::runtime_error("Cannot create " + output_dir + "/s.rgn");
    }
}

void StreamingBuilder::bootstrap_overlay_corpus_size(CorpusPos n) {
    if (!overlay_standoff_only_)
        throw std::runtime_error("bootstrap_overlay_corpus_size: not in overlay standoff mode");
    if (finalized_)
        throw std::logic_error("bootstrap_overlay_corpus_size called after finalize");
    if (n == 0)
        throw std::runtime_error("bootstrap_overlay_corpus_size: size must be > 0");
    corpus_size_ = n;
    for (const auto& name : token_group_standoff_memory_attrs_) {
        ensure_memory_attr(name);
        auto& col = token_group_memory_column_[name];
        col.assign(static_cast<size_t>(n), "_");
        auto it = attrs_.find(name);
        if (it != attrs_.end())
            it->second->written = n;
    }
}

StreamingBuilder::~StreamingBuilder() {
    if (dep_head_file_)      fclose(dep_head_file_);
    if (dep_euler_in_file_)  fclose(dep_euler_in_file_);
    if (dep_euler_out_file_) fclose(dep_euler_out_file_);
    if (sent_rgn_file_)      fclose(sent_rgn_file_);
    if (group_table_file_)   fclose(group_table_file_);
}

// ── Attribute management ────────────────────────────────────────────────

void StreamingBuilder::declare_token_group_layer(
        const std::string& struct_name,
        const std::string& membership_attr,
        const std::vector<std::string>& prop_attr_keys) {
    token_group_struct_.insert(struct_name);
    token_group_membership_attr_[struct_name] = membership_attr;
    token_group_membership_attrs_.insert(membership_attr);
    token_group_standoff_memory_attrs_.insert(membership_attr);
    token_group_prop_attrs_[struct_name] = prop_attr_keys;
    for (const auto& pk : prop_attr_keys) {
        if (pk.empty() || pk == "id" || pk == "group_id") continue;
        token_group_standoff_memory_attrs_.insert(struct_name + "_" + pk);
    }
}

void StreamingBuilder::ensure_memory_attr(const std::string& name) {
    if (attr_set_.count(name)) return;
    attr_set_.insert(name);
    attr_order_.push_back(name);

    auto state = std::make_unique<AttrState>();
    state->memory_strings = &token_group_memory_column_[name];
    state->str_to_id.reserve(256);
    state->id_to_str.reserve(256);

    attrs_[name] = std::move(state);
}

void StreamingBuilder::ensure_attr(const std::string& name) {
    if (attr_set_.count(name)) return;
    if (token_group_standoff_memory_attrs_.count(name)) {
        ensure_memory_attr(name);
        return;
    }
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
    if (state.memory_strings) {
        while (state.memory_strings->size() < static_cast<size_t>(corpus_size_)) {
            state.memory_strings->push_back("_");
            ++state.written;
        }
        return;
    }
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
        if (state.memory_strings) {
            state.memory_strings->push_back(value.empty() ? "_" : value);
            ++state.written;
        } else {
            int32_t id = state.get_or_assign(value);
            fwrite(&id, sizeof(int32_t), 1, state.dat_file);
            ++state.written;
        }
    }

    // Write placeholder "_" for attributes NOT in this token (fast path)
    for (auto& [name, state] : attrs_) {
        if (state->written > corpus_size_) continue;  // already written above
        backfill_attr(*state);
        if (state->memory_strings) {
            state->memory_strings->push_back("_");
            ++state->written;
        } else {
            int32_t id = state->get_placeholder();
            fwrite(&id, sizeof(int32_t), 1, state->dat_file);
            ++state->written;
        }
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

namespace {

std::string json_escape_attr(const std::string& s) {
    std::string o;
    o.reserve(s.size() + 8);
    for (char c : s) {
        if (c == '"' || c == '\\') o += '\\';
        o += c;
    }
    return o;
}

} // namespace

void StreamingBuilder::append_group_table_row(
        const std::string& struct_name,
        const std::string& group_id,
        const std::vector<std::pair<std::string, std::string>>& attrs) {
    std::string key;
    key.reserve(struct_name.size() + 1 + group_id.size());
    key = struct_name;
    key.push_back('\0');
    key += group_id;
    if (!group_table_row_keys_.insert(std::move(key)).second)
        return;

    if (!group_table_file_) {
        std::string path = output_dir_ + "/group_table.jsonl";
        group_table_file_ = fopen(path.c_str(), "wb");
        if (!group_table_file_)
            throw std::runtime_error("Cannot create " + path);
    }
    std::string line = "{\"struct\":\"" + json_escape_attr(struct_name) + "\",\"id\":\""
                       + json_escape_attr(group_id) + "\"";
    for (const auto& kv : attrs) {
        if (kv.first == "id" || kv.first == "group_id") continue;
        line += ",\"" + json_escape_attr(kv.first) + "\":\"" + json_escape_attr(kv.second) + "\"";
    }
    line += "}\n";
    fwrite(line.data(), 1, line.size(), group_table_file_);
}

// Phase A: emit `groups/<struct>.jsonl`, one record per group, sorted by
// (first_pos, gid) for deterministic doc-order iteration. Each record carries
// the envelope, the list of sub-spans in the order they were emitted, and the
// declared prop attrs captured from the original annotation events.
void StreamingBuilder::write_token_group_indexes() {
    if (token_group_records_.empty()) return;
    fs::create_directories(output_dir_ + "/groups");
    for (auto& kv : token_group_records_) {
        const std::string& sname = kv.first;
        std::vector<GroupRecord>& records = kv.second;
        if (records.empty()) continue;
        auto* rec_ptr = &records;
        std::vector<size_t> perm(records.size());
        std::iota(perm.begin(), perm.end(), size_t(0));
        std::sort(perm.begin(), perm.end(), [rec_ptr](size_t a, size_t b) {
            const auto& rs = *rec_ptr;
            if (rs[a].first_pos != rs[b].first_pos)
                return rs[a].first_pos < rs[b].first_pos;
            return rs[a].gid < rs[b].gid;
        });
        std::string path = output_dir_ + "/groups/" + sname + ".jsonl";
        FILE* f = fopen(path.c_str(), "wb");
        if (!f) throw std::runtime_error("Cannot create " + path);
        for (size_t i : perm) {
            const GroupRecord& r = records[i];
            std::string line = "{\"gid\":\"" + json_escape_attr(r.gid) + "\"";
            line += ",\"first\":" + std::to_string(r.first_pos);
            line += ",\"last\":"  + std::to_string(r.last_pos);
            line += ",\"spans\":[";
            for (size_t k = 0; k < r.spans.size(); ++k) {
                if (k) line += ",";
                line += "[" + std::to_string(r.spans[k].first) + ","
                            + std::to_string(r.spans[k].second) + "]";
            }
            line += "]";
            if (!r.props.empty()) {
                line += ",\"props\":{";
                for (size_t k = 0; k < r.props.size(); ++k) {
                    if (k) line += ",";
                    line += "\"" + json_escape_attr(r.props[k].first) + "\":\""
                                 + json_escape_attr(r.props[k].second) + "\"";
                }
                line += "}";
            }
            line += "}\n";
            fwrite(line.data(), 1, line.size(), f);
        }
        fclose(f);
        std::cerr << "  wrote " << records.size() << " token-group records for "
                  << sname << " -> groups/" << sname << ".jsonl\n";
    }
}

void StreamingBuilder::apply_token_group_annotation(
        const std::string& struct_name,
        CorpusPos start,
        CorpusPos end,
        const std::vector<std::pair<std::string, std::string>>& attrs) {
    auto it_m = token_group_membership_attr_.find(struct_name);
    if (it_m == token_group_membership_attr_.end()) return;
    if (corpus_size_ == 0 || start > end) return;
    if (start >= corpus_size_ || end >= corpus_size_) {
        static bool warned_oor = false;
        if (!warned_oor) {
            std::cerr << "Warning: token-group '" << struct_name
                      << "' annotation refers to positions [" << start << ".." << end
                      << "] beyond current corpus size (" << corpus_size_
                      << "); annotation dropped. Further occurrences will be silenced.\n";
            warned_oor = true;
        }
        if (start >= corpus_size_) return;
        if (end >= corpus_size_) end = corpus_size_ - 1;
    }

    std::string gid;
    for (const auto& kv : attrs) {
        if (kv.first == "id" || kv.first == "group_id") {
            gid = kv.second;
            break;
        }
    }
    if (gid.empty()) {
        static bool warned_no_gid = false;
        if (!warned_no_gid) {
            std::cerr << "Warning: token-group '" << struct_name
                      << "' annotation has no 'id'/'group_id' attribute; annotation dropped. "
                         "Further occurrences will be silenced.\n";
            warned_no_gid = true;
        }
        return;
    }

    const std::string& memb = it_m->second;
    ensure_memory_attr(memb);
    auto& col = token_group_memory_column_[memb];
    if (col.size() < static_cast<size_t>(corpus_size_))
        col.resize(static_cast<size_t>(corpus_size_), "_");

    for (CorpusPos p = start; p <= end; ++p)
        merge_mv_gid(col[static_cast<size_t>(p)], gid);

    auto it_prop = token_group_prop_attrs_.find(struct_name);
    if (it_prop != token_group_prop_attrs_.end()) {
        for (const std::string& pk : it_prop->second) {
            if (pk.empty() || pk == "id" || pk == "group_id") continue;
            std::string v;
            for (const auto& kv : attrs) {
                if (kv.first == pk) {
                    v = kv.second;
                    break;
                }
            }
            if (v.empty()) continue;
            const std::string col_name = struct_name + "_" + pk;
            ensure_memory_attr(col_name);
            auto& pcol = token_group_memory_column_[col_name];
            if (pcol.size() < static_cast<size_t>(corpus_size_))
                pcol.resize(static_cast<size_t>(corpus_size_), "_");
            for (CorpusPos p = start; p <= end; ++p)
                merge_mv_gid(pcol[static_cast<size_t>(p)], v);
        }
    }

    append_group_table_row(struct_name, gid, attrs);

    // Phase A: per-group record (envelope + sub-spans + prop attrs), used by
    // write_token_group_indexes() at finalize to emit groups/<struct>.jsonl.
    {
        auto& idx_map = token_group_record_idx_[struct_name];
        auto& records = token_group_records_[struct_name];
        size_t ridx;
        auto rit = idx_map.find(gid);
        if (rit == idx_map.end()) {
            ridx = records.size();
            idx_map.emplace(gid, ridx);
            records.emplace_back();
            records.back().gid = gid;
        } else {
            ridx = rit->second;
        }
        GroupRecord& rec = records[ridx];
        rec.spans.emplace_back(start, end);
        if (start < rec.first_pos) rec.first_pos = start;
        if (end   > rec.last_pos)  rec.last_pos  = end;
        if (it_prop != token_group_prop_attrs_.end() && rec.props.empty()) {
            for (const std::string& pk : it_prop->second) {
                if (pk.empty() || pk == "id" || pk == "group_id") continue;
                for (const auto& kv : attrs) {
                    if (kv.first == pk) {
                        rec.props.emplace_back(pk, kv.second);
                        break;
                    }
                }
            }
        }
    }
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
        if (state->dat_file) {
            fclose(state->dat_file);
            state->dat_file = nullptr;
        }
    }
    if (group_table_file_) {
        fclose(group_table_file_);
        group_table_file_ = nullptr;
    }
    // Phase A: emit per-struct sidecar listing every group's sub-spans + props.
    write_token_group_indexes();
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

    // Sentence structural attributes (s_<name>.val), parallel to s.rgn.
    // Overlay-only builds never write s.rgn, so skip the consistency probe.
    {
        if (!overlay_standoff_only_) {
            FILE* probe = fopen((output_dir_ + "/s.rgn").c_str(), "rb");
            if (probe) {
                fseek(probe, 0, SEEK_END);
                long sz = ftell(probe);
                fclose(probe);
                size_t n_sents = (sz > 0) ? static_cast<size_t>(sz) / sizeof(Region) : 0;
                if (n_sents != sentence_region_attr_rows_.size())
                    throw std::runtime_error("s.rgn / sentence_region_attr_rows_ size mismatch");
            }
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
        if (!overlay_standoff_only_)
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
        if (!token_group_struct_.empty()) {
            std::vector<std::string> tg(token_group_struct_.begin(), token_group_struct_.end());
            std::sort(tg.begin(), tg.end());
            info << "token_groups=";
            for (size_t i = 0; i < tg.size(); ++i) {
                if (i > 0) info << ",";
                info << tg[i];
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

    if (state.memory_strings) {
        if (static_cast<CorpusPos>(state.memory_strings->size()) != corpus_size_)
            throw std::runtime_error("standoff memory column '" + name + "' size mismatch");
        state.str_to_id.clear();
        state.id_to_str.clear();
        state.placeholder_id = -1;
        for (const auto& s : *state.memory_strings)
            state.get_or_assign(s);
        FILE* raw = fopen((base + ".dat").c_str(), "wb");
        if (!raw) throw std::runtime_error("Cannot create " + base + ".dat");
        for (const auto& s : *state.memory_strings) {
            int32_t id = state.str_to_id.at(s);
            fwrite(&id, sizeof(int32_t), 1, raw);
        }
        fclose(raw);
        state.memory_strings = nullptr;
    }

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
    if (declared_multivalue_.count(name)) {
        build_mv_reverse_index(base, lex_size, rev_width);
        // Stage 1 of PANDO-MULTIVALUE-FIELDS: forward MV index .mv.fwd / .mv.fwd.idx
        // Built unconditionally for any multivalue= attr; spec dev/PANDO-MVAL-FORMAT.md (v0.2)
        build_mv_forward_index(base, lex_size);
    }
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

    // Scalar multivalue (no '|' in any lex string): treat each distinct non-empty
    // string as its own MV component. Needed for TEITOK-style standoff group ids
    // (e.g. err_gid) and any MV attr whose joined values are single tokens.
    if (comp_set.empty()) {
        for (int32_t id = 0; id < lex_size; ++id) {
            int64_t a = loff[id];
            int64_t b = loff[id + 1];
            std::string_view sv(lbase + a, static_cast<size_t>(b - a));
            if (!sv.empty() && sv.back() == '\0') sv.remove_suffix(1);
            if (sv.empty()) continue;
            std::string s(sv);
            comp_set.insert(s);
            orig_to_comps[static_cast<size_t>(id)].push_back(std::move(s));
        }
    }

    if (comp_set.empty()) return;  // no MV components at all

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

// ── build_mv_forward_index: position → sorted set of MV component lex ids ──
//
// Stage 1 of PANDO-MULTIVALUE-FIELDS, spec dev/PANDO-MVAL-FORMAT.md (v0.2).
//
// Writes:
//   .mv.fwd      — packed MV component ids per position (int16 if mv_lex_size
//                  ≤ INT16_MAX, else int32). Sorted ascending, deduplicated.
//   .mv.fwd.idx  — int64[corpus_size+1] element offsets into .mv.fwd.
//                  idx[p+1] - idx[p] is the cardinality at position p.
//
// Self-contained: re-parses base.mv.lex (just written by build_mv_reverse_index)
// + base.lex to rebuild orig_lex_id → sorted unique mv_lex_ids, then does a
// two-pass scan over .dat to write the forward index. The .lex re-parse is
// cheap relative to the .dat scan, and avoids any surgery on the existing
// reverse-index function.
//
// Empty / scalar-only attrs: if no joined string contains '|', .mv.fwd is
// zero-length and .mv.fwd.idx is filled with zeros (zero-length runs).
void StreamingBuilder::build_mv_forward_index(const std::string& base,
                                              int32_t lex_size) {
    // 1. Read .mv.lex (sorted MV components) to build component → mv_id map.
    // build_mv_reverse_index may skip writing these files (no MV components);
    // MmapFile::open throws on ENOENT — check first.
    if (!fs::exists(base + ".mv.lex") || !fs::exists(base + ".mv.lex.idx")) {
        std::vector<int64_t> empty_idx(static_cast<size_t>(corpus_size_) + 1, 0);
        write_vec(base + ".mv.fwd.idx", empty_idx);
        write_file(base + ".mv.fwd", nullptr, 0);
        return;
    }
    MmapFile mv_lex_data = MmapFile::open(base + ".mv.lex", false);
    MmapFile mv_lex_idx  = MmapFile::open(base + ".mv.lex.idx", false);
    if (!mv_lex_data.valid() || !mv_lex_idx.valid()) {
        // No reverse index was written (no compounds at all): write empty
        // forward index so loaders see has_mv_fwd() == true with zero entries.
        std::vector<int64_t> empty_idx(static_cast<size_t>(corpus_size_) + 1, 0);
        write_vec(base + ".mv.fwd.idx", empty_idx);
        write_file(base + ".mv.fwd", nullptr, 0);
        return;
    }

    const int64_t* mv_off = mv_lex_idx.as<int64_t>();
    size_t n_mv_idx = mv_lex_idx.count<int64_t>();
    if (n_mv_idx < 2) {
        std::vector<int64_t> empty_idx(static_cast<size_t>(corpus_size_) + 1, 0);
        write_vec(base + ".mv.fwd.idx", empty_idx);
        write_file(base + ".mv.fwd", nullptr, 0);
        return;
    }
    int32_t mv_lex_size = static_cast<int32_t>(n_mv_idx - 1);
    const char* mv_base = static_cast<const char*>(mv_lex_data.data());

    std::unordered_map<std::string, int32_t> comp_to_mvid;
    comp_to_mvid.reserve(static_cast<size_t>(mv_lex_size));
    for (int32_t i = 0; i < mv_lex_size; ++i) {
        int64_t a = mv_off[i];
        int64_t b = mv_off[i + 1];
        std::string_view sv(mv_base + a, static_cast<size_t>(b - a));
        if (!sv.empty() && sv.back() == '\0') sv.remove_suffix(1);
        comp_to_mvid.emplace(std::string(sv), i);
    }

    // 2. Read .lex and build orig_lex_id → sorted unique mv_ids.
    MmapFile lex_data = MmapFile::open(base + ".lex", false);
    MmapFile lex_idx  = MmapFile::open(base + ".lex.idx", false);
    if (!lex_data.valid() || !lex_idx.valid())
        throw std::runtime_error("build_mv_forward_index: missing .lex for " + base);

    const int64_t* loff = lex_idx.as<int64_t>();
    const char* lbase = static_cast<const char*>(lex_data.data());

    // For every joined-string lex id, what sorted set of mv ids does it map to?
    std::vector<std::vector<int32_t>> orig_to_mvids(static_cast<size_t>(lex_size));
    for (int32_t id = 0; id < lex_size; ++id) {
        int64_t a = loff[id];
        int64_t b = loff[id + 1];
        std::string_view sv(lbase + a, static_cast<size_t>(b - a));
        if (!sv.empty() && sv.back() == '\0') sv.remove_suffix(1);

        auto& bucket = orig_to_mvids[static_cast<size_t>(id)];

        if (sv.find('|') == std::string_view::npos) {
            // Single value: only included in MV view if it also appears as
            // a component of some compound (matches reverse-index policy).
            std::string s(sv);
            auto it = comp_to_mvid.find(s);
            if (it != comp_to_mvid.end()) bucket.push_back(it->second);
        } else {
            size_t start = 0;
            while (start < sv.size()) {
                size_t p = sv.find('|', start);
                if (p == std::string_view::npos) p = sv.size();
                std::string comp(sv.substr(start, p - start));
                if (!comp.empty()) {
                    auto it = comp_to_mvid.find(comp);
                    if (it != comp_to_mvid.end())
                        bucket.push_back(it->second);
                }
                start = p + 1;
            }
            // Sort + dedup so per-position payload satisfies the spec's
            // "strictly sorted, no duplicates" invariant.
            std::sort(bucket.begin(), bucket.end());
            bucket.erase(std::unique(bucket.begin(), bucket.end()), bucket.end());
        }
    }

    // 3. Determine .dat element width (same probe shape as build_mv_reverse_index).
    std::string dat_path = base + ".dat";
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

    // 4. Pass 1 over .dat: build .mv.fwd.idx (cumulative counts).
    //    fwd_idx[p+1] - fwd_idx[p] == cardinality at position p.
    std::vector<int64_t> fwd_idx(static_cast<size_t>(corpus_size_) + 1, 0);

    FILE* dat = fopen(dat_path.c_str(), "rb");
    if (!dat) throw std::runtime_error("Cannot open " + dat_path);

    auto count_id = [&](size_t orig_id, CorpusPos pos) {
        fwd_idx[static_cast<size_t>(pos) + 1] =
            static_cast<int64_t>(orig_to_mvids[orig_id].size());
    };

    size_t nread;
    CorpusPos pos = 0;
    if (dat_width == 1) {
        std::vector<uint8_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 1, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) count_id(buf[i], pos++);
    } else if (dat_width == 2) {
        std::vector<uint16_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 2, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) count_id(buf[i], pos++);
    } else {
        std::vector<int32_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), sizeof(int32_t), IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i)
                count_id(static_cast<size_t>(buf[i]), pos++);
    }

    // Cumulative sum → element offsets.
    for (size_t i = 1; i < fwd_idx.size(); ++i)
        fwd_idx[i] += fwd_idx[i - 1];

    int64_t total = fwd_idx.back();
    write_vec(base + ".mv.fwd.idx", fwd_idx);

    // 5. Choose forward element width based on mv_lex_size.
    //    int16 if all ids fit, else int32. Bounded by int32 lex space.
    int fwd_width = (mv_lex_size <= INT16_MAX) ? 2 : 4;

    if (total == 0) {
        fclose(dat);
        write_file(base + ".mv.fwd", nullptr, 0);
        std::cerr << "    mv_forward: 0 entries (width=" << fwd_width << ")\n";
        return;
    }

    // 6. Pass 2 over .dat: fill .mv.fwd via writable mmap.
    size_t fwd_bytes = static_cast<size_t>(total) * static_cast<size_t>(fwd_width);
    std::string fwd_path = base + ".mv.fwd";
    int fd = ::open(fwd_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("Cannot create " + fwd_path);
    if (ftruncate(fd, static_cast<off_t>(fwd_bytes)) != 0) {
        ::close(fd);
        throw std::runtime_error("Cannot set size of " + fwd_path);
    }
    void* fwd_raw = mmap(nullptr, fwd_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (fwd_raw == MAP_FAILED) {
        ::close(fd);
        throw std::runtime_error("Cannot mmap " + fwd_path + " for writing");
    }

    auto write_at = [&](int64_t element_idx, int32_t mvid) {
        if (fwd_width == 2)
            static_cast<int16_t*>(fwd_raw)[element_idx] = static_cast<int16_t>(mvid);
        else
            static_cast<int32_t*>(fwd_raw)[element_idx] = static_cast<int32_t>(mvid);
    };

    rewind(dat);
    pos = 0;
    auto fill_id = [&](size_t orig_id, CorpusPos p) {
        int64_t off = fwd_idx[static_cast<size_t>(p)];
        const auto& mvids = orig_to_mvids[orig_id];
        // mvids are already sorted+deduped from step 2.
        for (size_t i = 0; i < mvids.size(); ++i)
            write_at(off + static_cast<int64_t>(i), mvids[i]);
    };

    if (dat_width == 1) {
        std::vector<uint8_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 1, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) fill_id(buf[i], pos++);
    } else if (dat_width == 2) {
        std::vector<uint16_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), 2, IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i) fill_id(buf[i], pos++);
    } else {
        std::vector<int32_t> buf(IO_CHUNK);
        while ((nread = fread(buf.data(), sizeof(int32_t), IO_CHUNK, dat)) > 0)
            for (size_t i = 0; i < nread; ++i)
                fill_id(static_cast<size_t>(buf[i]), pos++);
    }

    msync(fwd_raw, fwd_bytes, MS_SYNC);
    munmap(fwd_raw, fwd_bytes);
    ::close(fd);
    fclose(dat);

    std::cerr << "    mv_forward: " << total << " entries, width=" << fwd_width
              << "\n";
}

} // namespace manatree
