#include "pando_version.h"
#include "corpus/corpus.h"
#include "api/query_json.h"
#include "core/json_utils.h"
#include "core/count_hierarchy_json.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/dialect/cwb/cwb_translate.h"
#include "query/dialect/pmltq/pmltq_translate.h"
#include "query/dialect/tiger/tiger_translate.h"
#include <cctype>
#include "query/executor.h"
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <map>
#include <iomanip>
#include <vector>
#include <unistd.h>  // isatty
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <stdexcept>
#include <filesystem>

#ifdef PANDO_HAVE_LINENOISE
#include <linenoise.h>
#endif

using namespace pando;

namespace fs = std::filesystem;

// True if dir contains a pando index (corpus.info from pando-index / StreamingBuilder).
static bool is_corpus_dir(const std::string& path) {
    if (path.empty() || path == "-")
        return false;
    std::error_code ec;
    const fs::path p(path);
    if (!fs::exists(p, ec) || !fs::is_directory(p, ec))
        return false;
    return fs::is_regular_file(p / "corpus.info", ec);
}

// ANSI color for KWIC match highlighting on TTY; <! !> delimiters when piped.
static bool use_color() {
    static int cached = -1;
    if (cached < 0) cached = isatty(STDOUT_FILENO);
    return cached;
}

// ── CLI options ─────────────────────────────────────────────────────────

struct Options {
    std::string corpus_dir;
    std::string query;
    std::string cql_dialect = "native";  // native | cwb | pmltq | tiger
    bool json    = false;
    // Debug verbosity: 0 = off, 1 = basic (plan, timing), 2+ = more detail in future
    int  debug_level = 0;
    size_t limit  = 20;
    size_t offset = 0;
    size_t max_total = 0;  // 0 = no cap; with --total, stop counting at this (e.g. 10000)
    int context   = 5;
    /// True: read CQL from stdin with `pando>` prompt (see `--interactive`, or omit query after corpus path).
    bool interactive = false;
    /// `--print-all-steps`: print KWIC/JSON for every query step in a multi-statement program (not only the last).
    bool print_all_program_steps = false;
    bool total    = false;
    std::vector<std::string> attrs;  // empty = all attributes in JSON tokens; else only these
    bool count_only = false;  // print only total (for benchmarking)
    bool timing     = false;  // print open_sec, query_sec, fetch_sec, total, returned to stderr
    size_t sample   = 0;      // return N randomly sampled matches (reservoir sampling)
    uint32_t sample_seed = 0; // RNG seed for --sample (0 = non-deterministic)
    unsigned threads = 1;    // parallel seed processing when > 1 (multi-token queries)
    bool preload    = false; // load all mmap'd pages into RAM at open (slower open, faster first query)
    int  max_gap    = REPEAT_UNBOUNDED; // cap for + and * quantifiers (--max-gap)
    // When true: only /.../ is regex; "..." in [attr="..."] is always literal (legacy behavior).
    bool strict_quoted_strings = false;
    // For aggregation commands (count/group/freq), cap number of output rows; 0 = no cap.
    size_t group_limit = 1000;
    // When true, count/freq keep pipe-joined multivalue keys (lexicon strings) instead of RG-5f explode.
    bool no_mv_explode = false;
    // API mode: like --json but with cleaner, single-object responses for programmatic use
    bool api = false;
    // PML-TQ: emit ClickPMLTQ reference SQL (for DB-backed data); do not open corpus or run search
    bool pmltq_export_sql = false;
    // Text hits: emit full sentence CoNLL-U (requires sentence structure `s` in the index)
    bool conllu = false;
    bool print_version = false;
    /// Stand-off overlay index dirs (`pando-index --overlay-index`); merged with `overlay-<layer>-…` names.
    std::vector<std::string> overlay_dirs;
    // Collocation settings (--window, --left, --right, --min-freq, --measures, --max-items)
    int coll_left = 5;
    int coll_right = 5;
    size_t coll_min_freq = 5;
    size_t coll_max_items = 50;
    size_t coll_stoplist = 0; // 0 = disabled; otherwise top-N most frequent coll_attr forms are excluded
    std::vector<std::string> coll_measures;  // empty = default {"logdice"}
    // RG-REG-5: named-region binding policy when an anchor position is contained in
    // multiple candidate rows (nested/overlapping/zero-width). "fanout" (default)
    // emits one match per candidate row (Cartesian across multiple anchors);
    // "innermost" picks the tightest-enclosing row only. Flat types are unaffected.
    std::string anchor_binding = "fanout";  // "fanout" | "innermost"
    // Alignment filters (`:: a.attr = b.attr`): default ignores empty/"_" values.
    bool allow_empty_alignment = false;
};

struct QueryTiming {
    double open_sec = 0, query_sec = 0, fetch_sec = 0;
    size_t total = 0, returned = 0;
};

// ── JSON output ─────────────────────────────────────────────────────────

static void emit_json(const Corpus& corpus, const std::string& query_text,
                      const MatchSet& ms, const Options& opts,
                      double elapsed_ms) {
    auto emit_match_tokens_json = [&](std::ostream& out, const Match& m) {
        out << ", \"tokens\": [";
        const auto& attr_names = opts.attrs.empty()
            ? corpus.attr_names() : opts.attrs;
        bool first_tok = true;
        for (size_t t = 0; t < m.positions.size(); ++t) {
            if (m.positions[t] == NO_HEAD) continue;
            CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
            for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                if (!first_tok) out << ", ";
                first_tok = false;
                out << "{\"pos\": " << p;
                for (const auto& attr_name : attr_names) {
                    if (!corpus.has_attr(attr_name)) continue;
                    auto val = corpus.attr(attr_name).value_at(p);
                    if (val == "_") continue;
                    out << ", " << jstr(attr_name) << ": " << jstr(val);
                }
                out << "}";
            }
        }
        out << "]";
    };

    // #16: Parallel (Source | Target) result
    if (!ms.parallel_matches.empty()) {
        auto source_match_key = [](const Match& m) {
            std::string key;
            key.reserve(m.positions.size() * 16 + m.span_ends.size() * 16 + 8);
            key += "p:";
            for (CorpusPos p : m.positions) {
                key += std::to_string(p);
                key += ',';
            }
            key += "|e:";
            for (CorpusPos e : m.span_ends) {
                key += std::to_string(e);
                key += ',';
            }
            return key;
        };
        std::vector<std::vector<size_t>> groups;
        std::unordered_map<std::string, size_t> group_by_key;
        for (size_t i = 0; i < ms.parallel_matches.size(); ++i) {
            const auto& src = ms.parallel_matches[i].first;
            std::string key = source_match_key(src);
            auto it = group_by_key.find(key);
            if (it == group_by_key.end()) {
                size_t gid = groups.size();
                group_by_key.emplace(std::move(key), gid);
                groups.push_back({i});
            } else {
                groups[it->second].push_back(i);
            }
        }

        size_t total_groups = groups.size();
        size_t gstart = std::min(opts.offset, total_groups);
        size_t gend   = std::min(gstart + opts.limit, total_groups);
        size_t returned_pairs = 0;
        for (size_t g = gstart; g < gend; ++g) returned_pairs += groups[g].size();
        std::cout << "{\n  \"ok\": true,\n  \"backend\": \"pando\",\n  \"operation\": \"query\",\n";
        std::cout << "  \"result\": {\n    \"parallel\": true,\n";
        std::cout << "    \"query\": {\"language\": \"pando-cql\", \"text\": "
                  << jstr(query_text) << "},\n";
        std::cout << "    \"page\": {\"start\": " << gstart << ", \"size\": " << opts.limit
                  << ", \"returned\": " << (gend - gstart) << ", \"total\": " << total_groups
                  << ", \"returned_pairs\": " << returned_pairs
                  << ", \"total_pairs\": " << ms.total_count
                  << ", \"total_exact\": " << (ms.total_exact ? "true" : "false") << "},\n";
        std::cout << "    \"pairs\": [\n";
        bool first_pair = true;
        for (size_t g = gstart; g < gend; ++g) {
            for (size_t idx : groups[g]) {
            const auto& [s, t] = ms.parallel_matches[idx];
            auto doc_s = lookup_doc_id(corpus, s.first_pos());
            auto doc_t = lookup_doc_id(corpus, t.first_pos());
            CorpusPos ms_s = s.first_pos();
            CorpusPos me_s = s.last_pos();
            CorpusPos ms_t = t.first_pos();
            CorpusPos me_t = t.last_pos();
            int ctx_w_s = match_is_full_sentence_span(corpus, s) ? 0 : opts.context;
            int ctx_w_t = match_is_full_sentence_span(corpus, t) ? 0 : opts.context;
            auto ctx_s = build_context(corpus, s, ctx_w_s);
            auto ctx_t = build_context(corpus, t, ctx_w_t);
            if (!first_pair) std::cout << ",\n";
            first_pair = false;
            std::cout << "      {\"aligned\": true, \"alignment\": {\"kind\": \"with\", "
                      << "\"pair_index\": " << idx << "}, "
                      << "\"source\": {\"doc_id\": " << (doc_s.empty() ? "null" : jstr(doc_s))
                      << ", \"text_id\": " << (doc_s.empty() ? "null" : jstr(doc_s))
                      << ", \"match_start\": " << ms_s << ", \"match_end\": " << me_s
                      << ", \"context\": {\"left\": " << jstr(ctx_s.left)
                      << ", \"match\": " << jstr(ctx_s.match)
                      << ", \"right\": " << jstr(ctx_s.right) << "}";
            emit_match_tokens_json(std::cout, s);
            std::cout << ", \"aligned_to\": {\"side\": \"target\", \"text_id\": "
                      << (doc_t.empty() ? "null" : jstr(doc_t)) << "}"
                      << "}, \"target\": {\"doc_id\": " << (doc_t.empty() ? "null" : jstr(doc_t))
                      << ", \"text_id\": " << (doc_t.empty() ? "null" : jstr(doc_t))
                      << ", \"match_start\": " << ms_t << ", \"match_end\": " << me_t
                      << ", \"context\": {\"left\": " << jstr(ctx_t.left)
                      << ", \"match\": " << jstr(ctx_t.match)
                      << ", \"right\": " << jstr(ctx_t.right) << "}";
            emit_match_tokens_json(std::cout, t);
            std::cout << ", \"aligned_to\": {\"side\": \"source\", \"text_id\": "
                      << (doc_s.empty() ? "null" : jstr(doc_s)) << "}" << "}}";
            }
        }
        std::cout << "\n    ]\n  }\n}\n";
        return;
    }

    size_t stored = ms.matches.size();
    size_t start = std::min(opts.offset, stored);
    size_t end   = std::min(start + opts.limit, stored);
    size_t returned = end - start;

    std::cout << "{\n";
    std::cout << "  \"ok\": true,\n";
    std::cout << "  \"backend\": \"pando\",\n";
    std::cout << "  \"operation\": \"query\",\n";
    std::cout << "  \"result\": {\n";
    std::cout << "    \"query\": {\"language\": \"pando-cql\", \"text\": "
              << jstr(query_text) << "},\n";
    std::cout << "    \"page\": {\"start\": " << start
              << ", \"size\": " << opts.limit
              << ", \"returned\": " << returned
              << ", \"total\": " << ms.total_count
              << ", \"total_exact\": " << (ms.total_exact ? "true" : "false")
              << "},\n";

    std::cout << "    \"hits\": [\n";
    for (size_t i = start; i < end; ++i) {
        const auto& m = ms.matches[i];
        CorpusPos match_start = m.first_pos();
        CorpusPos match_end   = m.last_pos();
        auto doc_id = lookup_doc_id(corpus, match_start);
        auto ctx = build_context(corpus, m, opts.context);

        if (i > start) std::cout << ",\n";
        std::cout << "      {";
        std::cout << "\"doc_id\": " << (doc_id.empty() ? "null" : jstr(doc_id));
        std::cout << ", \"match_start\": " << match_start;
        std::cout << ", \"match_end\": " << match_end;
        std::cout << ", \"context\": {"
                  << "\"left\": " << jstr(ctx.left)
                  << ", \"match\": " << jstr(ctx.match)
                  << ", \"right\": " << jstr(ctx.right) << "}";

        emit_match_tokens_json(std::cout, m);

        std::cout << "}";
    }
    std::cout << "\n    ]";

    if (opts.debug_level > 0) {
        std::cout << ",\n    \"debug\": {\n";
        std::cout << "      \"corpus_size\": " << corpus.size() << ",\n";
        std::cout << "      \"has_deps\": " << (corpus.has_deps() ? "true" : "false") << ",\n";
        std::cout << "      \"elapsed_ms\": " << elapsed_ms << ",\n";
        std::cout << "      \"seed_token\": " << ms.seed_token << ",\n";
        std::cout << "      \"cardinalities\": [";
        for (size_t i = 0; i < ms.cardinalities.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << ms.cardinalities[i];
        }
        std::cout << "],\n";

        std::cout << "      \"attributes\": [";
        const auto& names = corpus.attr_names();
        for (size_t i = 0; i < names.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(names[i]);
        }
        std::cout << "],\n";

        std::cout << "      \"structures\": [";
        bool first_s = true;
        for (const auto& sn : {"s", "text"}) {
            if (corpus.has_structure(sn)) {
                if (!first_s) std::cout << ", ";
                const auto& sa = corpus.structure(sn);
                std::cout << "{\"name\": " << jstr(sn)
                          << ", \"regions\": " << sa.region_count()
                          << ", \"has_values\": "
                          << (sa.has_values() ? "true" : "false") << "}";
                first_s = false;
            }
        }
        std::cout << "]\n";
        std::cout << "    }";
    }

    std::cout << "\n  }\n}\n";
}

// ── KWIC text output (default) ──────────────────────────────────────────

// Build a character-width-limited left context string (right-aligned).
// Walks backwards from `first`, collecting words until the column is full.
static std::string build_left_chars(const Corpus& corpus, CorpusPos first, int col_width) {
    if (first == 0 || col_width <= 0) return std::string(col_width, ' ');
    const auto& form = corpus.attr("form");
    std::vector<std::string_view> words;
    size_t total_len = 0;
    for (CorpusPos p = first - 1; ; --p) {
        auto w = form.value_at(p);
        size_t need = w.size() + (words.empty() ? 0 : 1);  // +1 for space
        if (total_len + need > static_cast<size_t>(col_width)) break;
        words.push_back(w);
        total_len += need;
        if (p == 0) break;
    }
    // Reverse to get left-to-right order
    std::string text;
    for (auto it = words.rbegin(); it != words.rend(); ++it) {
        if (!text.empty()) text += ' ';
        text += *it;
    }
    // Right-align: pad on the left
    if (static_cast<int>(text.size()) < col_width)
        text.insert(0, col_width - text.size(), ' ');
    return text;
}

// Build a character-width-limited right context string (left-aligned).
static std::string build_right_chars(const Corpus& corpus, CorpusPos last, int col_width) {
    if (last + 1 >= corpus.size() || col_width <= 0) return "";
    const auto& form = corpus.attr("form");
    std::string text;
    for (CorpusPos p = last + 1; p < corpus.size(); ++p) {
        auto w = form.value_at(p);
        size_t need = w.size() + (text.empty() ? 0 : 1);
        if (text.size() + need > static_cast<size_t>(col_width)) break;
        if (!text.empty()) text += ' ';
        text += w;
    }
    return text;
}

// Build the match-span string for text KWIC output.
// Walks from first_pos to last_pos, wrapping contiguous matched positions
// in <! !> delimiters.  Unmatched positions between match groups are shown
// as plain text (gap between discontinuous match tokens).
static std::string build_match_span(const Corpus& corpus, const Match& m,
                                    const std::vector<std::string>& extra_attrs) {
    const auto& form = corpus.attr("form");
    auto matched = m.matched_positions();  // sorted, unique
    if (matched.empty()) return "";

    CorpusPos min_p = matched.front();
    CorpusPos max_p = matched.back();

    // Build a fast lookup (for small spans, linear scan is fine)
    auto is_matched = [&](CorpusPos p) {
        return std::binary_search(matched.begin(), matched.end(), p);
    };

    bool color = use_color();

    std::string text;
    bool in_highlight = false;
    for (CorpusPos p = min_p; p <= max_p; ++p) {
        bool hit = is_matched(p);
        if (hit && !in_highlight) {
            text += color ? "\033[1;31m" : " <!";
            in_highlight = true;
        } else if (!hit && in_highlight) {
            text += color ? "\033[0m" : " !>";
            in_highlight = false;
        }
        text += ' ';
        text += form.value_at(p);
        if (hit) {
            for (const auto& attr_name : extra_attrs) {
                if (attr_name == "form") continue;
                if (!corpus.has_attr(attr_name)) continue;
                text += '/';
                text += corpus.attr(attr_name).value_at(p);
            }
        }
    }
    if (in_highlight)
        text += color ? "\033[0m" : " !>";
    return text;
}

static std::string conllu_esc_field(std::string_view s) {
    std::string r(s);
    for (char& c : r) {
        if (c == '\t' || c == '\n' || c == '\r')
            c = ' ';
    }
    return r;
}

static std::string conllu_cell(const Corpus& corpus, const char* attr, CorpusPos p) {
    if (!corpus.has_attr(attr))
        return "_";
    std::string_view v = corpus.attr(attr).value_at(p);
    if (v.empty() || v == "_")
        return "_";
    return conllu_esc_field(v);
}

// Constituency bracket string from nested `node` regions + parent .par (RG-REG-1).
namespace {

static std::string constituency_label(const StructuralAttr& sa, size_t idx) {
    std::string_view v = sa.region_value("type", idx);
    std::string label = v.empty() ? std::string("X") : std::string(v);
    for (char& c : label) {
        if (c == '\t' || c == '\n' || c == '\r')
            c = ' ';
    }
    if (label.empty()) label = "X";
    return label;
}

static std::string constituency_terminal_range(const Corpus& corpus, CorpusPos a, CorpusPos b) {
    if (!corpus.has_attr("form") || a > b) return {};
    std::string s;
    for (CorpusPos p = a; p <= b; ++p) {
        if (!s.empty()) s += ' ';
        s += conllu_esc_field(corpus.attr("form").value_at(p));
    }
    return s;
}

static std::string constituency_emit_subtree(
        size_t idx,
        const Corpus& corpus,
        const StructuralAttr& sa,
        const std::unordered_map<size_t, std::vector<size_t>>& children) {
    Region r = sa.get(idx);
    std::string label = constituency_label(sa, idx);
    auto it = children.find(idx);
    if (it == children.end() || it->second.empty()) {
        std::string terms = constituency_terminal_range(corpus, r.start, r.end);
        return "(" + label + " " + terms + ")";
    }
    std::string inner;
    CorpusPos cur = r.start;
    for (size_t cidx : it->second) {
        Region cr = sa.get(cidx);
        if (cr.start > cur) {
            std::string gap = constituency_terminal_range(corpus, cur, cr.start - 1);
            if (!inner.empty() && !gap.empty()) inner += ' ';
            inner += gap;
        }
        if (!inner.empty()) inner += ' ';
        inner += constituency_emit_subtree(cidx, corpus, sa, children);
        cur = cr.end + 1;
    }
    if (cur <= r.end) {
        std::string tail = constituency_terminal_range(corpus, cur, r.end);
        if (!inner.empty() && !tail.empty()) inner += ' ';
        inner += tail;
    }
    return "(" + label + " " + inner + ")";
}

// Returns empty if no constituency tree for this sentence.
static std::string build_constituency_line(const Corpus& corpus, Region sent) {
    if (!corpus.has_structure("node") || !corpus.has_attr("form"))
        return {};
    const StructuralAttr& node_sa = corpus.structure("node");
    if (!node_sa.has_region_attr("type") || !node_sa.has_parent_region_id())
        return {};

    std::unordered_set<size_t> in_sent;
    std::vector<size_t> idxs;
    for (size_t i = 0; i < node_sa.region_count(); ++i) {
        Region r = node_sa.get(i);
        if (r.start > r.end) continue;
        if (r.start < sent.start || r.end > sent.end) continue;
        idxs.push_back(i);
        in_sent.insert(i);
    }
    if (idxs.empty()) return {};

    std::unordered_map<size_t, std::vector<size_t>> children;
    std::vector<size_t> roots;
    roots.reserve(idxs.size());
    for (size_t idx : idxs) {
        int32_t p = node_sa.parent_region_id(idx);
        if (p < 0 || in_sent.find(static_cast<size_t>(p)) == in_sent.end())
            roots.push_back(idx);
        else
            children[static_cast<size_t>(p)].push_back(idx);
    }
    for (auto& kv : children) {
        std::sort(kv.second.begin(), kv.second.end(), [&](size_t a, size_t b) {
            return node_sa.get(a).start < node_sa.get(b).start;
        });
    }
    std::sort(roots.begin(), roots.end(), [&](size_t a, size_t b) {
        return node_sa.get(a).start < node_sa.get(b).start;
    });

    std::string out;
    for (size_t ri = 0; ri < roots.size(); ++ri) {
        if (ri > 0) out += ' ';
        out += constituency_emit_subtree(roots[ri], corpus, node_sa, children);
    }
    return out;
}

}  // namespace

static void emit_conllu(const Corpus& corpus, const MatchSet& ms, const Options& opts) {
    if (!corpus.has_structure("s")) {
        std::cerr << "Error: --conllu requires sentence structure 's' (build from CoNLL-U / UD index).\n";
        return;
    }
    const StructuralAttr& sent = corpus.structure("s");
    const bool has_deps = corpus.has_deps();

    size_t stored = ms.matches.size();
    size_t start = std::min(opts.offset, stored);
    size_t end = std::min(start + opts.limit, stored);
    std::unordered_set<size_t> seen_sent;

    // Per match in [start,end): 1-based token indices within that match's sentence region.
    // Multiple matches in the same sentence become semicolon-separated groups on # pando_match_tokens.
    std::unordered_map<size_t, std::vector<std::vector<int>>> sent_match_token_groups;
    for (size_t j = start; j < end; ++j) {
        const auto& mm = ms.matches[j];
        CorpusPos fpp = mm.first_pos();
        int64_t rj = sent.find_region(fpp);
        if (rj < 0)
            continue;
        size_t riuj = static_cast<size_t>(rj);
        Region rgnj = sent.get(riuj);
        std::vector<int> group;
        for (CorpusPos p : mm.matched_positions()) {
            if (p < rgnj.start || p > rgnj.end)
                continue;
            group.push_back(static_cast<int>(p - rgnj.start + 1));
        }
        if (!group.empty())
            sent_match_token_groups[riuj].push_back(std::move(group));
    }

    if (opts.debug_level > 0) {
        std::cerr << "Plan: seed=token[" << ms.seed_token << "]";
        for (size_t i = 0; i < ms.cardinalities.size(); ++i)
            std::cerr << (i == 0 ? " cardinalities=[" : ", ") << ms.cardinalities[i];
        if (!ms.cardinalities.empty())
            std::cerr << "]";
        std::cerr << "\n";
    }

    for (size_t i = start; i < end; ++i) {
        const auto& m = ms.matches[i];
        CorpusPos fp = m.first_pos();
        int64_t ri = sent.find_region(fp);
        if (ri < 0) {
            std::cerr << "Warning: match at position " << fp
                      << " not inside any sentence region; skipping\n";
            continue;
        }
        size_t riu = static_cast<size_t>(ri);
        if (!seen_sent.insert(riu).second)
            continue;

        Region rgn = sent.get(riu);

        if (sent.has_region_attr("sent_id")) {
            std::string_view sid = sent.region_value("sent_id", riu);
            if (!sid.empty())
                std::cout << "# sent_id = " << sid << "\n";
        }
        if (corpus.has_attr("form")) {
            std::string text_line;
            for (CorpusPos p = rgn.start; p <= rgn.end; ++p) {
                if (!text_line.empty())
                    text_line += ' ';
                text_line += conllu_esc_field(corpus.attr("form").value_at(p));
            }
            if (!text_line.empty())
                std::cout << "# text = " << text_line << "\n";
        }

        std::string cst = build_constituency_line(corpus, rgn);
        if (!cst.empty())
            std::cout << "# constituency = " << cst << "\n";

        std::cout << "# pando_match_tokens = ";
        bool first_group = true;
        auto grp_it = sent_match_token_groups.find(riu);
        if (grp_it != sent_match_token_groups.end()) {
            for (const std::vector<int>& group : grp_it->second) {
                if (!first_group)
                    std::cout << ';';
                first_group = false;
                bool first_t = true;
                for (int tid : group) {
                    if (!first_t)
                        std::cout << ',';
                    first_t = false;
                    std::cout << tid;
                }
            }
        }
        std::cout << "\n";

        for (CorpusPos p = rgn.start; p <= rgn.end; ++p) {
            int tid = static_cast<int>(p - rgn.start + 1);
            std::string form = conllu_cell(corpus, "form", p);
            std::string lemma = conllu_cell(corpus, "lemma", p);
            std::string upos = conllu_cell(corpus, "upos", p);
            std::string xpos = conllu_cell(corpus, "xpos", p);
            std::string feats = conllu_cell(corpus, "feats", p);

            std::string head_col;
            std::string deprel = "_";
            if (has_deps) {
                CorpusPos h = corpus.deps().head(p);
                if (h == NO_HEAD)
                    head_col = "0";
                else
                    head_col = std::to_string(static_cast<int>(h - rgn.start + 1));
                deprel = conllu_cell(corpus, "deprel", p);
            } else {
                head_col = "_";
            }

            std::string misc = "_";
            if (corpus.has_attr("tuid")) {
                std::string_view tv = corpus.attr("tuid").value_at(p);
                if (!tv.empty())
                    misc = conllu_esc_field(tv);
            }

            std::cout << tid << '\t' << form << '\t' << lemma << '\t' << upos << '\t' << xpos << '\t'
                      << feats << '\t' << head_col << '\t' << deprel << "\t_\t" << misc << "\n";
        }
        std::cout << "\n";
    }

    if (end < stored) {
        if (ms.total_exact)
            std::cout << "# (" << ms.total_count << " matches, " << (ms.total_count - end) << " more)\n";
        else
            std::cout << "# (" << ms.total_count << "+ matches, use --total for exact count)\n";
    }
}

static void emit_kwic(const Corpus& corpus, const std::string& query_text,
                      const MatchSet& ms, const Options& opts,
                      double elapsed_ms) {
    // Character column width for text KWIC output (left and right context)
    static constexpr int KWIC_COL_WIDTH = 40;

    if (!ms.parallel_matches.empty()) {
        auto source_match_key = [](const Match& m) {
            std::string key;
            key.reserve(m.positions.size() * 16 + m.span_ends.size() * 16 + 8);
            key += "p:";
            for (CorpusPos p : m.positions) {
                key += std::to_string(p);
                key += ',';
            }
            key += "|e:";
            for (CorpusPos e : m.span_ends) {
                key += std::to_string(e);
                key += ',';
            }
            return key;
        };
        std::vector<std::vector<size_t>> groups;
        std::unordered_map<std::string, size_t> group_by_key;
        for (size_t i = 0; i < ms.parallel_matches.size(); ++i) {
            const auto& src = ms.parallel_matches[i].first;
            std::string key = source_match_key(src);
            auto it = group_by_key.find(key);
            if (it == group_by_key.end()) {
                size_t gid = groups.size();
                group_by_key.emplace(std::move(key), gid);
                groups.push_back({i});
            } else {
                groups[it->second].push_back(i);
            }
        }
        size_t gstart = std::min(opts.offset, groups.size());
        size_t gend   = std::min(gstart + opts.limit, groups.size());
        size_t shown_pairs = 0;
        for (size_t g = gstart; g < gend; ++g) {
            for (size_t idx : groups[g]) {
            const auto& [s, t] = ms.parallel_matches[idx];
            auto src_doc = lookup_doc_id(corpus, s.first_pos());
            auto tgt_doc = lookup_doc_id(corpus, t.first_pos());
            std::string left_s  = build_left_chars(corpus, s.first_pos(), KWIC_COL_WIDTH);
            std::string span_s  = build_match_span(corpus, s, opts.attrs);
            std::string right_s = build_right_chars(corpus, s.last_pos(), KWIC_COL_WIDTH);
            std::string left_t  = build_left_chars(corpus, t.first_pos(), KWIC_COL_WIDTH);
            std::string span_t  = build_match_span(corpus, t, opts.attrs);
            std::string right_t = build_right_chars(corpus, t.last_pos(), KWIC_COL_WIDTH);
            ++shown_pairs;

            std::cout << "[pair " << (idx + 1) << "] aligned\n";
            std::cout << "  source(text_id="
                      << (src_doc.empty() ? "null" : std::string(src_doc))
                      << "): " << left_s << span_s << " " << right_s << "\n";
            std::cout << "  target(text_id="
                      << (tgt_doc.empty() ? "null" : std::string(tgt_doc))
                      << "): " << left_t << span_t << " " << right_t << "\n\n";
            }
        }
        if (gend < groups.size()) {
            size_t more_pairs = ms.parallel_matches.size() > shown_pairs
                                ? (ms.parallel_matches.size() - shown_pairs) : 0;
            std::cout << "(" << groups.size() << " source hits, "
                      << ms.total_count << " aligned pairs, "
                      << more_pairs << " pairs in later source hits)\n";
        }
        return;
    }

    size_t stored = ms.matches.size();
    size_t start = std::min(opts.offset, stored);
    size_t end   = std::min(start + opts.limit, stored);

    if (opts.debug_level > 0) {
        std::cerr << "Plan: seed=token[" << ms.seed_token << "]";
        for (size_t i = 0; i < ms.cardinalities.size(); ++i)
            std::cerr << (i == 0 ? " cardinalities=[" : ", ")
                      << ms.cardinalities[i];
        if (!ms.cardinalities.empty()) std::cerr << "]";
        std::cerr << "\n";
        std::cerr << "Time: " << elapsed_ms << " ms, "
                  << ms.total_count << (ms.total_exact ? "" : "+")
                  << " total matches\n";
    }

    for (size_t i = start; i < end; ++i) {
        const auto& m = ms.matches[i];

        std::string left  = build_left_chars(corpus, m.first_pos(), KWIC_COL_WIDTH);
        std::string span  = build_match_span(corpus, m, opts.attrs);
        std::string right = build_right_chars(corpus, m.last_pos(), KWIC_COL_WIDTH);

        std::cout << left << span << " " << right << "\n";
    }

    if (end < stored) {
        if (ms.total_exact)
            std::cout << "(" << ms.total_count << " matches, "
                      << (ms.total_count - end) << " more)\n";
        else
            std::cout << "(" << ms.total_count << "+ matches, use --total for exact count)\n";
    }
}

static void emit_hits_text(const Corpus& corpus, const std::string& query_text,
                           const MatchSet& ms, const Options& opts, double elapsed_ms) {
    if (opts.conllu) {
        if (!ms.parallel_matches.empty()) {
            std::cerr << "CoNLL-U output is not supported for parallel (Source | Target) queries; "
                         "using KWIC.\n";
            emit_kwic(corpus, query_text, ms, opts, elapsed_ms);
            return;
        }
        emit_conllu(corpus, ms, opts);
        return;
    }
    emit_kwic(corpus, query_text, ms, opts, elapsed_ms);
}

// ── Command handling ────────────────────────────────────────────────────

static void emit_info_json(const Corpus& corpus) {
    std::cout << to_info_json(corpus, "info");
}

// Check whether a field's value may be pipe-separated (multivalue positional
// attr OR overlapping/nested region attribute).
static bool is_multi_value_field(const Corpus& corpus, const std::string& field) {
    if (field.size() >= 5 && field.compare(0, 5, "tcnt(") == 0) return false;
    std::string attr_spec = field;
    if (field.rfind("match.", 0) == 0 && field.size() > 6) {
        attr_spec = field.substr(6);
    } else {
        auto dot = field.find('.');
        if (dot != std::string::npos && dot > 0)
            attr_spec = field.substr(dot + 1);
    }
    // Multivalue positional attr (e.g. wsd with "artist|writer")
    if (corpus.is_multivalue(attr_spec))
        return true;
    // Overlapping/nested region attr
    RegionAttrParts parts;
    if (split_region_attr_name(attr_spec, parts) &&
        corpus.has_structure(parts.struct_name)) {
        return corpus.is_overlapping(parts.struct_name)
            || corpus.is_nested(parts.struct_name);
    }
    return false;
}

// Emit a field value as JSON: array for multi-region fields, string otherwise.
static void emit_field_json(std::ostream& out, const std::string& val, bool is_multi) {
    if (is_multi && val.find('|') != std::string::npos) {
        out << '[';
        size_t start = 0;
        bool first = true;
        while (start < val.size()) {
            size_t p = val.find('|', start);
            if (p == std::string::npos) p = val.size();
            if (!first) out << ", ";
            out << jstr(val.substr(start, p - start));
            first = false;
            start = p + 1;
        }
        out << ']';
    } else {
        out << jstr(val);
    }
}

// ── Aggregation helpers ─────────────────────────────────────────────────

static std::string make_key(const Corpus& corpus, const Match& m,
                            const NameIndexMap& name_map,
                            const std::vector<std::string>& fields) {
    std::string key;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) key += '\t';
        key += read_tabulate_field(corpus, m, name_map, fields[i]);
    }
    return key;
}

static bool parse_i64_strict(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    size_t i = 0;
    bool neg = false;
    if (s[i] == '+' || s[i] == '-') {
        neg = (s[i] == '-');
        ++i;
    }
    if (i >= s.size()) return false;
    int64_t v = 0;
    for (; i < s.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (!std::isdigit(c)) return false;
        int d = c - '0';
        if (v > (INT64_MAX - d) / 10) return false;
        v = v * 10 + d;
    }
    out = neg ? -v : v;
    return true;
}

static bool parse_roman_numeral(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    auto val = [](char c) -> int {
        switch (c) {
            case 'I': return 1; case 'V': return 5; case 'X': return 10; case 'L': return 50;
            case 'C': return 100; case 'D': return 500; case 'M': return 1000;
            default: return 0;
        }
    };
    int64_t total = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = static_cast<char>(std::toupper(static_cast<unsigned char>(s[i])));
        int cur = val(c);
        if (cur == 0) return false;
        int next = 0;
        if (i + 1 < s.size()) {
            char n = static_cast<char>(std::toupper(static_cast<unsigned char>(s[i + 1])));
            next = val(n);
            if (next == 0) return false;
        }
        if (next > cur) total -= cur;
        else total += cur;
    }
    out = total;
    return total > 0;
}

static bool compare_sort_values(std::string_view a, std::string_view b) {
    int64_t ai = 0, bi = 0;
    bool an = parse_i64_strict(a, ai) || parse_roman_numeral(a, ai);
    bool bn = parse_i64_strict(b, bi) || parse_roman_numeral(b, bi);
    if (an && bn && ai != bi) return ai < bi;
    return a < b;
}

static bool compare_group_keys(std::string_view a, std::string_view b) {
    size_t sa = 0, sb = 0;
    while (true) {
        size_t ea = a.find('\t', sa);
        size_t eb = b.find('\t', sb);
        if (ea == std::string_view::npos) ea = a.size();
        if (eb == std::string_view::npos) eb = b.size();
        std::string_view pa = a.substr(sa, ea - sa);
        std::string_view pb = b.substr(sb, eb - sb);
        if (pa != pb) return compare_sort_values(pa, pb);
        if (ea == a.size() && eb == b.size()) return false;
        sa = (ea < a.size()) ? ea + 1 : a.size();
        sb = (eb < b.size()) ? eb + 1 : b.size();
    }
}

static bool aggregate_command_targets_stmt(const Statement& stmt, const GroupCommand& ncmd) {
    if (!ncmd.freq_query_names.empty()) {
        if (ncmd.freq_query_names.size() > 1)
            return false;
        if (!stmt.name.empty())
            return ncmd.freq_query_names[0] == stmt.name;
        return ncmd.freq_query_names[0] == "Last";
    }
    if (ncmd.query_name.empty())
        return true;
    if (!stmt.name.empty())
        return ncmd.query_name == stmt.name;
    return ncmd.query_name == "Last";
}

// ── Session state for interactive REPL ──────────────────────────────────

struct Session {
    std::map<std::string, MatchSet> named_results;
    std::map<std::string, NameIndexMap> named_name_maps;
    /// Token labels for the target side of the last / named parallel query (`with`).
    std::map<std::string, NameIndexMap> named_target_name_maps;
    MatchSet last_ms;
    NameIndexMap last_name_map;
    NameIndexMap last_target_name_map;
    bool has_last = false;
};

static void emit_count(const Corpus& corpus, const MatchSet& ms,
                       const GroupCommand& cmd, const Options& opts,
                       const NameIndexMap& name_map) {
    if (cmd.fields.empty()) {
        std::cerr << "Error: count/group requires 'by' clause\n";
        return;
    }

    std::map<std::string, size_t> counts;
    if (ms.aggregate_buckets) {
        for (const auto& [k, c] : ms.aggregate_buckets->counts)
            counts[decode_aggregate_bucket_key(*ms.aggregate_buckets, k)] += c;
    } else {
        for (const auto& m : ms.matches)
            ++counts[make_key(corpus, m, name_map, cmd.fields)];
    }

    // RG-5f: For single-column grouping on a multivalue attribute,
    // explode pipe-separated keys so "artist|writer" contributes to
    // both "artist" and "writer" buckets.
    if (!opts.no_mv_explode && cmd.fields.size() == 1 && corpus.is_multivalue(cmd.fields[0])) {
        std::map<std::string, size_t> exploded;
        for (const auto& [key, count] : counts) {
            if (key.find('|') != std::string::npos) {
                size_t start = 0;
                while (start < key.size()) {
                    size_t p = key.find('|', start);
                    if (p == std::string::npos) p = key.size();
                    std::string comp = key.substr(start, p - start);
                    if (!comp.empty()) exploded[comp] += count;
                    start = p + 1;
                }
            } else {
                exploded[key] += count;
            }
        }
        counts = std::move(exploded);
    }

    // Sort by count descending
    std::vector<std::pair<std::string, size_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) {
                  if (a.second != b.second) return a.second > b.second;
                  return compare_group_keys(a.first, b.first);
              });

    size_t total = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();

    // Pagination for groups: default to opts.group_limit (1000) when set; else all.
    size_t g_start = 0;
    size_t g_end   = sorted.size();
    if (opts.group_limit > 0 && opts.group_limit < g_end)
        g_end = opts.group_limit;

    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"count\", \"last_command\": \"count\", \"result\": {\n";
        std::cout << "  \"total_matches\": " << total << ",\n";
        std::cout << "  \"groups\": " << sorted.size() << ",\n";
        std::cout << "  \"groups_returned\": " << (g_end - g_start) << ",\n";
        std::cout << "  \"fields\": [";
        for (size_t i = 0; i < cmd.fields.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(cmd.fields[i]);
        }
        std::cout << "],\n";
        if (cmd.fields.size() >= 2) {
            emit_count_result_hierarchy_json(std::cout, cmd.fields, counts, total, opts.group_limit);
            std::cout << "\n}}\n";
        } else {
            std::cout << "  \"rows\": [\n";
            for (size_t i = g_start; i < g_end; ++i) {
                if (i > g_start) std::cout << ",\n";
                double pct = 100.0 * static_cast<double>(sorted[i].second) / static_cast<double>(total);
                std::cout << "    {\"key\": " << jstr(sorted[i].first)
                          << ", \"count\": " << sorted[i].second
                          << ", \"pct\": " << pct << "}";
            }
            std::cout << "\n  ]\n}}\n";
        }
    } else {
        // Header
        for (const auto& f : cmd.fields)
            std::cout << f << "\t";
        std::cout << "count\t%\n";

        size_t shown = 0;
        for (size_t i = g_start; i < g_end; ++i) {
            const auto& key = sorted[i].first;
            size_t count = sorted[i].second;
            double pct = 100.0 * static_cast<double>(count) / static_cast<double>(total);
            std::cout << key << "\t" << count << "\t";
            std::cout << std::fixed;
            std::cout.precision(1);
            std::cout << pct << "%\n";
            ++shown;
        }
        std::cout << "Total: " << total << " matches, " << sorted.size()
                  << " groups (showing " << shown << ")\n";
    }
}

static void emit_sort(const Corpus& corpus, MatchSet& ms,
                      const GroupCommand& cmd, const Options& opts,
                      const NameIndexMap& name_map) {
    if (cmd.fields.empty()) {
        std::cerr << "Error: sort requires 'by' clause\n";
        return;
    }

    std::sort(ms.matches.begin(), ms.matches.end(),
              [&](const Match& a, const Match& b) {
                  return compare_group_keys(make_key(corpus, a, name_map, cmd.fields),
                                            make_key(corpus, b, name_map, cmd.fields));
              });

    emit_hits_text(corpus, "(sorted)", ms, opts, 0);
}

static void emit_size(const MatchSet& ms, const Options& opts) {
    size_t n = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"size\", \"last_command\": \"size\", \"result\": " << n << "}\n";
    } else {
        std::cout << n << " matches\n";
    }
}

static void emit_stats(const Corpus& corpus, const MatchSet& ms,
                       const GroupCommand& cmd, const Options& opts,
                       const NameIndexMap& name_map) {
    if (cmd.stats_metrics.empty()) {
        std::cerr << "Error: stats requires at least one metric\n";
        return;
    }
    std::vector<StatsMetricSpec> metrics;
    metrics.reserve(cmd.stats_metrics.size());
    for (const auto& sm : cmd.stats_metrics) {
        StatsMetricSpec spec;
        spec.kind = (sm.kind == GroupCommand::StatMetric::Kind::AVG)
                        ? StatsMetricSpec::Kind::Avg
                        : StatsMetricSpec::Kind::Median;
        spec.expr = sm.expr;
        metrics.push_back(std::move(spec));
    }
    std::vector<StatsRowResult> rows;
    if (!compute_stats_rows(corpus, ms, name_map, cmd.fields, metrics, rows)) {
        std::cerr << "Error: failed to compute stats\n";
        return;
    }
    auto metric_name = [](const GroupCommand::StatMetric& sm) {
        return std::string(sm.kind == GroupCommand::StatMetric::Kind::AVG ? "avg(" : "median(")
             + sm.expr + ")";
    };
    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"stats\", \"last_command\": \"stats\", \"result\": {\n";
        std::cout << "  \"by\": [";
        for (size_t i = 0; i < cmd.fields.size(); ++i) {
            if (i) std::cout << ", ";
            std::cout << jstr(cmd.fields[i]);
        }
        std::cout << "],\n  \"metrics\": [";
        for (size_t i = 0; i < cmd.stats_metrics.size(); ++i) {
            if (i) std::cout << ", ";
            std::cout << jstr(metric_name(cmd.stats_metrics[i]));
        }
        std::cout << "],\n  \"rows\": [\n";
        for (size_t ri = 0; ri < rows.size(); ++ri) {
            if (ri) std::cout << ",\n";
            const auto& row = rows[ri];
            std::cout << "    {\"key\": " << jstr(row.key) << ", \"n_total\": " << row.n_total << ", \"values\": [";
            for (size_t mi = 0; mi < row.metrics.size(); ++mi) {
                if (mi) std::cout << ", ";
                const auto& mr = row.metrics[mi];
                std::cout << "{\"n_valid\": " << mr.n_valid << ", \"value\": ";
                if (mr.has_value) std::cout << mr.value;
                else std::cout << "null";
                std::cout << "}";
            }
            std::cout << "]}";
        }
        std::cout << "\n  ]\n}}\n";
    } else {
        bool first_col = true;
        if (!cmd.fields.empty()) {
            for (const auto& f : cmd.fields) {
                if (!first_col) std::cout << '\t';
                first_col = false;
                std::cout << f;
            }
        } else {
            std::cout << "group";
            first_col = false;
        }
        std::cout << '\t' << "n_total";
        for (const auto& sm : cmd.stats_metrics) {
            std::cout << '\t' << "n_valid(" << metric_name(sm) << ")";
            std::cout << '\t' << metric_name(sm);
        }
        std::cout << '\n';
        for (const auto& row : rows) {
            std::cout << (row.key.empty() ? "*" : row.key) << '\t' << row.n_total;
            for (const auto& mr : row.metrics) {
                std::cout << '\t' << mr.n_valid << '\t';
                if (mr.has_value) std::cout << mr.value;
                else std::cout << "NA";
            }
            std::cout << '\n';
        }
    }
}

static void emit_tabulate(const Corpus& corpus, const MatchSet& ms,
                          const GroupCommand& cmd, const Options& opts,
                          const NameIndexMap& name_map) {
    if (cmd.fields.empty()) {
        std::cerr << "Error: tabulate requires at least one field\n";
        return;
    }

    try {

    const size_t n = ms.matches.size();
    const size_t start = std::min(cmd.tabulate_offset, n);
    const size_t end = std::min(start + cmd.tabulate_limit, n);
    const size_t total_hits = ms.total_count > 0 ? ms.total_count : n;

    if (opts.json) {
        // Precompute which fields are from overlapping/nested structures
        std::vector<bool> field_is_multi(cmd.fields.size(), false);
        for (size_t f = 0; f < cmd.fields.size(); ++f)
            field_is_multi[f] = is_multi_value_field(corpus, cmd.fields[f]);

        std::ostringstream json;
        json << "{\"ok\": true, \"operation\": \"tabulate\", \"last_command\": \"tabulate\", \"result\": {\n";
        json << "  \"fields\": [";
        for (size_t i = 0; i < cmd.fields.size(); ++i) {
            if (i > 0) json << ", ";
            json << jstr(cmd.fields[i]);
        }
        json << "],\n  \"total_matches\": " << total_hits << ",\n";
        json << "  \"offset\": " << cmd.tabulate_offset << ",\n";
        json << "  \"limit\": " << cmd.tabulate_limit << ",\n";
        json << "  \"rows_returned\": " << (end - start) << ",\n";
        json << "  \"rows\": [\n";
        for (size_t i = start; i < end; ++i) {
            if (i > start) json << ",\n";
            json << "    [";
            for (size_t f = 0; f < cmd.fields.size(); ++f) {
                if (f > 0) json << ", ";
                std::string val = read_tabulate_field(corpus, ms.matches[i], name_map, cmd.fields[f]);
                emit_field_json(json, val, field_is_multi[f]);
            }
            json << "]";
        }
        json << "\n  ]\n}}\n";
        std::cout << json.str();
    } else {
        if (start < end) {
            for (size_t f = 0; f < cmd.fields.size(); ++f)
                read_tabulate_field(corpus, ms.matches[start], name_map, cmd.fields[f]);
        }
        // Header
        for (size_t i = 0; i < cmd.fields.size(); ++i) {
            if (i > 0) std::cout << '\t';
            std::cout << cmd.fields[i];
        }
        std::cout << '\n';

        for (size_t i = start; i < end; ++i) {
            const auto& m = ms.matches[i];
            for (size_t f = 0; f < cmd.fields.size(); ++f) {
                if (f > 0) std::cout << '\t';
                std::cout << read_tabulate_field(corpus, m, name_map, cmd.fields[f]);
            }
            std::cout << '\n';
        }
        if (end < n || (total_hits > n && end == n))
            std::cout << "# (" << total_hits << " matches in query; showing " << (end - start)
                      << " at offset " << cmd.tabulate_offset << ")\n";
    }
    } catch (const std::exception& e) {
        if (opts.json) {
            std::cout << "{\"ok\": false, \"error\": " << jstr(e.what()) << "}\n";
        } else {
            std::cerr << "Error: " << e.what() << "\n";
        }
    }
}

static void emit_describe(const Corpus& corpus, const MatchSet& ms,
                          const GroupCommand& cmd, const Options& opts,
                          const NameIndexMap& name_map) {
    const size_t n = ms.matches.size();
    const size_t start = std::min(cmd.tabulate_offset, n);
    const size_t end = std::min(start + cmd.tabulate_limit, n);
    const size_t total_hits = ms.total_count > 0 ? ms.total_count : n;

    auto is_region_only_match = [&](const Match& m) {
        return m.token_group_match || (!m.named_regions.empty() && name_map.empty());
    };

    if (opts.json || opts.api) {
        std::ostringstream out;
        out << "{\"ok\": true, \"operation\": \"describe\", \"last_command\": \"describe\", \"result\": {\n";
        out << "  \"total_matches\": " << total_hits << ",\n";
        out << "  \"offset\": " << cmd.tabulate_offset << ",\n";
        out << "  \"limit\": " << cmd.tabulate_limit << ",\n";
        out << "  \"rows_returned\": " << (end - start) << ",\n";
        out << "  \"rows\": [\n";
        for (size_t i = start; i < end; ++i) {
            if (i > start) out << ",\n";
            const auto& m = ms.matches[i];
            const bool region_only = is_region_only_match(m);
            out << "    {\"match_index\": " << i
                << ", \"match_start\": " << m.first_pos()
                << ", \"match_end\": " << m.last_pos()
                << ", \"kind\": " << jstr(region_only ? "region" : "token");

            if (region_only) {
                out << ", \"regions\": [";
                bool first_region = true;
                for (const auto& [label, rr] : m.named_regions) {
                    if (!corpus.has_structure(rr.struct_name)) continue;
                    const auto& sa = corpus.structure(rr.struct_name);
                    if (rr.region_idx >= sa.region_count()) continue;
                    if (!first_region) out << ", ";
                    first_region = false;
                    Region r = sa.get(rr.region_idx);
                    out << "{\"label\": " << jstr(label)
                        << ", \"type\": " << jstr(rr.struct_name)
                        << ", \"index\": " << rr.region_idx
                        << ", \"start\": " << r.start
                        << ", \"end\": " << r.end
                        << ", \"attrs\": {";
                    const auto& ra = sa.region_attr_names();
                    bool first_ra = true;
                    for (size_t j = 0; j < ra.size(); ++j) {
                        std::string_view rv = sa.region_value(ra[j], rr.region_idx);
                        if (!describe_emit_attr(ra[j], rv)) continue;
                        if (!first_ra) out << ", ";
                        first_ra = false;
                        out << jstr(ra[j]) << ": " << jstr(std::string(rv));
                    }
                    out << "}}";
                }
                out << "]";
                if (!m.token_group_props.empty()) {
                    out << ", \"token_group_props\": {";
                    bool first_prop = true;
                    for (size_t j = 0; j < m.token_group_props.size(); ++j) {
                        const auto& [pk, pv] = m.token_group_props[j];
                        if (!describe_emit_attr(pk, pv)) continue;
                        if (!first_prop) out << ", ";
                        first_prop = false;
                        out << jstr(pk) << ": " << jstr(pv);
                    }
                    out << "}";
                }
            } else {
                out << ", \"tokens\": [";
                bool first_tok = true;
                const auto& attr_names = corpus.attr_names();
                for (size_t t = 0; t < m.positions.size(); ++t) {
                    if (m.positions[t] == NO_HEAD) continue;
                    CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
                    for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                        if (!first_tok) out << ", ";
                        first_tok = false;
                        out << "{\"corpus_pos\": " << p;
                        for (const auto& attr_name : attr_names) {
                            if (!corpus.has_attr(attr_name)) continue;
                            auto val = corpus.attr(attr_name).value_at(p);
                            if (!describe_emit_attr(attr_name, val)) continue;
                            out << ", " << jstr(attr_name) << ": " << jstr(val);
                        }
                        out << "}";
                    }
                }
                out << "]";
            }
            out << "}";
        }
        out << "\n  ]\n}}\n";
        std::cout << out.str();
        return;
    }

    for (size_t i = start; i < end; ++i) {
        const auto& m = ms.matches[i];
        const bool region_only = is_region_only_match(m);
        std::cout << "#" << i << " [" << m.first_pos() << "," << m.last_pos() << "] "
                  << (region_only ? "region" : "token") << "\n";
    }
    if (end < n || (total_hits > n && end == n))
        std::cout << "# (" << total_hits << " matches in query; showing " << (end - start)
                  << " at offset " << cmd.tabulate_offset << ")\n";
}

static void freq_build_counts(const Corpus& corpus, const MatchSet& ms,
                              const GroupCommand& cmd, const Options& opts,
                              const NameIndexMap& name_map,
                              std::map<std::string, size_t>& counts,
                              size_t& total_matches) {
    counts.clear();
    if (ms.aggregate_buckets) {
        for (const auto& [k, c] : ms.aggregate_buckets->counts)
            counts[decode_aggregate_bucket_key(*ms.aggregate_buckets, k)] += c;
    } else {
        for (const auto& m : ms.matches)
            ++counts[make_key(corpus, m, name_map, cmd.fields)];
    }
    if (!opts.no_mv_explode && cmd.fields.size() == 1 && corpus.is_multivalue(cmd.fields[0])) {
        std::map<std::string, size_t> exploded;
        for (const auto& [key, count] : counts) {
            if (key.find('|') != std::string::npos) {
                size_t s = 0;
                while (s < key.size()) {
                    size_t p = key.find('|', s);
                    if (p == std::string::npos) p = key.size();
                    std::string comp = key.substr(s, p - s);
                    if (!comp.empty()) exploded[comp] += count;
                    s = p + 1;
                }
            } else {
                exploded[key] += count;
            }
        }
        counts = std::move(exploded);
    }
    total_matches = ms.aggregate_buckets ? ms.aggregate_buckets->total_hits : ms.matches.size();
}

enum class FreqDateTransform { None, Year, Century, Decade, Month, Week, Day };

struct FreqSubcorpusSpec {
    const StructuralAttr* sa = nullptr;
    struct FieldSpec {
        std::string region_attr;
        FreqDateTransform transform = FreqDateTransform::None;
    };
    std::vector<FieldSpec> fields;
};

static std::optional<std::string> apply_date_bucket(std::string_view raw, FreqDateTransform t) {
    if (t == FreqDateTransform::None) return std::string(raw);
    struct ParsedDateParts {
        int64_t year = 0;
        int month = 0;
        int day = 0;
        bool has_month = false;
        bool has_day = false;
    };
    auto is_leap_year = [](int64_t y) {
        return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0));
    };
    auto days_in_month = [&](int64_t y, int m) {
        static const int kDays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
        if (m == 2) return is_leap_year(y) ? 29 : 28;
        return kDays[m - 1];
    };
    auto parse_date_parts_prefix = [&](std::string_view text) -> std::optional<ParsedDateParts> {
        size_t i = 0;
        while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
        if (i + 4 > text.size()) return std::nullopt;
        for (size_t k = 0; k < 4; ++k) {
            if (!std::isdigit(static_cast<unsigned char>(text[i + k])))
                return std::nullopt;
        }
        ParsedDateParts out;
        for (size_t k = 0; k < 4; ++k)
            out.year = out.year * 10 + static_cast<int64_t>(text[i + k] - '0');
        i += 4;
        if (i >= text.size() || (text[i] != '-' && text[i] != '/'))
            return out;
        char sep = text[i++];
        if (i + 2 > text.size()) return std::nullopt;
        if (!std::isdigit(static_cast<unsigned char>(text[i]))
            || !std::isdigit(static_cast<unsigned char>(text[i + 1])))
            return std::nullopt;
        out.month = static_cast<int>((text[i] - '0') * 10 + (text[i + 1] - '0'));
        if (out.month < 1 || out.month > 12) return std::nullopt;
        out.has_month = true;
        i += 2;
        if (i >= text.size() || text[i] != sep)
            return out;
        ++i;
        if (i + 2 > text.size()) return std::nullopt;
        if (!std::isdigit(static_cast<unsigned char>(text[i]))
            || !std::isdigit(static_cast<unsigned char>(text[i + 1])))
            return std::nullopt;
        out.day = static_cast<int>((text[i] - '0') * 10 + (text[i + 1] - '0'));
        if (out.day < 1 || out.day > days_in_month(out.year, out.month))
            return std::nullopt;
        out.has_day = true;
        return out;
    };
    auto weekday_iso = [](int64_t y, int m, int d) {
        static const int tt[12] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
        int64_t yy = y;
        if (m < 3) --yy;
        int w = static_cast<int>((yy + yy/4 - yy/100 + yy/400 + tt[m - 1] + d) % 7);
        if (w < 0) w += 7;
        return w == 0 ? 7 : w;
    };
    auto iso_weeks_in_year = [&](int64_t y) {
        int jan1 = weekday_iso(y, 1, 1);
        if (jan1 == 4) return 53;
        if (jan1 == 3 && is_leap_year(y)) return 53;
        return 52;
    };
    auto day_of_year = [&](int64_t y, int m, int d) {
        int doy = d;
        for (int mm = 1; mm < m; ++mm)
            doy += days_in_month(y, mm);
        return doy;
    };
    auto iso_week_number = [&](int64_t y, int m, int d) {
        int doy = day_of_year(y, m, d);
        int dow = weekday_iso(y, m, d);
        int week = (doy - dow + 10) / 7;
        if (week < 1) return iso_weeks_in_year(y - 1);
        int wiy = iso_weeks_in_year(y);
        if (week > wiy) return 1;
        return week;
    };

    auto p = parse_date_parts_prefix(raw);
    if (!p) return std::nullopt;
    switch (t) {
        case FreqDateTransform::Year:
            return std::to_string(p->year);
        case FreqDateTransform::Century:
            if (p->year <= 0) return std::nullopt;
            return std::to_string(((p->year - 1) / 100) + 1);
        case FreqDateTransform::Decade:
            return std::to_string((p->year / 10) * 10);
        case FreqDateTransform::Month:
            if (!p->has_month) return std::nullopt;
            return std::to_string(p->month);
        case FreqDateTransform::Week:
            if (!p->has_day) return std::nullopt;
            return std::to_string(iso_week_number(p->year, p->month, p->day));
        case FreqDateTransform::Day:
            if (!p->has_day) return std::nullopt;
            return std::to_string(p->day);
        case FreqDateTransform::None:
            break;
    }
    return std::nullopt;
}

static std::optional<FreqSubcorpusSpec> resolve_freq_subcorpus_spec(
        const Corpus& corpus, const std::vector<std::string>& fields) {
    if (fields.empty()) return std::nullopt;
    auto resolve_region = [&](const std::string& attr_spec, FreqDateTransform tr,
                              const StructuralAttr** expected_sa)
            -> std::optional<FreqSubcorpusSpec::FieldSpec> {
        RegionAttrParts parts;
        if (!split_region_attr_name(attr_spec, parts) || !corpus.has_structure(parts.struct_name))
            return std::nullopt;
        const auto& sa = corpus.structure(parts.struct_name);
        if (*expected_sa && *expected_sa != &sa) return std::nullopt;
        *expected_sa = &sa;
        auto rkey = resolve_region_attr_key(sa, parts.struct_name, parts.attr_name);
        if (!rkey) return std::nullopt;
        FreqSubcorpusSpec::FieldSpec fs;
        fs.region_attr = *rkey;
        fs.transform = tr;
        return fs;
    };

    auto parse_wrapped = [&](const std::string& f, const char* name)
            -> std::optional<std::string> {
        const size_t n = std::strlen(name);
        if (f.size() <= n + 2 || f.compare(0, n, name) != 0 || f[n] != '(' || f.back() != ')')
            return std::nullopt;
        std::string inner = f.substr(n + 1, f.size() - n - 2);
        if (inner.rfind("match.", 0) == 0 && inner.size() > 6)
            inner = inner.substr(6);
        return inner;
    };
    auto parse_field = [&](const std::string& f, const StructuralAttr** expected_sa)
            -> std::optional<FreqSubcorpusSpec::FieldSpec> {
        if (auto base = resolve_region(f, FreqDateTransform::None, expected_sa)) return base;
        if (auto inner = parse_wrapped(f, "year"))
            return resolve_region(*inner, FreqDateTransform::Year, expected_sa);
        if (auto inner = parse_wrapped(f, "century"))
            return resolve_region(*inner, FreqDateTransform::Century, expected_sa);
        if (auto inner = parse_wrapped(f, "decade"))
            return resolve_region(*inner, FreqDateTransform::Decade, expected_sa);
        if (auto inner = parse_wrapped(f, "month"))
            return resolve_region(*inner, FreqDateTransform::Month, expected_sa);
        if (auto inner = parse_wrapped(f, "week"))
            return resolve_region(*inner, FreqDateTransform::Week, expected_sa);
        if (auto inner = parse_wrapped(f, "day"))
            return resolve_region(*inner, FreqDateTransform::Day, expected_sa);
        return std::nullopt;
    };

    const StructuralAttr* common_sa = nullptr;
    FreqSubcorpusSpec spec;
    for (const auto& f : fields) {
        auto fs = parse_field(f, &common_sa);
        if (!fs) return std::nullopt;
        spec.fields.push_back(std::move(*fs));
    }
    spec.sa = common_sa;
    if (!spec.sa || spec.fields.empty()) return std::nullopt;
    return spec;
}

static std::unordered_map<std::string, double> build_freq_subcorpus_sizes(
        const FreqSubcorpusSpec& spec,
        const std::vector<std::string>& keys,
        double corpus_size) {
    std::unordered_map<std::string, double> out;
    if (!spec.sa) return out;

    if (spec.fields.size() == 1 && spec.fields[0].transform == FreqDateTransform::None) {
        const auto& f0 = spec.fields[0];
        for (const auto& key : keys) {
            size_t span = spec.sa->token_span_sum_for_attr_eq(f0.region_attr, key);
            out[key] = (span > 0 && span != SIZE_MAX) ? static_cast<double>(span) : corpus_size;
        }
        return out;
    }

    std::unordered_map<std::string, size_t> span_by_key;
    for (size_t ri = 0; ri < spec.sa->region_count(); ++ri) {
        std::string key;
        bool ok = true;
        for (size_t i = 0; i < spec.fields.size(); ++i) {
            const auto& fs = spec.fields[i];
            auto bucket = apply_date_bucket(spec.sa->region_value(fs.region_attr, ri), fs.transform);
            if (!bucket) { ok = false; break; }
            if (i) key.push_back('\t');
            key += *bucket;
        }
        if (!ok) continue;
        Region rg = spec.sa->get(ri);
        if (rg.end < rg.start) continue;
        span_by_key[key] += static_cast<size_t>(rg.end - rg.start + 1);
    }
    for (const auto& key : keys) {
        auto it = span_by_key.find(key);
        size_t span = (it != span_by_key.end()) ? it->second : 0;
        out[key] = (span > 0 && span != SIZE_MAX) ? static_cast<double>(span) : corpus_size;
    }
    return out;
}

static bool session_lookup_ms(Session& session, const std::string& qn,
                              MatchSet*& ms, NameIndexMap*& nm) {
    if (qn == "Last") {
        if (!session.has_last) return false;
        ms = &session.last_ms;
        nm = &session.last_name_map;
        return true;
    }
    auto it = session.named_results.find(qn);
    if (it == session.named_results.end()) return false;
    ms = &it->second;
    auto nm_it = session.named_name_maps.find(qn);
    nm = (nm_it != session.named_name_maps.end()) ? &nm_it->second : &session.last_name_map;
    return true;
}

static void emit_freq_compare(const Corpus& corpus, Session& session,
                              const GroupCommand& cmd, const Options& opts) {
    if (cmd.fields.empty()) {
        std::cerr << "Error: freq requires 'by' clause\n";
        return;
    }
    struct Src {
        std::string label;
        MatchSet* ms;
        NameIndexMap* nm;
    };
    std::vector<Src> srcs;
    srcs.reserve(cmd.freq_query_names.size());
    for (const std::string& qn : cmd.freq_query_names) {
        MatchSet* ms = nullptr;
        NameIndexMap* nm = nullptr;
        if (!session_lookup_ms(session, qn, ms, nm)) {
            if (opts.json)
                std::cout << "{\"ok\": false, \"error\": " << jstr("Unknown named query: " + qn) << "}\n";
            else
                std::cerr << "Error: unknown named query '" << qn << "'\n";
            return;
        }
        srcs.push_back({qn, ms, nm});
    }

    std::vector<std::map<std::string, size_t>> counts_per(srcs.size());
    std::vector<size_t> totals(srcs.size());
    for (size_t i = 0; i < srcs.size(); ++i)
        freq_build_counts(corpus, *srcs[i].ms, cmd, opts, *srcs[i].nm, counts_per[i], totals[i]);

    std::set<std::string> all_keys;
    for (const auto& m : counts_per)
        for (const auto& [k, c] : m)
            all_keys.insert(k);
    std::vector<std::string> sorted_keys(all_keys.begin(), all_keys.end());
    std::sort(sorted_keys.begin(), sorted_keys.end(),
              [&](const std::string& a, const std::string& b) {
                  size_t sa = 0, sb = 0;
                  for (const auto& m : counts_per) {
                      auto ia = m.find(a), ib = m.find(b);
                      if (ia != m.end()) sa += ia->second;
                      if (ib != m.end()) sb += ib->second;
                  }
                  if (sa != sb) return sa > sb;
                  return compare_group_keys(a, b);
              });

    double corpus_size = static_cast<double>(corpus.size());
    auto freq_spec = resolve_freq_subcorpus_spec(corpus, cmd.fields);
    const bool use_subcorpus_ipm = freq_spec.has_value();
    std::unordered_map<std::string, double> subcorpus_sizes =
            use_subcorpus_ipm
                ? build_freq_subcorpus_sizes(*freq_spec, sorted_keys, corpus_size)
                : std::unordered_map<std::string, double>{};
    auto ipm_denom = [&](const std::string& key) -> double {
        if (use_subcorpus_ipm) {
            auto it = subcorpus_sizes.find(key);
            if (it != subcorpus_sizes.end()) return it->second;
        }
        return corpus_size;
    };

    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"freq\", \"last_command\": \"freq\", \"result\": {\n";
        std::cout << "  \"compare_queries\": [";
        for (size_t i = 0; i < srcs.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(srcs[i].label);
        }
        std::cout << "],\n  \"corpus_size\": " << corpus.size() << ",\n";
        std::cout << "  \"per_subcorpus_ipm\": " << (use_subcorpus_ipm ? "true" : "false") << ",\n";
        std::cout << "  \"fields\": [";
        for (size_t i = 0; i < cmd.fields.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(cmd.fields[i]);
        }
        std::cout << "],\n  \"totals_per_query\": {";
        for (size_t i = 0; i < srcs.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(srcs[i].label) << ": " << totals[i];
        }
        std::cout << "},\n  \"rows\": [\n";
        for (size_t ri = 0; ri < sorted_keys.size(); ++ri) {
            const std::string& key = sorted_keys[ri];
            if (ri > 0) std::cout << ",\n";
            double denom = ipm_denom(key);
            std::cout << "    {\"key\": " << jstr(key);
            if (use_subcorpus_ipm) std::cout << ", \"subcorpus_size\": " << static_cast<size_t>(denom);
            std::cout << ", \"queries\": {";
            for (size_t qi = 0; qi < srcs.size(); ++qi) {
                if (qi > 0) std::cout << ", ";
                size_t c = 0;
                auto it = counts_per[qi].find(key);
                if (it != counts_per[qi].end()) c = it->second;
                double pct = totals[qi] > 0 ? 100.0 * static_cast<double>(c) / static_cast<double>(totals[qi]) : 0.0;
                double ipm = 1e6 * static_cast<double>(c) / denom;
                double q_ipm = totals[qi] > 0
                    ? 1e6 * static_cast<double>(c) / static_cast<double>(totals[qi])
                    : 0.0;
                std::cout << jstr(srcs[qi].label) << ": {\"count\": " << c
                          << ", \"pct\": " << pct
                          << ", \"ipm\": " << std::fixed << std::setprecision(2) << ipm
                          << ", \"q_ipm\": " << q_ipm;
                if (use_subcorpus_ipm) {
                    double rf_pct = denom > 0 ? 100.0 * static_cast<double>(c) / denom : 0.0;
                    std::cout << ", \"rf_pct\": " << std::setprecision(4) << rf_pct;
                }
                std::cout << "}";
            }
            std::cout << "}}";
        }
        std::cout << "\n  ]\n}}\n";
    } else {
        std::cout << "# freq compare: ";
        for (size_t i = 0; i < srcs.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << srcs[i].label;
        }
        std::cout << "\n";
        if (use_subcorpus_ipm)
            std::cout << "# Qn_% = share of that query's hits across rows; Qn_ipm and Qn_rf% = relative frequency per subcorpus (tokens)\n";
        for (const auto& f : cmd.fields) std::cout << f << "\t";
        for (size_t i = 0; i < srcs.size(); ++i) {
            std::cout << srcs[i].label << "_count\t" << srcs[i].label << "_%\t" << srcs[i].label
                      << "_ipm\t";
            if (use_subcorpus_ipm) std::cout << srcs[i].label << "_rf%\t";
        }
        if (use_subcorpus_ipm) std::cout << "subcorpus";
        std::cout << "\n";
        for (const std::string& key : sorted_keys) {
            double denom = ipm_denom(key);
            std::cout << key << "\t";
            for (size_t qi = 0; qi < srcs.size(); ++qi) {
                size_t c = 0;
                auto it = counts_per[qi].find(key);
                if (it != counts_per[qi].end()) c = it->second;
                double pct = totals[qi] > 0
                    ? 100.0 * static_cast<double>(c) / static_cast<double>(totals[qi])
                    : 0.0;
                double ipm = 1e6 * static_cast<double>(c) / denom;
                std::cout << c << "\t" << std::fixed << std::setprecision(1) << pct << "%\t"
                          << std::setprecision(2) << ipm << "\t";
                if (use_subcorpus_ipm) {
                    double rf_pct = denom > 0 ? 100.0 * static_cast<double>(c) / denom : 0.0;
                    std::cout << std::setprecision(2) << rf_pct << "%\t";
                }
            }
            if (use_subcorpus_ipm) std::cout << static_cast<size_t>(denom);
            std::cout << "\n";
        }
    }
}

static void emit_freq(const Corpus& corpus, const MatchSet& ms,
                      const GroupCommand& cmd, const Options& opts,
                      const NameIndexMap& name_map) {
    if (cmd.fields.empty()) {
        std::cerr << "Error: freq requires 'by' clause\n";
        return;
    }

    std::map<std::string, size_t> counts;
    size_t total_matches = 0;
    freq_build_counts(corpus, ms, cmd, opts, name_map, counts, total_matches);

    std::vector<std::pair<std::string, size_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    double corpus_size = static_cast<double>(corpus.size());

    // Per-subcorpus IPM: when grouping by a single region attribute (e.g. text_langcode),
    // use the token count of each subcorpus as the IPM denominator instead of the full
    // corpus size. This gives meaningful relative frequencies per group.
    auto freq_spec = resolve_freq_subcorpus_spec(corpus, cmd.fields);
    const bool use_subcorpus_ipm = freq_spec.has_value();
    std::vector<std::string> sorted_keys;
    sorted_keys.reserve(sorted.size());
    for (const auto& kv : sorted) sorted_keys.push_back(kv.first);
    std::unordered_map<std::string, double> subcorpus_sizes =
            use_subcorpus_ipm
                ? build_freq_subcorpus_sizes(*freq_spec, sorted_keys, corpus_size)
                : std::unordered_map<std::string, double>{};

    // Helper: get IPM denominator for a given key.
    auto ipm_denom = [&](const std::string& key) -> double {
        if (use_subcorpus_ipm) {
            auto it = subcorpus_sizes.find(key);
            if (it != subcorpus_sizes.end()) return it->second;
        }
        return corpus_size;
    };

    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"freq\", \"last_command\": \"freq\", \"result\": {\n";
        std::cout << "  \"corpus_size\": " << corpus.size() << ",\n";
        std::cout << "  \"total_matches\": " << total_matches << ",\n";
        std::cout << "  \"per_subcorpus_ipm\": " << (use_subcorpus_ipm ? "true" : "false") << ",\n";
        std::cout << "  \"fields\": [";
        for (size_t i = 0; i < cmd.fields.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(cmd.fields[i]);
        }
        std::cout << "],\n  \"rows\": [\n";
        for (size_t i = 0; i < sorted.size(); ++i) {
            if (i > 0) std::cout << ",\n";
            double denom = ipm_denom(sorted[i].first);
            double ipm = 1e6 * static_cast<double>(sorted[i].second) / denom;
            double pct = total_matches > 0
                ? 100.0 * static_cast<double>(sorted[i].second) / static_cast<double>(total_matches)
                : 0.0;
            std::cout << "    {\"key\": " << jstr(sorted[i].first)
                      << ", \"count\": " << sorted[i].second
                      << ", \"pct\": " << pct
                      << ", \"ipm\": " << std::fixed << std::setprecision(2) << ipm;
            if (use_subcorpus_ipm) {
                double rf_pct = denom > 0
                    ? 100.0 * static_cast<double>(sorted[i].second) / denom
                    : 0.0;
                std::cout << ", \"rf_pct\": " << std::setprecision(4) << rf_pct
                          << ", \"subcorpus_size\": " << static_cast<size_t>(denom);
            }
            std::cout << "}";
        }
        std::cout << "\n  ]\n}}\n";
    } else {
        for (const auto& f : cmd.fields) std::cout << f << "\t";
        std::cout << "count\t%\tipm";
        if (use_subcorpus_ipm) std::cout << "\trf%\tsubcorpus";
        std::cout << "\n";
        for (const auto& [key, count] : sorted) {
            double pct = total_matches > 0
                ? 100.0 * static_cast<double>(count) / static_cast<double>(total_matches)
                : 0.0;
            double denom = ipm_denom(key);
            double ipm = 1e6 * static_cast<double>(count) / denom;
            std::cout << key << "\t" << count << "\t" << std::fixed << std::setprecision(1) << pct
                      << "%\t" << std::setprecision(2) << ipm;
            if (use_subcorpus_ipm) {
                double rf_pct = denom > 0 ? 100.0 * static_cast<double>(count) / denom : 0.0;
                std::cout << "\t" << std::setprecision(2) << rf_pct << "%";
            }
            if (use_subcorpus_ipm) std::cout << "\t" << static_cast<size_t>(denom);
            std::cout << "\n";
        }
    }
}

static void emit_raw(const Corpus& corpus, const MatchSet& ms, const Options& opts) {
    const auto& form = corpus.attr("form");
    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"raw\", \"last_command\": \"raw\", \"result\": [\n";
        for (size_t i = 0; i < ms.matches.size(); ++i) {
            if (i > 0) std::cout << ",\n";
            const auto& m = ms.matches[i];
            auto positions = m.matched_positions();
            std::cout << "  {\"positions\": [";
            for (size_t j = 0; j < positions.size(); ++j) {
                if (j > 0) std::cout << ", ";
                std::cout << positions[j];
            }
            std::cout << "], \"tokens\": [";
            for (size_t j = 0; j < positions.size(); ++j) {
                if (j > 0) std::cout << ", ";
                std::cout << jstr(std::string(form.value_at(positions[j])));
            }
            std::cout << "]}";
        }
        std::cout << "\n]}\n";
    } else {
        for (const auto& m : ms.matches) {
            auto positions = m.matched_positions();
            // First column: first position
            std::cout << m.first_pos();
            // Tab-separated matched tokens
            for (CorpusPos p : positions)
                std::cout << "\t" << form.value_at(p);
            std::cout << "\n";
        }
    }
}

// ── Association measures ────────────────────────────────────────────────

struct CollEntry {
    LexiconId id;
    std::string form;
    size_t obs;       // observed co-occurrence count
    size_t f_coll;    // corpus frequency of collocate
    size_t f_node;    // total co-occurrence window positions (≈ N matches * window)
    size_t N;         // corpus size
};

static double compute_mi(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    if (expected == 0) return 0;
    return log2(static_cast<double>(e.obs) / expected);
}

static double compute_mi3(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    if (expected == 0) return 0;
    return log2(static_cast<double>(e.obs) * static_cast<double>(e.obs) * static_cast<double>(e.obs) / expected);
}

static double compute_tscore(const CollEntry& e) {
    if (e.obs == 0 || e.f_coll == 0 || e.f_node == 0 || e.N == 0) return 0;
    double expected = static_cast<double>(e.f_node) * static_cast<double>(e.f_coll) / static_cast<double>(e.N);
    return (static_cast<double>(e.obs) - expected) / sqrt(static_cast<double>(e.obs));
}

static double compute_dice(const CollEntry& e) {
    if (e.f_node == 0 && e.f_coll == 0) return 0;
    return 2.0 * static_cast<double>(e.obs) / (static_cast<double>(e.f_node) + static_cast<double>(e.f_coll));
}

static double compute_logdice(const CollEntry& e) {
    double d = compute_dice(e);
    if (d <= 0) return 0;
    return 14.0 + log2(d);  // Rychlý's logDice: 14 + log2(Dice)
}

static double compute_ll(const CollEntry& e) {
    // Log-likelihood ratio (G²)
    // 2x2 contingency table:
    //                 in window   not in window
    //   collocate:      a            b          (f_coll)
    //   not collocate:  c            d
    //                 (f_node)       ...        (N)
    double a = static_cast<double>(e.obs);
    double b = static_cast<double>(e.f_coll) - a;
    double c = static_cast<double>(e.f_node) - a;
    double d = static_cast<double>(e.N) - a - b - c;
    if (b < 0) b = 0;
    if (c < 0) c = 0;
    if (d < 0) d = 0;
    double n = static_cast<double>(e.N);
    auto xlogx = [](double x, double total) -> double {
        if (x <= 0 || total <= 0) return 0;
        return x * log(x / total);
    };
    double row1 = a + b, row2 = c + d;
    double col1 = a + c, col2 = b + d;
    double ll = 2.0 * (xlogx(a, row1 * col1 / n) +
                        xlogx(b, row1 * col2 / n) +
                        xlogx(c, row2 * col1 / n) +
                        xlogx(d, row2 * col2 / n));
    return ll;
}

static double compute_measure(const std::string& name, const CollEntry& e) {
    if (name == "mi")       return compute_mi(e);
    if (name == "mi3")      return compute_mi3(e);
    if (name == "t")        return compute_tscore(e);
    if (name == "tscore")   return compute_tscore(e);
    if (name == "dice")     return compute_dice(e);
    if (name == "logdice")  return compute_logdice(e);
    if (name == "ll")       return compute_ll(e);
    return compute_logdice(e);  // fallback
}

static std::unordered_set<LexiconId> build_stoplist_ids(const PositionalAttr& pa, size_t top_n) {
    std::unordered_set<LexiconId> out;
    if (top_n == 0) return out;
    std::vector<std::pair<LexiconId, size_t>> items;
    const auto& lex = pa.lexicon();
    items.reserve(lex.size());
    for (LexiconId id = 0; id < lex.size(); ++id) {
        size_t c = pa.count_of_id(id);
        if (c == 0) continue;
        items.push_back({id, c});
    }
    std::sort(items.begin(), items.end(), [&](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return std::string(lex.get(a.first)) < std::string(lex.get(b.first));
    });
    size_t keep = std::min(top_n, items.size());
    for (size_t i = 0; i < keep; ++i) out.insert(items[i].first);
    return out;
}

// ── coll: window-based collocation ─────────────────────────────────────

static void emit_coll(const Corpus& corpus, const MatchSet& ms,
                      const GroupCommand& cmd, const Options& opts,
                      const NameIndexMap& name_map,
                      const NameIndexMap* target_name_map) {
    // Determine which attribute to group collocates by (default: lemma, fallback form)
    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);

    std::vector<std::string> measures = opts.coll_measures;
    if (measures.empty()) measures = {"logdice"};

    // Collect co-occurrence counts
    std::unordered_map<LexiconId, size_t> obs_counts;
    size_t total_window_positions = 0;

    auto count_coll_token = [&](CorpusPos p) {
        // Parallel tiers often leave lemma (etc.) empty; counting those positions collapses into
        // one high-frequency "" bucket and hides real collocates.
        if (pa.value_at(p).empty()) return;
        ++obs_counts[pa.id_at(p)];
        ++total_window_positions;
    };

    auto add_coll_envelope = [&](const Match& m) {
        auto matched = m.matched_positions();
        std::set<CorpusPos> matched_set(matched.begin(), matched.end());
        CorpusPos first = m.first_pos();
        CorpusPos last = m.last_pos();

        CorpusPos left_start =
                (first > static_cast<CorpusPos>(opts.coll_left)) ? first - opts.coll_left : 0;
        for (CorpusPos p = left_start; p < first; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
        CorpusPos right_end = std::min(last + static_cast<CorpusPos>(opts.coll_right) + 1,
                                         static_cast<CorpusPos>(corpus.size()));
        for (CorpusPos p = last + 1; p < right_end; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
    };

    auto add_coll_hub = [&](const std::set<CorpusPos>& matched_set, CorpusPos hub) {
        CorpusPos left_start =
                (hub > static_cast<CorpusPos>(opts.coll_left)) ? hub - opts.coll_left : 0;
        for (CorpusPos p = left_start; p < hub; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
        CorpusPos right_end = std::min(hub + static_cast<CorpusPos>(opts.coll_right) + 1,
                                         static_cast<CorpusPos>(corpus.size()));
        for (CorpusPos p = hub + 1; p < right_end; ++p) {
            if (matched_set.count(p)) continue;
            count_coll_token(p);
        }
    };

    if (!ms.parallel_matches.empty()) {
        for (const auto& [s, t] : ms.parallel_matches) {
            if (cmd.coll_on_label.empty()) {
                add_coll_envelope(s);
            } else {
                CorpusPos hub =
                        resolve_token_label_pair(s, name_map, t, target_name_map, cmd.coll_on_label);
                if (hub == NO_HEAD) continue;
                std::set<CorpusPos> excl;
                for (CorpusPos p : s.matched_positions()) excl.insert(p);
                for (CorpusPos p : t.matched_positions()) excl.insert(p);
                add_coll_hub(excl, hub);
            }
        }
    } else {
        for (const auto& m : ms.matches) {
            if (cmd.coll_on_label.empty()) {
                add_coll_envelope(m);
            } else {
                CorpusPos hub = resolve_name(m, name_map, cmd.coll_on_label);
                if (hub == NO_HEAD) continue;
                auto matched = m.matched_positions();
                std::set<CorpusPos> matched_set(matched.begin(), matched.end());
                add_coll_hub(matched_set, hub);
            }
        }
    }

    // Build entries, filter by min freq
    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (stop_ids.count(id)) continue;
        if (obs < opts.coll_min_freq) continue;
        std::string w = std::string(pa.lexicon().get(id));
        if (w.empty()) continue;
        size_t f_coll = pa.count_of_id(id);
        entries.push_back({id, std::move(w), obs, f_coll, total_window_positions, N});
    }

    // Sort by primary measure descending
    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });

    size_t show = std::min(entries.size(), opts.coll_max_items);

    const size_t coll_match_n =
            !ms.parallel_matches.empty() ? ms.parallel_matches.size() : ms.matches.size();

    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"coll\", \"last_command\": \"coll\", \"result\": {\n";
        std::cout << "  \"attribute\": " << jstr(coll_attr) << ",\n";
        std::cout << "  \"window\": [" << opts.coll_left << ", " << opts.coll_right << "],\n";
        if (!cmd.coll_on_label.empty())
            std::cout << "  \"on\": " << jstr(cmd.coll_on_label) << ",\n";
        std::cout << "  \"matches\": " << coll_match_n << ",\n";
        std::cout << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
        std::cout << "  \"measures\": [";
        for (size_t i = 0; i < measures.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(measures[i]);
        }
        std::cout << "],\n  \"collocates\": [\n";
        for (size_t i = 0; i < show; ++i) {
            if (i > 0) std::cout << ",\n";
            std::cout << "    {\"word\": " << jstr(entries[i].form)
                      << ", \"obs\": " << entries[i].obs
                      << ", \"freq\": " << entries[i].f_coll;
            for (const auto& m : measures)
                std::cout << ", " << jstr(m) << ": " << std::fixed << std::setprecision(3) << compute_measure(m, entries[i]);
            std::cout << "}";
        }
        std::cout << "\n  ]\n}}\n";
    } else {
        // Header
        std::cout << coll_attr << "\tobs\tfreq";
        for (const auto& m : measures) std::cout << "\t" << m;
        std::cout << "\n";
        for (size_t i = 0; i < show; ++i) {
            std::cout << entries[i].form << "\t" << entries[i].obs << "\t" << entries[i].f_coll;
            for (const auto& m : measures)
                std::cout << "\t" << std::fixed << std::setprecision(3) << compute_measure(m, entries[i]);
            std::cout << "\n";
        }
    }
}

// ── dcoll: dependency-based collocation ────────────────────────────────

static void emit_dcoll(const Corpus& corpus, const MatchSet& ms,
                       const GroupCommand& cmd, const Options& opts,
                       const NameIndexMap& name_map,
                       const NameIndexMap* target_name_map) {
    if (!corpus.has_deps()) {
        std::cerr << "Error: dcoll requires dependency index\n";
        return;
    }

    std::string coll_attr = "lemma";
    if (!cmd.fields.empty()) coll_attr = cmd.fields[0];
    if (!corpus.has_attr(coll_attr)) coll_attr = "form";
    const auto& pa = corpus.attr(coll_attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);
    const auto& deps = corpus.deps();

    // Deprel attribute for filtering child relations
    bool has_deprel_attr = corpus.has_attr("deprel");
    const PositionalAttr* deprel_pa = has_deprel_attr ? &corpus.attr("deprel") : nullptr;

    // Classify relations: separate "head", "descendants" from deprel names
    // If relations is empty → default to all children
    bool want_head = false;
    bool want_descendants = false;
    bool want_all_children = false;
    std::set<std::string> deprel_filter;

    if (cmd.relations.empty()) {
        want_all_children = true;
    } else {
        for (const auto& rel : cmd.relations) {
            if (rel == "head") want_head = true;
            else if (rel == "descendants") want_descendants = true;
            else if (rel == "children") want_all_children = true;
            else deprel_filter.insert(rel);  // specific deprel on children
        }
    }
    // If we have specific deprels, we also need children
    bool want_filtered_children = !deprel_filter.empty();

    std::vector<std::string> measures = opts.coll_measures;
    if (measures.empty()) measures = {"logdice"};

    std::unordered_map<LexiconId, size_t> obs_counts;
    size_t total_related = 0;

    auto emit_one_dcoll = [&](const Match& m, const Match* tgt_parallel) {
        CorpusPos node_pos = m.first_pos();
        if (!cmd.dcoll_anchor.empty()) {
            CorpusPos ap =
                    tgt_parallel ? resolve_token_label_pair(m, name_map, *tgt_parallel,
                                                             target_name_map, cmd.dcoll_anchor)
                                 : resolve_name(m, name_map, cmd.dcoll_anchor);
            if (ap != NO_HEAD) node_pos = ap;
        }

        auto count_token = [&](CorpusPos rp) {
            if (rp == node_pos) return;  // skip self
            ++obs_counts[pa.id_at(rp)];
            ++total_related;
        };

        if (want_head) {
            auto h = deps.head(node_pos);
            if (h != NO_HEAD) count_token(h);
        }

        if (want_descendants) {
            for (CorpusPos rp : deps.subtree(node_pos))
                count_token(rp);
        }

        if (want_all_children) {
            for (CorpusPos rp : deps.children(node_pos))
                count_token(rp);
        }

        if (want_filtered_children) {
            for (CorpusPos rp : deps.children(node_pos)) {
                if (deprel_pa) {
                    std::string dr(deprel_pa->value_at(rp));
                    if (deprel_filter.count(dr)) count_token(rp);
                }
            }
        }
    };

    if (!ms.parallel_matches.empty()) {
        for (const auto& [s, t] : ms.parallel_matches)
            emit_one_dcoll(s, &t);
    } else {
        for (const auto& m : ms.matches)
            emit_one_dcoll(m, nullptr);
    }

    // Build entries
    std::vector<CollEntry> entries;
    size_t N = corpus.size();
    for (const auto& [id, obs] : obs_counts) {
        if (stop_ids.count(id)) continue;
        if (obs < opts.coll_min_freq) continue;
        size_t f_coll = pa.count_of_id(id);
        entries.push_back({id, std::string(pa.lexicon().get(id)), obs, f_coll, total_related, N});
    }

    std::sort(entries.begin(), entries.end(), [&](const CollEntry& a, const CollEntry& b) {
        return compute_measure(measures[0], a) > compute_measure(measures[0], b);
    });

    size_t show = std::min(entries.size(), opts.coll_max_items);

    // Relations label for output
    std::string rel_label;
    if (cmd.relations.empty()) rel_label = "children";
    else {
        for (size_t i = 0; i < cmd.relations.size(); ++i) {
            if (i > 0) rel_label += ",";
            rel_label += cmd.relations[i];
        }
    }

    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"dcoll\", \"last_command\": \"dcoll\", \"result\": {\n";
        std::cout << "  \"attribute\": " << jstr(coll_attr) << ",\n";
        std::cout << "  \"relations\": [";
        for (size_t i = 0; i < cmd.relations.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(cmd.relations[i]);
        }
        std::cout << "],\n";
        if (!cmd.dcoll_anchor.empty())
            std::cout << "  \"anchor\": " << jstr(cmd.dcoll_anchor) << ",\n";
        {
            const size_t dcoll_match_n =
                    !ms.parallel_matches.empty() ? ms.parallel_matches.size() : ms.matches.size();
            std::cout << "  \"matches\": " << dcoll_match_n << ",\n";
        }
        std::cout << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
        std::cout << "  \"measures\": [";
        for (size_t i = 0; i < measures.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << jstr(measures[i]);
        }
        std::cout << "],\n  \"collocates\": [\n";
        for (size_t i = 0; i < show; ++i) {
            if (i > 0) std::cout << ",\n";
            std::cout << "    {\"word\": " << jstr(entries[i].form)
                      << ", \"obs\": " << entries[i].obs
                      << ", \"freq\": " << entries[i].f_coll;
            for (const auto& meas : measures)
                std::cout << ", " << jstr(meas) << ": " << std::fixed << std::setprecision(3) << compute_measure(meas, entries[i]);
            std::cout << "}";
        }
        std::cout << "\n  ]\n}}\n";
    } else {
        std::cout << coll_attr << "\tobs\tfreq";
        for (const auto& meas : measures) std::cout << "\t" << meas;
        std::cout << "\n";
        for (size_t i = 0; i < show; ++i) {
            std::cout << entries[i].form << "\t" << entries[i].obs << "\t" << entries[i].f_coll;
            for (const auto& meas : measures)
                std::cout << "\t" << std::fixed << std::setprecision(3) << compute_measure(meas, entries[i]);
            std::cout << "\n";
        }
    }
}

// ── keyness: subcorpus keyword extraction (#40) ────────────────────────
//
// Compares word frequencies in the match set ("focus corpus") against the
// whole corpus ("reference corpus") using log-likelihood (G²).
// Syntax: keyness by <attr>
//
// G² = 2 * sum_i( O_i * ln(O_i / E_i) )
// where O = observed count in focus/reference, E = expected count under H0.

struct KeynessEntry {
    std::string form;
    size_t focus_freq;     // count in matched positions
    size_t ref_freq;       // count in rest of corpus
    double keyness;        // G² score (positive = overuse in focus)
};

static double safe_ln(double x) { return x > 0 ? std::log(x) : 0.0; }

static double log_likelihood(size_t a, size_t b, size_t c, size_t d) {
    // a = focus freq, b = ref freq, c = focus size, d = ref size
    // G² = 2 * (a*ln(a/E1) + b*ln(b/E2))  where E1 = c*(a+b)/(c+d), E2 = d*(a+b)/(c+d)
    double N = static_cast<double>(c + d);
    double E1 = static_cast<double>(c) * static_cast<double>(a + b) / N;
    double E2 = static_cast<double>(d) * static_cast<double>(a + b) / N;
    double G2 = 0.0;
    if (a > 0) G2 += static_cast<double>(a) * safe_ln(static_cast<double>(a) / E1);
    if (b > 0) G2 += static_cast<double>(b) * safe_ln(static_cast<double>(b) / E2);
    return 2.0 * G2;
}

static void emit_keyness(const Corpus& corpus, const MatchSet& ms,
                          const GroupCommand& cmd, const Options& opts,
                          const NameIndexMap& name_map,
                          const MatchSet* ref_ms = nullptr) {
    // Determine attribute (default: lemma, fallback form)
    std::string attr = "lemma";
    if (!cmd.fields.empty()) attr = cmd.fields[0];
    if (!corpus.has_attr(attr)) attr = "form";
    const auto& pa = corpus.attr(attr);
    const auto stop_ids = build_stoplist_ids(pa, opts.coll_stoplist);

    // Count occurrences in focus (all matched positions)
    std::unordered_map<LexiconId, size_t> focus_counts;
    size_t focus_size = 0;
    for (const auto& m : ms.matches) {
        auto positions = m.matched_positions();
        for (CorpusPos p : positions) {
            ++focus_counts[pa.id_at(p)];
            ++focus_size;
        }
    }

    if (focus_size == 0) {
        if (opts.json) {
            std::cout << "{\"ok\": true, \"operation\": \"keyness\", \"last_command\": \"keyness\", \"result\": {\"rows\": []}}\n";
        } else {
            std::cout << "(no matches)\n";
        }
        return;
    }

    // Reference counts: either from a named query or the whole corpus
    std::unordered_map<LexiconId, size_t> ref_counts;
    size_t ref_size = 0;

    if (ref_ms) {
        // "vs N" mode: count from reference match set
        for (const auto& m : ref_ms->matches) {
            auto positions = m.matched_positions();
            for (CorpusPos p : positions) {
                ++ref_counts[pa.id_at(p)];
                ++ref_size;
            }
        }
    } else {
        // Default: reference = rest of corpus (computed per-word from global counts)
        ref_size = corpus.size() - focus_size;
    }

    // Build keyness entries
    std::vector<KeynessEntry> entries;
    // Collect all word types from both focus and ref
    std::set<LexiconId> all_ids;
    for (const auto& [id, _] : focus_counts)
        if (!stop_ids.count(id)) all_ids.insert(id);
    if (ref_ms) {
        for (const auto& [id, _] : ref_counts)
            if (!stop_ids.count(id)) all_ids.insert(id);
    }

    for (LexiconId id : all_ids) {
        size_t ffreq = 0;
        auto fit = focus_counts.find(id);
        if (fit != focus_counts.end()) ffreq = fit->second;

        size_t rfreq = 0;
        if (ref_ms) {
            auto rit = ref_counts.find(id);
            if (rit != ref_counts.end()) rfreq = rit->second;
        } else {
            size_t corpus_freq = pa.count_of_id(id);
            rfreq = corpus_freq > ffreq ? corpus_freq - ffreq : 0;
        }

        if (ffreq == 0 && rfreq == 0) continue;

        double g2 = log_likelihood(ffreq, rfreq, focus_size, ref_size);
        double expected = static_cast<double>(focus_size) * static_cast<double>(ffreq + rfreq)
                          / static_cast<double>(focus_size + ref_size);
        if (static_cast<double>(ffreq) < expected) g2 = -g2;
        entries.push_back({std::string(pa.lexicon().get(id)), ffreq, rfreq, g2});
    }

    // Sort by signed keyness descending: strongest focus-overrepresented items first.
    // Keep strongest negative keywords at the bottom (most negative last).
    std::sort(entries.begin(), entries.end(), [](const KeynessEntry& a, const KeynessEntry& b) {
        if (a.keyness != b.keyness) return a.keyness > b.keyness;
        if (a.focus_freq != b.focus_freq) return a.focus_freq > b.focus_freq;
        return a.form < b.form;
    });

    size_t show = std::min(entries.size(), opts.coll_max_items);

    if (opts.json) {
        std::cout << "{\"ok\": true, \"operation\": \"keyness\", \"last_command\": \"keyness\", \"result\": {\n";
        std::cout << "  \"attribute\": " << jstr(attr) << ",\n";
        std::cout << "  \"focus_size\": " << focus_size << ",\n";
        std::cout << "  \"ref_size\": " << ref_size << ",\n";
        std::cout << "  \"corpus_size\": " << corpus.size() << ",\n";
        std::cout << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
        std::cout << "  \"rows\": [\n";
        for (size_t i = 0; i < show; ++i) {
            if (i > 0) std::cout << ",\n";
            std::cout << "    {\"word\": " << jstr(entries[i].form)
                      << ", \"focus_freq\": " << entries[i].focus_freq
                      << ", \"ref_freq\": " << entries[i].ref_freq
                      << ", \"keyness\": " << std::fixed << std::setprecision(2) << entries[i].keyness
                      << ", \"effect\": " << jstr(entries[i].keyness >= 0 ? "+" : "-")
                      << "}";
        }
        std::cout << "\n  ]\n}}\n";
    } else {
        std::cout << attr << "\tfocus\tref\tkeyness\teffect\n";
        for (size_t i = 0; i < show; ++i) {
            std::cout << entries[i].form << "\t"
                      << entries[i].focus_freq << "\t"
                      << entries[i].ref_freq << "\t"
                      << std::fixed << std::setprecision(2) << entries[i].keyness << "\t"
                      << (entries[i].keyness >= 0 ? "+" : "-") << "\n";
        }
    }
}

// ── Query dispatch ──────────────────────────────────────────────────────

// If prog[si+1] is a single-token `label:dep_subtree(src)`, returns &src.
static const std::string* next_stmt_dep_subtree_source(const Program& prog, size_t si) {
    if (si + 1 >= prog.size()) return nullptr;
    const Statement& n = prog[si + 1];
    if (!n.has_query || n.query.tokens.size() != 1) return nullptr;
    if (!n.query.tokens[0].is_dep_subtree) return nullptr;
    return &n.query.tokens[0].dep_subtree_source;
}

static bool query_has_labeled_token(const TokenQuery& q, const std::string& label) {
    for (const auto& tok : q.tokens) {
        if (!tok.is_anchor() && tok.name == label) return true;
    }
    return false;
}

static void run_query(const Corpus& corpus, const std::string& input,
                      Options& opts, Session& session, QueryTiming* out_timing = nullptr) {
    Program prog;
    std::string d = opts.cql_dialect;
    for (char& c : d)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (d.empty())
        d = "native";
    ParserOptions parse_opts{opts.strict_quoted_strings};
    if (d == "native") {
        Parser parser(input, parse_opts);
        prog = parser.parse();
    } else if (d == "cwb") {
        std::string cwb_trace;
        prog = translate_cwb_program(input, opts.debug_level,
                                       opts.debug_level > 0 ? &cwb_trace : nullptr);
        if (opts.debug_level > 0 && !cwb_trace.empty())
            std::cerr << cwb_trace;
    } else if (d == "pmltq") {
        std::string pmltq_trace;
        prog = translate_pmltq_program(input, opts.debug_level,
                                       opts.debug_level > 0 ? &pmltq_trace : nullptr);
        if (opts.debug_level > 0 && !pmltq_trace.empty())
            std::cerr << pmltq_trace;
    } else if (d == "tiger") {
        std::string tiger_trace;
        prog = translate_tiger_program(input, opts.debug_level,
                                       opts.debug_level > 0 ? &tiger_trace : nullptr, parse_opts);
        if (opts.debug_level > 0 && !tiger_trace.empty())
            std::cerr << tiger_trace;
    } else {
        throw std::runtime_error("Unknown --cql dialect: " + opts.cql_dialect);
    }

    // Apply --max-gap: clamp unbounded repetition to user-configured cap
    if (opts.max_gap != REPEAT_UNBOUNDED) {
        for (auto& stmt : prog) {
            for (auto& tok : stmt.query.tokens) {
                if (tok.max_repeat == REPEAT_UNBOUNDED)
                    tok.max_repeat = opts.max_gap;
            }
            if (stmt.is_parallel) {
                for (auto& tok : stmt.target_query.tokens)
                    if (tok.max_repeat == REPEAT_UNBOUNDED)
                        tok.max_repeat = opts.max_gap;
            }
        }
    }

    QueryExecutor executor(corpus);
    executor.set_anchor_binding_mode(opts.anchor_binding == "innermost"
                                     ? QueryExecutor::AnchorBindingMode::Innermost
                                     : QueryExecutor::AnchorBindingMode::Fanout);
    executor.set_include_empty_alignment_values(opts.allow_empty_alignment);

    // In `--api` mode we must emit a single JSON object for the whole CQL program.
    // We therefore determine the last statement that would produce JSON output, and
    // suppress any earlier JSON-producing steps.
    size_t last_json_output_si = prog.size();  // sentinel: no output
    if (opts.api) {
        auto command_emits_json = [&](CommandType t) -> bool {
            switch (t) {
                case CommandType::COUNT:
                case CommandType::GROUP:
                case CommandType::FREQ:
                case CommandType::SIZE:
                case CommandType::TABULATE:
                case CommandType::DESCRIBE:
                case CommandType::STATS:
                case CommandType::RAW:
                case CommandType::COLL:
                case CommandType::DCOLL:
                case CommandType::KEYNESS:
                    return true;
                default:
                    return false;  // e.g. SORT emits KWIC text, not JSON
            }
        };

        for (size_t i = prog.size(); i-- > 0;) {
            const auto& st = prog[i];
            if (st.has_command) {
                if (command_emits_json(st.command.type)) {
                    last_json_output_si = i;
                    break;
                }
            } else if (st.has_query) {
                // For query (concordance) outputs, only the final query statement is printed
                // in multi-step programs unless --print-all-steps is enabled.
                const bool is_last_statement = (i + 1 == prog.size());
                if (is_last_statement) {
                    last_json_output_si = i;
                    break;
                }
            }
        }
    }

    for (size_t si = 0; si < prog.size(); ++si) {
        auto& stmt = prog[si];
        bool next_is_command = (si + 1 < prog.size() && prog[si + 1].has_command);

        if (stmt.has_query) {
            const std::string* dep_chain_src = next_stmt_dep_subtree_source(prog, si);
            const bool is_dep_subtree_prelude =
                    stmt.name.empty() && dep_chain_src
                    && query_has_labeled_token(stmt.query, *dep_chain_src);

            size_t max_m = 0;
            bool count_t = false;
            size_t max_total_cap = 0;
            if (opts.count_only) {
                max_m = 0;
                count_t = true;
            } else if (opts.sample > 0) {
                max_m = 0;
                count_t = true;  // need full count for reservoir sampling
            } else if (!next_is_command) {
                // Concordance page limit (offset+limit) applies to anonymous queries only.
                // Named binds (Q1 = …) are stored for later freq/count; truncating here makes
                // Q1 a tiny sample while Q2 before `freq` uses max_m=0 (full set).
                // Same for a dep_subtree chain prelude: `barenoun:[…]; barenp:dep_subtree(barenoun)…`
                // must materialize the full head match set for the next statement.
                if (!stmt.name.empty() || is_dep_subtree_prelude) {
                    max_m = 0;
                    count_t = opts.total;
                    max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;
                } else {
                    // Parallel queries fan out source hits into many aligned pairs.
                    // Pair-based pre-truncation skews pages toward the first source hit(s),
                    // so keep full pair sets and page by source-hit groups at render time.
                    max_m = stmt.is_parallel ? 0 : (opts.offset + opts.limit);
                    count_t = opts.total;
                    max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;
                }
            }

            const std::vector<std::string>* aggregate_by = nullptr;
            if (next_is_command && !stmt.is_parallel && opts.sample == 0 && !opts.count_only) {
                const GroupCommand& ncmd = prog[si + 1].command;
                if (!ncmd.fields.empty()
                    && (ncmd.type == CommandType::COUNT || ncmd.type == CommandType::GROUP
                        || ncmd.type == CommandType::FREQ)
                    && aggregate_command_targets_stmt(stmt, ncmd))
                    aggregate_by = &ncmd.fields;
            }

            // Resolve `<… > where MM, NN` references against the named match
            // sets the session has accumulated. Each ref becomes a sorted vector
            // of token positions; the executor's Phase B group handler ANDs them.
            for (auto& tok : stmt.query.tokens) {
                if (tok.where_refs.empty()) continue;
                tok.where_positions.clear();
                tok.where_positions.reserve(tok.where_refs.size());
                for (const auto& ref : tok.where_refs) {
                    auto it = session.named_results.find(ref);
                    if (it == session.named_results.end())
                        throw std::runtime_error("Unknown match set in `where`: '" + ref + "'");
                    std::vector<CorpusPos> ps;
                    for (const auto& m : it->second.matches) {
                        auto mp = m.matched_positions();
                        ps.insert(ps.end(), mp.begin(), mp.end());
                    }
                    std::sort(ps.begin(), ps.end());
                    ps.erase(std::unique(ps.begin(), ps.end()), ps.end());
                    tok.where_positions.push_back(std::move(ps));
                }
            }

            // Refresh anchor-binding mode in case `set anchor-binding = …` was used.
            executor.set_anchor_binding_mode(opts.anchor_binding == "innermost"
                                             ? QueryExecutor::AnchorBindingMode::Innermost
                                             : QueryExecutor::AnchorBindingMode::Fanout);
            executor.set_include_empty_alignment_values(opts.allow_empty_alignment);
            auto t0 = std::chrono::high_resolution_clock::now();
            if (stmt.is_parallel) {
                session.last_ms = executor.execute_parallel(stmt.query, stmt.target_query, max_m, count_t);
            } else if (stmt.has_query && stmt.query.tokens.size() == 1
                       && stmt.query.tokens[0].is_dep_subtree) {
                const std::string& src_q = stmt.query.tokens[0].dep_subtree_source;
                // Prefer a session named query (`src = [...];`). Otherwise resolve `src` as a
                // *token label* against the previous query's match + name map (`Last`), not as a
                // stored result alias.
                const MatchSet* src_ms = nullptr;
                const NameIndexMap* src_nm = nullptr;
                auto named_it = session.named_results.find(src_q);
                if (named_it != session.named_results.end()) {
                    src_ms = &named_it->second;
                    auto nm_it = session.named_name_maps.find(src_q);
                    if (nm_it != session.named_name_maps.end()) src_nm = &nm_it->second;
                } else if (session.has_last && session.last_name_map.count(src_q)) {
                    src_ms = &session.last_ms;
                    src_nm = &session.last_name_map;
                }
                if (!src_ms) {
                    throw std::runtime_error(
                            "dep_subtree: no matches for token label '" + src_q + "'. Use a prior "
                            "query that labels that token (e.g. `" + src_q + ":[...]; ...`), or "
                            "`" + src_q + " = [...];`, or one query: `" + src_q + ":[...] "
                            "sub:dep_subtree(" + src_q + ") [:: ...]`.");
                }
                const NameIndexMap& source_nm = src_nm ? *src_nm : NameIndexMap{};
                session.last_ms = executor.execute_dep_subtree_from_named(
                        stmt.query, *src_ms, source_nm, max_m, count_t, max_total_cap);
            } else {
                session.last_ms = executor.execute(stmt.query, max_m, count_t, max_total_cap,
                                          opts.sample, opts.sample_seed, opts.threads,
                                          aggregate_by);
            }
            session.has_last = true;
            session.last_name_map = stmt.is_parallel
                ? build_name_map(stmt.query)
                : QueryExecutor::build_name_map_for_stripped_query(stmt.query);
            session.last_target_name_map =
                    stmt.is_parallel ? build_name_map(stmt.target_query) : NameIndexMap{};

            // Always store as "Last" (CQP convention)
            session.named_results["Last"] = session.last_ms;
            session.named_name_maps["Last"] = session.last_name_map;
            session.named_target_name_maps["Last"] = session.last_target_name_map;

            // If this statement has a name, also store under that name
            if (!stmt.name.empty()) {
                session.named_results[stmt.name] = session.last_ms;
                session.named_name_maps[stmt.name] = session.last_name_map;
                session.named_target_name_maps[stmt.name] = session.last_target_name_map;
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            double query_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
            if (out_timing) {
                out_timing->query_sec += query_ms / 1000.0;
                out_timing->total = session.last_ms.total_count;
                out_timing->returned = session.last_ms.matches.size();
            }

            if (!next_is_command) {
                if (opts.count_only) {
                    std::cout << session.last_ms.total_count << "\n";
                    return;
                }
                // `Name = …` binds a match set only; do not print concordance for that
                // statement (CQP-style). Multi-statement programs: show KWIC/JSON only for the
                // final query step (intermediate steps feed `Last` / chaining). Use
                // `--print-all-steps` to print every query step (e.g. while debugging pipelines).
                const bool is_last_statement = (si + 1 == prog.size());
                const bool is_named_assignment = !stmt.name.empty();
                const bool emit_query_concordance =
                        (!is_named_assignment || (opts.api && is_last_statement))
                        && (is_last_statement || opts.print_all_program_steps);
                const bool should_emit_output = !opts.api || si == last_json_output_si;
                if (emit_query_concordance && should_emit_output) {
                    if (out_timing) {
                        auto t2 = std::chrono::high_resolution_clock::now();
                        if (opts.json)
                            emit_json(corpus, input, session.last_ms, opts, query_ms);
                        else
                            emit_hits_text(corpus, input, session.last_ms, opts, query_ms);
                        auto t3 = std::chrono::high_resolution_clock::now();
                        out_timing->fetch_sec =
                            std::chrono::duration<double, std::milli>(t3 - t2).count() / 1000.0;
                    } else {
                        if (opts.json)
                            emit_json(corpus, input, session.last_ms, opts, query_ms);
                        else
                            emit_hits_text(corpus, input, session.last_ms, opts, query_ms);
                    }
                }
            }
        }

        if (stmt.has_command) {
            // Commands that don't need a MatchSet
            if (stmt.command.type == CommandType::DROP) {
                if (stmt.command.query_name == "all") {
                    size_t n = session.named_results.size();
                    session.named_results.clear();
                    session.named_name_maps.clear();
                    session.named_target_name_maps.clear();
                    std::cout << "Dropped all " << n << " named queries\n";
                } else {
                    auto it = session.named_results.find(stmt.command.query_name);
                    if (it != session.named_results.end()) {
                        session.named_results.erase(it);
                        session.named_name_maps.erase(stmt.command.query_name);
                        session.named_target_name_maps.erase(stmt.command.query_name);
                        std::cout << "Dropped '" << stmt.command.query_name << "'\n";
                    } else {
                        std::cerr << "Named query '" << stmt.command.query_name << "' not found\n";
                    }
                }
                continue;
            }
            if (stmt.command.type == CommandType::SET) {
                const std::string& name = stmt.command.set_name;
                const std::string& val  = stmt.command.set_value;
                auto to_size = [&](size_t& target) {
                    try { target = std::stoull(val); }
                    catch (...) { std::cerr << "Error: invalid value for " << name << ": " << val << "\n"; }
                };
                auto to_int = [&](int& target) {
                    try { target = std::stoi(val); }
                    catch (...) { std::cerr << "Error: invalid value for " << name << ": " << val << "\n"; }
                };
                auto split_csv = [](const std::string& s) -> std::vector<std::string> {
                    std::vector<std::string> out;
                    std::string cur;
                    for (char c : s) {
                        if (c == ',' || c == ' ') {
                            if (!cur.empty()) { out.push_back(cur); cur.clear(); }
                        } else { cur += c; }
                    }
                    if (!cur.empty()) out.push_back(cur);
                    return out;
                };

                if (name == "limit")          to_size(opts.limit);
                else if (name == "offset")    to_size(opts.offset);
                else if (name == "context")   to_int(opts.context);
                else if (name == "left")      to_int(opts.coll_left);
                else if (name == "right")     to_int(opts.coll_right);
                else if (name == "window")    { to_int(opts.coll_left); opts.coll_right = opts.coll_left; opts.context = opts.coll_left; }
                else if (name == "max-total" || name == "max_total")  to_size(opts.max_total);
                else if (name == "max-items" || name == "max_items")  to_size(opts.coll_max_items);
                else if (name == "min-freq" || name == "min_freq")    to_size(opts.coll_min_freq);
                else if (name == "stoplist") to_size(opts.coll_stoplist);
                else if (name == "group-limit" || name == "group_limit") to_size(opts.group_limit);
                else if (name == "no-mv-explode" || name == "no_mv_explode")
                    opts.no_mv_explode = (val == "true" || val == "1" || val == "on");
                else if (name == "max-gap" || name == "max_gap")      to_int(opts.max_gap);
                else if (name == "measures")  opts.coll_measures = split_csv(val);
                else if (name == "attrs") {
                    if (val == "all" || val == "*" || val.empty())
                        opts.attrs.clear();
                    else
                        opts.attrs = split_csv(val);
                }
                else if (name == "total")     opts.total = (val == "true" || val == "1" || val == "on");
                else if (name == "timing")    opts.timing = (val == "true" || val == "1" || val == "on");
                else if (name == "json")      { opts.json = (val == "true" || val == "1" || val == "on"); }
                else if (name == "debug")     to_int(opts.debug_level);
                else if (name == "threads")   { int t; to_int(t); opts.threads = static_cast<unsigned>(t); }
                else if (name == "sample")    to_size(opts.sample);
                else if (name == "anchor-binding" || name == "anchor_binding") {
                    if (val != "fanout" && val != "innermost") {
                        std::cerr << "anchor-binding must be 'fanout' or 'innermost'\n";
                        continue;
                    }
                    opts.anchor_binding = val;
                }
                else if (name == "allow-empty-alignment" || name == "allow_empty_alignment")
                    opts.allow_empty_alignment = (val == "true" || val == "1" || val == "on");
                else {
                    std::cerr << "Unknown setting: " << name << "\n";
                    continue;
                }
                if (!opts.json) std::cout << name << " = " << val << "\n";
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_SETTINGS) {
                auto join = [](const std::vector<std::string>& v) {
                    std::string r;
                    for (size_t i = 0; i < v.size(); ++i) {
                        if (i > 0) r += ", ";
                        r += v[i];
                    }
                    return r.empty() ? "(all)" : r;
                };
                if (opts.json) {
                    std::cout << "{\"ok\": true, \"operation\": \"show_settings\", \"result\": {\n";
                    std::cout << "  \"limit\": " << opts.limit << ",\n";
                    std::cout << "  \"offset\": " << opts.offset << ",\n";
                    std::cout << "  \"context\": " << opts.context << ",\n";
                    std::cout << "  \"left\": " << opts.coll_left << ",\n";
                    std::cout << "  \"right\": " << opts.coll_right << ",\n";
                    std::cout << "  \"max_total\": " << opts.max_total << ",\n";
                    std::cout << "  \"max_items\": " << opts.coll_max_items << ",\n";
                    std::cout << "  \"min_freq\": " << opts.coll_min_freq << ",\n";
                    std::cout << "  \"stoplist\": " << opts.coll_stoplist << ",\n";
                    std::cout << "  \"group_limit\": " << opts.group_limit << ",\n";
                    std::cout << "  \"no_mv_explode\": " << (opts.no_mv_explode ? "true" : "false") << ",\n";
                    std::cout << "  \"max_gap\": " << opts.max_gap << ",\n";
                    std::cout << "  \"total\": " << (opts.total ? "true" : "false") << ",\n";
                    std::cout << "  \"timing\": " << (opts.timing ? "true" : "false") << ",\n";
                    std::cout << "  \"debug\": " << opts.debug_level << ",\n";
                    std::cout << "  \"threads\": " << opts.threads << ",\n";
                    std::cout << "  \"sample\": " << opts.sample << ",\n";
                    std::cout << "  \"allow_empty_alignment\": "
                              << (opts.allow_empty_alignment ? "true" : "false") << ",\n";
                    std::cout << "  \"measures\": " << jstr(join(opts.coll_measures.empty()
                        ? std::vector<std::string>{"logdice"} : opts.coll_measures)) << ",\n";
                    std::cout << "  \"attrs\": " << jstr(join(opts.attrs)) << "\n";
                    std::cout << "}}\n";
                } else {
                    std::cout << "limit       = " << opts.limit << "\n";
                    std::cout << "offset      = " << opts.offset << "\n";
                    std::cout << "context     = " << opts.context << "\n";
                    std::cout << "left        = " << opts.coll_left << "\n";
                    std::cout << "right       = " << opts.coll_right << "\n";
                    std::cout << "max-total   = " << opts.max_total << "\n";
                    std::cout << "max-items   = " << opts.coll_max_items << "\n";
                    std::cout << "min-freq    = " << opts.coll_min_freq << "\n";
                    std::cout << "stoplist    = " << opts.coll_stoplist << "\n";
                    std::cout << "group-limit = " << opts.group_limit << "\n";
                    std::cout << "no-mv-explode = " << (opts.no_mv_explode ? "on" : "off") << "\n";
                    std::cout << "max-gap     = " << opts.max_gap << "\n";
                    std::cout << "total       = " << (opts.total ? "on" : "off") << "\n";
                    std::cout << "timing      = " << (opts.timing ? "on" : "off") << "\n";
                    std::cout << "debug       = " << opts.debug_level << "\n";
                    std::cout << "threads     = " << opts.threads << "\n";
                    std::cout << "sample      = " << opts.sample << "\n";
                    std::cout << "allow-empty-alignment = "
                              << (opts.allow_empty_alignment ? "on" : "off") << "\n";
                    std::cout << "measures    = " << join(opts.coll_measures.empty()
                        ? std::vector<std::string>{"logdice"} : opts.coll_measures) << "\n";
                    std::cout << "attrs       = " << join(opts.attrs) << "\n";
                }
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_NAMED) {
                // Named queries
                if (session.named_results.empty()) {
                    std::cout << "No named queries\n";
                } else {
                    std::cout << "Named queries:\n";
                    for (const auto& [name, ms] : session.named_results) {
                        std::cout << "  " << name << ": " << ms.matches.size() << " matches";
                        auto nm_it = session.named_name_maps.find(name);
                        if (nm_it != session.named_name_maps.end() && !nm_it->second.empty()) {
                            std::cout << "  (tokens:";
                            for (const auto& [tname, idx] : nm_it->second)
                                std::cout << " " << tname << "[" << idx << "]";
                            std::cout << ")";
                        }
                        std::cout << "\n";
                    }
                }
                // Token names from last query
                if (!session.last_name_map.empty()) {
                    std::cout << "Last query token names:\n";
                    for (const auto& [tname, idx] : session.last_name_map)
                        std::cout << "  " << tname << ": position " << idx << "\n";
                }
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_ATTRS) {
                std::cout << "Positional attributes:\n";
                for (const auto& a : corpus.attr_names())
                    std::cout << "  " << a << "\n";
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_REGIONS) {
                const std::string& type_name = stmt.command.query_name;
                if (!type_name.empty()) {
                    // "show regions <type>" — list individual regions with their attributes
                    if (!corpus.has_structure(type_name)) {
                        std::cerr << "Unknown structure type: " << type_name << "\n";
                        continue;
                    }
                    const auto& sa = corpus.structure(type_name);
                    const auto& ra_names = sa.region_attr_names();
                    size_t n = sa.region_count();
                    size_t limit = std::min(n, opts.group_limit > 0 ? opts.group_limit : n);
                    if (opts.json || opts.api) {
                        std::cout << "{\n  \"ok\": true,\n  \"operation\": \"show_regions\",\n";
                        std::cout << "  \"result\": {\n";
                        std::cout << "    \"type\": " << jstr(type_name) << ",\n";
                        std::cout << "    \"total\": " << n << ",\n";
                        std::cout << "    \"returned\": " << limit << ",\n";
                        std::cout << "    \"regions\": [\n";
                        for (size_t i = 0; i < limit; ++i) {
                            Region rgn = sa.get(i);
                            if (i > 0) std::cout << ",\n";
                            std::cout << "      {\"index\": " << i
                                      << ", \"start\": " << rgn.start
                                      << ", \"end\": " << rgn.end
                                      << ", \"tokens\": " << (rgn.end - rgn.start + 1);
                            for (const auto& attr : ra_names) {
                                std::string_view v = sa.region_value(attr, i);
                                std::cout << ", " << jstr(type_name + "_" + attr) << ": " << jstr(std::string(v));
                            }
                            std::cout << "}";
                        }
                        std::cout << "\n    ]\n  }\n}\n";
                    } else {
                        std::cout << "Regions of type '" << type_name << "' (" << n << " total):\n";
                        // Print header
                        std::cout << std::right << std::setw(6) << "#"
                                  << std::setw(10) << "start"
                                  << std::setw(10) << "end"
                                  << std::setw(8) << "tokens";
                        for (const auto& attr : ra_names)
                            std::cout << "  " << std::left << std::setw(20) << (type_name + "_" + attr);
                        std::cout << "\n";
                        for (size_t i = 0; i < limit; ++i) {
                            Region rgn = sa.get(i);
                            std::cout << std::right << std::setw(6) << i
                                      << std::setw(10) << rgn.start
                                      << std::setw(10) << rgn.end
                                      << std::setw(8) << (rgn.end - rgn.start + 1);
                            for (const auto& attr : ra_names) {
                                std::string_view v = sa.region_value(attr, i);
                                std::cout << "  " << std::left << std::setw(20) << v;
                            }
                            std::cout << "\n";
                        }
                        if (limit < n)
                            std::cout << "  ... (" << (n - limit) << " more, use --group-limit to show more)\n";
                    }
                } else if (opts.json) {
                    std::cout << "{\n  \"ok\": true,\n  \"operation\": \"show_regions\",\n";
                    std::cout << "  \"result\": {\n";
                    std::cout << "    \"structures\": [";
                    const auto& s_names = corpus.structure_names();
                    for (size_t i = 0; i < s_names.size(); ++i) {
                        const auto& s = s_names[i];
                        if (i > 0) std::cout << ", ";
                        const auto& sa = corpus.structure(s);
                        std::cout << "{"
                                  << "\"name\": " << jstr(s) << ", "
                                  << "\"regions\": " << sa.region_count() << ", "
                                  << "\"has_values\": " << (sa.has_values() ? "true" : "false") << ", "
                                  << "\"attrs\": [";
                        const auto& ra = sa.region_attr_names();
                        for (size_t j = 0; j < ra.size(); ++j) {
                            if (j > 0) std::cout << ", ";
                            std::cout << jstr(ra[j]);
                        }
                        std::cout << "]}";
                    }
                    std::cout << "],\n";
                    std::cout << "    \"region_attrs\": [";
                    const auto& ra_all = corpus.region_attr_names();
                    for (size_t i = 0; i < ra_all.size(); ++i) {
                        if (i > 0) std::cout << ", ";
                        std::cout << jstr(ra_all[i]);
                    }
                    std::cout << "]\n  }\n}\n";
                } else {
                    std::cout << "Structural attributes:\n";
                    const auto& s_names = corpus.structure_names();
                    for (const auto& s : s_names) {
                        const auto& sa = corpus.structure(s);
                        std::cout << "  " << s << " (regions=" << sa.region_count()
                                  << ", has_values=" << (sa.has_values() ? "yes" : "no") << ")\n";
                        const auto& ra = sa.region_attr_names();
                        if (!ra.empty()) {
                            std::cout << "    attrs:";
                            for (const auto& a : ra)
                                std::cout << " " << a;
                            std::cout << "\n";
                        }
                    }
                }
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_VALUES) {
                const std::string& attr_name = stmt.command.query_name;

                // Check positional attributes first (pipe-split MV: RG-5f / multivalue_eq)
                if (corpus.has_attr(attr_name)) {
                    const auto& pa = corpus.attr(attr_name);
                    bool is_mv = corpus.is_multivalue(attr_name);
                    std::vector<std::pair<std::string, size_t>> entries =
                        positional_attr_show_values_mv(pa, is_mv);

                    size_t limit = std::min(entries.size(), opts.group_limit > 0 ? opts.group_limit : entries.size());

                    if (opts.json || opts.api) {
                        std::cout << "{\n  \"ok\": true,\n  \"operation\": \"show_values\",\n";
                        std::cout << "  \"result\": {\n";
                        std::cout << "    \"attr\": " << jstr(attr_name) << ",\n";
                        std::cout << "    \"type\": \"positional\",\n";
                        if (is_mv) std::cout << "    \"multivalue\": true,\n";
                        std::cout << "    \"unique\": " << entries.size() << ",\n";
                        std::cout << "    \"returned\": " << limit << ",\n";
                        std::cout << "    \"values\": [\n";
                        for (size_t i = 0; i < limit; ++i) {
                            if (i > 0) std::cout << ",\n";
                            std::cout << "      {\"value\": " << jstr(entries[i].first)
                                      << ", \"count\": " << entries[i].second << "}";
                        }
                        std::cout << "\n    ]\n  }\n}\n";
                    } else {
                        std::cout << "Values for '" << attr_name << "' (positional, "
                                  << entries.size() << " unique):\n";
                        for (size_t i = 0; i < limit; ++i) {
                            std::cout << std::right << std::setw(10) << entries[i].second
                                      << "  " << entries[i].first << "\n";
                        }
                        if (limit < entries.size())
                            std::cout << "  ... (" << (entries.size() - limit) << " more, use --group-limit to show more)\n";
                    }
                    continue;
                }

                // Check region attributes: split text_genre → struct "text", attr "genre"
                auto us = attr_name.find('_');
                if (us != std::string::npos) {
                    std::string struct_name = attr_name.substr(0, us);
                    std::string region_attr = attr_name.substr(us + 1);
                    if (corpus.has_structure(struct_name)) {
                        const auto& sa = corpus.structure(struct_name);
                        auto rkey = resolve_region_attr_key(sa, struct_name, region_attr);
                        if (rkey) {
                            bool is_mv = corpus.is_multivalue(attr_name);
                            std::vector<std::pair<std::string, size_t>> entries =
                                region_attr_show_values_mv(sa, *rkey, is_mv);

                            size_t limit = std::min(entries.size(), opts.group_limit > 0 ? opts.group_limit : entries.size());

                            if (opts.json || opts.api) {
                                std::cout << "{\n  \"ok\": true,\n  \"operation\": \"show_values\",\n";
                                std::cout << "  \"result\": {\n";
                                std::cout << "    \"attr\": " << jstr(attr_name) << ",\n";
                                std::cout << "    \"type\": \"region\",\n";
                                std::cout << "    \"structure\": " << jstr(struct_name) << ",\n";
                                std::cout << "    \"region_attr\": " << jstr(region_attr) << ",\n";
                                std::cout << "    \"unique\": " << entries.size() << ",\n";
                                std::cout << "    \"returned\": " << limit << ",\n";
                                std::cout << "    \"values\": [\n";
                                for (size_t i = 0; i < limit; ++i) {
                                    if (i > 0) std::cout << ",\n";
                                    std::cout << "      {\"value\": " << jstr(entries[i].first)
                                              << ", \"count\": " << entries[i].second << "}";
                                }
                                std::cout << "\n    ]\n  }\n}\n";
                            } else {
                                std::cout << "Values for '" << attr_name << "' (region attr on '"
                                          << struct_name << "', " << entries.size() << " unique):\n";
                                for (size_t i = 0; i < limit; ++i) {
                                    std::cout << std::right << std::setw(10) << entries[i].second
                                              << "  " << entries[i].first << "\n";
                                }
                                if (limit < entries.size())
                                    std::cout << "  ... (" << (entries.size() - limit) << " more, use --group-limit to show more)\n";
                            }
                            continue;
                        }
                    }
                }

                std::cerr << "Unknown attribute: " << attr_name << "\n";
                continue;
            }

            // Size without prior query: show corpus info
            if (stmt.command.type == CommandType::SIZE && stmt.command.query_name.empty() && !session.has_last) {
                if (opts.json)
                    emit_info_json(corpus);
                else
                    std::cout << "Corpus size: " << corpus.size() << " tokens\n";
                continue;
            }
            if (stmt.command.type == CommandType::SHOW_INFO) {
                // Extract corpus name from directory path
                std::string name = corpus.dir();
                if (!name.empty() && name.back() == '/') name.pop_back();
                auto slash = name.rfind('/');
                if (slash != std::string::npos) name = name.substr(slash + 1);
                // Strip _idx suffix if present
                if (name.size() > 4 && name.substr(name.size() - 4) == "_idx") name = name.substr(0, name.size() - 4);

                if (opts.json) {
                    std::cout << to_info_json(corpus, "show_info");
                } else {
                    std::cout << "Corpus: " << name << "\n";
                    std::cout << "Path:   " << corpus.dir() << "\n";
                    std::cout << "Tokens: " << corpus.size() << "\n";
                    std::cout << "Dependencies: " << (corpus.has_deps() ? "yes" : "no") << "\n";
                    std::cout << "Positional attributes:\n";
                    for (const auto& a : corpus.attr_names())
                        std::cout << "  " << a << " (vocab=" << corpus.attr(a).lexicon().size() << ")\n";
                    const auto& s_names = corpus.structure_names();
                    if (!s_names.empty()) {
                        std::cout << "Structures:\n";
                        for (const auto& s : s_names) {
                            const auto& sa = corpus.structure(s);
                            std::cout << "  " << s << " (" << sa.region_count() << " regions)";
                            const auto& ra = sa.region_attr_names();
                            if (!ra.empty()) {
                                std::cout << " attrs:";
                                for (const auto& a : ra) std::cout << " " << a;
                            }
                            if (corpus.is_nested(s))     std::cout << " [nested]";
                            if (corpus.is_overlapping(s)) std::cout << " [overlapping]";
                            if (corpus.is_zerowidth(s))   std::cout << " [zerowidth]";
                            std::cout << "\n";
                        }
                    }
                }
                continue;
            }

            // Commands that need a MatchSet
            MatchSet* ms_to_use = nullptr;
            const NameIndexMap* nm_to_use = nullptr;
            const bool should_emit_output = !opts.api || si == last_json_output_si;
            GroupCommand cmd_to_run = stmt.command;

            if (cmd_to_run.type == CommandType::FREQ && cmd_to_run.freq_query_names.size() >= 2) {
                if (should_emit_output) emit_freq_compare(corpus, session, cmd_to_run, opts);
                continue;
            }
            if (cmd_to_run.type == CommandType::FREQ && cmd_to_run.freq_query_names.size() == 1) {
                MatchSet* ms = nullptr;
                NameIndexMap* nm = nullptr;
                if (!session_lookup_ms(session, cmd_to_run.freq_query_names[0], ms, nm)) {
                    if (opts.json)
                        std::cout << "{\"ok\": false, \"error\": "
                                  << jstr("Unknown named query: " + cmd_to_run.freq_query_names[0])
                                  << "}\n";
                    else
                        std::cerr << "Error: unknown named query '" << cmd_to_run.freq_query_names[0]
                                  << "'\n";
                    continue;
                }
                ms_to_use = ms;
                nm_to_use = nm;
            } else if (!cmd_to_run.query_name.empty()) {
                auto it = session.named_results.find(cmd_to_run.query_name);
                if (it != session.named_results.end()) {
                    ms_to_use = &it->second;
                    auto nm_it = session.named_name_maps.find(cmd_to_run.query_name);
                    nm_to_use = (nm_it != session.named_name_maps.end()) ? &nm_it->second : &session.last_name_map;
                } else {
                    // dcoll ambiguity fallback: parse-time may interpret
                    // `dcoll amod by lemma` as query_name="amod".
                    if (cmd_to_run.type == CommandType::DCOLL && cmd_to_run.relations.empty()) {
                        cmd_to_run.relations.push_back(cmd_to_run.query_name);
                        cmd_to_run.query_name.clear();
                    } else {
                        std::cerr << "Warning: named query '" << cmd_to_run.query_name
                                  << "' not found, using last result\n";
                    }
                    if (!session.has_last) { std::cerr << "No query to operate on\n"; continue; }
                    ms_to_use = &session.last_ms;
                    nm_to_use = &session.last_name_map;
                }
            } else {
                if (!session.has_last) { std::cerr << "No query to operate on\n"; continue; }
                ms_to_use = &session.last_ms;
                nm_to_use = &session.last_name_map;
            }

            const NameIndexMap* nm_tgt_parallel = nullptr;
            if (!ms_to_use->parallel_matches.empty()) {
                if (!cmd_to_run.query_name.empty()) {
                    auto tit = session.named_target_name_maps.find(cmd_to_run.query_name);
                    nm_tgt_parallel = (tit != session.named_target_name_maps.end()) ? &tit->second
                                                                                    : nullptr;
                } else {
                    nm_tgt_parallel = &session.last_target_name_map;
                }
            }

            switch (cmd_to_run.type) {
                case CommandType::COUNT:
                case CommandType::GROUP:
                    if (should_emit_output) emit_count(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use);
                    break;

                case CommandType::SORT:
                    // SORT mutates the ordering of ms_to_use, so we always apply it.
                    // In `--api` mode, we suppress KWIC text output unless this is the last JSON step.
                    if (cmd_to_run.fields.empty()) {
                        std::cerr << "Error: sort requires 'by' clause\n";
                        return;
                    }
                    std::sort(ms_to_use->matches.begin(), ms_to_use->matches.end(),
                              [&](const Match& a, const Match& b) {
                                  return make_key(corpus, a, *nm_to_use, cmd_to_run.fields)
                                       < make_key(corpus, b, *nm_to_use, cmd_to_run.fields);
                              });
                    if (should_emit_output) emit_hits_text(corpus, "(sorted)", *ms_to_use, opts, 0);
                    break;

                case CommandType::SIZE:
                    if (should_emit_output) emit_size(*ms_to_use, opts);
                    break;

                case CommandType::TABULATE:
                    if (should_emit_output) {
                        emit_tabulate(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use);
                    }
                    break;
                case CommandType::DESCRIBE:
                    if (should_emit_output) {
                        emit_describe(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use);
                    }
                    break;

                case CommandType::STATS:
                    if (should_emit_output) {
                        emit_stats(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use);
                    }
                    break;

                case CommandType::FREQ:
                    if (should_emit_output) {
                        emit_freq(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use);
                    }
                    break;

                case CommandType::RAW:
                    if (should_emit_output) emit_raw(corpus, *ms_to_use, opts);
                    break;

                case CommandType::COLL:
                    if (should_emit_output)
                        emit_coll(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use, nm_tgt_parallel);
                    break;

                case CommandType::DCOLL:
                    if (should_emit_output)
                        emit_dcoll(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use, nm_tgt_parallel);
                    break;

                case CommandType::KEYNESS: {
                    const MatchSet* ref_ms = nullptr;
                    if (!cmd_to_run.ref_query_name.empty()) {
                        auto rit = session.named_results.find(cmd_to_run.ref_query_name);
                        if (rit == session.named_results.end()) {
                            if (opts.json || opts.api)
                                std::cout << "{\"ok\": false, \"error\": {\"stage\": \"keyness\", \"message\": "
                                          << jstr("Unknown reference query: " + cmd_to_run.ref_query_name) << "}}\n";
                            else
                                std::cerr << "Error: unknown reference query '" << cmd_to_run.ref_query_name << "'\n";
                            break;
                        }
                        ref_ms = &rit->second;
                    }
                    if (should_emit_output) emit_keyness(corpus, *ms_to_use, cmd_to_run, opts, *nm_to_use, ref_ms);
                    break;
                }

                default:
                    std::cout << "(command not yet implemented)\n";
                    break;
            }
        }
    }
}

/// Persistent path for REPL history (~/.pando_history). Empty if $HOME is unset.
static std::string repl_history_path() {
    const char* h = std::getenv("HOME");
    if (!h || !*h) return {};
    return std::string(h) + "/.pando_history";
}

static std::string repl_trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && (s[a] == ' ' || s[a] == '\t' || s[a] == '\r')) ++a;
    while (b > a && (s[b - 1] == ' ' || s[b - 1] == '\t' || s[b - 1] == '\r')) --b;
    return s.substr(a, b - a);
}

static bool repl_line_is_help(const std::string& line) {
    std::string t = repl_trim(line);
    for (char& c : t)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return t == "help";
}

/// Print REPL-only commands and the main CQL task verbs (not full CLI --help).
static void print_interactive_repl_help(const Options& opts) {
    std::cout
        << "REPL (read one CQL program per line; session keeps named queries).\n"
        << "  Front-end: --cql " << opts.cql_dialect << "\n\n"
        << "  help              This summary\n"
        << "  quit, exit        Leave the REPL\n\n"
        << "  Query / concordance — a token pattern, or a named bind, then optional commands:\n"
        << "    [lemma=\"run\"]+\n"
        << "    Nouns = [pos=\"N.*\"]+\n\n"
        << "  Aggregation / listing:\n"
        << "    count … | group … | sort … | freq … | size\n\n"
        << "  Collocation / comparison:\n"
        << "    coll | dcoll | keyness …\n\n"
        << "  Tables / dumps:\n"
        << "    tabulate … | describe … | raw | cat\n\n"
        << "  Corpus / session:\n"
        << "    show attributes | show regions | show named | show info | show values <attr>\n"
        << "    show settings | set <name> <value> | drop <name> | drop all\n\n"
        << "  Output format and limits follow the flags you passed on the pando command line\n"
        << "  (e.g. --json, --limit, --context). Run `pando` with no args for full CLI help.\n";
}

/// Read CQL from stdin; with a TTY, use linenoise when available (arrows + history).
static void run_interactive_repl(const Corpus& corpus, Options& opts, Session& session) {
    const bool stdin_tty = isatty(STDIN_FILENO);
#ifdef PANDO_HAVE_LINENOISE
    if (stdin_tty && !opts.json && !opts.api) {
        std::string hist = repl_history_path();
        linenoiseHistorySetMaxLen(1000);
        linenoiseSetMultiLine(1);
        if (!hist.empty())
            linenoiseHistoryLoad(hist.c_str());
        char* raw = nullptr;
        while ((raw = linenoise("pando> ")) != nullptr) {
            std::string line = raw;
            linenoiseFree(raw);
            if (line.empty() || line == "quit" || line == "exit") break;
            if (repl_line_is_help(line)) {
                print_interactive_repl_help(opts);
                continue;
            }
            linenoiseHistoryAdd(line.c_str());
            try {
                run_query(corpus, line, opts, session);
            } catch (const std::exception& e) {
                if (opts.json || opts.api) {
                    std::cout << "{\"ok\": false, \"error\": {\"stage\": \"query\", \"message\": "
                              << jstr(e.what()) << "}}\n";
                } else {
                    std::cerr << "Error: " << e.what() << "\n";
                }
            }
        }
        if (!hist.empty())
            linenoiseHistorySave(hist.c_str());
        return;
    }
#endif
    std::string line;
    if (stdin_tty && !opts.json && !opts.api)
        std::cout << "pando> " << std::flush;
    while (std::getline(std::cin, line)) {
        if (line.empty() || line == "quit" || line == "exit") break;
        if (repl_line_is_help(line)) {
            print_interactive_repl_help(opts);
            continue;
        }
        try {
            run_query(corpus, line, opts, session);
        } catch (const std::exception& e) {
            if (opts.json || opts.api) {
                std::cout << "{\"ok\": false, \"error\": {\"stage\": \"query\", \"message\": "
                          << jstr(e.what()) << "}}\n";
            } else {
                std::cerr << "Error: " << e.what() << "\n";
            }
        }
        if (stdin_tty && !opts.json && !opts.api)
            std::cout << "pando> " << std::flush;
    }
}

// ── Argument parsing ────────────────────────────────────────────────────

static Options parse_args(int argc, char* argv[]) {
    Options opts;
    std::vector<std::string> positional;
    bool request_interactive_repl = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--json")        { opts.json = true; }
        else if (arg == "--conllu") { opts.conllu = true; }
        else if (arg == "--api")    { opts.api = true; opts.json = true; }
        else if (arg == "--format" && i + 1 < argc) {
            std::string f = argv[++i];
            for (char& c : f)
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            if (f == "conllu")
                opts.conllu = true;
            else if (f == "json")
                opts.json = true;
            else {
                std::cerr << "Unknown --format value: " << f << " (expected json or conllu)\n";
                std::exit(1);
            }
        }
        else if (arg.rfind("--format=", 0) == 0) {
            std::string f = arg.substr(9);
            for (char& c : f)
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            if (f == "conllu")
                opts.conllu = true;
            else if (f == "json")
                opts.json = true;
            else {
                std::cerr << "Unknown --format value: " << f << " (expected json or conllu)\n";
                std::exit(1);
            }
        }
        // --debug  (optional level), e.g. --debug, --debug 2, or --debug=2
        else if (arg == "--debug")  {
            // bare --debug → level 1 (unless already higher)
            if (opts.debug_level < 1) opts.debug_level = 1;
            // If next arg looks like a number, treat it as level
            if (i + 1 < argc && argv[i + 1][0] >= '0' && argv[i + 1][0] <= '9') {
                try {
                    opts.debug_level = std::stoi(argv[++i]);
                } catch (...) {
                    // ignore parse error, keep level 1
                }
            }
        }
        else if (arg.rfind("--debug=", 0) == 0) {
            std::string lvl = arg.substr(8);
            try {
                opts.debug_level = std::stoi(lvl);
            } catch (...) {
                opts.debug_level = 1;
            }
        }
        else if (arg == "--total")  { opts.total = true; }
        else if (arg == "--max-total" && i + 1 < argc) { opts.max_total = std::stoul(argv[++i]); }
        else if (arg == "--limit"  && i + 1 < argc) { opts.limit  = std::stoul(argv[++i]); }
        else if (arg == "--offset" && i + 1 < argc) { opts.offset = std::stoul(argv[++i]); }
        else if (arg == "--context" && i + 1 < argc) { opts.context = std::stoi(argv[++i]); }
        else if (arg == "--count-only") { opts.count_only = true; }
        else if (arg == "--interactive") { request_interactive_repl = true; }
        else if (arg == "--print-all-steps") { opts.print_all_program_steps = true; }
        else if (arg == "--timing")     { opts.timing = true; }
        else if (arg == "--sample" && i + 1 < argc) { opts.sample = std::stoul(argv[++i]); }
        else if (arg == "--seed" && i + 1 < argc)  { opts.sample_seed = static_cast<uint32_t>(std::stoul(argv[++i])); }
        else if (arg == "--threads" && i + 1 < argc) { opts.threads = static_cast<unsigned>(std::stoul(argv[++i])); }
        else if (arg == "--cql" && i + 1 < argc) {
            opts.cql_dialect = argv[++i];
        }
        else if (arg == "--pmltq-export-sql") {
            opts.pmltq_export_sql = true;
        }
        else if (arg == "--version" || arg == "-V") {
            opts.print_version = true;
        }
                else if (arg == "--overlay" && i + 1 < argc) { opts.overlay_dirs.push_back(argv[++i]); }
        else if (arg == "--preload") { opts.preload = true; }
        else if (arg == "--no-mv-explode") { opts.no_mv_explode = true; }
        else if (arg == "--anchor-binding" && i + 1 < argc) {
            std::string v = argv[++i];
            if (v != "fanout" && v != "innermost") {
                throw std::runtime_error("--anchor-binding must be 'fanout' or 'innermost'");
            }
            opts.anchor_binding = std::move(v);
        }
        else if (arg == "--allow-empty-alignment") { opts.allow_empty_alignment = true; }
        else if (arg == "--max-gap" && i + 1 < argc) { opts.max_gap = std::stoi(argv[++i]); }
        else if (arg == "--strict-quoted-strings") { opts.strict_quoted_strings = true; }
        else if (arg == "--window" && i + 1 < argc) {
            int w = std::stoi(argv[++i]);
            opts.coll_left = w; opts.coll_right = w;
        }
        else if (arg == "--left"  && i + 1 < argc) { opts.coll_left  = std::stoi(argv[++i]); }
        else if (arg == "--right" && i + 1 < argc) { opts.coll_right = std::stoi(argv[++i]); }
        else if (arg == "--min-freq" && i + 1 < argc) { opts.coll_min_freq = std::stoul(argv[++i]); }
        else if (arg == "--max-items" && i + 1 < argc) { opts.coll_max_items = std::stoul(argv[++i]); }
        else if (arg == "--stoplist" && i + 1 < argc) { opts.coll_stoplist = std::stoul(argv[++i]); }
        else if (arg == "--measures" && i + 1 < argc) {
            std::string list = argv[++i];
            opts.coll_measures.clear();
            for (size_t pos = 0; ; ) {
                size_t comma = list.find(',', pos);
                std::string part = list.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                while (!part.empty() && part.front() == ' ') part.erase(0, 1);
                while (!part.empty() && part.back() == ' ') part.erase(part.size() - 1, 1);
                if (!part.empty()) opts.coll_measures.push_back(part);
                if (comma == std::string::npos) break;
                pos = comma + 1;
            }
        }
        else if (arg == "--attrs" && i + 1 < argc) {
            std::string list = argv[++i];
            opts.attrs.clear();
            for (size_t pos = 0; ; ) {
                size_t comma = list.find(',', pos);
                std::string part = list.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                while (!part.empty() && part.front() == ' ') part.erase(0, 1);
                while (!part.empty() && part.back() == ' ') part.erase(part.size() - 1, 1);
                if (!part.empty()) opts.attrs.push_back(part);
                if (comma == std::string::npos) break;
                pos = comma + 1;
            }
        }
        else if (arg.substr(0, 2) == "--") {
            std::cerr << "Unknown option: " << arg << "\n";
        } else {
            positional.push_back(arg);
        }
    }

    if (opts.print_version) {
        std::cout << "pando " << PANDO_VERSION << "\n";
        std::exit(0);
    }

    if (positional.empty()) {
        std::cerr << "Usage: pando [options] <corpus_dir> [query]\n\n"
                  << "If the first argument is not a corpus directory but ./pando/ contains corpus.info "
                     "(TEITOK-style layout), the corpus defaults to ./pando and all arguments are the query. "
                     "If the current directory is already a corpus (./corpus.info), it is used the same way.\n\n"
                  << "If [query] is omitted, CQL is read from stdin. With a terminal (stdin is a TTY), "
                     "an interactive REPL runs with a pando> prompt; with a pipe, queries are read "
                     "line by line with no prompt. Use --interactive to open the same REPL even when "
                     "a query string is present on the command line (it is then ignored).\n\n"
                  << "Shell quoting: CQL string literals often use double quotes (e.g. form=\"can't\"). "
                     "Wrap the whole query in double quotes and backslash-escape those inner quotes:\n"
                  << "  ./pando CORP \"[form=\\\"can't\\\" | contr_form=\\\"can't\\\"]+\"\n"
                  << "Avoid nesting single quotes like '\"can''t\"' — the shell drops the apostrophe.\n\n"
                  << "Options:\n"
                  << "  --version, -V    Print version and exit\n"
                  << "  --json           Output as JSON (human-/tool-friendly)\n"
                  << "  --conllu         Text hits: full sentence per match as CoNLL-U (needs sentence structure s)\n"
                  << "  --format json|conllu  Same as --json / --conllu\n"
                  << "  --api            API mode: JSON only, single-object responses\n"
                  << "  --debug[=N]      Include debug info (plan, timing, cardinalities); N>=1 for verbosity\n"
#if defined(PANDO_WITH_CWB_DIALECT)
                  << "  --cql native|cwb|pmltq|tiger  Query language front-end (default: native; "
                     "cwb=CWB/CQP subset, pmltq=PML-TQ, tiger=TIGERSearch-style macros for constituency)\n"
#else
                  << "  --cql native|pmltq|tiger  Query language front-end (default: native; pmltq=PML-TQ, "
                     "tiger=TIGERSearch-style macros; cwb: -DPANDO_CWB_DIALECT=ON)\n"
#endif
                  << "  --pmltq-export-sql  With --cql pmltq: print ClickPMLTQ SQL only (needs PMLTQ_GOLD_JS_DIR + "
                     "pmltq2sql-optimized.js); skips corpus load; use corpus path '-' or any placeholder\n"
                  << "  --total          Compute exact total match count even with --limit\n"
                  << "  --max-total N    Cap total count at N when using --total (fast UI total)\n"
                  << "  --limit N        Max hits to return (default: 20)\n"
                  << "  --offset N       Skip first N hits (default: 0)\n"
                  << "  --context N      Context width in tokens for JSON output (default: 5)\n"
                  << "  --attrs A,B,...  Token attributes to show (text: appended with /; JSON: in token objects; default: form only in text, all in JSON)\n"
                  << "  --count-only     Print only total match count (for benchmarking)\n"
                  << "  --interactive    Open stdin REPL (`pando>`): type one CQL command per line (session persists).\n"
                  << "                    On a TTY, line editing uses linenoise (arrows, history); history file: ~/.pando_history\n"
                  << "  --print-all-steps  Multi-statement CQL: print hits for each query step, not only the last\n"
                  << "  --timing         Print open_sec, query_sec, fetch_sec, total, returned to stderr\n"
                  << "  --sample N       Return N randomly sampled matches (reservoir sampling)\n"
                  << "  --seed N         RNG seed for --sample (reproducible runs)\n"
                  << "  --threads N      Parallel seed processing for multi-token queries (default: 1)\n"
                  << "  --overlay DIR    Merge stand-off overlay index (repeatable); attrs are overlay-<layer>-…\n"
                  << "  --allow-empty-alignment  Alignment join (`:: a.attr=b.attr`) may match empty/_ values (legacy)\n"
                  << "  --max-gap N      Cap for + and * quantifiers (default: " << REPEAT_UNBOUNDED << ")\n"
                  << "  --strict-quoted-strings  Only /pattern/ is regex; quoted strings are always literal\n"
                  << "  --no-mv-explode  count/freq: keep pipe-joined multivalue keys (no per-component buckets)\n"
                  << "\nCollocation options (coll/dcoll commands):\n"
                  << "  --window N       Symmetric window size (default: 5)\n"
                  << "  --left N         Left window size (overrides --window)\n"
                  << "  --right N        Right window size (overrides --window)\n"
                  << "  --min-freq N     Minimum co-occurrence frequency (default: 5)\n"
                  << "  --max-items N    Maximum collocates to display (default: 50)\n"
                  << "  --stoplist N     Exclude N most frequent words for coll/dcoll/keyness (default: 0)\n"
                  << "  --measures M,... Association measures: logdice,mi,mi3,tscore,ll,dice (default: logdice)\n";
        std::exit(1);
    }

    // Resolve corpus path: explicit dir, or TEITOK ./pando, or cwd when already inside an index.
    if (is_corpus_dir(positional[0])) {
        opts.corpus_dir = positional[0];
        if (positional.size() > 1) {
            for (size_t i = 1; i < positional.size(); ++i) {
                if (i > 1) opts.query += " ";
                opts.query += positional[i];
            }
        } else {
            opts.interactive = true;
        }
    } else if (is_corpus_dir("pando")) {
        opts.corpus_dir = "pando";
        for (size_t i = 0; i < positional.size(); ++i) {
            if (i > 0) opts.query += " ";
            opts.query += positional[i];
        }
    } else if (is_corpus_dir(".")) {
        opts.corpus_dir = ".";
        for (size_t i = 0; i < positional.size(); ++i) {
            if (i > 0) opts.query += " ";
            opts.query += positional[i];
        }
    } else {
        opts.corpus_dir = positional[0];
        if (positional.size() > 1) {
            for (size_t i = 1; i < positional.size(); ++i) {
                if (i > 1) opts.query += " ";
                opts.query += positional[i];
            }
        } else {
            opts.interactive = true;
        }
    }

    if (request_interactive_repl)
        opts.interactive = true;

    return opts;
}

// ── Main ────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    Options opts = parse_args(argc, argv);

    if (opts.json && opts.conllu) {
        std::cerr << "Error: --json and --conllu are mutually exclusive.\n";
        return 1;
    }

    if (opts.pmltq_export_sql) {
        std::string d = opts.cql_dialect;
        for (char& c : d)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (d != "pmltq") {
            std::cerr << "Error: --pmltq-export-sql requires --cql pmltq\n";
            return 1;
        }
        if (opts.interactive || opts.query.empty()) {
            std::cerr << "Error: --pmltq-export-sql requires a query on the command line\n";
            return 1;
        }
        std::string sql;
        std::string err;
        if (!translate_pmltq_export_click_sql(opts.query, &sql, &err)) {
            if (opts.json || opts.api)
                std::cout << "{\"ok\": false, \"error\": {\"stage\": \"pmltq_sql\", \"message\": "
                          << jstr(err) << "}}\n";
            else
                std::cerr << "PML-TQ SQL export: " << err << "\n";
            return 1;
        }
        if (opts.json || opts.api)
            std::cout << "{\"ok\": true, \"sql\": " << jstr(sql) << "}\n";
        else
            std::cout << sql << "\n";
        return 0;
    }

    QueryTiming timing;
    if (opts.timing)
        timing.open_sec = 0;  // set below

    Corpus corpus;
    try {
        auto t_open0 = std::chrono::high_resolution_clock::now();
        corpus.open(opts.corpus_dir, opts.preload, opts.overlay_dirs);
        if (opts.timing)
            timing.open_sec = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - t_open0).count();
        if (opts.debug_level > 0)
            std::cerr << "Corpus loaded: " << corpus.size() << " tokens, "
                      << corpus.attr_names().size() << " attributes"
                      << (opts.overlay_dirs.empty() ? "" : " (with overlay(s))") << "\n";
    } catch (const std::exception& e) {
        if (opts.json || opts.api) {
            std::cout << "{\"ok\": false, \"error\": {\"stage\": \"open\", \"message\": "
                      << jstr(e.what()) << "}}\n";
        } else {
            std::cerr << "Error loading corpus: " << e.what() << "\n";
        }
        return 1;
    }

    Session session;

    if (opts.interactive && !opts.query.empty() && !opts.json && !opts.api)
        std::cerr << "Note: --interactive reads CQL from stdin; the command-line query is ignored.\n";

    if (!opts.interactive) {
        try {
            run_query(corpus, opts.query, opts, session, opts.timing ? &timing : nullptr);
            if (opts.timing)
                std::cerr << "open_sec=" << std::fixed << std::setprecision(3) << timing.open_sec
                          << " query_sec=" << timing.query_sec
                          << " fetch_sec=" << timing.fetch_sec
                          << " total=" << timing.total
                          << " returned=" << timing.returned << "\n";
        } catch (const std::exception& e) {
            if (opts.json || opts.api) {
                std::cout << "{\"ok\": false, \"error\": {\"stage\": \"query\", \"message\": "
                          << jstr(e.what()) << "}}\n";
            } else {
                std::cerr << "Query error: " << e.what() << "\n";
            }
            return 1;
        }
    } else {
        run_interactive_repl(corpus, opts, session);
    }

    return 0;
}
