#include "api/query_json.h"
#include "index/index_publish.h"
#include "core/json_utils.h"
#include "core/build_info.h"
#include "query/parser.h"
#include "index/bitmap_index.h"
#include "index/dep_pair_index.h"
#include "index/fold_index.h"
#include <cctype>
#include <filesystem>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <map>
#include <iostream>
#include <set>
#include <cmath>
#include <unordered_map>
#include <iomanip>
#include <string_view>
#include <functional>

namespace pando {

// Same pipe boundaries as query/executor multivalue_eq (RG-5f).
static void add_mv_counts(std::string_view stored, size_t weight,
                          std::unordered_map<std::string, size_t>& counts) {
    if (stored.find('|') == std::string_view::npos) {
        counts[std::string(stored)] += weight;
        return;
    }
    size_t start = 0;
    while (start < stored.size()) {
        size_t p = stored.find('|', start);
        if (p == std::string_view::npos) p = stored.size();
        std::string_view seg = stored.substr(start, p - start);
        if (!seg.empty())
            counts[std::string(seg)] += weight;
        start = p + 1;
    }
}

std::vector<std::pair<std::string, size_t>> positional_attr_show_values_mv(const PositionalAttr& pa,
                                                                           bool split_mv) {
    std::unordered_map<std::string, size_t> counts;
    const auto& lex = pa.lexicon();
    for (LexiconId id = 0; id < lex.size(); ++id) {
        size_t cnt = pa.count_of_id(id);
        if (cnt == 0) continue;
        if (split_mv)
            add_mv_counts(lex.get(id), cnt, counts);
        else
            counts[std::string(lex.get(id))] += cnt;
    }
    std::vector<std::pair<std::string, size_t>> entries(counts.begin(), counts.end());
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    return entries;
}

std::vector<std::pair<std::string, size_t>> region_attr_show_values_mv(const StructuralAttr& sa,
                                                                         const std::string& region_attr,
                                                                         bool split_mv) {
    std::unordered_map<std::string, size_t> counts;
    size_t n = sa.region_count();
    for (size_t i = 0; i < n; ++i) {
        std::string_view v = sa.region_value(region_attr, i);
        if (split_mv)
            add_mv_counts(v, 1, counts);
        else
            counts[std::string(v)] += 1;
    }
    std::vector<std::pair<std::string, size_t>> entries(counts.begin(), counts.end());
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    return entries;
}

namespace {

std::string corpus_json_name(const Corpus& corpus) { return corpus.display_name(); }

std::string xml_esc(std::string_view v) {
    std::string o;
    o.reserve(v.size() + 8);
    for (char c : v) {
        switch (c) {
            case '&': o += "&amp;"; break;
            case '<': o += "&lt;"; break;
            case '>': o += "&gt;"; break;
            case '"': o += "&quot;"; break;
            default: o += c;
        }
    }
    return o;
}

bool xml_name_ok(const std::string& n) {
    if (n.empty() || !(std::isalpha(static_cast<unsigned char>(n[0])) || n[0] == '_')) return false;
    for (char c : n)
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' || c == '.')) return false;
    return true;
}

/// Token id for the fragment / highlight map: the corpus' own `id` attribute
/// when it has one (TEITOK xml:ids), else `w-<position + 1>`.
std::string fragment_tok_id(const Corpus& corpus, CorpusPos p) {
    if (corpus.has_attr("id")) {
        std::string_view v = corpus.attr("id").value_at(p);
        if (!v.empty() && v != "_") return std::string(v);
    }
    return "w-" + std::to_string(p + 1);
}

/// The context [lo, hi] as TEITOK-style XML: `<s id="s-N">` around sentence
/// parts, `<tok id=… attr=… head=…>form</tok>`, tokens separated by spaces.
std::string build_fragment(const Corpus& corpus, CorpusPos lo, CorpusPos hi,
                           const std::vector<std::string>& attr_names) {
    const auto& form = corpus.attr("form");
    const StructuralAttr* s = corpus.has_structure("s") ? &corpus.structure("s") : nullptr;
    std::vector<std::pair<std::string, const PositionalAttr*>> attrs;
    for (const auto& a : attr_names) {
        if (a == "form" || a == "id" || !corpus.has_attr(a) || Corpus::is_internal_attr_name(a) || !xml_name_ok(a))
            continue;
        attrs.emplace_back(a, &corpus.attr(a));
    }
    std::string out;
    int64_t open_s = -1;
    for (CorpusPos p = lo; p <= hi; ++p) {
        const int64_t ri = s ? s->find_region(p) : -1;
        if (ri != open_s) {
            if (open_s >= 0) out += "</s>";
            if (!out.empty()) out += ' ';
            if (ri >= 0) out += "<s id=\"s-" + std::to_string(ri + 1) + "\">";
            open_s = ri;
        } else if (p > lo) {
            out += ' ';
        }
        out += "<tok id=\"" + xml_esc(fragment_tok_id(corpus, p)) + "\"";
        for (const auto& [name, pa] : attrs) {
            std::string_view v = pa->value_at(p);
            if (v.empty() || v == "_") continue;
            out += ' ' + name + "=\"" + xml_esc(v) + "\"";
        }
        if (corpus.has_deps()) {
            const CorpusPos h = corpus.deps().head(p);
            if (h != NO_HEAD && h >= 0) out += " head=\"" + xml_esc(fragment_tok_id(corpus, h)) + "\"";
        }
        out += '>' + xml_esc(form.value_at(p)) + "</tok>";
    }
    if (open_s >= 0) out += "</s>";
    return out;
}

size_t region_attr_vocab(const Corpus& corpus,
                         const StructuralAttr& sa,
                         const std::string& struct_name,
                         const std::string& rkey) {
    const std::string composite = struct_name + "_" + rkey;
    bool split_mv = corpus.is_multivalue(composite);
    if (sa.has_region_value_reverse(rkey) && !split_mv)
        return static_cast<size_t>(sa.region_attr_lex_size(rkey));
    return region_attr_show_values_mv(sa, rkey, split_mv).size();
}

}  // namespace

std::pair<MatchSet, double> run_single_query(const Corpus& corpus,
                                            const std::string& query_text,
                                            const QueryOptions& opts) {
    return run_single_query(corpus, query_text, opts, nullptr);
}

bool query_is_parallel(const std::string& query_text, bool strict_quoted_strings) {
    if (query_text.find("with") == std::string::npos) return false;
    try {
        Parser parser(query_text, ParserOptions{strict_quoted_strings});
        Program prog = parser.parse();
        return !prog.empty() && prog[0].has_query && prog[0].is_parallel;
    } catch (const std::exception&) {
        return false;
    }
}

std::pair<MatchSet, double> run_single_query(const Corpus& corpus,
                                            const std::string& query_text,
                                            const QueryOptions& opts,
                                            ExecProgress* progress) {
    Parser parser(query_text, ParserOptions{opts.strict_quoted_strings});
    Program prog = parser.parse();
    if (prog.empty() || !prog[0].has_query)
        return {MatchSet{}, 0.0};

    QueryExecutor executor(corpus);
    executor.set_include_empty_alignment_values(opts.allow_empty_alignment);
    if (progress) executor.set_progress(progress);
    size_t max_m = opts.offset + opts.limit;
    bool count_t = opts.total;
    size_t max_total_cap = (opts.total && opts.max_total > 0) ? opts.max_total : 0;

    auto t0 = std::chrono::high_resolution_clock::now();
    if ((opts.sample > 0 || opts.shuffle) && !prog[0].is_parallel) {
        // the hits with the smallest hash of (seed, hit): a sample of N, or the first
        // offset + limit of the shuffled order (HitSample)
        size_t k = opts.sample;
        if (opts.shuffle) k = opts.sample > 0 ? std::min(opts.sample, max_m) : max_m;
        MatchSet ms = executor.execute(prog[0].query, 0, true, 0, std::max<size_t>(k, 1), opts.seed, 1);
        if (k == 0) ms.matches.clear();
        const size_t population = ms.total_count;
        if (!opts.shuffle) sort_matches_by_position(ms.matches);
        ms.sample_population = population;
        ms.total_count = opts.sample > 0 ? std::min(opts.sample, population) : population;
        ms.total_exact = true;
        auto t1 = std::chrono::high_resolution_clock::now();
        return {std::move(ms), std::chrono::duration<double, std::milli>(t1 - t0).count()};
    }
    if (prog[0].is_parallel) {
        // every aligned pair (a page is cut from them in to_query_result_json); with
        // max_total, at most that many and the total marked inexact when reached
        const size_t cap = opts.max_total;
        MatchSet ms = executor.execute_parallel(prog[0].query, prog[0].target_query, cap, cap == 0);
        if (cap > 0 && ms.parallel_matches.size() >= cap) ms.total_exact = false;
        ms.total_count = ms.parallel_matches.size();
        auto t1 = std::chrono::high_resolution_clock::now();
        return {std::move(ms), std::chrono::duration<double, std::milli>(t1 - t0).count()};
    }
    MatchSet ms = executor.execute(prog[0].query, max_m, count_t, max_total_cap, 0, 0,
                                   std::max(1u, opts.threads));
    auto t1 = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double, std::milli>(t1 - t0).count();
    return {std::move(ms), elapsed};
}

// One hit of a /query page: doc, match span, context, named regions, tokens, and in
// fragment mode its TEITOK-style XML with a highlight map. `group_offset` numbers the
// query tokens of an aligned target after those of its source.
static void emit_hit_json(std::ostringstream& out, const Corpus& corpus, const Match& m,
                          const QueryOptions& opts,
                          const std::function<std::string(size_t)>& group_name,
                          size_t group_offset = 0) {
    CorpusPos match_start = m.first_pos();
    CorpusPos match_end   = m.last_pos();
    auto doc_id = lookup_doc_id(corpus, match_start);
    auto ctx = build_context(corpus, m, opts.context, opts.sentence);

    out << "      {";
    out << "\"doc_id\": " << (doc_id.empty() ? "null" : jstr(doc_id));
    out << ", \"match_start\": " << match_start << ", \"match_end\": " << match_end;
    out << ", \"context\": {\"left\": " << jstr(ctx.left)
        << ", \"match\": " << jstr(ctx.match) << ", \"right\": " << jstr(ctx.right) << "}";
    if (!m.named_regions.empty()) {
        out << ", \"named_regions\": {";
        bool first_nr = true;
        for (const auto& [nm, rr] : m.named_regions) {
            if (!first_nr) out << ", ";
            first_nr = false;
            out << jstr(nm) << ": {\"struct\": " << jstr(rr.struct_name)
                << ", \"region_idx\": " << rr.region_idx << "}";
        }
        out << "}";
    }
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
            if (opts.fragment)
                out << ", \"id\": " << jstr(fragment_tok_id(corpus, p)) << ", \"group\": " << (t + group_offset);
            for (const auto& attr_name : attr_names) {
                if (!corpus.has_attr(attr_name) || Corpus::is_internal_attr_name(attr_name)) continue;
                auto val = corpus.attr(attr_name).value_at(p);
                if (val == "_") continue;
                out << ", " << jstr(attr_name) << ": " << jstr(val);
            }
            out << "}";
        }
    }
    out << "]";
    if (opts.fragment) {
        CorpusPos lo = 0, hi = 0;
        context_bounds(corpus, m, opts.context, opts.sentence, lo, hi);
        out << ", \"fragment\": " << jstr(build_fragment(corpus, lo, hi, attr_names));
        // highlight_map (flexicorp highlight_contract): matched ids, by query token
        std::vector<std::string> all;
        std::map<size_t, std::vector<std::string>> by_group;
        for (size_t t = 0; t < m.positions.size(); ++t) {
            if (m.positions[t] == NO_HEAD) continue;
            CorpusPos span_end = (!m.span_ends.empty()) ? m.span_ends[t] : m.positions[t];
            for (CorpusPos p = m.positions[t]; p <= span_end; ++p) {
                std::string id = fragment_tok_id(corpus, p);
                by_group[t + group_offset].push_back(id);
                all.push_back(std::move(id));
            }
        }
        auto id_list = [&](const std::vector<std::string>& v) {
            std::string o = "[";
            for (size_t k = 0; k < v.size(); ++k) o += (k ? ", " : "") + jstr(v[k]);
            return o + "]";
        };
        out << ", \"highlight_map\": {\"default\": {\"tok_ids\": " << id_list(all)
            << "}, \"match\": " << id_list(all) << ", \"groups\": [";
        if (by_group.size() > 1) {
            bool fg = true;
            for (const auto& [t, ids] : by_group) {
                out << (fg ? "" : ", ") << "{\"id\": " << jstr(group_name(t)) << ", \"tok_ids\": " << id_list(ids) << "}";
                fg = false;
            }
        }
        out << "]}";
    }
    out << "}";
}

std::string to_query_result_json(const Corpus& corpus,
                                 const std::string& query_text,
                                 const MatchSet& ms,
                                 const QueryOptions& opts,
                                 double elapsed_ms,
                                 std::string_view extra_result_fields,
                                 size_t matches_offset) {
    std::ostringstream out;
    size_t stored = matches_offset + ms.matches.size();
    size_t start = std::max(matches_offset, std::min(opts.offset, stored));
    size_t end   = std::min(start + opts.limit, stored);
    size_t returned = end - start;

    // fragment mode: highlight groups are the query's tokens (their labels, else t1, t2, …);
    // an aligned query numbers its target's tokens after the source's
    std::vector<std::string> group_names;
    size_t source_tokens = 0;
    if (opts.fragment || ms.parallel) {
        try {
            Parser parser(query_text, ParserOptions{opts.strict_quoted_strings});
            Program prog = parser.parse();
            if (!prog.empty() && prog[0].has_query) {
                auto add = [&](const TokenQuery& q) {
                    for (const auto& tok : q.tokens)
                        group_names.push_back(tok.name.empty() ? "t" + std::to_string(group_names.size() + 1) : tok.name);
                };
                add(prog[0].query);
                source_tokens = group_names.size();
                if (prog[0].is_parallel) add(prog[0].target_query);
            }
        } catch (const std::exception&) {
        }
    }
    std::function<std::string(size_t)> group_name = [&](size_t t) {
        return t < group_names.size() ? group_names[t] : "t" + std::to_string(t + 1);
    };
    auto emit_debug_and_extra = [&]() {
        if (opts.debug) {
            out << ",\n    \"debug\": {\n";
            out << "      \"corpus_size\": " << corpus.size() << ",\n";
            out << "      \"has_deps\": " << (corpus.has_deps() ? "true" : "false") << ",\n";
            out << "      \"elapsed_ms\": " << elapsed_ms << ",\n";
            out << "      \"plan_path\": " << jstr(ms.plan_path) << ",\n";
            out << "      \"seed_token\": " << ms.seed_token << ",\n";
            out << "      \"cardinalities\": [";
            for (size_t i = 0; i < ms.cardinalities.size(); ++i) {
                if (i > 0) out << ", ";
                out << ms.cardinalities[i];
            }
            out << "]\n    }";
        }
        if (!extra_result_fields.empty()) out << ",\n    " << extra_result_fields;
        out << "\n  }\n}\n";
    };

    // `a:[…] with b:[…] :: a.x = b.x`: aligned pairs (source, target), paged by pair in
    // corpus order of the source hit (as execute_parallel sorts them). page.total counts
    // pairs; total_sources the distinct source hits among them.
    if (ms.parallel) {
        const auto& pm = ms.parallel_matches;
        const size_t pstart = std::min(opts.offset, pm.size());
        const size_t pend = opts.limit == 0 ? pm.size() : std::min(pstart + opts.limit, pm.size());
        size_t sources = 0;
        for (size_t i = 0; i < pm.size(); ++i)
            if (i == 0 || pm[i].first.positions != pm[i - 1].first.positions
                || pm[i].first.span_ends != pm[i - 1].first.span_ends) ++sources;
        out << "{\n  \"ok\": true,\n  \"backend\": \"pando\",\n  \"operation\": \"query\",\n";
        out << "  \"last_command\": \"query\",\n  \"result\": {\n    \"parallel\": true,\n";
        out << "    \"query\": {\"language\": \"pando-cql\", \"text\": " << jstr(query_text) << "},\n";
        out << "    \"page\": {\"start\": " << pstart << ", \"size\": " << opts.limit
            << ", \"returned\": " << (pend - pstart) << ", \"total\": " << pm.size()
            << ", \"returned_pairs\": " << (pend - pstart) << ", \"total_pairs\": " << pm.size()
            << ", \"total_sources\": " << sources
            << ", \"total_exact\": " << (ms.total_exact ? "true" : "false") << "},\n";
        out << "    \"hits\": [],\n    \"pairs\": [\n";
        for (size_t i = pstart; i < pend; ++i) {
            const auto& [src, tgt] = pm[i];
            if (i > pstart) out << ",\n";
            auto doc_s = lookup_doc_id(corpus, src.first_pos());
            auto doc_t = lookup_doc_id(corpus, tgt.first_pos());
            out << "      {\"aligned\": true, \"alignment\": {\"kind\": \"with\", \"pair_index\": " << i << "},\n";
            out << "       \"source\":\n";
            emit_hit_json(out, corpus, src, opts, group_name, 0);
            out << ",\n       \"target\":\n";
            emit_hit_json(out, corpus, tgt, opts, group_name, source_tokens);
            out << ",\n       \"source_text_id\": " << (doc_s.empty() ? "null" : jstr(doc_s))
                << ", \"target_text_id\": " << (doc_t.empty() ? "null" : jstr(doc_t)) << "}";
        }
        out << "\n    ]";
        if (group_names.size() > 1) {
            out << ",\n    \"legend\": [";
            for (size_t t = 0; t < group_names.size(); ++t)
                out << (t ? ", " : "") << "{\"id\": " << jstr(group_names[t]) << ", \"name\": " << jstr(group_names[t])
                    << ", \"side\": " << (t < source_tokens ? "\"source\"" : "\"target\"") << "}";
            out << "]";
        }
        emit_debug_and_extra();
        return out.str();
    }

    out << "{\n";
    out << "  \"ok\": true,\n";
    out << "  \"backend\": \"pando\",\n";
    out << "  \"operation\": \"query\",\n";
    out << "  \"last_command\": \"query\",\n";
    out << "  \"result\": {\n";
    out << "    \"query\": {\"language\": \"pando-cql\", \"text\": " << jstr(query_text) << "},\n";
    out << "    \"page\": {\"start\": " << start << ", \"size\": " << opts.limit
        << ", \"returned\": " << returned << ", \"total\": " << ms.total_count
        << ", \"total_exact\": " << (ms.total_exact ? "true" : "false") << "},\n";
    out << "    \"hits\": [\n";


    for (size_t i = start; i < end; ++i) {
        if (i > start) out << ",\n";
        emit_hit_json(out, corpus, ms.matches[i - matches_offset], opts, group_name);
    }
    out << "\n    ]";
    if (opts.fragment && group_names.size() > 1) {
        out << ",\n    \"legend\": [";
        for (size_t t = 0; t < group_names.size(); ++t)
            out << (t ? ", " : "") << "{\"id\": " << jstr(group_names[t]) << ", \"name\": " << jstr(group_names[t]) << "}";
        out << "]";
    }

    emit_debug_and_extra();
    return out.str();
}

std::string to_info_json(const Corpus& corpus, std::string_view operation,
                         std::string_view extra_result_fields) {
    std::ostringstream out;
    out << "{\n  \"ok\": true,\n  \"operation\": " << jstr(operation) << ",\n";
    out << "  \"result\": {\n";
    out << "    \"name\": " << jstr(corpus_json_name(corpus)) << ",\n";
    out << "    \"path\": " << jstr(corpus.dir()) << ",\n";
    const CorpusPos ntok = corpus.size();
    out << "    \"size\": " << ntok << ",\n";
    out << "    \"tokens\": " << ntok << ",\n";
    out << "    \"has_deps\": " << (corpus.has_deps() ? "true" : "false") << ",\n";
    out << "    \"attributes\": [";
    const auto& names = corpus.attr_names();
    for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) out << ", ";
        out << "{\"name\": " << jstr(names[i])
            << ", \"vocab\": " << corpus.attr(names[i]).lexicon().size() << "}";
    }
    out << "],\n";

    const auto& s_names = corpus.structure_names();
    size_t total_regions = 0;
    for (const auto& sn : s_names)
        total_regions += corpus.structure(sn).region_count();

    out << "    \"total_regions\": " << total_regions << ",\n";
    out << "    \"structures\": [";
    for (size_t i = 0; i < s_names.size(); ++i) {
        if (i > 0) out << ", ";
        const std::string& sn = s_names[i];
        const auto& sa = corpus.structure(sn);
        out << "{\"name\": " << jstr(sn)
            << ", \"regions\": " << sa.region_count()
            << ", \"has_values\": " << (sa.has_values() ? "true" : "false");
        const auto& ra = sa.region_attr_names();
        if (!ra.empty()) {
            out << ", \"attrs\": [";
            for (size_t j = 0; j < ra.size(); ++j) {
                if (j > 0) out << ", ";
                out << jstr(ra[j]);
            }
            out << "]";
        }
        out << ", \"region_attrs\": [";
        for (size_t j = 0; j < ra.size(); ++j) {
            if (j > 0) out << ", ";
            out << "{\"name\": " << jstr(ra[j])
                << ", \"vocab\": " << region_attr_vocab(corpus, sa, sn, ra[j]) << "}";
        }
        out << "]";
        if (corpus.is_nested(sn))     out << ", \"nested\": true";
        if (corpus.is_overlapping(sn)) out << ", \"overlapping\": true";
        if (corpus.is_zerowidth(sn))   out << ", \"zerowidth\": true";
        out << "}";
    }
    out << "],\n";

    out << "    \"region_attrs\": [";
    const auto& flat_ra = corpus.region_attr_names();
    for (size_t i = 0; i < flat_ra.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(flat_ra[i]);
    }
    out << "],\n";
    out << "    \"multivalue\": [";
    const auto& mv = corpus.multivalue_attrs();
    for (size_t i = 0; i < mv.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(mv[i]);
    }
    out << "],\n";
    out << "    \"kv_pipe\": [";
    const auto& kvp = corpus.kv_pipe_attrs();
    for (size_t i = 0; i < kvp.size(); ++i) {
        if (i > 0) out << ", ";
        out << jstr(kvp[i]);
    }
    out << "],\n";
    out << "    \"pando\": {" << build_json_fields() << "},\n";
    out << "    \"index\": {" << index_status_json_fields(corpus) << "}";
    if (!extra_result_fields.empty()) out << ",\n    " << extra_result_fields;
    out << "\n  }\n}\n";
    return out.str();
}

std::string index_identity_json_fields(const Corpus& corpus) {
    auto opt_str = [](const std::string& v) { return v.empty() ? std::string("null") : jstr(v); };
    const std::string root = IndexPublish::root_of(corpus.dir());
    std::string current;
    bool is_current = true;
    if (!root.empty()) {
        current = IndexPublish::current_version(root);
        is_current = current == corpus.dir();
    }
    // on disk now (a rebuild in place, or a newer published version, changes it)
    const std::string disk_id = IndexPublish::read_index_id(root.empty() ? corpus.dir() : current);
    const std::string id = corpus.info().index_id;
    const bool changed = !root.empty() ? !is_current : (!id.empty() && disk_id != id);
    return "\"index_id\": " + opt_str(id) + ", \"index_identity\": " + jstr(corpus.index_identity())
        + ", \"index_dir\": " + jstr(corpus.dir()) + ", \"published_root\": " + opt_str(root)
        + ", \"current_version\": " + opt_str(current)
        + ", \"newer_on_disk\": " + (changed ? "true" : "false");
}

std::string index_status_json_fields(const Corpus& corpus) {
    namespace fs = std::filesystem;
    std::ostringstream out;
    auto opt_str = [](const std::string& v) { return v.empty() ? std::string("null") : jstr(v); };
    const CorpusInfo& ci = corpus.info();
    out << "\"indexed_with\": " << opt_str(ci.indexed_with)
        << ", \"upgraded_with\": " << opt_str(ci.upgraded_with);
    std::error_code ec;
    auto exists = [&](const std::string& p) { return fs::exists(p, ec); };
    out << ", " << index_identity_json_fields(corpus);

    // what the directory holds (index profile): plain / packed forms, sizes
    {
        uint64_t bytes = 0;
        for (const auto& e : fs::directory_iterator(corpus.dir(), ec)) {
            std::error_code ec2;
            if (e.is_regular_file(ec2)) bytes += e.file_size(ec2);
        }
        char bpt[32];
        std::snprintf(bpt, sizeof bpt, "%.1f",
                      corpus.size() > 0 ? static_cast<double>(bytes) / static_cast<double>(corpus.size()) : 0.0);
        out << ", \"disk_bytes\": " << bytes << ", \"bytes_per_token\": " << bpt;
        auto form = [&](const std::string& base, const char* plain, const char* packed) -> const char* {
            const bool p = exists(base + plain), k = exists(base + packed);
            return p && k ? "both" : p ? "plain" : k ? "packed" : "none";
        };
        out << ", \"attrs\": [";
        bool f = true;
        for (const auto& name : corpus.attr_names()) {
            const std::string base = corpus.dir() + "/" + name;
            out << (f ? "" : ", ") << "{\"attr\": " << jstr(name) << ", \"dat\": \"" << form(base, ".dat", ".dat.pk")
                << "\", \"rev\": \"" << form(base, ".rev", ".rev.pfb") << "\"}";
            f = false;
        }
        out << "], \"head_attrs\": [";
        f = true;
        for (const auto& a : ci.head_attrs) {
            out << (f ? "" : ", ") << jstr(a);
            f = false;
        }
        out << "], \"dep_files\": [";
        f = true;
        for (const char* d : {"dep.head", "dep.head_rel", "dep.head_rel8", "dep.euler_in", "dep.euler_out"})
            if (exists(corpus.dir() + "/" + d)) {
                out << (f ? "" : ", ") << jstr(d);
                f = false;
            }
        out << "]";
    }

    // attribute bitmaps: the --bitmaps auto candidates plus any attribute with a .bm file
    out << ", \"bitmaps\": [";
    bool first = true;
    for (const auto& name : corpus.attr_names()) {
        if (corpus.is_multivalue(name)) continue;
        const auto& pa = corpus.attr(name);
        const bool present = exists(BitmapIndex::idx_path(pa.base_path()));
        if (!present && pa.lexicon().size() > BitmapIndex::kAutoMaxValues) continue;
        BitmapIndex bi;
        const char* st = !present ? "missing" : (bi.open(pa, corpus.size()) ? "ok" : "stale");
        out << (first ? "" : ", ") << "{\"attr\": " << jstr(name) << ", \"values\": "
            << pa.lexicon().size() << ", \"status\": \"" << st << "\"}";
        first = false;
    }
    out << "]";

    out << ", \"structure_bitmaps\": [";
    first = true;
    for (const auto& name : corpus.structure_names()) {
        if (corpus.is_nested(name) || corpus.is_overlapping(name) || corpus.is_zerowidth(name)) continue;
        const std::string base = BitmapIndex::structure_base(corpus.dir(), name);
        const bool present = exists(BitmapIndex::idx_path(base));
        BitmapIndex bi;
        const char* st = !present ? "missing" : (bi.open_structure(base, corpus.size()) ? "ok" : "stale");
        out << (first ? "" : ", ") << "{\"struct\": " << jstr(name) << ", \"status\": \"" << st << "\"}";
        first = false;
    }
    out << "]";

    // edge postings: every dep.pair.<H>.<C>.rev in the directory
    out << ", \"dep_pairs\": [";
    first = true;
    if (corpus.has_deps()) {
        std::vector<std::pair<std::string, std::string>> pairs;
        for (const auto& e : fs::directory_iterator(corpus.dir(), ec)) {
            const std::string fn = e.path().filename().string();
            const std::string pre = "dep.pair.", suf = ".rev";
            if (fn.rfind(pre, 0) != 0 || fn.size() <= pre.size() + suf.size()
                || fn.compare(fn.size() - suf.size(), suf.size(), suf) != 0)
                continue;
            const std::string mid = fn.substr(pre.size(), fn.size() - pre.size() - suf.size());
            const size_t dot = mid.find('.');
            if (dot == std::string::npos) continue;
            pairs.emplace_back(mid.substr(0, dot), mid.substr(dot + 1));
        }
        std::sort(pairs.begin(), pairs.end());
        for (const auto& [h, c] : pairs) {
            DepPairIndex di;
            const bool ok = di.open(corpus, h, c);
            out << (first ? "" : ", ") << "{\"head\": " << jstr(h) << ", \"child\": " << jstr(c)
                << ", \"status\": \"" << (ok ? "ok" : "stale") << "\"}";
            first = false;
        }
    }
    out << "]";
    out << ", \"dep_head_rel\": "
        << (corpus.has_deps() ? (static_cast<bool>(corpus.deps().head_rel_data()) ? "true" : "false") : "null");

    size_t fold_ok = 0, fold_missing = 0;
    const FoldMode modes[] = {FoldMode::Lower, FoldMode::NoAccents, FoldMode::LowerNoAccents};
    for (const auto& name : corpus.attr_names()) {
        if (corpus.is_multivalue(name)) continue;
        const auto& pa = corpus.attr(name);
        for (auto m : modes) {
            FoldIndex fi;
            if (fi.open(pa.base_path(), m, pa.lexicon().size())) ++fold_ok;
            else ++fold_missing;
        }
    }
    out << ", \"fold_indexes\": {\"ok\": " << fold_ok << ", \"missing\": " << fold_missing << "}";
    return out.str();
}

std::string to_values_json(const Corpus& corpus, const std::string& attr_name, size_t limit) {
    std::ostringstream out;
    if (Corpus::is_internal_attr_name(attr_name)) return "";   // head#A: storage only

    // Try positional attribute first
    if (corpus.has_attr(attr_name)) {
        const auto& pa = corpus.attr(attr_name);
        bool is_mv = corpus.is_multivalue(attr_name);
        std::vector<std::pair<std::string, size_t>> entries = positional_attr_show_values_mv(pa, is_mv);

        size_t cap = (limit > 0) ? std::min(entries.size(), limit) : entries.size();
        out << "{\n  \"ok\": true,\n  \"operation\": \"values\",\n";
        out << "  \"result\": {\n";
        out << "    \"attr\": " << jstr(attr_name) << ",\n";
        out << "    \"type\": \"positional\",\n";
        out << "    \"unique\": " << entries.size() << ",\n";
        out << "    \"returned\": " << cap << ",\n";
        out << "    \"values\": [\n";
        for (size_t i = 0; i < cap; ++i) {
            if (i > 0) out << ",\n";
            out << "      {\"value\": " << jstr(entries[i].first)
                << ", \"count\": " << entries[i].second << "}";
        }
        out << "\n    ]\n  }\n}\n";
        return out.str();
    }

    // Try region attribute: split text_genre → struct "text", attr "genre"
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

                size_t cap = (limit > 0) ? std::min(entries.size(), limit) : entries.size();
                out << "{\n  \"ok\": true,\n  \"operation\": \"values\",\n";
                out << "  \"result\": {\n";
                out << "    \"attr\": " << jstr(attr_name) << ",\n";
                out << "    \"type\": \"region\",\n";
                out << "    \"structure\": " << jstr(struct_name) << ",\n";
                out << "    \"region_attr\": " << jstr(region_attr) << ",\n";
                out << "    \"unique\": " << entries.size() << ",\n";
                out << "    \"returned\": " << cap << ",\n";
                out << "    \"values\": [\n";
                for (size_t i = 0; i < cap; ++i) {
                    if (i > 0) out << ",\n";
                    out << "      {\"value\": " << jstr(entries[i].first)
                        << ", \"count\": " << entries[i].second << "}";
                }
                out << "\n    ]\n  }\n}\n";
                return out.str();
            }
        }
    }

    return {};  // not found
}

std::string to_regions_json(const Corpus& corpus, const std::string& type_name, size_t limit) {
    if (!corpus.has_structure(type_name)) return {};

    const auto& sa = corpus.structure(type_name);
    const auto& ra_names = sa.region_attr_names();
    size_t n = sa.region_count();
    size_t cap = (limit > 0) ? std::min(n, limit) : n;

    std::ostringstream out;
    out << "{\n  \"ok\": true,\n  \"operation\": \"regions\",\n";
    out << "  \"result\": {\n";
    out << "    \"type\": " << jstr(type_name) << ",\n";
    out << "    \"total\": " << n << ",\n";
    out << "    \"returned\": " << cap << ",\n";
    out << "    \"regions\": [\n";
    for (size_t i = 0; i < cap; ++i) {
        Region rgn = sa.get(i);
        if (i > 0) out << ",\n";
        out << "      {\"index\": " << i
            << ", \"start\": " << rgn.start
            << ", \"end\": " << rgn.end
            << ", \"tokens\": " << (rgn.end - rgn.start + 1);
        for (const auto& attr : ra_names) {
            std::string_view v = sa.region_value(attr, i);
            out << ", " << jstr(type_name + "_" + attr) << ": " << jstr(std::string(v));
        }
        out << "}";
    }
    out << "\n    ]\n  }\n}\n";
    return out.str();
}

} // namespace pando
