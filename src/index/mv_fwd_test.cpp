// Stage 1 of PANDO-MULTIVALUE-FIELDS — round-trip test for the .mv.fwd /
// .mv.fwd.idx forward index.
//
// Spec: dev/PANDO-MVAL-FORMAT.md (v0.2)
//
// Coverage (matches §7 of the spec):
//   1. round-trip basics (single, compound, empty values)
//   2. sortedness invariant (out-of-order components in joined string)
//   3. dedup (repeated component in joined string)
//   4. empty position (empty value lex)
//   5. reverse/forward consistency (component → positions matches both ways)
//   6. old-corpus compatibility (delete forward files, has_mv_fwd() == false)
//   7. sentinel sanity (corrupt .mv.fwd.idx → open_mv_fwd throws)
//
// Run via the BUILD_TESTING CMake target (see CMakeLists.txt). The test
// builds a tiny corpus on the fly using StreamingBuilder, opens it via
// PositionalAttr, and asserts on the new forward API.

#include "corpus/streaming_builder.h"
#include "index/positional_attr.h"
#include "core/mmap_file.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <set>
#include <string>
#include <vector>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace manatree;

namespace {

// Make a unique scratch directory under /tmp for one test run.
fs::path make_scratch(const std::string& tag) {
    fs::path base = fs::temp_directory_path() /
        ("pando_mv_fwd_test_" + tag + "_" + std::to_string(::getpid()));
    if (fs::exists(base)) fs::remove_all(base);
    fs::create_directories(base);
    return base;
}

// Build a corpus where attribute `domain` is multivalue and takes one of the
// supplied per-token strings; positions are 0..N-1 in token order.
//
// `form` gets a stable scalar value so we have a non-MV attr to keep the
// builder happy.
void build_corpus(const fs::path& dir,
                  const std::vector<std::string>& domain_values) {
    StreamingBuilder b(dir.string());
    b.declare_multivalue("domain");
    for (size_t i = 0; i < domain_values.size(); ++i) {
        std::unordered_map<std::string, std::string> attrs;
        attrs["form"]   = "tok" + std::to_string(i);
        attrs["domain"] = domain_values[i];
        b.add_token(attrs, /*head=*/-1);
    }
    b.end_sentence();
    b.finalize();
}

PositionalAttr open_attr(const fs::path& dir, const std::string& name,
                         CorpusPos size) {
    PositionalAttr pa;
    pa.open((dir / name).string(), size);
    pa.open_mv((dir / name).string());
    pa.open_mv_fwd((dir / name).string());
    return pa;
}

// Read mv components at pos as a vector of strings (using the mv lexicon).
std::vector<std::string> mv_strings_at(const PositionalAttr& pa, CorpusPos pos) {
    std::vector<std::string> out;
    pa.for_each_mv_fwd_at(pos, [&](LexiconId id) {
        out.push_back(std::string(pa.mv_lexicon().get(id)));
        return true;
    });
    return out;
}

// Components at pos via the mv lexicon — used for the consistency check.
std::vector<std::string> mv_strings_at_via_lex(const PositionalAttr& pa,
                                                CorpusPos pos) {
    std::vector<std::string> out;
    pa.for_each_mv_fwd_at(pos, [&](LexiconId id) {
        // The forward index stores MV component lex ids; the lexicon for
        // those lives in the separate mv_lexicon. We don't expose that
        // directly, so we round-trip through mv_lookup → id; cheaper to
        // simply track ids and compare to per-component reverse lookup.
        out.push_back(std::to_string(id));
        return true;
    });
    return out;
}

#define CHECK(cond) do { \
    if (!(cond)) { \
        std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" \
                  << __LINE__ << "\n"; \
        std::exit(1); \
    } \
} while (0)

#define CHECK_EQ(a, b) do { \
    auto _aval = (a); auto _bval = (b); \
    if (!(_aval == _bval)) { \
        std::cerr << "FAIL: " << #a << " == " << #b << "\n  lhs="; \
        std::cerr << _aval << "\n  rhs=" << _bval << "\n  at " \
                  << __FILE__ << ":" << __LINE__ << "\n"; \
        std::exit(1); \
    } \
} while (0)

// Test 1 + 2 + 3 + 4 combined: a single corpus that exercises round-trip,
// sortedness, dedup, and empty positions.
void test_basics_sort_dedup_empty() {
    auto dir = make_scratch("basics");
    // Per position: scalar, compound, swapped-order compound, dup component,
    // empty value, repeat of an existing compound.
    std::vector<std::string> vals = {
        "Hunting",          // pos 0: scalar appearing in compound below
        "Hunting|Fishing",  // pos 1: compound, alphabetically already sorted
        "zebra|apple",      // pos 2: out-of-order — should be sorted
        "a|a|b",            // pos 3: dup component a
        "",                 // pos 4: empty value
        "Fishing",          // pos 5: scalar component of pos 1
    };
    build_corpus(dir, vals);

    PositionalAttr pa = open_attr(dir, "domain", static_cast<CorpusPos>(vals.size()));
    CHECK(pa.has_mv_fwd());

    // pos 0: "Hunting" (component of compound) → {Hunting}
    {
        auto v = mv_strings_at(pa, 0);
        CHECK_EQ(v.size(), size_t{1});
        CHECK_EQ(v[0], std::string("Hunting"));
        CHECK_EQ(pa.mv_fwd_count_at(0), size_t{1});
    }

    // pos 1: "Hunting|Fishing" → sorted [Fishing, Hunting]
    {
        auto v = mv_strings_at(pa, 1);
        CHECK_EQ(v.size(), size_t{2});
        CHECK_EQ(v[0], std::string("Fishing"));
        CHECK_EQ(v[1], std::string("Hunting"));
    }

    // pos 2: "zebra|apple" → sorted [apple, zebra]
    {
        auto v = mv_strings_at(pa, 2);
        CHECK_EQ(v.size(), size_t{2});
        CHECK_EQ(v[0], std::string("apple"));
        CHECK_EQ(v[1], std::string("zebra"));
    }

    // pos 3: "a|a|b" → dedup [a, b]
    {
        auto v = mv_strings_at(pa, 3);
        CHECK_EQ(v.size(), size_t{2});
        CHECK_EQ(v[0], std::string("a"));
        CHECK_EQ(v[1], std::string("b"));
    }

    // pos 4: "" → empty (cardinality 0). Note: empty string is not a
    // component of any compound, so the forward set is the empty set.
    {
        CHECK_EQ(pa.mv_fwd_count_at(4), size_t{0});
        bool any = false;
        pa.for_each_mv_fwd_at(4, [&](LexiconId) { any = true; return true; });
        CHECK(!any);
    }

    // pos 5: "Fishing" → {Fishing}
    {
        auto v = mv_strings_at(pa, 5);
        CHECK_EQ(v.size(), size_t{1});
        CHECK_EQ(v[0], std::string("Fishing"));
    }

    fs::remove_all(dir);
    std::cerr << "  PASS test_basics_sort_dedup_empty\n";
}

// Test 5: reverse/forward consistency on a slightly larger synthetic corpus.
//
// For every MV component c, the set of positions reported by the forward
// index (filter f_each_mv_fwd) must equal the set reported by the existing
// reverse index (for_each_position_mv).
void test_reverse_forward_consistency() {
    auto dir = make_scratch("rfconsist");
    // Mix of compounds and scalars across ~200 positions.
    std::vector<std::string> vals;
    const std::vector<std::string> palette = {
        "a", "b", "c", "a|b", "b|c", "a|c", "a|b|c", "", "b|a", "c|a|b"
    };
    for (int i = 0; i < 200; ++i)
        vals.push_back(palette[static_cast<size_t>(i) % palette.size()]);
    build_corpus(dir, vals);

    PositionalAttr pa = open_attr(dir, "domain",
                                  static_cast<CorpusPos>(vals.size()));
    CHECK(pa.has_mv_fwd());

    // For each MV component, gather positions from both directions.
    // Walk components by id by probing well-known names.
    for (const std::string& comp : {"a", "b", "c"}) {
        LexiconId mv_id = pa.mv_lookup(comp);
        CHECK(mv_id != UNKNOWN_LEX);

        // Reverse: existing API.
        std::set<CorpusPos> reverse_set;
        pa.for_each_position_mv(mv_id, [&](CorpusPos p) {
            reverse_set.insert(p);
            return true;
        });

        // Forward: scan every position; collect those whose set contains mv_id.
        std::set<CorpusPos> forward_set;
        for (CorpusPos p = 0; p < static_cast<CorpusPos>(vals.size()); ++p) {
            pa.for_each_mv_fwd_at(p, [&](LexiconId id) {
                if (id == mv_id) forward_set.insert(p);
                return true;
            });
        }

        if (reverse_set != forward_set) {
            std::cerr << "FAIL: component '" << comp << "' mismatch\n"
                      << "  reverse size=" << reverse_set.size()
                      << " forward size=" << forward_set.size() << "\n";
            std::exit(1);
        }
    }

    fs::remove_all(dir);
    std::cerr << "  PASS test_reverse_forward_consistency\n";
}

// Test 6: old-corpus compatibility — delete the forward files and re-open.
void test_old_corpus_no_fwd() {
    auto dir = make_scratch("nofwd");
    build_corpus(dir, {"Hunting|Fishing", "Hunting"});

    fs::remove(dir / "domain.mv.fwd");
    fs::remove(dir / "domain.mv.fwd.idx");

    PositionalAttr pa;
    pa.open((dir / "domain").string(), 2);
    pa.open_mv((dir / "domain").string());
    pa.open_mv_fwd((dir / "domain").string());
    CHECK(!pa.has_mv_fwd());
    CHECK_EQ(pa.mv_fwd_count_at(0), size_t{0});
    bool any = false;
    pa.for_each_mv_fwd_at(0, [&](LexiconId) { any = true; return true; });
    CHECK(!any);

    fs::remove_all(dir);
    std::cerr << "  PASS test_old_corpus_no_fwd\n";
}

// Test 7: corrupted .mv.fwd.idx is rejected.
void test_corrupt_idx_rejected() {
    auto dir = make_scratch("corrupt");
    build_corpus(dir, {"a|b", "b"});

    // Truncate .mv.fwd.idx so its size doesn't match (corpus_size + 1) * 8.
    {
        FILE* f = fopen((dir / "domain.mv.fwd.idx").c_str(), "wb");
        CHECK(f != nullptr);
        int64_t junk = 0;
        fwrite(&junk, sizeof(int64_t), 1, f);  // 1 element instead of 3
        fclose(f);
    }

    PositionalAttr pa;
    pa.open((dir / "domain").string(), 2);
    pa.open_mv((dir / "domain").string());
    bool threw = false;
    try {
        pa.open_mv_fwd((dir / "domain").string());
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    fs::remove_all(dir);
    std::cerr << "  PASS test_corrupt_idx_rejected\n";
}

}  // namespace

int main() {
    std::cerr << "running mv_fwd_test\n";
    test_basics_sort_dedup_empty();
    test_reverse_forward_consistency();
    test_old_corpus_no_fwd();
    test_corrupt_idx_rejected();
    std::cerr << "all mv_fwd tests passed\n";
    return 0;
}
