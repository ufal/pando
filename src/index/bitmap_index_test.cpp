// P3.1 / P3.2: chunked bitmap index and the bit-parallel sequence kernel.
//
// Builds a synthetic corpus spanning several 2^16-position chunks (a full
// chunk, dense bitmap chunks, sparse array chunks, a short last chunk), checks
// every container against the `.rev` postings, and compares kernel results
// (totals, first page, within, region filter) with a brute-force scan.
//
// Run: cmake --build build --target bitmap_index_test && ./build/bitmap_index_test

#include "corpus/corpus.h"
#include "corpus/streaming_builder.h"
#include "index/bitmap_index.h"
#include "query/executor.h"
#include "query/parser.h"

#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace pando;

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
            std::exit(1); \
        } \
    } while (0)

namespace {

constexpr CorpusPos kChunk = BitmapIndex::kChunk;
constexpr CorpusPos kN = 4 * kChunk + 1234;
constexpr int kSentLen = 7;

std::string value_at(CorpusPos p) {
    const CorpusPos c = p / kChunk, o = p % kChunk;
    switch (c) {
        case 0: return "A";                                        // full container
        case 1: return (o % 3 == 0) ? "A" : "B";                   // dense bitmaps
        case 2: return (o % 700 == 5) ? "C" : (o % 2 ? "B" : "D"); // array C, bitmaps B / D
        case 3: return (o < 64 || o >= kChunk - 64) ? "A" : "B";   // A as array at the borders
        default: return (o % 5 == 0) ? "C" : ((o % 5 == 1) ? "A" : "B");
    }
}

void build_sample(const fs::path& dir) {
    StreamingBuilder b(dir.string());
    for (CorpusPos p = 0; p < kN; ++p) {
        std::unordered_map<std::string, std::string> attrs;
        attrs["form"] = "w" + std::to_string(p % 5000);
        attrs["t"] = value_at(p);
        b.add_token(attrs, -1);
        if ((p + 1) % kSentLen == 0 || p + 1 == kN) b.end_sentence();
    }
    b.add_region("text", 0, 2 * kChunk + 99, std::vector<std::pair<std::string, std::string>>{{"lang", "x"}});
    b.add_region("text", 2 * kChunk + 100, kN - 1, std::vector<std::pair<std::string, std::string>>{{"lang", "y"}});
    b.finalize();
}

void test_containers(const Corpus& corpus) {
    const PositionalAttr& pa = corpus.attr("t");
    std::string err;
    BitmapIndex::BuildStats st;
    CHECK(BitmapIndex::build(pa, corpus.size(), &err, &st));
    CHECK(st.fulls >= 1 && st.bitmaps >= 1 && st.arrays >= 1);
    BitmapIndex bi;
    CHECK(bi.open(pa, corpus.size()));
    CHECK(bi.nchunks() == 5);
    for (LexiconId v = 0; v < pa.lexicon().size(); ++v) {
        const RevSpan sp = pa.rev_span_of_id(v);
        std::vector<CorpusPos> got;
        for (const auto* e = bi.begin(v); e != bi.end(v); ++e) {
            std::vector<uint64_t> w(BitmapIndex::kWords, 0);
            bi.or_into(*e, w.data());
            size_t card = 0;
            for (size_t i = 0; i < w.size(); ++i)
                for (int j = 0; j < 64; ++j)
                    if (w[i] >> j & 1) {
                        got.push_back(static_cast<CorpusPos>(e->chunk) * kChunk + static_cast<CorpusPos>(i * 64 + j));
                        ++card;
                    }
            CHECK(card == e->card);
            CHECK((w[0]) == bi.first_word(*e));
        }
        CHECK(got.size() == sp.count);
        for (size_t i = 0; i < got.size(); ++i) CHECK(got[i] == sp.at(i));
    }
    std::cerr << "  PASS test_containers (" << st.fulls << " full, " << st.bitmaps << " bitmap, "
              << st.arrays << " array)\n";
}

struct Pattern {
    std::string cql;
    std::vector<std::function<bool(const std::string&)>> tok;   // one per token, empty = []
    bool within_s = false;
    bool lang_y = false;   // :: match.text_lang="y"
};

void test_kernel(const Corpus& corpus) {
    QueryExecutor ex(corpus);
    auto is = [](const char* v) { return [v](const std::string& s) { return s == v; }; };
    auto isnt = [](const char* v) { return [v](const std::string& s) { return s != v; }; };
    std::vector<Pattern> pats = {
        {R"([t="A"] [t="B"])", {is("A"), is("B")}},
        {R"([t="A"] [t="A"] [t="A"])", {is("A"), is("A"), is("A")}},
        {R"([t="A"] [] [t="B"])", {is("A"), nullptr, is("B")}},
        {R"([t!="B"] [t="A"])", {isnt("B"), is("A")}},
        {R"([t="C"|t="D"] [t="B"])", {[](const std::string& s) { return s == "C" || s == "D"; }, is("B")}},
        {R"([t="B"] [t="A"] within s)", {is("B"), is("A")}, true},
        {R"([t="A"] [] [] [] [] [] [t="A"] within s)", {is("A"), nullptr, nullptr, nullptr, nullptr, nullptr, is("A")}, true},
        {R"([t="A"] [t="B"] :: match.text_lang="y")", {is("A"), is("B")}, false, true},
        {R"([t!="B"])", {isnt("B")}},
        {R"([t="A" | t="C"])", {[](const std::string& s) { return s == "A" || s == "C"; }}},
        {R"([t!="A"] within s)", {isnt("A")}, true},
    };
    const PositionalAttr& pa = corpus.attr("t");
    for (const auto& pt : pats) {
        const CorpusPos L = static_cast<CorpusPos>(pt.tok.size());
        std::vector<CorpusPos> want;
        for (CorpusPos s = 0; s + L <= kN; ++s) {
            bool ok = true;
            for (CorpusPos k = 0; k < L && ok; ++k)
                if (pt.tok[static_cast<size_t>(k)] && !pt.tok[static_cast<size_t>(k)](std::string(pa.value_at(s + k)))) ok = false;
            if (ok && pt.within_s && s / kSentLen != (s + L - 1) / kSentLen) ok = false;
            if (ok && pt.lang_y && s < 2 * kChunk + 100) ok = false;
            if (ok) want.push_back(s);
        }
        Parser p(pt.cql + ";");
        Program prog = p.parse();
        CHECK(prog.size() == 1u);
        MatchSet all = ex.execute(prog[0].query, 0, true);
        if (all.plan_path != "seq_bitmap")
            std::cerr << "  note: " << pt.cql << " ran on " << all.plan_path << "\n";
        CHECK(all.plan_path == "seq_bitmap");
        CHECK(all.total_count == want.size());
        CHECK(all.matches.size() == want.size());
        for (size_t i = 0; i < want.size(); ++i) CHECK(all.matches[i].first_pos() == want[i]);
        MatchSet page = ex.execute(prog[0].query, 10, true);
        CHECK(page.total_count == want.size());
        CHECK(page.matches.size() == std::min<size_t>(10, want.size()));
        for (size_t i = 0; i < page.matches.size(); ++i) CHECK(page.matches[i].first_pos() == want[i]);
        if (want.size() > 20) {
            MatchSet cap = ex.execute(prog[0].query, 10, true, want.size() / 2);
            CHECK(cap.total_count == want.size() / 2);
        }
        std::cerr << "  PASS " << pt.cql << " (" << want.size() << ")\n";
    }
}

}  // namespace

int main() {
    fs::path dir = fs::temp_directory_path() / ("pando_bitmap_test_" + std::to_string(::getpid()));
    if (fs::exists(dir)) fs::remove_all(dir);
    fs::create_directories(dir);
    build_sample(dir);
    {
        Corpus corpus;
        corpus.open(dir.string(), false);
        test_containers(corpus);
    }
    {
        Corpus corpus;   // reopen: the executor finds the new .bm files
        corpus.open(dir.string(), false);
        test_kernel(corpus);
    }
    fs::remove_all(dir);
    std::cerr << "bitmap_index_test: all passed\n";
    return 0;
}
