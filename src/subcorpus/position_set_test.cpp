// EF-12: PositionSet in both encodings against a plain std::set reference; the
// definition parser (set expressions, names, errors).

#include "subcorpus/position_set.h"
#include "subcorpus/subcorpus.h"

#include <cstdio>
#include <cstdlib>
#include <random>
#include <set>
#include <string>

using namespace pando;

static int failures = 0;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); ++failures; } } while (0)

using Ref = std::set<int64_t>;

static PositionSet build(const std::vector<std::pair<int64_t, int64_t>>& runs, const std::string& enc) {
    PositionSet::Builder b;
    for (auto [a, e] : runs) b.add(a, e);
    return b.finish(enc);
}

static Ref ref_of(const std::vector<std::pair<int64_t, int64_t>>& runs) {
    Ref r;
    for (auto [a, e] : runs)
        for (int64_t p = a; p <= e; ++p) r.insert(p);
    return r;
}

static size_t ref_runs(const Ref& r) {
    size_t n = 0;
    int64_t prev = -2;
    for (int64_t p : r) {
        if (p != prev + 1) ++n;
        prev = p;
    }
    return n;
}

static void same(const PositionSet& s, const Ref& r, int64_t limit, const char* what) {
    if (s.tokens() != r.size() || s.runs() != ref_runs(r)) {
        std::fprintf(stderr, "FAIL %s (%s): tokens %llu/%zu runs %zu/%zu\n", what, s.encoding_name(),
                     (unsigned long long)s.tokens(), r.size(), s.runs(), ref_runs(r));
        ++failures;
        return;
    }
    for (int64_t p = 0; p < limit; ++p)
        if (s.contains(p) != (r.count(p) > 0)) {
            std::fprintf(stderr, "FAIL %s (%s): contains(%lld)\n", what, s.encoding_name(), (long long)p);
            ++failures;
            return;
        }
    Ref back;
    s.for_each_run([&](int64_t a, int64_t b) { for (int64_t p = a; p <= b; ++p) back.insert(p); });
    if (back != r) { std::fprintf(stderr, "FAIL %s (%s): runs differ\n", what, s.encoding_name()); ++failures; }
    // count_in on a few windows
    for (int64_t a : {int64_t{0}, int64_t{5}, int64_t{65530}, int64_t{70000}, limit / 3})
        for (int64_t len : {int64_t{0}, int64_t{1}, int64_t{10}, int64_t{70000}}) {
            uint64_t n = 0;
            for (auto it = r.lower_bound(a); it != r.end() && *it <= a + len; ++it) ++n;
            if (s.count_in(a, a + len) != n) {
                std::fprintf(stderr, "FAIL %s (%s): count_in(%lld,%lld) %llu != %llu\n", what, s.encoding_name(),
                             (long long)a, (long long)(a + len), (unsigned long long)s.count_in(a, a + len),
                             (unsigned long long)n);
                ++failures;
                return;
            }
            if (s.overlaps(a, a + len) != (n > 0)) { std::fprintf(stderr, "FAIL overlaps\n"); ++failures; return; }
        }
}

int main() {
    const int64_t N = 300000;   // crosses several 2^16 chunks
    std::mt19937 rng(7);
    for (int round = 0; round < 12; ++round) {
        // dense random tokens (bitmap containers), sparse ones (arrays), long ranges, both mixed
        std::vector<std::pair<int64_t, int64_t>> ra, rb;
        auto fill = [&](std::vector<std::pair<int64_t, int64_t>>& v, int kind) {
            if (kind == 0) for (int i = 0; i < 40; ++i) { int64_t a = rng() % N; v.push_back({a, std::min(N - 1, a + int64_t(rng() % 20000))}); }
            if (kind == 1) for (int i = 0; i < 3000; ++i) { int64_t a = rng() % N; v.push_back({a, a}); }
            if (kind == 2) for (int64_t p = 60000; p < 140000; ++p) if (rng() % 3 == 0) v.push_back({p, p});
            if (kind == 3) { v.push_back({0, 65535}); v.push_back({65536, 65600}); v.push_back({131071, 131072}); }
        };
        fill(ra, round % 4);
        fill(rb, (round + 1) % 4);
        if (round >= 8) fill(ra, (round + 2) % 4);
        const Ref A = ref_of(ra), B = ref_of(rb);
        for (const char* ea : {"ranges", "roaring", ""})
            for (const char* eb : {"ranges", "roaring"}) {
                PositionSet a = build(ra, ea), b = build(rb, eb);
                same(a, A, N, "build a");
                same(b, B, N, "build b");
                Ref u = A, i, d;
                u.insert(B.begin(), B.end());
                for (int64_t p : A) { if (B.count(p)) i.insert(p); else d.insert(p); }
                same(PositionSet::unite(a, b), u, N, "union");
                same(PositionSet::intersect(a, b), i, N, "intersection");
                same(PositionSet::subtract(a, b), d, N, "difference");
                same(a.reencoded(PositionSet::Encoding::Bitmap), A, N, "reencoded bitmap");
                same(a.reencoded(PositionSet::Encoding::Ranges), A, N, "reencoded ranges");
            }
    }
    // the automatic choice: few long ranges → ranges; many single positions → roaring
    {
        PositionSet::Builder b;
        for (int64_t d = 0; d < 100; ++d) b.add(d * 10000, d * 10000 + 5000);
        CHECK(b.finish().encoding() == PositionSet::Encoding::Ranges);
        PositionSet::Builder c;
        for (int64_t p = 0; p < 2000000; p += 2) c.add(p);
        PositionSet s = c.finish();
        CHECK(s.encoding() == PositionSet::Encoding::Bitmap);
        CHECK(s.tokens() == 1000000);
        CHECK(s.runs() == 1000000);
    }
    // positions out of order and overlapping
    {
        PositionSet::Builder b;
        b.add(10, 20); b.add(5, 7); b.add(8, 9); b.add(15, 30); b.add(100);
        PositionSet s = b.finish("ranges");
        CHECK(s.tokens() == 27);
        CHECK(s.runs() == 2);
    }
    // the definition parser
    {
        auto e = parse_subcorpus_definition("(t:<text> :: t.text_genre = \"a|b\") & (x:<text year=\"1990\">) | B - (C)");
        CHECK(e->kind == SubcorpusExpr::Kind::Op && e->op == '-');
        CHECK(e->left->op == '|');
        CHECK(e->left->left->op == '&');
        CHECK(e->left->left->left->kind == SubcorpusExpr::Kind::Query);
        CHECK(e->left->left->left->text == "t:<text> :: t.text_genre = \"a|b\"");
        CHECK(e->left->right->kind == SubcorpusExpr::Kind::Name && e->left->right->text == "B");
        CHECK(e->right->kind == SubcorpusExpr::Kind::Name && e->right->text == "C");
        CHECK(parse_subcorpus_definition("[upos=\"NOUN\"];")->kind == SubcorpusExpr::Kind::Query);
        CHECK(is_subcorpus_set_expression("A & B"));
        CHECK(!is_subcorpus_set_expression("a:[upos=\"NOUN\"]"));
        CHECK(!is_subcorpus_set_expression("[upos=\"NOUN\" & lemma=\"x\"]"));
        auto bad = [](const std::string& s) {
            try { parse_subcorpus_definition(s); } catch (const SubcorpusError&) { return true; }
            return false;
        };
        CHECK(bad(""));
        CHECK(bad("(A) &"));
        CHECK(bad("(A & B"));
        CHECK(bad("(A) ^ (B)"));
        CHECK(bad("(A) (B)"));
    }
    if (failures) { std::fprintf(stderr, "%d failure(s)\n", failures); return 1; }
    std::printf("position_set: ok\n");
    return 0;
}
