#pragma once

// EF-12: subcorpus definitions, resolved to position sets (position_set.h) and cached.
//
// A definition is
//   - a query (pando-CQL, or a language prefix `CWB …`): region-based
//     (`t:<text> :: t.text_genre = "fiction"`, `t:<text genre="fiction">`): each hit's
//     span; token-based (`[upos="NOUN"]`): its tokens, merged into runs;
//   - a set expression over parenthesised definitions and names:
//     `(A) & (B)` intersection, `(A) | (B)` union, `(A) - (B)` difference; `&` binds
//     tighter than `|` and `-`, which go left to right; a name is a subcorpus or hit
//     set of the session (SubcorpusLookup), expanded into its definition.
// A resolved set is cached by (index identity, the definition with its names expanded).

#include "subcorpus/position_set.h"

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace pando {

class Corpus;
class PositionalAttr;
struct ExecProgress;

/// An invalid definition (HTTP 400 with this message).
struct SubcorpusError : std::runtime_error {
    explicit SubcorpusError(const std::string& m) : std::runtime_error(m) {}
};

struct SubcorpusExpr {
    enum class Kind { Query, Name, Op } kind = Kind::Query;
    std::string text;     // Query: the query; Name: the name
    char op = 0;          // Op: '&', '|', '-'
    std::shared_ptr<const SubcorpusExpr> left, right;
};
using SubcorpusExprPtr = std::shared_ptr<const SubcorpusExpr>;

/// Parse a definition (throws SubcorpusError).
SubcorpusExprPtr parse_subcorpus_definition(const std::string& text);
/// True when `text` is a set expression (starts with `(`, or NAME followed by an operator),
/// not a query.
bool is_subcorpus_set_expression(const std::string& text);
/// A valid subcorpus / hit set name: letters, digits, '_' (not starting with a digit).
bool is_subcorpus_name(const std::string& s);

/// name → its definition text (nullopt: unknown name).
using SubcorpusLookup = std::function<std::optional<std::string>(const std::string& name)>;

/// A resolved subcorpus: its positions, and what statistics over it need (cached with it).
class ResolvedSubcorpus {
public:
    ResolvedSubcorpus(std::string key, std::string index_id, std::shared_ptr<const PositionSet> set)
        : key_(std::move(key)), index_id_(std::move(index_id)), set_(std::move(set)) {}
    /// The definition with names expanded, fully parenthesised (the cache key).
    const std::string& key() const { return key_; }
    const std::string& index_id() const { return index_id_; }
    const PositionSet& set() const { return *set_; }
    const std::shared_ptr<const PositionSet>& set_ptr() const { return set_; }
    /// Occurrences of every lexicon id of `pa` inside the subcorpus (collocation baselines,
    /// keyness reference): counted once per attribute, then kept.
    std::shared_ptr<const std::vector<uint64_t>> id_counts(const std::string& attr_name, const PositionalAttr& pa) const;
    /// Anything else derived from the subcorpus once and kept with it (dcoll relation totals, …).
    std::shared_ptr<const void> memo(const std::string& key, const std::function<std::shared_ptr<const void>()>& make) const;
private:
    std::string key_, index_id_;
    std::shared_ptr<const PositionSet> set_;
    mutable std::mutex mu_;
    mutable std::map<std::string, std::shared_ptr<const std::vector<uint64_t>>> id_counts_;
    mutable std::map<std::string, std::shared_ptr<const void>> memo_;
};
using ResolvedSubcorpusPtr = std::shared_ptr<const ResolvedSubcorpus>;

struct SubcorpusResolveOptions {
    SubcorpusLookup lookup;              // names (none: a name is an error)
    ExecProgress* progress = nullptr;    // the definition queries' progress / cancel
};

/// Resolve `definition` on `corpus` (cached). Throws SubcorpusError for an invalid
/// definition (also a query that does not parse), QueryCancelled when cancelled.
ResolvedSubcorpusPtr resolve_subcorpus(const Corpus& corpus, const std::string& definition,
                                       const SubcorpusResolveOptions& opts = {});
/// The intersection of two resolved subcorpora (`use A` with `within B`), cached as `(A) & (B)`.
ResolvedSubcorpusPtr intersect_subcorpora(const Corpus& corpus, const ResolvedSubcorpusPtr& a,
                                          const ResolvedSubcorpusPtr& b);

/// The resolver cache's size limit (bytes of position sets; default 256 MiB) and drop.
void set_subcorpus_cache_bytes(size_t bytes);
void clear_subcorpus_cache();

/// A subcorpus as a request or session applies it: `use` (the subcorpus is the corpus:
/// N, ipm, collocation baselines, keyness reference) or `within` (a filter, statistics of
/// the whole corpus).
struct AppliedSubcorpus {
    std::string name;      // for reporting ("" = none given)
    std::string mode = "use";
    ResolvedSubcorpusPtr sc;
    bool is_use() const { return mode == "use"; }
};
using AppliedSubcorpusPtr = std::shared_ptr<const AppliedSubcorpus>;

/// `"subcorpus": {"name", "mode", "tokens", "ranges", "encoding"}` (a JSON member, no braces around it).
std::string subcorpus_json_member(const AppliedSubcorpus& a);
/// The same member inserted at the top of a JSON object answer.
std::string with_subcorpus_member(std::string json, const AppliedSubcorpus& a);

} // namespace pando
