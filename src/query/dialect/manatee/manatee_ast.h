#pragma once

// The parse tree of a Manatee CQL query, one node kind per construct of Manatee's grammar (query/cqpeval.y):
// manatee_parser builds it, manatee_lower turns it into pando's AST. Kept close to the grammar so what Manatee
// accepts and rejects can be checked rule by rule.

#include <memory>
#include <string>
#include <vector>

namespace pando::manatee {

// ── attribute conditions inside […] (attvallist) ──
struct AttVal;
using AttValPtr = std::shared_ptr<AttVal>;
struct AttVal {
    enum class Kind {
        Cmp,        // attr OP "re"
        Not,        // ! attval
        And, Or,    // a & b, a | b (left, right)
        PosNum,     // #N or #N -M: the corpus position(s)
        Func,       // ws(…), term(…), swap(…), ccoll(…), a thesaurus ~ in a […]
    } kind = Kind::Cmp;
    std::string attr;
    enum class Op { Eq, Neq, EqLit, NeqLit, Leq, NotLeq, Geq, NotGeq } op = Op::Eq;
    std::string value;          // the regexp (unescaped)
    AttValPtr left, right;      // Not: left; And / Or: both
    long long num1 = 0, num2 = 0;
    std::string func;           // Func: its name
    size_t pos = 0;
};

// ── a structure: <s>, <s/>, </s>, <doc file="x" | id="y"/> ──
struct Structure {
    std::string name;
    AttValPtr attrs;            // null: the structure as a whole
    size_t pos = 0;
};

struct Query;
using QueryPtr = std::shared_ptr<Query>;

// ── one position (oneposition, with an optional numeric label) ──
struct Position {
    enum class Kind { Regexp, Bracket, Any, Thesaurus, Meet, Union } kind = Kind::Any;
    long long label = -1;       // N: …; -1 = none
    std::string regexp;         // Regexp: on the default attribute
    AttValPtr attrs;            // Bracket
    std::shared_ptr<Position> a, b;   // Meet / Union operands
    bool meet_window = false;   // meet with its two numbers
    long long from = 0, to = 0;
    size_t pos = 0;
};
using PositionPtr = std::shared_ptr<Position>;

// ── a repetition: an atom with an optional repeat, or a structure tag ──
struct Repetition {
    enum class Kind { Position, Group, StructBegin, StructWhole, StructEnd } kind = Kind::Position;
    PositionPtr position;
    QueryPtr group;             // ( sequence ) or ( query with within / containing )
    Structure structure;
    bool repeated = false;
    long long min = 1, max = 1; // max -1 = unbounded
    size_t pos = 0;
};

// a seq: repetitions one after the other
struct Seq {
    bool negated = false;       // ! seq (its complement)
    std::vector<Repetition> items;
};

// a sequence: alternatives of seqs (`A | B`)
struct Sequence {
    std::vector<Seq> alternatives;
};

// ── global conditions after `&` ──
struct Cond;
using CondPtr = std::shared_ptr<Cond>;
struct Cond {
    enum class Kind { And, Or, Not, AttrEq, AttrNeq, Freq } kind = Kind::AttrEq;
    CondPtr left, right;        // And / Or: both; Not: left
    long long l1 = 0, l2 = 0;   // labels
    std::string a1, a2;         // attributes
    enum class Cmp { Eq, Neq, Leq, Geq, Lt, Gt } cmp = Cmp::Eq;   // Freq
    long long value = 0;
    size_t pos = 0;
};

// ── a within / containing suffix ──
struct Suffix {
    enum class Kind { Number, NotNumber, Sequence, Aligned } kind = Kind::Sequence;
    long long number = 0;
    Sequence sequence;          // Sequence, Aligned
    std::string corpus;         // Aligned: the aligned corpus's name
    bool aligned_not = false;   // within ! corpus: …
    size_t pos = 0;
};

struct Clause {
    bool containing = false;    // else within
    bool negated = false;       // ! within / ! containing
    Suffix suffix;
};

// ── a query: a sequence, with global conditions or within / containing clauses (left to right) ──
struct Query {
    Sequence sequence;
    CondPtr conditions;         // sequence & conditions
    std::vector<Clause> clauses;
};

}  // namespace pando::manatee
