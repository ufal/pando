# Query languages

In Pando, queries default to **pando-CQL**, described in **[PANDO-CQL.md](PANDO-CQL.md)** in this folder. The wiki pages summarize topics (multivalue, regions, dependencies, alignment, …) and link there for normative syntax.

For continuity with other tools, Pando can accept **dialect front-ends** (`--cql …`) that translate into the native AST and then run on the same engine. Translation covers **only** what the dialect layer implements; anything outside that surface should be written in **native** CQL.

| Dialect | Role |
|--------|------|
| **`native`** | Default Pando CQL |
| **`pmltq`** | PML-TQ–flavoured surface; translated to the native AST (see below) |
| **`tiger`** | TIGERSearch-style **macros** for constituency-oriented fragments |
| **`cwb`** | CWB/CQP **subset** adapter (optional build; see below) |

Run **`pando --help`** for the dialects compiled into your binary (`cwb` appears only when that dialect is enabled).

The same dialects through the server and the library (pando-server, libflexicorp_pando, the C API):
the request field **`"dialect"`** on `/query` and `/run` (`native` / `pando-cql`, `cwb` / `cwb-cql`,
`pmltq`, `tiger`; an unknown name is a 400). One entry point for the CLI and the server,
`parse_in_dialect` (`src/query/dialect/dialect.h`); the dialect is part of the result cache's and the
count jobs' keys, and the tier's denied features are checked on the translated query. FQS passes its
`language` flag on as this field. pando-server also takes the language as the query's first word,
in capitals, as FQS does (`PMLTQ a-node [ lemma = "be" ]`; `PANDO`, `CWB`, `CQP`, `PMLTQ`, `PML-TQ`,
`TIGER`): the word is taken off and replaces `"dialect"`, so a front end without a language choice
can send another dialect as it is. The answer's `query.language` names the language used.

A program can start with a query in a dialect and go on with pando's own steps: when the dialect
cannot read the whole text, the part up to the first `;` (outside quotes and brackets) is read in the
dialect and the rest as pando CQL (`a-node [ lemma = "be" ]; freq by upos` with `pmltq`). This is how
PML-TQ queries are followed up: pando's `freq` / `count by` / `coll` steps instead of PML-TQ's `>>`
output filters.

---

## Availability in builds

| Dialect | In tree | CMake |
|--------|---------|--------|
| **native** | always | — |
| **tiger** | always | linked into `pando` (no separate toggle) |
| **cwb** | optional | **`PANDO_CWB_DIALECT`** (default **ON**); **OFF** replaces the translator with a stub that errors at runtime |
| **pmltq** | optional | **`PANDO_PMLTQ_DIALECT`** (default **ON**); **OFF** replaces the translator with a stub |

To disable a dialect: configure with **`-DPANDO_CWB_DIALECT=OFF`** or **`-DPANDO_PMLTQ_DIALECT=OFF`**. The CLI **`--help`** text matches what was linked in.

---

## CWB / CQP (`--cql cwb`)

**Intent:** A **partial** CWB/CQP-style front-end (lexer + recursive descent) that turns **token query** text into Pando’s `Program`: **named** assignments (`Name = pattern`), **anonymous** search patterns, and selected **shell-style** commands mapped to native `GroupCommand` (e.g. **`count by`**, **`group by`**, **`sort by`**, **`size`**, **`cat`**, **`tabulate`**). Multi-statement input is split on **`;`** (outside quoted strings). Output formatting is **Pando’s**, not CQP’s.

**Well supported relative to other dialects** because pando-CQL is modelled on CWB-style token patterns and attribute tests. Quoted-string behaviour for **`=`** inside **`[ ]`** follows the same **literal vs regex** heuristic as native CQL when **`--strict-quoted-strings`** is off; a backslash reaches the pattern as in CQP (`[word="\."]` is a full stop; see the [CQL reference](PANDO-CQL-Reference.md#values-and-regular-expressions)).

**Supported (representative):**

- **Named and anonymous** `RegWordfExpr`-style token sequences with **`[ attr op value ]`**, **`[]`**, repetition **`? * + {m,n}`**, sequence concatenation.
- **Bare strings** as in CQP: `"the"` is `[word="the"]` (one token per hit), with flags (`"the" %c`) and in sequences (`"in" "the"`).
- **`!=` with a pattern** (`[lemma != ".*e.*"]`: tokens whose lemma does not match), and the flags **`%c`** (also on patterns: `"the.*"%c`, as in CQP — native pando-CQL keeps `%c` for literal comparisons), **`%d`** (literal comparisons) and **`%l`** (literal, also after `!=`).
- **`count by <attribute>`** with basic forms (see trace output for ignored modifiers).
- **`group by <field>[, <field> …]`** mapped to the native **`group`** command. Fields use Pando’s **`name.attribute`** form; CWB-style **`name attribute`** (space instead of a dot) is accepted and normalized (e.g. `match lemma` → `match.lemma`). **`match.lemma`** is also accepted as a single dotted token.
- **`sort [Name] by <field> [%c|%d|%cd] [on <from>..<to>]`** → native **`sort`** of the named query (or `Last`), with CQP's semantics: the key is the whole match (`match`..`matchend`) unless boundaries are given; `%c` / `%d` sort case- / diacritic-insensitively; boundaries are `match`, `matchend` or a token label with an optional offset (`on matchend[1]..matchend[5]` sorts on the right context, `on match[-1]..match[-5]` on the left context read from right to left). Boundaries beyond the corpus are clamped to its first / last token, as in CQP. Equal keys keep corpus order (CQP's order among them is arbitrary).
- **`size`** → native **`size`** (optional query name).
- **`cat [Name] [from [to]]`** → native **`cat`**: the hits in their current order (sorted after `sort`); `from` / `to` are hit numbers from 0, `to` included. Without `to`, pando shows a page (the limit) from `from`, where CQP shows every hit to the end; redirection (`> "file"`) is not supported.
- **`tabulate`** → native **`tabulate`**: `tabulate [offset limit] field[, …]`, `tabulate QueryName offset limit field[, …]`, or `tabulate field[, …]` (default offset `0`, limit `1000`).

**Not supported in this dialect:** the parser stops with a message starting with `Unsupported:`.
It reads global constraints `::` (before `within`, as CQP), the `@` target marker, XML tags,
`->` and lookahead; what it does not read (`|` between sequences, repeated groups, `within N`, `cut`,
MU queries, set operations on named results, shell commands, …), what it reads
differently from CQP, and what to write in pando CQL instead are listed per construct in
**[CWB-Dialect.md](CWB-Dialect.md)**.

**Security / I/O:** There is no **`into outfile`** or other query-driven disk write; use shell redirection on the CLI if you need a file.

---

## PML-TQ (`--cql pmltq`)

**Intent:** Accept **PML-TQ–shaped** text, parse it, and lower it to a native **`Program`**. PML-TQ in the wild targets **multi-layer PML trees**; Pando’s engine is **token- and region-centric**. The bridge therefore targets **token + region** corpora (e.g. TEITOK-style imports), not arbitrary PML XML graphs.

**Two-stage translation:**

1. **Native lexer/parser** — follows the query part of perl-pmltq's grammar (`tree_query_grammar.txt`) and builds pando's query AST directly (no CQL text in between). If the input does not fit, parsing fails and the implementation may try (2).
2. **Gold AST bridge** (optional) — if environment variable **`PMLTQ_GOLD_JS_DIR`** is set, the query string is passed to an external **Node.js** pipeline (PEG parse → JSON AST). That AST is then lowered to a native program. **`PMLTQ_GOLD_SCRIPT`** can override the parser script path.

If neither path succeeds, translation fails (the error text mentions whether the gold bridge was unavailable).

**What the native parser reads:**

- **One top-level selector**: a node type (`a-node`, `t-node`, `node`, `*`, … — all read as a token; or none: `[ … ]`), an optional **`$name :=`**, and the conditions in `[ … ]`.
- **Conditions** joined by **`,`** or **`and`**: tests with **`=` / `!=`** (the value as it is), **`~` / `!~`** (a regular expression anywhere in the value), **`~*` / `!~*`** (ignoring case), **`<` `>` `<=` `>=`**, **`in { … }` / `!in { … }`**; **`or`**, **`!`** and parentheses.
- **Nested nodes** with **`child`** (the default), **`parent`**, **`descendant`**, **`ancestor`**, **`sibling`**. As in PML-TQ, a nested node in the condition list is a **node of the match**: `a-node [ upos = "VERB", child a-node [ deprel = "obj" ] ]` gives one result per verb–object pair, and becomes the chain `n1:[upos="VERB"] > n2:[deprel="obj"]`. Names given with `$o :=` are the tokens' names, so later steps can use them (`; freq by o.lemma`).
- **Subqueries** — a nested node with **occurrences** (`0x`, `1+x`, `2x`, `2-x`, `1..3x`, alternatives `0|2+x`), or after `and` / under `or` / `!` (as 1+x) — are **conditions** on the outer node: `child [...]`, `not child [...]`, `count(child [...]) >= 2`. One result per outer node.
- **References** between nodes of the match: `lemma = $v.lemma` (the same value on both), `lemma != $v.lemma` (different values), `$a != $b` (different nodes).
- **Word order of two nodes:** `order-follows $o1` / `order-precedes $o1` in a node's condition list relate it to the node named `$o1` elsewhere in the query: pando's position order `:: o2 > o1` / `:: o2 < o1`. Other relations to a named node (`child $o`) are refused (write the node there: `child a-node $o := [ … ]`), as are these references under `!` / `or` or in a subquery. `child $o := [ … ]` (a named node without a type) is read as `child a-node $o := [ … ]`.
- Different nodes are different tokens (PML-TQ; `+` before a nested node lets it overlap).
- **`a-root`**: pando has no root node, so it is a condition — `a-root [ child a-node X ]` is X with `deprel = "root"` (the sentence's root word), `a-root [ descendant a-node X ]` is any X, `parent a-root []` is `deprel = "root"` (with `0x`: not the root word), `ancestor a-root []` always holds. Next to that one node, **counted nodes** under `a-root` are conditions on the sentence's dependency tree: `a-root [ 20-x descendant a-node [], descendant a-node $v := [ … ] ]` is `$v` in a sentence with at most 20 words in the tree (root words and words with a head, as PML-TQ counts the nodes below its technical root), `1+x descendant a-node [ upos = "INTJ" ]` a sentence with an interjection, `2x child a-node []` a sentence with two root words. The node of the match itself may be any word of the sentence. `a-root` alone, with other conditions, with counted nodes only, or with two nodes of the match is refused.

**Shapes of the match.** pando matches the nodes as a chain of tokens. Nodes with the same head — the
direct dependents of one node, and siblings — follow each other with the relation `~` (same head):

```text
a-node [ upos = "VERB", child a-node [ deprel = "nsubj" ], child a-node [ deprel = "obj" ],
         child a-node [ deprel = "iobj" ] ]
→ n1:[upos="VERB"] > n2:[deprel="nsubj"] ~ n3:[deprel="obj"] ~ n4:[deprel="iobj"]
```

So a node may have any number of direct dependents in the match, and a parent or ancestor above
it, as long as **at most one** of those dependents has matched nodes of its own (it ends the run:
`… ~ n4 > x`). One result per combination of nodes, all different words, as in PML-TQ. A tree that
branches further — two dependents each with their own dependents, and a parent above — is refused
with a message saying so; give the extra nodes occurrences to make them conditions
(`1+x child a-node [ … ]`). `descendant` / `ancestor` nodes are related by their path, not by `~`.

**What it does not read:** trees that branch further than above, optional nodes (`?child`), relations other than the five above (`order-precedes`, `depth-first-…`, `same-tree-as`, PML references), lengths (`descendant{1,3}`), functions in tests (`sons()`, `lower()`), several top-level selectors, and `member`. Of the **`>>`** output filters only `>> distinct $a.attr, …` and `count(over $a.attr)`; a query is followed up with pando's own steps instead: `a-node $v := [ … ]; freq by v.lemma` (see above).

**`--pmltq-export-sql`:** Emits **ClickPMLTQ-style SQL** only (no corpus load). Requires **`--cql pmltq`**, a query on the command line, and the **JavaScript** helpers described in **`pando --help`** (typically a **`pmltq2sql-optimized.js`** next to the PEG bundle, plus **`PMLTQ_GOLD_JS_DIR`**).

**Gaps / difficulty:** What the dialect does not lower (especially **`>>` output filters**, trees that branch further than one run of same-head nodes, multi-selector queries) is inventoried with effort estimates in **[`dev/PMLTQ-ROADMAP.md`](../dev/PMLTQ-ROADMAP.md)**.

---

## TIGERSearch-style macros (`--cql tiger`)

**Intent:** Small **line-oriented** preprocessor for constituency-friendly queries on corpora with **`node`** (or similar) **region** annotations. Each non-empty, non-comment line is either expanded or passed through unchanged, then the combined text is parsed as **native CQL**.

| Line prefix | Expansion (conceptually) |
|-------------|---------------------------|
| **`dom PARENT CHILD`** | Two named **`node`** anchors and **`:: contains(parent, child) = 1`** (geometric containment of spans). |
| **`idom PARENT CHILD`** | Two named **`node`** anchors and **`:: rchild(parent, child) = 1`** (immediate dominance via **`.par`** on nested structures). |
| **`cat CATEGORY`** | Region anchor for **`node type=CATEGORY`** and a token. |
| **`# …`** | Comment (line skipped). |
| *anything else* | Treated as **native CQL** for that segment. |

Lines are joined with **`;`** between expansions. **`idom`** needs a **nested** constituency-style index with parent links (see [Constituency and nested regions](Constituency-and-Nested-Regions.md)).

**Examples:**

```text
dom S NP
idom VP NP
cat NP
```

```text
dom S NP
cat VP
```

**Scope:** This is **not** a full TIGERSearch or TIGER XML query language — only the macros above plus pass-through native CQL.

---

## Hit counts compared with Manatee / KonText and CQP

The same query on the same data can give a different number of hits in pando, Manatee and CQP.
When it does, it is one of these counting rules:

- **Sentence boundaries.** Like Manatee and CQP, pando does not keep a sequence inside a sentence
  unless the query says so (`[] [] within s`). An index can set a default: `default_within`
  (`"default_within": "text"` in the JSONL header, then `default_within=text` in `corpus.info`)
  applies to every query without a `within` of its own. If pando counts fewer hits than Manatee,
  check `corpus.info` for `default_within` first.
- **Variable lengths.** For a pattern of variable length (`[upos="ADJ"] []{0,3} [upos="NOUN"]`)
  pando and Manatee count every variant that matches; CQP keeps at most one match per start
  position (its default matching strategy), so it reports fewer.
- **Dependency queries.** Some engines report a head once however many matching dependents it
  has; pando reports one hit per combination of matched tokens (as PML-TQ does).
- **Regular expressions and quoted strings.** In pando-CQL `"…"` is a regular expression when it
  contains regex characters, unless `--strict-quoted-strings` is set; `%c` / `%d` fold case and
  diacritics as in CQP (see [the reference](PANDO-CQL-Reference.md#values-and-regular-expressions)).

[Engine benchmark](Engine-Benchmark.md#count-discrepancies) lists the differences measured on real
corpora.

## Quick comparison

|  | **cwb** | **pmltq** | **tiger** |
|--|---------|-----------|-----------|
| **Build** | Optional library | Optional library | Always |
| **Typical use** | CQP-like token queries, `count by` | PML-TQ experiments, TEITOK token+region | Constituency macros |
| **Full language parity** | No — subset | No — native grammar small; gold bridge optional | No — three macros + pass-through |

For **native** syntax, see the tutorial **[PANDO-CQL.md](PANDO-CQL.md)** and the **[reference](PANDO-CQL-Reference.md)**. For CLI flags that affect parsing (e.g. **`--strict-quoted-strings`**), see [CLI reference](CLI-Reference.md).
