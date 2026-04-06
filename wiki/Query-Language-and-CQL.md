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

**Intent:** A **partial** CWB/CQP-style front-end (lexer + recursive descent) that turns **token query** text into Pando’s `Program`: **named** assignments (`Name = pattern`), **anonymous** search patterns, and restricted **`count by <attr>`** / **`group by <field>[, …]`** commands. Multi-statement input is split on **`;`** (outside quoted strings).

**Well supported relative to other dialects** because pando-CQL is modelled on CWB-style token patterns and attribute tests. Quoted-string behaviour for **`=`** inside **`[ ]`** follows the same **literal vs regex** heuristic as native CQL when **`--strict-quoted-strings`** is off (see [PANDO-CQL.md](PANDO-CQL.md)).

**Supported (representative):**

- **Named and anonymous** `RegWordfExpr`-style token sequences with **`[ attr op value ]`**, **`[]`**, repetition **`? * + {m,n}`**, sequence concatenation.
- **`count by <attribute>`** with basic forms (see trace output for ignored modifiers).
- **`group by <field>[, <field> …]`** mapped to the native **`group`** command. Fields use Pando’s **`name.attribute`** form; CWB-style **`name attribute`** (space instead of a dot) is accepted and normalized (e.g. `match lemma` → `match.lemma`). **`match.lemma`** is also accepted as a single dotted token.

**Not supported in this dialect** (the parser throws with a specific message; use **`--cql native`** for full Pando):

- **CQP shell commands** at statement start: `sort`, `cat`, `save`, `show`, `set`, `tabulate`, `discard`, **`intersect` / `union` / `diff` / `join` / `subset`**, `meet`, `info`, `dump`, `size`, `cut`, `mu`, `tab`, `exec`, macros, and similar keywords.
- **`meet` / `join` on named query results** (same class as shell combinators above).
- **Pattern features** not mapped to the native condition AST, including: **`within` / `containing`** after the pattern; **global `::`** constraints after the pattern; **alternation `|`** between full token-sequence patterns at the top level; **parenthesised groups** with repetition in some forms; **lookahead** `[: … :]` / `[::]`; **boolean `!`** and **`->`** inside **`[ ]`**; **MU** / **TAB** query forms; **XML/anchor tags** in the pattern; **region append `<<`**; **`cut` / `show match …`** tails; **redirection** / **`into outfile`** (Pando has no query-directed file output).
- **`count` / `group`**: `on match` / anchor boundaries, **`cut`**, shell-style **`>`** redirection.

**Security / I/O:** There is no **`into outfile`** or other query-driven disk write; use shell redirection on the CLI if you need a file.

---

## PML-TQ (`--cql pmltq`)

**Intent:** Accept **PML-TQ–shaped** text, parse it, and lower it to a native **`Program`**. PML-TQ in the wild targets **multi-layer PML trees**; Pando’s engine is **token- and region-centric**. The bridge therefore targets **token + region** corpora (e.g. TEITOK-style imports), not arbitrary PML XML graphs.

**Two-stage translation:**

1. **Native lexer/parser** — accepts a **single** query shape: one **selector** (`a-node`, `a-root`, `tok`, or a bare **`[ … ]`** token selector), optional **`$name :=`**, a condition in **`[ … ]`**, and an optional **`>>`** output block. If the input does not fit this grammar, parsing fails and the implementation may try (2).
2. **Gold AST bridge** (optional) — if environment variable **`PMLTQ_GOLD_JS_DIR`** is set, the query string is passed to an external **Node.js** pipeline (PEG parse → JSON AST). That AST is then lowered to a native program. **`PMLTQ_GOLD_SCRIPT`** can override the parser script path. This path is for **broader** PML-TQ surface coverage when the tooling is installed.

If neither path succeeds, translation fails (the error text mentions whether the gold bridge was unavailable).

**Native selector keywords:** **`a-node`**, **`a-root`**, **`tok`** — not arbitrary PML node type names.

**Note (`t-node` and fuller PML-TQ):** In many PML-TQ corpora, **`t-node`** names the **terminal / token** layer (distinct from **`a-node`** analytics). The **native** parser here does **not** yet treat **`t-node`** as a selector keyword (use **`tok`** or the gold bridge path where applicable). Whether **`t-node`** should become a first-class synonym for **`tok`**, or whether a richer mapping from PML layers is needed, depends on **what PML-TQ users expect** in this corpus model and on **how far** broader PML-TQ support is taken (native grammar growth vs gold bridge vs staying token+region–focused). That is a **product/roadmap** decision, not fixed in the engine alone.

**`--pmltq-export-sql`:** Emits **ClickPMLTQ-style SQL** only (no corpus load). Requires **`--cql pmltq`**, a query on the command line, and the **JavaScript** helpers described in **`pando --help`** (typically a **`pmltq2sql-optimized.js`** next to the PEG bundle, plus **`PMLTQ_GOLD_JS_DIR`**).

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

## Quick comparison

|  | **cwb** | **pmltq** | **tiger** |
|--|---------|-----------|-----------|
| **Build** | Optional library | Optional library | Always |
| **Typical use** | CQP-like token queries, `count by` | PML-TQ experiments, TEITOK token+region | Constituency macros |
| **Full language parity** | No — subset | No — native grammar small; gold bridge optional | No — three macros + pass-through |

For normative **native** syntax, **[PANDO-CQL.md](PANDO-CQL.md)** remains the reference. For CLI flags that affect parsing (e.g. **`--strict-quoted-strings`**), see [CLI reference](CLI-Reference.md).
