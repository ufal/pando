# Pando-CQL reference

This page collects the precise rules and details of pando-CQL. For a gentle introduction, start with
[PANDO-CQL.md](PANDO-CQL.md); this page assumes you know the basics from there. Other dialects
(`--cql cwb`, `--cql pmltq`, `--cql tiger`) are described in [Query-Language-and-CQL.md](Query-Language-and-CQL.md).

## Values and regular expressions

- **Quoted values** (`[form="the"]`) are compared as whole values. For compatibility with CWB and
  Manatee, a quoted value that contains regular-expression characters is a regular expression that
  must match the **whole** value: `[form=".*tion"]` finds words ending in *-tion*. The setting
  `strict_quoted_strings` turns this off, so quoted values are always literal (`".*tion"` is then the
  string *.\*tion*), also after `!=`.
- **Escapes.** A backslash keeps a special character literal inside a quoted pattern: `[form="\."]`
  is a full stop, `[form="\?"]` a question mark (`"."` alone is any one-character token, as in CWB).
  A value that is a single character with no meaning as a whole-token pattern (`"?"`, `"*"`, `"+"`,
  `"("`, `")"`, `"["`, `"]"`, `"{"`, `"}"`, `"|"`, `"\\"`) is that character.
- **Slash values** (`[form=/pan.*tion/]`) are plain regular expressions that match **anywhere** in the
  value: `/tion/` finds *conditional*. Anchor them yourself: `/tion$/`, `/^un/`.
- **`!=`** takes both forms and matches the tokens whose value does *not* match:
  `[lemma != ".*e.*"]` (whole value) and `[lemma != /e/]` (substring) both find lemmas without an *e*.
- **`!`** negates a whole condition: `[!(lemma="the" | upos="NOUN")]` = `[lemma!="the" & upos!="NOUN"]`;
  `[!lemma="be.*"]` = `[lemma!="be.*"]` (the CWB dialect accepts it too). `!` is not allowed on
  structural conditions; write `not child [...]`, `not next [...]`.
- **Regex engine.** With RE2 (the recommended build) the syntax is RE2's, a Perl/PCRE subset as in CWB
  and Manatee: `.` matches one character, including non-ASCII ones (`"h.t"` finds *hát*), `(?i)` makes a
  pattern case-insensitive (`[lemma="(?i)praha"]`; for a plain word `%c` is faster), classes like `\d`,
  `\w`, `[[:upper:]]` work. Backreferences and lookaround, which RE2 lacks, fall back to std::regex
  (ECMAScript). A build without RE2 uses std::regex throughout, where `.` matches one *byte* of UTF-8.
- **How a regex is evaluated.** A regex on an attribute is matched once per query against the
  attribute's lexicon (its distinct values), not per token. A literal prefix (`un.*`) narrows that to a
  range of the sorted lexicon; a literal the value must contain (`.*ung`, also with `%c` or `(?i)`) is
  checked before the regex; a large lexicon is scanned by several threads. On a 38M-token corpus with
  3.1M word forms `[form=".*ung"]` takes about 20 ms, `[form="[A-Z].*"]` (no literal) about 150 ms on two
  cores. The set of lexicon entries a pattern matches is kept per corpus, so the page, the background
  total and later pages of the same query do not scan again.

## Case and diacritics: `%c`, `%d`, `%cd`

The flags work on literal comparisons and on UTF-8: `[form="řeka" %c]` finds *Řeka*, *řeka* and *ŘEKA*,
`[form="zena" %cd]` also *Žena*. Folding covers Latin (including Central European letters), Greek and
Cyrillic; as in Unicode decomposition, letters such as *ø*, *ł*, *æ* and *ß* have no accent to remove.
For regular expressions use `(?i)` instead of `%c`. Indexes built before this folding (October 2026)
get their lookup files from `pando-index --upgrade`; until then `%c` / `%d` still work, more slowly.
`sort … by attr %c|%d|%cd` uses the same folding.

## Repetitions

`?` (0–1), `*` (0 or more), `+` (1 or more), `{m,n}`, `{m}`, `{m,}` follow a query token.

- Repetition of a **single** token uses **maximal contiguous** stretches: one hit per uninterrupted
  run of tokens that each satisfy the condition; shorter sub-spans inside that run are not returned as
  separate hits. `*` uses the same maximal-run logic as `+` for non-empty spans; a minimum of 0 only
  affects the lower bound. A pure zero-token match is not returned as a hit.
- A run longer than the maximum (the engine's practical cap, e.g. 100 for bare `+`/`*`) is split into
  consecutive tiles of at most that many tokens.
- A dependency operator (`<`, `>`, `<<`, `>>`) cannot involve a repeated token: next to
  `[upos="ADJ"]*` it would be unclear which token it relates, and with zero repetitions it would relate
  the neighbouring token. Such a query is an error; use a relation inside the token instead
  (`[upos="NOUN" & child [upos="ADJ"]]`).

## Quoted words and contractions

`"the"` on its own is short for a token query on the form, `/the.*/` for its regex variant.

In TEITOK and UD a contraction is often described on two levels (surface *aux* vs. underlying *à* +
*les*). Pando does not keep a second token layer: the parts are normal tokens, and for a multi-word
token (a CoNLL-U range line) the indexer adds a `contr` region over those tokens with a mandatory
`form` attribute holding the surface string (*can't*, *aux*). A quoted word therefore expands to a
one-or-more repetition so that one hit can cover the parts of a contraction:

```text
"can't"  →  [form="can't" | contr_form="can't"]+
/can.*t/ →  [form=/can.*t/ | contr_form=/can.*t/]+
```

In the sample corpus, sentence `sample-en_p6-s1` (*I can't believe it.*) has *can't* as a multi-word
token (lemmas `can` + `not`, surface `can` + `n't`, `contr_form="can't"`): `"can't"` finds both its
parts as one hit, as well as *can't* stored as a single token. Side-effect: `"had"` in *John had had
enough* gives the two *had* as one hit. A corpus without contractions (no `contr_form`) has no such
spans: `"the"` is just `[form="the"]` as in CWB (*the the* gives two hits). An explicit repetition
(`"the"{2}`) stays as written.

## Dependency relations

| operator | meaning |
| --- | --- |
| `A < B` | A is a dependent (child) of B |
| `A > B` | A is the head (parent) of B |
| `A << B` | A is a descendant of B |
| `A >> B` | A is an ancestor of B |
| `A !< B` | B without such a dependent A (A is not part of the hit) |

Sequence and dependency relations can be mixed in one chain; each operator relates the two tokens next
to it. Inside a token, relations are written as restrictions (PML-TQ style): `child [...]`,
`parent [...]`, `ancestor [...]`, `descendant [...]`, `sibling [...]`, and the order relations
`next [...]` / `prev [...]` (the token directly after / before), `after [...]` / `before [...]` (any later /
earlier token in the same sentence; they need `<s>` regions). All can be negated with `not` and counted:
`count(child [upos="ADJ"]) >= 3`, `count(after [upos="PUNCT"]) >= 2`. Only directly before `[` are
these words relations, so an attribute called `next` or `after` can still be queried as `[after="x"]`.
For "a verb without a subject" use `[upos="VERB" & not child [deprel="nsubj"]]`.

## Feature fields (`feats/Feature`)

Attributes declared `kv_pipe` hold `Key=Value|Key=Value` strings, as UD `feats`. `[feats/Number="Plur"]`
matches a token whose feats contain `Number=Plur`; `feats/Number` works wherever an attribute does
(`count by feats/Number`, `tabulate a.feats/Number`).

## Regions

Region attributes are written `struct_attr` (structure, underscore, attribute): `text_genre`, `s_id`.

| form | meaning |
| --- | --- |
| `[text_genre="Book"]` | the token is inside a region with that attribute |
| `<text genre="Book"> []` | the token is the first token of such a region (attributes optional) |
| `[] </s>` | the token is the last token of a sentence |
| `a:[] :: a.text_genre = "Book"` | the named token is inside such a region |
| `[] :: text_lang = "Dutch"` | short for `:: match.text_lang = "Dutch"` (the name must contain `_`) |
| `[] within text_genre="Book"` | the whole match is inside one such region (attributes optional) |
| `[] within s having [form="cat"]` | … inside a sentence that also contains *cat* (Manatee) |
| `[] containing s` | the match contains a whole sentence |
| `[] containing subtree [upos="NOUN"]` | the match contains a whole dependency subtree headed by a noun |
| `not within`, `not containing` | negations; the Manatee spelling `within <s/>` is accepted |

- **Anchors attach to tokens.** A start anchor `<x>` attaches to the token after it, an end anchor
  `</x>` to the token before it. An anchor with no token to attach to is an error (`[form="the"] <s>`),
  except a single named region on its own (see below). `<s> [lemma="cat"] </s>` is a sentence that
  consists of the single word *cat*; for *cat* anywhere in a sentence use `[lemma="cat"] within s`.
- **Zero-width regions** (no tokens, e.g. pauses between words) work with anchors:
  `[form="the"] </pause>` (*the* followed by a pause), `<pause> [form="the"]` (a pause before *the*).
- **Super-region fallback.** An attribute that a region does not have is looked up on the region that
  contains it: `<s text_lang="Dutch">` uses `text_lang` of the enclosing text.
- **Index sidecars.** Region-attribute columns can carry reverse-index files (built by `pando-index`).
  They speed up equality in `::` filters and the planning of region conditions in tokens, without
  changing what matches; older indexes keep the same results with slower paths.
- An unknown structure after `containing` is an error.

## Named regions and derived bindings

- **Naming a region anchor** (`s:<s> [upos="VERB"]`) binds the region the anchor attached to, so it can
  be used after `::` (`:: s.s_sent_id = "s01"`) and in commands (`tabulate s.id`, `count by s.id`). In
  `tabulate` the part after `s.` is looked up on that structure first, then with the super-region
  fallback (`tabulate a.text_id` for `a:<article>`).
- **A region on its own.** A query consisting of one named region and conditions lists regions:
  `t:<text genre="Book">`, `np:<node cat="NP"> :: tcnt(np) > 5`, `n:<err>; count by n.type`. Only one
  region without a token is possible per query for now (relating independent regions is being added).
- **Derived bindings come after `::`**: `n:[deprel="nsubj"] :: np:dep_subtree(n) & tcnt(np) > 6`.
  `name:` binds, `=` compares. The subtree is the head and all its descendants, in sentence order; it
  need not be contiguous. `Name = dep_subtree(…)` as a query of its own is not supported.
- **Region functions** (in `::` and as `tabulate`/`count` fields):

| function | meaning |
| --- | --- |
| `tcnt(x)` | number of tokens in region or subtree `x` (0 for a zero-width region) |
| `gaps(x)` | number of breaks in `x`'s tokens: 0 = contiguous (regions always), > 0 = a non-projective subtree |
| `forms(x)`, `spellout(x, attr)` | the forms / the values of `attr` of `x`'s tokens, with `...` at gaps |
| `contains(a, b) = 1` | region `a`'s span contains region `b`'s span |
| `rchild(a, b) = 1` | nested regions with a parent index (`.par`): `b`'s parent is `a` |
| `rcontains(a, b) = 1` | nested regions with `.par`: `a` is `b` or one of its ancestors |

- **Several bindings per token.** When several regions of a nested or overlapping type start at the
  same token (an NP inside a clause), every possible binding gives a hit (fanout); the CLI option
  `--anchor-binding innermost` takes the smallest one instead.

## Multivalue attributes and overlapping regions

Attributes can be declared multivalue (values separated by `|`) and region types nested or
overlapping. That changes how conditions are read:

| query | interpretation |
| --- | --- |
| `genre="Book"` | *Book* is one of the values |
| `genre!="Book"` | *Book* is not one of the values |
| `genre=/B.+k/` | at least one value matches |
| `a.genre = b.genre` | the value sets of a and b share a value |
| `a.genre != b.genre` | they share no value |
| `node_type="NP"` | the token is in some node region of type NP |
| `within node_type="NP"` | the match stays within some NP |
| `count by genre` | each value is counted separately (totals can exceed the number of tokens) |
| `tabulate a.genre` | values joined by `\|` in text output, arrays in JSON |

"A value other than *Book*" needs a regex: `genre = /^(?!Book$).+/`.
`nvals(attr)` counts the non-empty `|`-separated values (for nested/overlapping region attributes: the
distinct values across all covering regions): `[nvals(genre) > 2]`, `:: nvals(a.wsd) > 1`. It works on
positional attributes, `feats/Feature` (0 or 1) and region attributes such as `text_genre`; an attribute
that is not multivalue and has no `|` counts 0 (empty or `_`) or 1.

## Token groups (stand-off annotation) and overlays

Discontinuous annotations (an error spread over two places, a split phrasal verb) are stored as
**token groups**: each member token carries the group id (`err_gid`) and the group's properties
(`err_code`), so `[err_code="SPLIT"]` is as fast as any token attribute. `<err code="SPLIT">` on its own
returns one hit per group with all its parts; `n:<err>; count by n.code` counts groups. Token groups
cannot be used with `within` / `containing`. `Over = [lemma="over"]; <err type="SPLIT"> where Over`
keeps the groups that contain a token of `Over` (resolved in the CLI).

**Overlay corpora** add annotation layers in a separate folder on top of a corpus; their groups are
queried as `<overlay-NAME-struct …>`, e.g. `<overlay-mine-mwe type="split-phrasal">`.

## Aligned corpora

TEITOK-style alignment uses a shared attribute (`tuid`, on sentences `s_tuid`, on paragraphs
`p_tuid`); for many-to-many alignment the ids are `|`-separated multivalues. A later query can refer to
a named token of an earlier one: `eng:[lemma="property"] :: eng.text_lang="English"` then
`nld:[upos="NOUN"] :: match.text_lang="Dutch" & eng.s_tuid = nld.s_tuid`. `[form="property"] with
[form="bezit"]` is the faster built-in form for sentence-aligned corpora. See
[Aligned-Corpora-and-Parallel-Queries.md](Aligned-Corpora-and-Parallel-Queries.md).

## Named queries and sessions

`Matches = …` names a result; an unnamed query is called `Last`. Names of queries and of query tokens
persist in the session (a pando-server client session or the server's shared program session), so a
later command can use them. When a program defines a name again with exactly the same query, the
session's result is reused instead of running the query again, and the answer lists it in
`"reused": ["Matches"]`; a changed query, a result sorted since, and the last statement of a program
run as usual.

## Global functions

Used after `::`, mostly with named tokens:

| function | example | meaning |
| --- | --- | --- |
| `a < b`, `a > b` | `:: a > b` | token a comes after token b in the corpus |
| `distance` | `distance(a,b) < 5` | a is less than 5 tokens before b |
| `distabs` | `distabs(a,b) < 5` | a and b are less than 5 tokens apart |
| `strlen` | `strlen(a.lemma) = 3` | length of the value (characters) |
| `f` | `f(a.form) >= 100` | corpus frequency of the value |
| `year`, `century`, `decade` | `year(text_date) >= 2000` | from a date-like value (4-digit year prefix; century of 2001 = 21; decade = `(year/10)*10`) |
| `month`, `week`, `day` | `month(a.text_date) = 5` | from `YYYY-MM[-DD]` values (ISO week) |
| `nchildren`, `ndescendants`, `depth` | `nchildren(a) > 3` | dependency tree of token a (not regions) |
| `tcnt`, `gaps`, `contains`, `rchild`, `rcontains` | `tcnt(np) > 5` | regions and subtrees, see above |
| `nvals` | `nvals(a.wsd) > 1` | number of values of a multivalue attribute |

Date functions accept `name.attr` or a region attribute directly (`text_date`); they also work as
`tabulate` fields.

## Commands

For a named query `M` (default: the last one):

| command | meaning |
| --- | --- |
| `cat [M]` | the hits as KWIC |
| `size [M]` | number of hits |
| `count [M] by att+` / `group` | frequency per value (combinations for several attributes); with a region attribute also the relative frequency |
| `freq [M] by att` | as count, in instances per million |
| `tabulate [M] [from n] att+` | a table; fields include `a.attr`, `a.cpos` (corpus position), date functions, `tcnt(x)`, `gaps(x)`, `forms(x)`, `spellout(x, attr)` |
| `sort [M] by att [%c\|%d\|%cd] [on from..to]` | sort the hits |
| `stats avg(expr), median(expr) [by att+]` | numeric aggregates per group |
| `raw [M]` | one line per hit with corpus positions and forms |
| `coll [M] by att` | collocations in a window |
| `dcoll [M] [rels] by att[, att2]` | dependency collocations; `rels`: deprels, `head`, `children`, `descendants`; `att2` is tallied per collocate |
| `keyness [M] [vs N] by att` | values overrepresented in M vs. the rest of the corpus or vs. N (log-likelihood G²) |

- `sort M by form` sorts on the first token of each hit; `on from..to` sorts on a range:
  `on match..matchend` (whole hit), `on matchend[1]..matchend[5]` (right context),
  `on match[-1]..match[-5]` (left context, nearest first); `from`/`to` are `match`, `matchend` or a
  token label with an optional offset. Equal keys keep corpus order. The CWB dialect sorts on the whole
  hit by default, as CQP does.
- `tabulate M 0 100 a.lemma, a.form` tabulates the first 100 hits.
- `stats`: values that are not numbers are skipped per metric (`n_valid`); the bucket size is
  `n_total`. Example: `a:[]; stats avg(strlen(a.form)), median(strlen(a.form)) by a.text_id`.
- Collocation and keyness measures, windows, minimum frequency and result counts are settings
  (below) or command-line flags (`--window`, `--measures`, `--min-freq`, `--max-items`).

## Settings

In interactive mode `set name value` changes a setting and `show settings` lists them; the names are
those of the command-line flags without `--`.

| setting | default | meaning |
| --- | --- | --- |
| context | 5 | KWIC context width in tokens |
| left / right | 5 | left / right context and collocation window |
| window | 5 | left and right at once |
| limit | 20 | maximum results shown (hits, collocates, keywords) |
| sample | 0 | random sample of this many hits (0: all) |
| seed | 0 | random seed for `sample` (0: new each time) |
| offset | 0 | skip the first N results |
| measures | logdice | association measures for coll/dcoll/keyness (logdice, mi, mi3, tscore, ll, dice) |
| min-freq | 5 | minimum co-occurrence frequency for collocations |
| attrs | (all) | token attributes shown in output |
| anchor-binding | fanout | `innermost`: one binding per anchor (CLI) |

## Corpus information and management

| command | meaning |
| --- | --- |
| `drop M` | drop a named query (or `drop all`) |
| `show named` | named queries and token names |
| `show attributes` | token attributes |
| `show regions [TYPE]` | region structures, or the regions of TYPE with their attributes |
| `show values ATTR` | values with counts (`|`-separated values split) |
| `show info` | corpus overview |
| `show settings` | current settings |

## Limitations

- A query is one sequence: `|` between sequences (`[a] [b] | [c]`) and groups of tokens
  (`([a] [b])+`) are not supported yet and give an error. `|` inside a token works.
- Only one region without a token per query (see Named regions).
