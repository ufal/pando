# CWB / CQP queries in pando (`--cql cwb`)

pando reads queries written for the IMS Open Corpus Workbench (CQP) and runs them on a pando index.
It translates the CQP text into pando's own query language (pando CQL), so the results are pando's:
the same hits and counts, but pando's output format. The full CQP language is described in the
[CQP manual](https://cwb.sourceforge.io/files/CQP_Manual/); this page lists what pando reads as CQP
does, what it reads differently, and what to write in [pando CQL](PANDO-CQL-Reference.md) for what
the CWB dialect does not read.

How to choose the dialect: `pando --cql cwb …` on the command line, `"dialect": "cwb"` in a
pando-server request, the word `CWB` (or `CQP`) as the first word of the query, or *CWB CQL* as the
query language in flexicorp.

## Read as in CQP

- Token patterns: `[attr="value"]`, `[]`, `"word"` (= `[word="word"]`), sequences, repetition
  `? * + {m,n}`, `&` `|` `!` `->` and parentheses inside `[ ]`, `!=` (also with a pattern).
- Quoted values: a value with regular-expression characters is a regex over the whole value;
  a backslash keeps a character literal (`[word="\."]`, `"\(státní občanství\)"`); `[(]` works too.
- Flags `%c` (also on patterns), `%d`, `%cd`, `%l` (literal, also after `!=`).
- The token's own region values: `[lemma="go" & _.text_genre="Book"]`.
- XML tags: `<s> [pos="DET"]`, `[] </s>`, `<text_genre="Book"> []`.
- Named tokens and the target marker: `a:[pos="ADJ"]`, `@[pos="NOUN"]` (pando's label `target`; `@1` is `keyword`).
- Lookahead: `[pos="DET"] [:pos="NOUN":]` (a determiner followed by a noun; the match is the determiner).
- Global constraints before `within`, as CQP has them:
  `[lemma="go"] :: match.text_genre="Book" within text`, `a:[pos="ADJ"] b:[pos="NOUN"] :: a.lemma != b.lemma`.
- `within s` / `within text` (any structure name).
- Named queries `A = …` and the commands `count by`, `group by`, `sort … by … [on …]`, `size`,
  `cat`, `tabulate` (details in [Query-Language-and-CQL.md](Query-Language-and-CQL.md#cwb--cqp---cql-cwb)).

## The largest difference: working on result sets

CQP is a session in which a query result is a named set that later commands keep working on:
`B = subset A where match: [pos="NN"]`, `intersect A B C`, `union`, `diff`, `join`, MU queries
(`MU(meet A B s)`), `reduce A to 100`, `randomize`, `set A target …`, `cut`, and piping a result to
another program (`cat A > "| sort | uniq -c"`). pando keeps named queries and the commands
`count`, `group`, `sort`, `size`, `cat` and `tabulate` on them, but not the set operations on them or
the piping. In pando, write the conditions into one query instead (`within`, `::`, named tokens) and
use pando's commands (`count by`, `freq`, `coll`, `tabulate`) for what CQP would pipe to a script.

## Not read by the CWB dialect, and what to write instead

The CWB dialect stops with an error message starting with `Unsupported:` for these. The right
column is the same search in pando CQL (choose *pando CQL* as the query language).

| CQP | Example | In pando CQL |
| --- | --- | --- |
| `\|` between sequences | `[a] [b] \| [c]` | two queries, or the Manatee dialect (`MANATEE …`), which expands it |
| Repeated group of tokens | `([pos="ADJ"] [pos="NOUN"])+` | the Manatee dialect expands it: `MANATEE ([pos="ADJ"] [pos="NOUN"]){1,2}` |
| `within` a number of tokens | `… within 10` | a gap in the pattern: `[a] []{0,8} [b]` |
| `cut` | `A = [lemma="go"] cut 100` | `A = [lemma="go"]; cat A 0 99`, or the `limit` setting |
| New match boundaries `show match … matchend` | `[a] [b] [c] show match[1] ... matchend` | no equivalent; name the tokens and use them: `a:[a] b:[b] [c]; tabulate b.form` |
| MU queries | `MU(meet [a] [b] s)` | a sequence with a gap, `within s`: `[a] []* [b] within s` (one order) |
| Set operations on named queries | `intersect A B C`, `union`, `diff`, `join`, `subset` | no equivalent |
| `<match>`, `</target>` anchor tags | `<match> [pos="DET"]` | not needed: the match starts at the first token |
| Shell and session commands | `set`, `show`, `save`, `discard`, `info`, `dump`, `exec`, macros, `> "file"` | not part of pando (settings and output are the caller's) |

CQP itself rejects `within` before `::` (`[lemma="go"] within text :: …`: a syntax error, after
which CQP runs the part before `::` without the condition); the CWB dialect rejects it too. In pando
CQL that is the order to write.

## Read, but different from CQP

| CQP | In CQP | In pando |
| --- | --- | --- |
| `[text_id="t1"]` (region attribute without `_.`) | no hits | the tokens in that text |
| `cat A 20` | every hit from 20 to the end | one page of hits from 20 |
| `sort … descending` / `reverse` | reversed order | corpus order (not applied) |
| Output of `cat`, `tabulate`, `count` | CQP's text format | pando's format (text or JSON) |

## Same query, different counts

These run in both, and the hits are the same kind of thing, but the numbers differ (UD collection, 38 M tokens):

| Query | CQP | pando | Why |
| --- | ---: | ---: | --- |
| `[upos="DET"]? [upos="NOUN"]` | 8,541,955 | 10,119,888 | CQP's matching strategy keeps one match per noun; pando (as Manatee) counts *the cat* and *cat* both |
| `[upos="ADJ"]+ [upos="NOUN"]` | 1,296,236 | 1,409,290 | the same: CQP one match per noun, pando each run of adjectives before it |
| `[upos="DET"] []{0,2} [upos="NOUN"]` | 2,013,818 | 2,427,669 | the same, for gaps |
| `a:[…] b:[…] :: a.lemma = b.lemma` | 24,678 | 95 | pando does not count two `_` (no lemma) as equal; with `--allow-empty-alignment` it does, as CQP and Manatee |
| `[(lemma="be") -> (upos="AUX")]` | 38,177,974 | 38,177,977 | CQP gives 3 fewer than its own parts add up to (38,181,322 − 3,345) |

## Where this is tracked

The CWB dialect is in `src/query/dialect/cwb/`; its tests are in `cwb_dialect_test.cpp`. When a row
above changes, update this page and the error message in `cwb_parser.cpp` together.
