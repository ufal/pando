# Pando Corpus Query Language

The native query language of pando is called pando-CQL, heavily modeled after [CWB-CQL](https://cwb.sourceforge.io/files/CQP_Manual/), with additions that are partially taken from [SketchEngine](https://www.sketchengine.eu/documentation/corpus-querying/), and partially from [PML-TQ](https://ufal.mff.cuni.cz/pmltqdoc/doc/pmltq_tutorial_web_client.html). These guidelines give a general introduction to pando-CQL (henceforth simply CQL when there is no confusion), with example queries illustrated against the **sample corpus** shipped with this repository: `test/data/sample.conllu`, which is annotated using [Universal Dependencies](https://universaldependencies.org/).
See [Sample-Corpora.md](Sample-Corpora.md) for bundled CoNLL-U, JSONL fixtures, and the full-UD download script.

## Token Queries

The base query in CQL searches for a token (a word or punctuation mark), with optionally one or more restrictions. A (query) token is represented with two square brackets, with any restrictions we want to place on the tokens we want to find inside the brackets. So if we want to look for a word with the lemma "book", we express that in CQL as `[lemma="book"]`, which will find all examples of *book* and *books*, as a noun, but also *book*, *books*, *booked* and *booking* as as a verb.

We can add multiple restrictions an a token, putting a **&** between the restrictions to say that all restrictions should be met (a **|** would instead match if any of the restrictions is met). So if we only want to find occurrences of the verbs *book*: `[lemma="book" & upos="VERB"]` will match only examples of *book*, *booking*, etc. as a verb. We can instead also look for either the lemma *book* or *cat*: `[lemma="book" | lemma="cat"]`.

CQL is a sequence-based query language, meaning we can look for sequences of tokens. This is done by putting several tokens in a row in our query. So if we want to only find occurrences of *book* that are preceded by a determiner, we express that as follows: `[upos="DET"] [lemma="book"]`.

## Regular expressions

To look for patterns inside words, CQL allows regular expressions in token restrictions. In regular expressions, you can look for optional letters, repeated letters, etc. In pando-CQL, the explicit regex form uses slash delimiters: `[form = /pan.*tion/]`.

For compatible behaviour with CWB and Mantee, normal string matches are interpreted as bound regular expressions when they contain regular expression characters. So `[form = ".*tion"]` matches words that end in *-tion*, like CWB. This behaviour can be turned off by setting `strict_quoted_strings`, which will make the previous query look for the literal string *.\*tion*.

Slash-regex values are plain regular expressions. For example `[form = /.*tion/]` matches any substring *tion* inside the token (e.g. *conditional*), not only *-tion* suffixes; anchoring is up to your pattern (e.g. `[form = /tion$/]`).


## Token repetitions

Since the original CQL is purely sequence based, the last query above will not find examples where there is anything between the determiner and the noun, like in *the green book*, which we probably intended to find. That is why CQL introduces token repetitions operators, that are placed directly after the closing bracket. For instance, `[]?` means any optional token (technically 0 or 1 occurrences of the token with in this case no restrictions). So the query `a:[upos="DET"] []? [lemma="book"]` will still find *the book*, but allows a single token in between the determiner and book, so *the green book*, *some interesting book*, etc. But it will also include probably unintented results like *some people book* (their hotel early). We could prevent that by tightening the query to only allow adjectives, and saying there can be multiple adjectives `a:[upos="DET"] [upos="ADJ"]* [lemma="book"]`, where the * stands for "0 or more". 

There are four different repititions, depending on how many tokens are allowed or required:

| symbol | interpretation |
| ----- | ----- |
| []? | 0 or 1 tokens |
| []* | 0 or more tokens |
| []+ | 1 or more tokens |
| []{m,n} | between m and n tokens |
| []{m} | exactly m tokens |
| []{m,} | m or more tokens |

Repetition (**`+`**, **`*`**, **`{n,m}`**, …) on a **single** token uses **maximal contiguous** stretches: one hit per uninterrupted run of tokens that each satisfy the token condition (including the `form` \| `contr_form` OR). Shorter sub-spans inside that run are **not** returned as separate hits. **`*`** uses the **same** maximal-run logic as **`+`** for non-empty spans; `min_repeat == 0` only affects the **lower** bound (e.g. the last tile may be shorter when splitting a run longer than `max_repeat`). A **pure** zero-token match (matching nowhere in the corpus) is not emitted as a separate hit yet—`match_end` / `NO_HEAD` conventions need a dedicated representation first.

If a run is longer than `max_repeat` (the engine’s practical cap, e.g. 100 for bare `+`/`*`), the run is split into consecutive tiles of at most `max_repeat` tokens. The same expansion applies to regex shorthands: `/can.*t/` → `[form=/can.*t/ | contr_form=/can.*t/]+`.

## Dependency relations

Our query `[upos="DET"] []* [lemma="book"]` was probably intended to look for any word *book* modified by a determiner, something that you cannot really express in CWB/CQL, only approximate by expressing what exactly can occur between a determiner and a noun inside an NP. That is why pando-CQL introduces the option to look for dependency relations. Dependency relations are expressed by two query tokens with an operator in between: < or > depending on which is the head. Or >> and << for descendants and ancestors.

With that, our query becomes `a:[upos="DET"] < [lemma="book"]`, which will find any determiner governed by the word *book*, so it will not longer find *some people book*. The relation can also be expressed the other way around: `[lemma="book"] > [upos="DET"]`. It will find any relation, so if we look for `[upos="ADJ"] < [lemma="book"]`, it will find any adjective that has a dependency relation to *book* in the corpus, so it will not only find *green book*, but also the same words in longer expressions like *some green and when it is raining also wet books*, or predicative uses as in *The book that was on the table was green.*. 

Dependency and sequence relations can be combined, with the token being interpreted by the symbol between them, so `[upos="DET"] [upos="ADJ"] < [lemma="book"]` requires the adjective to be to the right of the determiner, as well as the adjective to be governed by the word *book*. And it is possible to negate dependencies: `[upos="DET"] !< [lemma="book"]` for occurrences of *book* without a determiner.

Intead of using sequence notation, it is also possible to define dependency relations as token restriction, in a notation similar to that used in PML-TQ. In that case, to look for a noun modified by a determiner, we specify inside the token that we are looking for a child that is a determiner: `[upos="NOUN" & child [upos="DET"] ]`. This notation has the advantage that you can specify multiple children, and still have the option to furthermore look for words to the left or the right. And there are more option in the token-restriction notation: you can look not only for `child`, but also for `parent`, `ancestor`, `descendant`, or `sibling`.

Also for depenencies as token restrictions, we can use negations: `[upos="VERB" & not child [deprel="nsubj"]]` to look for any verbs without a nominal subject (*!>* as an operator leads to semantic problems so is not supported). 



## Regions

Corpora not only have tokens, but also *regions* that group several tokens into a unit like a sentence, a paragraph, a text, an utterance, etc. And like tokens, regions can have attributes, in CWB called *sattributes*. 

We can restrict our query by properties of the region it is in. The notation for the attributes of regions is the name of the region, an underscore, and the name of the attribute, so the genre of the text is named `text_genre`, which can have values in the same way positions (token) attributes do. We can use those in a number of different ways:

| example | type  | explanation |
| ----- | -----  | ----- |
| `[ text_genre="Book" ]` | token-restriction | the token is in a given region+attribute |
| `<text genre="Book"> []` | region-hook | look only for tokens directly following the start of a region+attribute (attribute is optional) |
| `a:[] :: a.text_genre = "Book"` | global condition | the named token lies in a text with those attributes |
| `[] :: text_lang = "Dutch"` | global condition | shorthand for `:: match.text_lang = "Dutch"` (the attribute token must contain `_`, e.g. `text_lang`, `s_tuid`) |
| `[] within text_genre="Book"` | within restriction | the whole match has to appear inside a region+attribute (attribute is optional) |

When the corpus is indexed with **`pando-index`**, region-attribute columns can carry optional **reverse index** sidecars (next to the `.val` files). That speeds up equality on global `::` filters and improves query planning for region-attribute token restrictions, without changing which strings match. Older index directories without those files keep the same semantics with slower paths.

And of course, those can be combined in queries, so if we want to look for the lemma *cat* with an adjective, where *cat* has to appear in an English text, and the whole things has to be inside a text of genre *Book*, we can express that as `[lemma="cat" & text_lang="Book"] > [upos="ADJ"] within text_genre="Book"`. Since there are no texts within texts, the two restrictions in practice work in exactly the same way.

Pando explicitly allows for zero-width regions: regions that have no tokens inside. Those are helpful in for instance spoken corpora, where pauses are often very relevant, but are between tokens, not tokens themselves. By treating pauses as zero-width regions, we can use them in corpus queries, so `[form="the" ] <pause>` will look for the word *the* followed by the beginning (as well as the end) of a pause "region".

In pando, regions are first-class citizens: we can look for regions directly, without relying on the tokens they contain or border. So we can directly look for regions all texts of the book genre: `<text genre="Book">`, so that we concentrate on the regions themselves, not on their tokens. 


## Within and containing

Like in Manatee, we can add a (single) token restriction on the the `within` clause - if we want to look for an interjection in a sentence that needs to contain the word *cat* we can do that by saying `[upos="INTJ"] within s having [form="cat"] :: match.text_genre = "Book" & match.text_lang = "English"` (Universal Dependencies tag `INTJ`; in the sample corpus that sentence is in the English book subcorpus).

Inversely, we can also say that a the match need to contain a given region, which we can express with `containing name`, meaning that our result has to contain an entire region named `<name>`. In CWB-CQL, a common example of that is to require something to contain an entire NP, but syntactic elements like NP are not regions, and in dependency parsed corpora, they are implicitly encoded in dependency subtrees. That is why in pando you can say `containing subtree [upos="NOUN"]`, which will require the result to contain all leaves of a dependency subtree heading by an noun, which is the dependency equivalent of a noun phrase. 

Both within and containing can also be negated: `[] not within s`.

## Named region bindings

You can **label** a region-start anchor like a token name: **`np:<s>`** binds the label **`np`** to the **sentence region row** that contains the match (structure type **`s`** in this example). Optional attributes on the anchor work as usual: **`<s sent_id="s01">`**.

- **Global filters** can use that label with the usual **`struct_attr`** spelling (underscore between structure and attribute): **`:: np.s_sent_id = "s01"`** resolves the region via **`np`**, not via a token position.
- **Region-only query** (no token in the query — only the anchor + `::`): **`np:<s> :: np.s_sent_id = "s01"`** enumerates sentence regions and keeps those that pass the filter. When you add tokens (e.g. **`np:<s> []`**) the anchor binds to the first real token as before.
- **`tabulate`** (and similar commands on the last result) can project **short** region-attribute names on the bound structure: **`tabulate np.id`** or **`tabulate np.sent_id`** — the part after **`np.`** must match a **region attribute** on that structure (as in the index), not the combined `s_sent_id` token form used inside token restrictions.
- **Anchors + named tokens:** Queries like **`<s> sent:[]`** strip the anchor before execution; **named token indices** in the session align with **`Match.positions`** (so **`tabulate sent.form`** resolves **`sent`** correctly).

**Layer A global `::` geometry:** **`contains(outer, inner)`** — both arguments must be **named region bindings** (e.g. `s:<s> np:<node type="NP">`). True iff the inner region’s inclusive token span lies inside the outer’s (`inner.start ≥ outer.start` and `inner.end ≤ outer.end`). Example: **`:: contains(s, np) = 1`** (or **`> 0`**). Tabulate / aggregate fields support **`tcnt(label)`** for token counts inside a named region (separate from **`contains`**).

**Layer B (nested structural types with a `.par` parent index):** **`rchild(parent, child)`** — both arguments are **named region bindings** for the **same** structural type (e.g. two `node` labels). True iff **`child`’s** stored **`parent_region_id`** equals **`parent`’s** region row index (immediate dominance in the tree). Example: **`vp:<node type="VP"> np:<node type="NP"> [] :: rchild(vp, np) = 1`**. Requires the corpus to declare that type as **`nested`** and to ship **`.par`** (see indexing docs).

**`rcontains(ancestor, descendant)`** — same **named-region** rules, **`.par`** required. True iff **`ancestor`** is on the **parent chain** from **`descendant`** up to the root (reflexive: a row is its own ancestor). Example: **`:: rcontains(s, np) = 1`** when the sentence region **`s`** tree-dominates **`np`**. Differs from **`contains`** when spans alone are ambiguous: **`rcontains`** uses the tree index only.

**Peer clauses on region anchors (whitespace-separated, any order):** **`rchild(vp)`** — same test as **`:: rchild(vp, …)`** on this binding (immediate parent row must be **`vp`**’s row; **`.par`** required). Use **`rchild`**, not **`child`**, so **`child`** stays reserved for **dependency** relations in **`[]`**. **`rcontains(vp)`** — this row must **tree-dominate** **`vp`** (same as **`:: rcontains(this, vp)`**). **`contains(vp)`** — this row’s span must geometrically **contain** **`vp`**’s span (Layer A). Example: **`np:<node contains(vp) type="NP" rchild(pp)>`**. Each peer label must be bound by an **earlier** anchor. At least one **non-anchor** token is required so anchors can bind to token positions.

With named regions, there are some functions to compare and count regions. For two named regions *a* and *b*, there are the following functions

| query | interpretation |
| ------ | ------ | 
| contains(a,b)  | the region *a* contains the region *b* (token span ⊆ span) |
| rchild(a,b)  | only for nested regions: *b* is a direct child of *a* on `.par` |
| rcontains(a,b)  | nested + `.par`: *a* is an ancestor of *b* on the parent chain (reflexive) |
| tcnt(a)  | renders the amount of tokens inside the region a |
| forms(a)  | renders the forms of the tokens inside the region a |
| spellout(a, attr)  | renders the join of attr for the tokens inside the region a |


## Named tokens and aligned corpora

You can give a name to the tokens in your query, so that you can then refer back to it: `a:[] b:[] :: a.form = b.form` will find all sequence of two identical words in a row like in *I knew that that book was yours*. When a global condition refers to a named token (`eng.text_lang`, alignment, etc.), that name is required; restrictions that only concern the match as a whole can use `match.` or the `struct_attr = value` shorthand (`text_lang = "Dutch"`).

In contrast to CWB-CQL, where the life-span of names is restricted to the query, names in pando-CQL are persistent, so that you can refer to them in subsequent grouping queries or other queries.

The fact that names are persistent allows us to use named tokens to search through aligned corpora, if the alignment is done in the set-up used by TEITOK. That settings models alignment by having a shared attribute, in TEITOK that is *tuid* for *translation unit identifier*, although it can also be used to align version of a text in the same language. How that works is best explained with an example. For **many-to-many** bitext, ids are often **pipe-separated** multivalues; see the wiki guide [Aligned corpora and parallel queries](../wiki/Aligned-Corpora-and-Parallel-Queries.md).

If we have an aligned corpus English-Dutch, with a tuid on sentences, we can look for a word in our English text in the first query, say the word *property*. We do that by a regular token query, that we can then name *eng* for convenience: `eng:[lemma="property"] :: eng.text_lang = "English"`. Then in a subsequent query, we can look for nouns in Dutch, but only in sentences that are translations of the English sentences we found, which we can find by making sure that the tuid attribute of the sentence that the `eng` token is in (`eng.s_tuid`) is the same as the tuid of the sentence we are using for the translation: `nld:[upos="NOUN"] :: match.text_lang = "Dutch" & eng.s_tuid = nld.s_tuid`. This way, we get the nouns in translation of the English sentences containing the word *property*.

Corpora that are aligned with *tuid* there can be alignments on various levels at the same time, so for instance also at a paragraph (*p_tuid*), or a the token-level (*tuid*). So `nld:[] :: match.text_lang = "Dutch" & eng.tuid = nld.tuid` will directly give us the Dutch words that are used as translations of the English word *property*.

For a more efficient but more limited way of searching through sentence-aligned corpora, pando also offers an easier syntax: `[form="property"] with [form="bezit"]` will only find occurrences of *property* that are in a sentence that is aligned with a sentence in which the word *bezit* occurs.



## Matching strategy flags

It is often desirable to look for words case or diacritics insensitively. In CQL, that is done by putting a flag after the condition: `[ form = "the" %c ]` will match both *the* and *The*, and `[ form = "een" %d ] :: match.text_lang = "Dutch"` will match both *een* and *één* in Dutch texts. 

The case flags only work on literal comparisons, not on regular expressions, to get a case-sensitive regular expression, you have to use regular expression flags like `/(?i)^hello/` to search for *hello* case-insensitively at the beginning of the word.

## Named queries and frequencies

Like in CWB, you can name queries, so that you can later refer back to them in results:  `Matches = a:[lemma="book" & text_genre="Book"]`. In fact, pando always names each query, where if no explicit name is provided, the query will be implicitly called `Last`.

After a query has been executed, frequency results can be obtained from it, in which the name of the query, and the name(s) of the query tokens can be used: `Matches = a:[lemma="book" & text_genre="Book"]; count Matches by a.form;` will look for all occurences of the lemma *book*, store it as *Matches* and then provide the frequency of each form in which it is used in corpus. Counting can be done by more than one attribute: `count Matches by a.form, a.text_century` will give an overview of which form of *book* was used in which frequency in each century - and not only that, but if one of the attributes used in the grouping is not a token attribute but a region attribute, the system will futhermore return the *relative frequency*, that is to say in the example, how frequent each form is in each century, relative to the total number of tokens for that century.

## Corpus position 

It is possible to use comparisons between the corpus positions of tokens to ensure that one is after the other - for sequential searches that is not that relevant, but we can look for all modifying adjectives (in Spanish or French, where both occur) that appear before a noun, by making comparing their positions: `a:[upos="NOUN" & text_lang="French"] > b:[upos="ADJ"] :: a > b` (pre-nominal adjective in the French sentence in the sample).

## Raw queries and contractions

Like in other dialects of CQL, you can use `"the"` as a shorthand to mean `[form="the"]`. But there are two differences with respect to for instance CWB-CQL. Firstly, we can also write `/the.*/` to get a regular-expression variant of a simple queries. And secondly, there is a special handling for contractions.

In TEITOK and UD, a contraction is often described with multiple analysis layers (surface *aux* vs underlying *à* + *les*). **Pando does not duplicate that layered token graph:** the indexer stores normal **tokens** for the parts and, when the CoNLL-U has a multi-word token (MWT) range line, a reserved **`contr`** region over those token positions with a mandatory **`form`** attribute set to the **surface** string (e.g. *can't*, *aux*). No second “virtual” token tier is required for querying—only this **dual representation** (tokens + one region row) is enough.

A **quoted raw word** expands to a **one-or-more repetition** of a disjunction so that a single hit can cover several token positions when matching a contraction:

```text
"can't"  →  [form="can't" | contr_form="can't"]+
```

The sample corpus sentence **`sample-en_p6-s1`** (*I can't believe it.*) indexes *can't* as an MWT: lemmas **`can`** + **`not`** on the sub-tokens (surface **`can`** + **`n't`**), with **`contr.form="can't"`** on the spanning region. Searching **`"can't"`** finds all the tokens within that contraction, as well as occurrences of *can't* that are in the corpus as single tokens. There is one side-effect: `"had"` in a sentence like *John had had enough* will find the two sequential occurrences of *had* as a single result, not as two separate ones. 

## Multivalue fields and overlapping regions

In pando, it is possible to have fields that can contains more than one value. For instance for texts genres, we might want to classify something as both *Poem* and *Song* if it is a song in poetry form. Likewise, it is possible for a single token to appear in two regions of the same type - in constituency syntax, we can have *node* regions with a *type* that indicates what kind of node it is - but an NP can be nested inside a VP. By default, token attributes are simple values and regions are non-overlapping. But in the registry, this behaviour can be modified for attributes and regions that need it (with a somewhat reduced query speed). 

This does not really affect the query much, mostly how queries get interpreted. So if genre is a multi-value field, then `[genre="Book"]` does not mean the value only has to be *Book*, but rather that *Book* is in the set of genres of the token. Similarly, if *node* is a possibly overlapping region type, then `[node_type="NP"]` means that the token is in any NP region, not that it is uniquely inside an NP. Below is a list search queries that are affected by either multivalue fields or overlapping regions, with the way they are interpreted in pando.

| query | interpretation |
| ------ | ------ | 
| genre="Book"  | *Book* is in the set of  genres |
| genre!="Book"  | *Book* is not in the set of  genres |
| genre=/B.+k/  | there is at least one item in the set of genres that matches the regex |
| a.genre = b.genre  | the intersection of the genres of a and b is non-empty |
| a.genre != b.genre  | the intersection of the genres of a and b is empty |
| node_type="NP"  | the token is in any node region that has type NP |
| within node_type="NP"  | the match has to stay within some NP |
| count by genre | each value in the set of genres is counted separately |
| tabulate a.genre | values joined by `|` in text output, arrays in JSON |

Since aggregation functions count each item separately, the total counts for token-level attributes can exceed the number of tokens in the corpus, so percentages become more tricky to calculate.

Notice that there is no straightforward way to look for tokens that have value in their genres that is not *Book*, but it can be done by looking for a regular expression that matches anything except Book, such as *genre = /^(?!Book$).+/*.

The **`nvals(attr)`** function counts **non-empty** pipe-separated components of a value (or, for nested/overlapping region attributes, the number of **distinct** components across all covering regions at the token). Use a comparison and a numeric right-hand side: **`nvals(genre) > 2`**, **`nvals(wsd)=2`**. It applies to positional attributes, **`feats/Feature`** (0 or 1), and composite region attributes such as **`text_genre`**. For attributes not declared multivalue and with no `|` in the stored string, the count is 0 (empty or `_`) or 1 otherwise.


## Stand-off regions

Apart from overlapping, nesting and zero-width regions there is one more type of region in pando: discontinous regions. These are relevant if we for instance want to model phrasal verbs in preposition stranding cases, like *Which boss did he PICK the package UP FOR*. If we want to mark out only the phrasal verb, we have a gap inside the region. That is what discontinuous regions are for, also called stand-off annotations. 

Stand-off annotations are not treated as full regions, since there are various region-related functions that are ill-defined when used with discontinuous regions. So instead, such regions are treated as grouped token-level attributes, that partially behave like regions. 

Stand-off annotations can also be added as external additions to a corpus. Say we have a large online corpus, to which a user wants to add some custom annotations. In that case, we can have a separate folder with a "dependent" corpus that specifies additional annotations. Such corpora are called *overlay* corpora. When querying a corpus, we can specify one or more overlay corpora, that we can then search as if it was part of the main corpus. If I have an overlay corpus called *mine*, in which I annotate multi-word-expressions (mwe), with an attribute *type*, I can find all split phrasal verbs I annotated by using `<overlay-mine-mwe type="split-phrasal">`. 


## Key-Value fields

The pando system was built with support for UD in mind, and in UD, the features are stored as a single, structured field. To provide support for this, pando fields can be kv_pipe fileds, which are exactly of the type of the features in UD. To use those in searches, pando uses the syntax followed by PML-TQ: `[feats/Number="Plur"]` looks for all tokens that have a *Number=Plur* in the feats attribute. And this notation can be used like any other token attribute, so you can also look for the distribution of number over number for adjectives: `[upos="ADJ"]; freq by feats/Number`.


## Tokens to Regions

There are two types of result groups in pando: token results and region results, and it depends on what we want to count or see which of the two we should use. We can directly name a region query item, and aggregate over it, so we can count all error annotations by type simply as follows: `n:<err>; count by n.type`. 

But if we want to find regions that contain specific things, a direct syntax becomes very tricky. Instead, to find regions with token-based restrictions, you use two different queries: you first look for the tokens(say all lemmas *over*) in a named query. And then we restrict our region search on regions that contain at least one token from that named query. So we can find all *split* errors that involve the lemma *over* as follows: `Over = [lemma="over"]; <err type="SPLIT"> where Over`. 

Notice that that gives entire regions, not (just) the tokens inside them - which you could of course find more directly using `[ lemma="over" & err_type="SPLIT" ]`.

### Dependency subtrees

Another way to get from tokens to regions is by selecting the entire dependency subtree for a node, so that we can print it, count it, etc. in the same way we can with a discontinuous region. So to get long bare nouns, we can first search for bare nouns (nouns not have a determiner), and then select the subtrees for those, but only if they have more than 2 tokens in it: `barenoun:[upos="NOUN" & not child [upos="DET"] ]; longbarenp:dep_subtree(barenoun) :: tnct(longbarenp) > 2`.


## Collocations

After keywords-in-context, collocations are one of the most popular queries in corpus processors. Pando offers the option to directly calculate collocations in the CQL, similar to the way *count* works - we search for something, and then look for the most characteristic (the most unexpectedly frequent) words in its context. So we can search for the most typical words to appear next to the verb *book* as follows: `[upos="VERB" & lemma="book"]; coll by lemma`. This will list the most prominent words appearing next to the verb *book* as counted by lemma. 

Apart from the left/right context, we can also look in the dependency context to see which words most typically modify a given word. This is done with `dcoll`, which takes a list of dependency relations to traverse. Relations can be specific deprel labels like `amod` or `nsubj` (which select children with that label), or the special keywords `head` (go up to the governor), `children` (all children), or `descendants` (full subtree). If no relation is specified, all children are collected by default.

For example: `[upos="NOUN" & lemma="book"]; dcoll amod by lemma` gives the most typical adjectival modifiers of the noun *book*. To see which words govern *book*: `dcoll head by lemma`. Relations can be combined: `dcoll head, amod by lemma` collects both heads and amod children.

For multi-token queries, use a named token as anchor with dot notation: `a:[upos="DET"] [upos="NOUN" & lemma="book"]; dcoll a.amod by lemma` — this anchors the dependency collocation on the determiner rather than the default first matched token.

The CQL commands only define the search, not the way the results are produced in the output. In pando, the context window, the collocation measures (logdice, mi, mi3, tscore, ll, dice), the minimum frequency, the maximum number of results, etc. are provided on the command line with flags like `--window`, `--measures`, `--min-freq`, and `--max-items`.

## Keyness

Keyness identifies words that are statistically overrepresented in a subcorpus compared to a reference. Since named queries in pando effectively define subcorpora, keyness works much like `count` and `coll` — you run a query that selects the tokens of interest, then ask which words stand out.

For example, to find keywords in the French subcorpus compared to the rest of the corpus: `[text_lang="French"]; keyness by lemma`. Without an explicit reference, the comparison is against the complement — all corpus positions not in the target query.

To compare two specific subcorpora, use the `vs` keyword: `Fr = [text_lang="French"]; En = [text_lang="English"]; keyness Fr vs En by upos`. This would show which parts of speech are statistically more common in French than in English — e.g. French might show overuse of `DET` (more articles) while English might show more `PART` (infinitival *to*). A more typical use would be to look for lemmas that are more frequent in one text genre versus another.

Like collocations, the keyness measure (log-likelihood, log-ratio, chi-squared, etc.) and output settings are controlled via command-line flags.

## Controlling the output

By default, if no futher queries are provided, pando will return results as keywords-in-context (KWIC), that is to say, the matches in the middle, with some words to the left and some words to the right of the result, which can also be explicitly triggered with the command `cat Matches`, where *Matches* is the name of the query.  

To get more control over the output, we can also explicitly choose what should appear in the output: `tabulate Matches a.form, a.lemma, a.upos, a.text_genre, a.text_lang`

will give a table with those columns for query token *a* in a query named *Matches*: the form, lemma, and upos of the token, and the genre and the language of the text it is in.

## Global condition functions

We can add counts as global queries in, adding additional restrictions on tokens. Those include string functions, frequency functions, sequential functions, and tree functions. The use of those is hence always referring to a named token in the query: `a:[upos="NOUN"] :: f(a.lemma) > 100`. Multivalue cardinality also works as a global filter: `a:[] :: nvals(a.wsd) > 1` (same semantics as `[nvals(wsd) > 1]` on a single token, but the attribute is given as `namedtoken.attr`).

| function | syntax | description |
| ------ | ------ | ------ |
| distance  | distance(a,b) < 5 | token a is more than 5 tokens before token b |
| distabs  | distabs(a,b) < 5 | the sequential distance between tokens a and b is less than 5 |
| strlen  | strlen(a.lemma) = 3 | the length of the lemma of a is 3  |
| f | f(a.form) >= 100 | a.form occurs at least 100 times in the corpus |
| nchildren | nchildren(a) > 3 | node a has more than 3 children |
| ndescendants | ndescendants(a) < 3 | node a has less than 3 children |
| depth | depth(a) < 4 | node a appears less than 4 levels below the root |
| nvals | nvals(a.wsd) > 1 | pipe-separated component count of `a`’s attribute (see multivalue `nvals` in token conditions) |

<!--{% comment %}
There is one additional counting function that is not a global filter but a token filter: `[upos="NOUN" & count(child[upos="ADJ"]) >= 3]` gives only nouns that have at least 3 adjectival children
{% endcomment %}-->

## Non-query functions overview

Between the frequency and the output functions, the following functions are supported for a query name M :

| function | syntax | description |
| ------ | ------ | ------ |
| size  | size [M] | count how many results there are in M |
| count  | count [M] by att+ | count how many results there are for each token or region attribute |
| group | group [M] by att+ | synonym for count |
| sort | sort [M] by att+ | sort the result on a token or region attribute |
| cat | cat [M] | produce a KWIC list of the results of M |
| freq | freq [M] by att | similar to count but gives instances per million (IPM) |
| raw | raw [M] | one line per match with corpus positions and token forms |
| tabulate | tabulate [M] att+ | produce a table with the given columns |
| coll | coll [M] by att | window-based collocations sorted by association measure |
| dcoll | dcoll [M] [rels] by att | dependency-based collocations, optionally filtered by deprel/direction |
| keyness | keyness [M] [vs N] by att | words overrepresented in M vs rest of corpus (or vs named query N), using log-likelihood G² |

The tabulate command can also take a start and offset in the command: `M = a:[lemma="book"]; tabulate M 0 100 a.lemma, a.form` will tabulate the first 100 occurrences of *book* by lemma and form.

## Interactive settings

In interactive mode, output settings can be changed at any time with `set` and inspected with `show settings`. The setting names are the same as the corresponding command-line flags (without the `--` prefix), so that behaviour is consistent between batch and interactive use. The most important settings are:

| setting | default | description |
| ------ | ------ | ------ |
| context | 5 | symmetric KWIC context width in tokens |
| left | 5 | left context / collocation window |
| right | 5 | right context / collocation window |
| window | 5 | set left and right at once |
| limit | 20 | maximum number of results to display (KWIC hits, collocates, keywords, etc.) |
| offset | 0 | skip the first N results |
| measures | logdice | association measures for coll/dcoll/keyness (comma-separated) |
| min-freq | 5 | minimum co-occurrence frequency for collocations |
| attrs | (all) | token attributes to show in output |

For example: `set context 10` widens the KWIC display, `set measures logdice, mi` changes the default collocation measures, and `show settings` prints all current values.

## Corpus management

Apart from corpus query options, pando-CQL also offers some options related to
corpus management:

| function | description |
| ------ | ------ |
| drop M | drop the named query (or drop all) |
| show named | show all active named queries and token names |
| show attributes | show all token attributes in the corpus |
| show regions | show all region structures in the corpus |
| show regions TYPE | list individual regions of TYPE with their attributes |
| show values ATTR | list values + counts; pipe-separated segments are split into separate rows (same convention as multivalue `=`) |
| show info | show corpus overview: name, size, structures, attributes |
| show settings | show current values of all interactive settings |

