# Pando Corpus Query Language

The native query language of pando is called pando-CQL, heavily modeled after [CWB-CQL](https://cwb.sourceforge.io/files/CQP_Manual/), with additions that are partially taken from [SketchEngine](https://www.sketchengine.eu/documentation/corpus-querying/), and partially from [PML-TQ](https://ufal.mff.cuni.cz/pmltqdoc/doc/pmltq_tutorial_web_client.html). These guidelines give a general introduction to pando-CQL (henceforth simply CQL when there is no confusion), with example queries illustrated against the **sample corpus** shipped with this repository: `test/data/sample.conllu`, which is annotated using [Universal Dependencies](https://universaldependencies.org/).
See [SAMPLE-CORPUS.md](SAMPLE-CORPUS.md) for how to build an index from `test/data/sample.conllu` and run queries locally. 

## Token Queries

The base query in CQL searches for a token (a word or punctuation mark), with optionally one or more restrictions. A (query) token is represented with two square brackets, with any restrictions we want to place on the tokens we want to find inside the brackets. So if we want to look for a word with the lemma "book", we express that in CQL as `[lemma="book"]`, which will find all examples of *book* and *books*, as a noun, but also *book*, *books*, *booked* and *booking* as as a verb.

We can add multiple restrictions an a token, putting a **&** between the restrictions to say that all restrictions should be met (a **|** would instead match if any of the restrictions is met). So if we only want to find occurrences of the verbs *book*: `[lemma="book" & upos="VERB"]` will match only examples of *book*, *booking*, etc. as a verb. We can instead also look for either the lemma *book* or *cat*: `[lemma="book" | lemma="cat"]`.

CQL is a sequence-based query language, meaning we can look for sequences of tokens. This is done by putting several tokens in a row in our query. So if we want to only find occurrences of *book* that are preceded by a determiner, we express that as follows: `[upos="DET"] [lemma="book"]`.

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

## Dependency relations

Our query `[upos="DET"] []* [lemma="book"]` was probably intended to look for any word *book* modified by a determiner, something that you cannot really express in CWB/CQL, only approximate by expressing what exactly can occur between a determiner and a noun inside an NP. That is why pando-CQL introduces the option to look for dependency relations. Dependency relations are expressed by two query tokens with an operator in between: < or > depending on which is the head. Or >> and << for descendants and ancestors.

With that, our query becomes `a:[upos="DET"] < [lemma="book"]`, which will find any determiner governed by the word *book*, so it will not longer find *some people book*. The relation can also be expressed the other way around: `[lemma="book"] > [upos="DET"]`. It will find any relation, so if we look for `[upos="ADJ"] < [lemma="book"]`, it will find any adjective that has a dependency relation to *book* in the corpus, so it will not only find *green book*, but also the same words in longer expressions like *some green and when it is raining also wet books*, or predicative uses as in *The book that was on the table was green.*. 

Dependency and sequence relations can be combined, with the token being interpreted by the symbol between them, so `[upos="DET"] [upos="ADJ"] < [lemma="book"]` requires the adjective to be to the right of the determiner, as well as the adjective to be governed by the word *book*. And it is possible to negate dependencies: `[upos="DET"] !< [lemma="book"]` for occurrences of *book* without a determiner.

Intead of using sequence notation, it is also possible to define dependency relations as token restriction, in a notation similar to that used in PML-TQ. In that case, to look for a noun modified by a determiner, we specify inside the token that we are looking for a child that is a determiner: `[upos="NOUN" & child [upos="DET"] ]`. This notation has the advantage that you can specify multiple children, and still have the option to furthermore look for words to the left or the right. And there are more option in the token-restriction notation: you can look not only for `child`, but also for `parent`, `ancestor`, `descendant`, or `sibling`.

Also for depenencies as token restrictions, we can use negations: `[upos="VERB" & not child [deprel="nsubj"]]` to look for any verbs without a nominal subject. 

## Zero-width and overlapping regions

Pando explicitly allows for zero-width regions: regions that have no tokens inside. Those are helpful in for instance spoken corpora, where pauses are often very relevant, but are between tokens, not tokens themselves. By treating pauses as zero-width regions, we can use them in corpus queries, so `[form="the" ] <pause>` will look for the word *the* followed by the beginning (as well as the end) of a pause "region".


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

## Within and containing

Like in Manatee, we can add a (single) token restriction on the the `within` clause - if we want to look for an interjection in a sentence that needs to contain the word *cat* we can do that by saying `[upos="INTJ"] within s having [form="cat"] :: match.text_genre = "Book" & match.text_lang = "English"` (Universal Dependencies tag `INTJ`; in the sample corpus that sentence is in the English book subcorpus).

Inversely, we can also say that a the match need to contain a given region, which we can express with `containing name`, meaning that our result has to contain an entire region named `<name>`. In CWB-CQL, a common example of that is to require something to contain an entire NP, but syntactic elements like NP are not regions, and in dependency parsed corpora, they are implicitly encoded in dependency subtrees. That is why in pando you can say `containing subtree [upos="NOUN"]`, which will require the result to contain all leaves of a dependency subtree heading by an noun, which is the dependency equivalent of a noun phrase. 

Both within and containing can also be negated: `[] not within s`.

## Named tokens and aligned corpora

You can give a name to the tokens in your query, so that you can then refer back to it: `a:[] b:[] :: a.form = b.form` will find all sequence of two identical words in a row like in *I knew that that book was yours*. When a global condition refers to a named token (`eng.text_lang`, alignment, etc.), that name is required; restrictions that only concern the match as a whole can use `match.` or the `struct_attr = value` shorthand (`text_lang = "Dutch"`).

In contrast to CWB-CQL, where the life-span of names is restricted to the query, names in pando-CQL are persistent, so that you can refer to them in subsequent grouping queries or other queries.

The fact that names are persistent allows us to use named tokens to search through aligned corpora, if the alignment is done in the set-up used by TEITOK. That settings models alignment by having a shared attribute, in TEITOK that is *tuid* for *translation unit identifier*, although it can also be used to align version of a text in the same language. How that works is best explained with an example. For **many-to-many** bitext, ids are often **pipe-separated** multivalues; see the wiki guide [Aligned corpora and parallel queries](../wiki/Aligned-Corpora-and-Parallel-Queries.md).

If we have an aligned corpus English-Dutch, with a tuid on sentences, we can look for a word in our English text in the first query, say the word *property*. We do that by a regular token query, that we can then name *eng* for convenience: `eng:[lemma="property"] :: eng.text_lang = "English"`. Then in a subsequent query, we can look for nouns in Dutch, but only in sentences that are translations of the English sentences we found, which we can find by making sure that the tuid attribute of the sentence that the `eng` token is in (`eng.s_tuid`) is the same as the tuid of the sentence we are using for the translation: `nld:[upos="NOUN"] :: match.text_lang = "Dutch" & eng.s_tuid = nld.s_tuid`. This way, we get the nouns in translation of the English sentences containing the word *property*.

Corpora that are aligned with *tuid* there can be alignments on various levels at the same time, so for instance also at a paragraph (*p_tuid*), or a the token-level (*tuid*). So `nld:[] :: match.text_lang = "Dutch" & eng.tuid = nld.tuid` will directly give us the Dutch words that are used as translations of the English word *property*.

For a more efficient but more limited way of searching through sentence-aligned corpora, pando also offers an easier syntax: `[form="property"] with [form="bezit"]` will only find occurrences of *property* that are in a sentence that is aligned with a sentence in which the word *bezit* occurs.

## Regular expressions

To look for patterns inside words, CQL allows regular expressions in token restrictions. In regular expressions, you can look for optional letters, repeated letters, etc. In pando-CQL, regular expressions use the format that is made popular by Python: `[form = /pan.*tion/]`. Contrary to CWB-CQL, regular expressions are used directly, and not as "word-bound matches". So where in CWB, `[form = ".*tion"]` will match all and only words ending in *-tion*, in pando-CQL, `[form = /.*tion/]` will give an words that contain any number of characters followed by *tion*, so including *conditional*. It will have the exact same matches as `[form = /tion/]`, since the optional characters at the beginning do not have any effect in this particular query. To mimic the query behaviour of CWB, the query has to be explictly bound to the word boundaries: `[form = /^.*tion$/]`, which is more naturally expressed as `[form = /tion$/]`.


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

In both TEITOK and UD, a contraction like the French *aux* is treated as multi-layered, encoding both *aux* as a word, and the words *à* and *les* that it consists of. Pando does not have a multi-layered set-up, so instead, it treats the individual words as tokens, and the contracted form as a reserved region *contr*, with obligatorily a *form* attribute. And a simple query `"aux"` will look either for a token or a all the tokens of a contraction, so it will look for `[ form="aux" | contr_form="aux" ]+`.

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
| tabulate a.genre | values joined by | in text output, arrays in JSON |

Since aggregation functions count each item separately, the total counts for token-level attributes can exceed the number of tokens in the corpus, so percentages become more tricky to calculate.

Notice that there is no straightforward way to look for tokens that have value in their genres that is not *Book*, but it can be done by looking for a regular expression that matches anything except Book, such as *genre = /^(?!Book$).+/*.

The **`nvals(attr)`** function counts **non-empty** pipe-separated components of a value (or, for nested/overlapping region attributes, the number of **distinct** components across all covering regions at the token). Use a comparison and a numeric right-hand side: **`nvals(genre) > 2`**, **`nvals(wsd)=2`**. It applies to positional attributes, **`feats.X`** (0 or 1), and composite region attributes such as **`text_genre`**. For attributes not declared multivalue and with no `|` in the stored string, the count is 0 (empty or `_`) or 1 otherwise.


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
| depth | depth(a) < 4 | node a appears at most 4 levels below the root |
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

Mol