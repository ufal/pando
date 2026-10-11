# Pando Corpus Query Language

The native query language of pando is called pando-CQL, heavily modeled after [CWB-CQL](https://cwb.sourceforge.io/files/CQP_Manual/), with additions that are partially taken from [SketchEngine](https://www.sketchengine.eu/documentation/corpus-querying/), and partially from [PML-TQ](https://ufal.mff.cuni.cz/pmltqdoc/doc/pmltq_tutorial_web_client.html). These guidelines give a general introduction to pando-CQL (henceforth simply CQL when there is no confusion), with example queries illustrated against the **sample corpus** shipped with this repository: `test/data/sample.conllu`, which is annotated using [Universal Dependencies](https://universaldependencies.org/).
See [Sample-Corpora.md](Sample-Corpora.md) for the bundled CoNLL-U and the full-UD download script.

This page starts from scratch: no experience with corpus queries is needed. If you already know CQL from CWB or Manatee, most of it will look familiar: things pando offers that traditional CQL does not are marked with a red star <span style="color:#d00">★</span>. All details and the complete lists of functions and commands are in the [reference](PANDO-CQL-Reference.md).

## Token Queries

The base query in CQL searches for a token (a word or punctuation mark), with optionally one or more restrictions. A (query) token is represented with two square brackets, with any restrictions we want to place on the tokens we want to find inside the brackets. So if we want to look for a word with the lemma "book", we express that in CQL as `[lemma="book"]`, which will find all examples of *book* and *books*, as a noun, but also *book*, *books*, *booked* and *booking* as a verb.

We can add several restrictions on a token, putting a **&** between the restrictions to say that all restrictions should be met (a **|** would instead match if any of the restrictions is met). So if we only want to find occurrences of the verbs *book*: `[lemma="book" & upos="VERB"]` will match only examples of *book*, *booking*, etc. as a verb. We can instead also look for either the lemma *book* or *cat*: `[lemma="book" | lemma="cat"]`.

CQL is a sequence-based query language, meaning we can look for sequences of tokens. This is done by putting several tokens in a row in our query. So if we want to only find occurrences of *book* that are preceded by a determiner, we express that as follows: `[upos="DET"] [lemma="book"]`.

## What is in my corpus?

Which restrictions you can use depends on how the corpus is annotated. In a corpus annotated with Universal Dependencies, every token typically has a `form` (the word as written), a `lemma` (its dictionary form), a `upos` (its part of speech, such as `NOUN`, `VERB` or `DET`), and more. To find out what your corpus has, ask it: `show attributes` lists the token attributes, `show values upos` lists all values of `upos` with how often each occurs, and `show regions` lists the larger units such as sentences and texts (see [Regions](#regions) below). A web interface usually shows the same information in its menus.

## Regular expressions

To look for patterns inside words, CQL allows *regular expressions*: patterns in which some characters have a special meaning. The most useful ones are `.`, which stands for any single letter, and `*`, which means "the previous thing any number of times", so `.*` stands for "anything". In pando-CQL, a pattern can be written in two ways. Between quotes, as in CWB and Manatee, the pattern has to match the whole word: `[form=".*tion"]` finds words that end in *-tion*. Between slashes, as in many programming languages, it can match anywhere in the word: `[form=/book/]` finds *book*, but also *notebook*, *booking* and *bookshelves*, and `[form=/^un.*able$/]` uses `^` and `$` to say where the word starts and ends. The [reference](PANDO-CQL-Reference.md#values-and-regular-expressions) lists what else regular expressions can do.

To find words that do *not* match, use `!=` instead of `=`: `[upos="NOUN" & lemma!="book"]` finds all nouns except *book*, and `[lemma!=/e/]` all lemmas without an *e*. A whole condition can be negated by putting `!` in front of it: `[!(upos="NOUN" | upos="VERB")]` finds everything that is neither a noun nor a verb.

Because `.`, `?`, `*` and a few other characters have a special meaning in patterns, you need a backslash to search for the character itself: `[form="\."]` finds full stops (without the backslash, `"."` means "any one character"). A single `?` or `*` on its own needs no backslash: `[form="?"]` finds question marks.



## Token repetitions

CQL is originally purely sequence based, so a query like `[upos="DET"] [upos="NOUN"]` will not find cases with anything between the determiner and the noun, as in *the green book*, which we probably intended to find. That is why CQL has repetition operators, placed directly after the closing bracket. For instance, `[]?` means any optional token (technically 0 or 1 occurrences of the token with in this case no restrictions). So the query `[upos="DET"] []? [lemma="book"]` will still find *the book*, but allows a single token in between the determiner and book, so *the green book*, *some interesting books*, etc. But it will also include results we probably did not intend, like *some people book* (their hotel early). We could prevent that by tightening the query to only allow adjectives, and saying there can be any number of adjectives: `[upos="DET"] [upos="ADJ"]* [lemma="book"]`, where the * stands for "0 or more". 

There are several kinds of repetition, depending on how many tokens are allowed or required:

| symbol | interpretation |
| ----- | ----- |
| []? | 0 or 1 tokens |
| []* | 0 or more tokens |
| []+ | 1 or more tokens |
| []{m,n} | between m and n tokens |
| []{m} | exactly m tokens |
| []{m,} | m or more tokens |


## Giving tokens a name

You can give a query token a *name* by putting a name and a colon in front of it: `a:[upos="DET"]` is a token called *a*. The name does not change what is found, but it lets us refer to that token later in the query, after a double colon `::`. What comes after `::` are conditions on the match as a whole, which can talk about any named token.

That makes it possible to compare tokens with each other, which you cannot do inside a single token. For instance, `a:[] b:[] :: a.form = b.form` finds two identical words in a row, like *that that* in *I knew that that book was yours*. A condition about just one named token works the same as putting it inside the token: `a:[upos="VERB"] :: a.lemma = "say"` finds exactly what `[upos="VERB" & lemma="say"]` finds.

After `::` you can also use functions. For instance, `f` gives the frequency of a word in the whole corpus, so `a:[upos="NOUN"] :: f(a.lemma) > 10` only finds nouns whose lemma occurs more than 10 times, and `a:[upos="NOUN"] [] b:[upos="NOUN"] :: a.lemma = b.lemma` two occurrences of the same noun with one word in between. Several conditions are joined with `&`. You will see more functions along the way; the full list is in the [reference](PANDO-CQL-Reference.md#global-functions).

<span style="color:#d00">★</span> Names do not disappear when the query is done. You can use them afterwards to count or show results (see [Counting and comparing](#counting-and-comparing)), and even in a later query, which is how [parallel corpora](#parallel-corpora) are searched.

## Capitals and accents

`[form="the"]` finds *the*, but not *The* at the start of a sentence. To ignore the difference between capitals and small letters, put `%c` after the value: `[form="the" %c]`. In the same way, `%d` ignores accents, so `[form="een" %d]` finds the Dutch *een* as well as *één*, and `%cd` ignores both. In a regular expression, put `(?i)` at the start instead: `[form=/(?i)^the/]`.

## Words in quotes

For the most common query of all, a single word, there is a shortcut: `"book"` on its own means the same as `[form="book"]`, and `/book.*/` as `[form=/book.*/]`.

<span style="color:#d00">★</span> In corpora that split contractions into their parts, like French *aux* (*à* + *les*) or English *can't* (*can* + *n't*), a word in quotes also finds the contraction as a whole: `"can't"` finds *can't*, whether it is stored as one token or as two. The [reference](PANDO-CQL-Reference.md#quoted-words-and-contractions) explains how.

<a id="dependency-relations"></a>
## Dependency relations <span style="color:#d00">★</span>

Our query `[upos="DET"] [upos="ADJ"]* [lemma="book"]` was really an attempt to find the word *book* with a determiner that belongs to it. A sequence can only approximate that, by spelling out what may come in between. In a corpus annotated with dependency syntax, such as Universal Dependencies, every word is linked to the word it depends on (its *head*), and pando lets you search those links directly.

A dependency relation is written as an arrow between two tokens, pointing from the head to its dependent: `[lemma="book"] > [upos="DET"]` finds *book* with a determiner that depends on it, and `[upos="DET"] < [lemma="book"]` is the same thing written the other way around. This no longer finds *some people book*, because there *some* does not depend on *book*. And it finds the relation wherever the words are: `[lemma="book"] > [upos="ADJ"]` finds *green book*, but also *books that are green and wet*, where *green* is far away from *book*. `>>` and `<<` do the same for any distance in the tree: a word anywhere below or above the other.

Sequence and dependency can be mixed: in `[upos="DET"] [upos="ADJ"] < [lemma="book"]` the determiner comes directly before the adjective, and the adjective depends on *book*. A relation can also be negated: `[upos="DET"] !< [lemma="book"]` finds *book* without a determiner.

The arrows always connect two single tokens, so they cannot be used next to a repeated token like `[upos="ADJ"]*`. For more complex cases there is a second notation, borrowed from PML-TQ: the relation is written *inside* the token, as a restriction. `[upos="NOUN" & child [upos="DET"]]` is a noun with a determiner as its dependent. This notation lets you ask for several dependents at once, and besides `child` you can use `parent`, `ancestor`, `descendant` and `sibling`. Negation works with `not`: `[upos="VERB" & not child [deprel="nsubj"]]` finds verbs without a subject.

You can also count dependents: `[upos="NOUN" & count(child [upos="ADJ"]) >= 2]` finds nouns with at least two adjectives.

Inside a token you can also look at the words around it: `next [...]` and `prev [...]` are the word right after and right before it, `after [...]` and `before [...]` any word later or earlier in the same sentence. That is useful mostly with `not`: `[upos="ADJ" & not next [upos="NOUN"]]` finds adjectives that are not directly followed by a noun, something a sequence cannot express.

A dependency relation says nothing about word order: the adjective in `[upos="ADJ"] < [upos="NOUN"]` can come before or after its noun. With names, you can add the order after `::`, where `<` and `>` compare positions: `a:[upos="ADJ"] < b:[upos="NOUN"] :: a > b` finds adjectives that come *after* the noun they belong to, as in French *un livre vert*. (Inside the token sequence, `<` and `>` are dependency arrows; after `::` they compare.)

Universal Dependencies stores grammatical features (number, case, tense, …) together in one field, `feats`, like `Number=Plur|Case=Nom`. You can search a single feature with a slash: `[upos="NOUN" & feats/Number="Plur"]` finds plural nouns.

## Regions

A corpus is more than a long row of words: the words are grouped into sentences, paragraphs, texts, or in spoken corpora into utterances by different speakers. We call such groups *regions*. Regions can have properties of their own: a text has a title, a genre, a language, perhaps a date; an utterance has a speaker. In CWB these are called *structural attributes*.

The property of a region is written as the name of the region, an underscore, and the name of the property: the genre of a text is `text_genre`, the speaker of an utterance `u_who`. The simplest way to use them is inside a token, like any other restriction: `[lemma="cat" & text_genre="Book"]` finds *cat* in texts of the genre *Book*.

Often we want the whole match to stay inside one region. `[upos="ADJ"] [lemma="book"] within s` does not find an adjective at the end of one sentence followed by *book* at the start of the next; `within text_genre="Book"` keeps the whole match inside a text of genre *Book*. You can also ask for a region that contains something else: `[upos="VERB"] within s having [lemma="cat"]` finds verbs in sentences that also contain the word *cat*. And `not within` does the opposite.

Regions also have a beginning and an end, which you can use in a sequence, as in CWB: `<s>` is the start of a sentence and `</s>` its end, so `<s> [upos="PRON"]` finds pronouns at the start of a sentence, and `[upos="PUNCT"] </s>` punctuation at the end. You can add properties: `<text genre="Book"> []` is the first word of each book. Be careful that `<s> [lemma="cat"] </s>` means a sentence that consists of only the word *cat*; for *cat* anywhere in a sentence, use `within s`.

<span style="color:#d00">★</span> Pando also allows regions without any words in them. In a spoken corpus, a pause is not a word, but it is very relevant, so it can be stored as an empty region between two words. You use it in the same way: `[form="the"] </pause>` finds *the* followed by a pause, and `<pause> [form="the"]` a pause right before *the*.

<a id="searching-for-regions"></a>
## Searching for regions <span style="color:#d00">★</span>

So far, regions only restricted where we looked for words. But in pando you can also look for the regions themselves. Give the region a name, as with tokens, and leave out the tokens: `t:<text genre="Book">` finds all texts of the genre *Book*, and `s:<s> :: tcnt(s) > 25` finds all sentences of more than 25 words (`tcnt` counts the tokens in a region). If the corpus has error annotation, `e:<err type="agr">` finds all agreement errors as a whole.

A named region can also be combined with tokens: `s:<s> [upos="VERB"]` finds sentences that start with a verb, and gives that sentence the name *s*, so you can count or show its properties afterwards. For now, a query can contain only one region on its own like this; relating several regions to each other directly is being added.

<a id="phrases-in-a-dependency-tree"></a>
## Phrases in a dependency tree <span style="color:#d00">★</span>

Dependency syntax has no regions for phrases: the noun phrase *the old man* is simply *man* with everything that depends on it, its *subtree*. Pando can turn such a subtree into something you can count and show like a region. You first find its head, and then, after `::`, give the subtree a name: `n:[deprel="nsubj"] :: np:dep_subtree(n) & tcnt(np) > 6` finds subjects (the whole phrase, not just its head word) of more than six words.

A subtree is not always one continuous stretch of words: in *a man came who smiled*, the subject is *a man ... who smiled*, with *came* in between. `gaps(np)` counts such interruptions, so `n:[deprel="nsubj"] :: np:dep_subtree(n) & gaps(np) > 0` finds subjects that are split up, which in linguistics are called *non-projective*.

<a id="attributes-with-several-values-and-regions-inside-regions"></a>
## Attributes with several values, and regions inside regions <span style="color:#d00">★</span>

Some properties can have more than one value at the same time: a text can be both a *Poem* and a *Song*. In pando, such a property can be stored with several values, and `[text_genre="Song"]` then means that *Song* is one of them. In the same way, regions of the same kind can sit inside each other, like phrases in a syntax tree, where a noun phrase can be part of a verb phrase. `[node_type="NP"]` then means that the word is inside some noun phrase. You rarely have to think about this when writing a query, but it does change how some things are counted; the [reference](PANDO-CQL-Reference.md#multivalue-attributes-and-overlapping-regions) has the details.

<a id="annotations-with-gaps"></a>
## Annotations with gaps <span style="color:#d00">★</span>

Not every annotation covers neighbouring words. A phrasal verb can be split, as in *Which boss did he PICK the package UP FOR*, and an error can involve two words far apart. Pando stores such annotations as groups of words. You can search them as a whole, `<err code="SPLIT">` gives each such error with all its parts, or use their properties inside a token like anything else: `[lemma="over" & err_type="SPLIT"]`.

Such annotations can also come from someone else than the corpus makers: an *overlay* corpus adds your own annotations on top of an existing corpus, without changing it, and you search them in the same way: `<overlay-mine-mwe type="split-phrasal">` finds the split phrasal verbs annotated in the overlay *mine*.

## Parallel corpora

In a parallel corpus, texts are aligned with their translations, for instance sentence by sentence. The simplest search is with `with`: `[form="property"] with [form="bezit"]` finds *property* in English sentences whose Dutch translation contains *bezit*.

For more control, you can use names across queries. Names in pando do not disappear after a query, so you can first look for English *property*, `eng:[lemma="property"] :: eng.text_lang = "English"`, and then for Dutch nouns in the translations of exactly those sentences: `nld:[upos="NOUN"] :: nld.text_lang = "Dutch" & eng.s_tuid = nld.s_tuid`, where `s_tuid` is the identifier that links a sentence to its translation. See [Aligned corpora and parallel queries](Aligned-Corpora-and-Parallel-Queries.md) for more.

## Counting and comparing

Finding examples is often only the first step: we want to know how often something occurs. The simplest case is a frequency list: `[upos="NOUN"]; count by lemma` lists all nouns in the corpus, the most frequent first. The `;` separates the query from what you want to do with its results.

You can also give a whole query a name, and then ask questions about its results: `Books = a:[lemma="book"]; count Books by a.form` finds all forms of *book* and counts how often each of them occurs. If you do not name a query, it is called `Last`, and commands without a name use the last query, so `a:[lemma="book"]; count by a.form` does the same.

You can count by several properties at once: `count Books by a.form, a.text_genre` gives the forms of *book* per genre. <span style="color:#d00">★</span> When one of the properties belongs to a region, like the genre here, pando also gives the *relative* frequency: how frequent each form is compared to the size of that genre in the corpus, so that a large and a small genre can be compared. `freq` gives the same as *instances per million* words. If the texts in your corpus have a date, you can also count over time: `a:[lemma="book"]; count by decade(a.text_date)` (or `year`, `century`).

<span style="color:#d00">★</span> *Collocations* are the words that typically occur together with a word, more often than you would expect by chance. `[lemma="book" & upos="VERB"]; coll by lemma` lists the words most typical around the verb *book*. With dependencies, we can look at the words that are grammatically connected instead of just nearby: `[lemma="book" & upos="NOUN"]; dcoll amod by lemma` gives the adjectives that most typically modify the noun *book*, and `dcoll head by lemma` the words that *book* most typically depends on.

<span style="color:#d00">★</span> *Keyness* shows which words are typical for part of a corpus compared to the rest: `[text_lang="French"]; keyness by lemma` gives the lemmas that are most characteristic of the French texts, and `Fr = [text_lang="French"]; En = [text_lang="English"]; keyness Fr vs En by upos` compares two parts with each other.

## Choosing what you see

By default, the results of a query are shown as a concordance (*keyword in context*): each match with a few words to its left and right. Instead, you can ask for a table with exactly the information you want: `Books = a:[lemma="book"]; tabulate Books a.form, a.upos, a.text_genre` gives the form, part of speech and genre of each match. `sort Books by form` sorts the matches, and `size Books` only tells you how many there are. When a query has thousands of results, `set sample 50` shows 50 random ones instead of the first 50, which gives a fairer picture; `set context 10` shows more words around each match.

How much context you see, how many results, which collocation measures, and so on, are settings rather than part of the query. They, and all other commands and functions, are listed in the [reference](PANDO-CQL-Reference.md).
