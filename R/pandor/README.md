# pandor

R client for [pando](../../README.md): run CQL on a pando corpus index and get **counts** and **hit
tables** as data frames, ready for analysis and ggplot2. The R counterpart of the Python package
[`pando_cql`](../../python/README.md); like it, it only runs the `pando` program (`--api --json`):
no compiled code, no changes to pando.

## Install

From the pando repository:

```r
install.packages("jsonlite")                 # the only dependency
install.packages("pando/R/pandor", repos = NULL, type = "source")
```

It needs the `pando` program: on the `PATH`, or `Sys.setenv(PANDO_BIN = "/path/to/pando/build/pando")`.

## Use

```r
library(pandor)
corp <- pando_corpus("/path/to/index")        # a TEITOK project folder works too (its pando/)

# counts: one row per combination of values, all of them (no display limits)
pando_count(corp, '[lemma="say"]', by = c("text_lang", "form"))

# hits: one row per hit, a column per field; corpus positions as numbers
t <- pando_table(corp, 'h:[upos="VERB"] > d:[deprel="obj"]',
                 c("h.text_lang", "h.cpos", "d.cpos"), sample = 10000, seed = 1)
mean(t$d.cpos < t$h.cpos)                    # share of objects before their verb

# any program; named queries in the same text
pando_run(corp, 'A = [lemma="say"]; count A by text_lang')
```

`attr(x, "total")` gives the query's hits (before sampling). Errors from pando (a query mistake,
an unknown attribute) are R errors with pando's message.

## Example with ggplot2

[`inst/examples/word_order.R`](inst/examples/word_order.R): Greenberg's word-order correlations
(object before verb ~ postpositions) measured on a corpus with dependency trees, one point per
language:

```sh
Rscript inst/examples/word_order.R /path/to/index text_lang word_order.png
```

## Tests

```r
testthat::test_dir("tests/testthat")         # with PANDO_BIN set, also against a small index built on the fly
```

## Corpora on a server

This package, like the Python one, reads an index on the machine it runs on. Querying corpora that
live only on a server (such as LINDAT's) through FQS over HTTP is planned as a second way in, once
access for scripts is settled.
