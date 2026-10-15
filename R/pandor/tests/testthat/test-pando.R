# With the pando programs (PANDO_BIN, and pando-index next to it): a small index built here.

pando_programs <- function() {
  bin <- Sys.getenv("PANDO_BIN", Sys.which("pando")[[1]])
  index <- file.path(dirname(bin), "pando-index")
  if (!nzchar(bin) || !file.exists(bin) || !file.exists(index)) return(NULL)
  list(bin = bin, index = index)
}

build_corpus <- function(p) {
  dir <- tempfile("pandor-")
  dir.create(dir)
  vrt <- file.path(dir, "c.vrt")
  lines <- "<!-- positional-attributes: word lemma upos -->"
  for (lang in c("English", "Czech")) {
    lines <- c(lines, sprintf('<text id="%s" lang="%s">', tolower(lang), lang), "<s>")
    words <- if (lang == "English") c("she said that", "he says no", "they said it") else c("ona řekla to", "on říká ne")
    for (w in unlist(strsplit(words, " "))) {
      pos <- if (w %in% c("said", "says", "řekla", "říká")) "VERB" else "X"
      lemma <- if (w %in% c("said", "says")) "say" else if (w %in% c("řekla", "říká")) "říct" else w
      lines <- c(lines, paste(w, lemma, pos, sep = "\t"))
    }
    lines <- c(lines, "</s>", "</text>")
  }
  writeLines(lines, vrt, useBytes = TRUE)
  idx <- file.path(dir, "idx")
  system2(p$index, c("--format", "vertical", shQuote(vrt), shQuote(idx)), stdout = FALSE, stderr = FALSE)
  idx
}

test_that("counts and tables from a real index", {
  p <- pando_programs()
  skip_if(is.null(p), "pando and pando-index not found (set PANDO_BIN)")
  corp <- pando_corpus(build_corpus(p), bin = p$bin)
  expect_s3_class(corp, "pando_corpus")
  expect_true("lemma" %in% corp$attributes)

  df <- pando_count(corp, '[upos="VERB"]', by = c("text_lang", "lemma"))
  expect_equal(sum(df$freq), 5)
  expect_equal(df$freq[df$text_lang == "English" & df$lemma == "say"], 3)

  one <- pando_count(corp, '[lemma="say"]', by = "word")
  expect_equal(sort(one$word), c("said", "says"))

  t <- pando_table(corp, 'v:[upos="VERB"] n:[]', c("v.text_lang", "v.cpos", "n.cpos"))
  expect_equal(nrow(t), 5)
  expect_true(all(t$n.cpos - t$v.cpos == 1))
  s <- pando_table(corp, 'v:[upos="VERB"]', c("v.word"), sample = 2, seed = 1)
  expect_equal(nrow(s), 2)
  expect_equal(s, pando_table(corp, 'v:[upos="VERB"]', c("v.word"), sample = 2, seed = 1))

  expect_error(pando_run(corp, "[upos="), "pando")
})

test_that("clear errors for a wrong folder or program", {
  expect_error(pando_corpus(tempdir()), "not a pando index")
  expect_error(pando_bin("/no/such/pando"), "does not exist")
})
