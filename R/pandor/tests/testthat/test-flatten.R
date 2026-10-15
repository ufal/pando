# pando's count answers in the shapes the CLI gives (one field: rows; several: a tree)

test_that("one field: rows", {
  res <- list(fields = list("form"), total_matches = 9,
              rows = list(list(key = "said", count = 6, pct = 66.7), list(key = "says", count = 3, pct = 33.3)))
  df <- pando_flatten_count(res)
  expect_equal(names(df), c("form", "freq"))
  expect_equal(df$form, c("said", "says"))
  expect_equal(df$freq, c(6, 3))
  expect_equal(attr(df, "total"), 9)
})

test_that("several fields: the leaves of the tree, one row each", {
  res <- jsonlite::fromJSON('{"fields": ["text_lang", "upos"], "total_matches": 7, "hierarchy": [
    {"field": "text_lang", "value": "English", "count": 5, "children": [
      {"field": "upos", "value": "VERB", "count": 4}, {"field": "upos", "value": "NOUN", "count": 1}]},
    {"field": "text_lang", "value": "Czech", "count": 2, "children": [
      {"field": "upos", "value": "VERB", "count": 2}]}]}', simplifyVector = FALSE)
  df <- pando_flatten_count(res)
  expect_equal(nrow(df), 3)
  expect_equal(df$text_lang, c("English", "English", "Czech"))
  expect_equal(df$upos, c("VERB", "NOUN", "VERB"))
  expect_equal(df$freq, c(4, 1, 2))
})

test_that("a custom count column, and an empty result", {
  res <- list(fields = list("form"), rows = list(list(key = "a", count = 1)))
  expect_equal(names(pando_flatten_count(res, freq = "n")), c("form", "n"))
  empty <- pando_flatten_count(list(fields = list("form"), rows = list()))
  expect_equal(nrow(empty), 0)
  expect_error(pando_flatten_count(list()), "no fields")
})

test_that("the last JSON object of several is the answer", {
  lines <- c('{"ok": true, "operation": "query"', '}', '{"ok": true, "operation": "count",', '"result": {"x": 1}}')
  expect_equal(pandor:::last_json(lines)$operation, "count")
})

test_that("answers longer than a million characters are read whole", {
  rows <- paste(sprintf('    ["Swedish", "%d", "%d"]', 1:60000, 2:60001), collapse = ",\n")
  lines <- strsplit(paste0('{"ok": true, "operation": "tabulate", "result": {\n  "fields": ["l", "a.cpos", "b.cpos"],\n',
                           '  "rows": [\n', rows, '\n  ]\n}}'), "\n")[[1]]
  expect_gt(sum(nchar(lines)), 1e6)
  expect_length(pandor:::last_json(lines)$result$rows, 60000)
})

test_that("tabulate rows: positions as numbers, the rest as text", {
  df <- pandor:::tabulate_frame(list(fields = list("h.text_lang", "h.cpos", "d.cpos"), total_matches = 78436,
                                     rows = list(list("Swedish", "2572837", "2572838"), list("Czech", "2972533", "2972531"))))
  expect_equal(df$h.text_lang, c("Swedish", "Czech"))
  expect_equal(df$d.cpos - df$h.cpos, c(1, -2))
  expect_equal(attr(df, "total"), 78436)
})
