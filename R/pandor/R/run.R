#' Run a pando CQL program
#'
#' The whole program runs in one call of the pando program, so named queries can be used in the
#' same text: `'A = [lemma="say"]; count A by form'`.
#'
#' @param corpus A corpus from [pando_corpus()].
#' @param cql The program.
#' @param args Extra command-line arguments for this call.
#' @param raw Return pando's JSON answer (a list) as it is.
#' @return For `count`/`group`: a data frame (see [pando_flatten_count()]); for `tabulate`: a data
#'   frame (see [pando_table()]); otherwise pando's `result` as a list.
#' @export
pando_run <- function(corpus, cql, args = character(), raw = FALSE) {
  stopifnot(inherits(corpus, "pando_corpus"))
  out <- suppressWarnings(system2(corpus$bin, c("--api", "--json", corpus$args, args, shQuote(corpus$dir), shQuote(cql)),
                                  stdout = TRUE, stderr = TRUE))
  payload <- last_json(out)
  if (raw) return(payload)
  if (isFALSE(payload$ok)) stop("pando: ", error_text(payload$error), call. = FALSE)
  op <- payload$operation %||% payload$last_command %||% ""
  result <- payload$result
  if (op %in% c("count", "group")) return(pando_flatten_count(result))
  if (op == "tabulate") return(tabulate_frame(result))
  result
}

#' Count a query's hits by one or more fields
#'
#' All groups are returned (pando's display limits on groups are switched off), one row per
#' combination of values, with its count in `freq`.
#'
#' @param corpus A corpus from [pando_corpus()].
#' @param query The query (without a `count` command).
#' @param by The fields: token attributes (`"lemma"`), region attributes (`"text_lang"`), labelled
#'   tokens (`"a.form"`), as in pando's `count by`.
#' @return A data frame with a column per field and `freq`; the query's hits in `attr(, "total")`.
#' @examples
#' \dontrun{
#' pando_count(corp, '[lemma="say"]', by = c("text_lang", "form"))
#' }
#' @export
pando_count <- function(corpus, query, by) {
  program <- paste0("set group_limit 0; set child_limit 0; ", strip_semicolons(query),
                    "; count by ", paste(by, collapse = ", "))
  pando_run(corpus, program)
}

#' Tabulate a query's hits
#'
#' One row per hit, one column per field, as pando's `tabulate`. Corpus positions (`cpos` fields)
#' are numbers, other values text as in the corpus.
#'
#' @param corpus A corpus from [pando_corpus()].
#' @param query The query, with labels for the tokens you want fields of (`h:[…] > d:[…]`).
#' @param fields Fields such as `"h.lemma"`, `"h.cpos"`, `"h.text_lang"`.
#' @param sample A random sample of this many hits (`NULL`: all hits).
#' @param seed The sample's seed: the same seed gives the same sample.
#' @param limit At most this many rows (default: all, or the sample).
#' @return A data frame; the query's hits (before sampling) in `attr(, "total")`.
#' @examples
#' \dontrun{
#' t <- pando_table(corp, 'h:[upos="VERB"] > d:[deprel="obj"]',
#'                  c("h.text_lang", "h.cpos", "d.cpos"), sample = 10000, seed = 1)
#' }
#' @export
pando_table <- function(corpus, query, fields, sample = NULL, seed = NULL, limit = NULL) {
  n <- if (!is.null(limit)) limit else if (!is.null(sample)) sample else 2^52
  args <- character()
  if (!is.null(sample)) args <- c(args, "--sample", format(sample, scientific = FALSE))
  if (!is.null(seed)) args <- c(args, "--seed", format(seed, scientific = FALSE))
  program <- paste0(strip_semicolons(query), "; tabulate Last 0 ", format(n, scientific = FALSE), " ",
                    paste(fields, collapse = ", "))
  pando_run(corpus, program, args = args)
}

# ── helpers ─────────────────────────────────────────────────────────────────

`%||%` <- function(a, b) if (is.null(a)) b else a

strip_semicolons <- function(q) sub("[;[:space:]]+$", "", trimws(q))

error_text <- function(e) {
  if (is.list(e)) e <- e$message %||% jsonlite::toJSON(e, auto_unbox = TRUE)
  as.character(e)
}

# pando prints one JSON object per statement; the answer is the last one
last_json <- function(lines) {
  text <- paste(lines, collapse = "\n")
  starts <- gregexpr("(?m)^\\{", text, perl = TRUE)[[1]]
  if (starts[1] < 0) stop("pando gave no answer:\n  ", paste(utils::tail(lines, 5), collapse = "\n  "), call. = FALSE)
  # substring() stops after a million characters unless told where to end: answers are often longer
  jsonlite::fromJSON(substring(text, max(starts), nchar(text)), simplifyVector = FALSE)
}

tabulate_frame <- function(result) {
  fields <- unlist(result$fields)
  rows <- result$rows
  cols <- lapply(seq_along(fields), function(i) {
    v <- vapply(rows, function(r) as.character(r[[i]] %||% NA_character_), character(1))
    if (fields[i] == "cpos" || endsWith(fields[i], ".cpos")) as.numeric(v) else v
  })
  df <- as.data.frame(stats::setNames(cols, fields), stringsAsFactors = FALSE, check.names = FALSE)
  attr(df, "total") <- result$total_matches
  df
}
