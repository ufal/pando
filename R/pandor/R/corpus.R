#' Open a pando corpus index
#'
#' Checks that `dir` is a pando index (it has a `corpus.info`) and that the pando program works.
#' For a TEITOK project folder, its `pando/` index is used.
#'
#' @param dir The index folder.
#' @param bin Path to the `pando` program (see [pando_bin()]).
#' @param args Extra command-line arguments for every query (e.g. `c("--threads", "4")`).
#' @return A `pando_corpus` object for [pando_run()], [pando_count()] and [pando_table()].
#' @examples
#' \dontrun{
#' corp <- pando_corpus("/data/ud/pando")
#' pando_count(corp, '[upos="VERB"]', by = c("text_lang", "lemma"))
#' }
#' @export
pando_corpus <- function(dir, bin = NULL, args = character()) {
  if (missing(dir) || !nzchar(dir)) stop("give the folder of a pando index", call. = FALSE)
  dir <- normalizePath(path.expand(dir), mustWork = FALSE)
  if (!dir.exists(dir)) stop("corpus folder does not exist: ", dir, call. = FALSE)
  if (!file.exists(file.path(dir, "corpus.info"))) {
    if (file.exists(file.path(dir, "pando", "corpus.info"))) {
      dir <- file.path(dir, "pando")   # a TEITOK project: its index
    } else {
      stop("not a pando index (no corpus.info): ", dir, call. = FALSE)
    }
  }
  info <- readLines(file.path(dir, "corpus.info"), warn = FALSE)
  value <- function(key) {
    line <- grep(paste0("^", key, "="), info, value = TRUE)
    if (length(line)) strsplit(sub("^[^=]*=", "", line[1]), ",", fixed = TRUE)[[1]] else character()
  }
  structure(list(dir = dir, bin = pando_bin(bin), args = as.character(args),
                 size = as.numeric(value("size")), attributes = value("positional"),
                 structures = value("structural"), region_attributes = value("region_attrs")),
            class = "pando_corpus")
}

#' @export
print.pando_corpus <- function(x, ...) {
  cat("<pando corpus> ", x$dir, "\n", sep = "")
  cat("  ", format(x$size, big.mark = ","), " tokens; attributes: ", paste(x$attributes, collapse = ", "), "\n", sep = "")
  if (length(x$structures)) cat("  structures: ", paste(x$structures, collapse = ", "), "\n", sep = "")
  invisible(x)
}
