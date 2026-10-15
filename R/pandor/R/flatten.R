#' Flatten a pando count result into a data frame
#'
#' pando answers a `count` by one field with a list of rows and by several fields with a tree
#' (each value of the first field, the values of the next under it, …). Only the leaves become rows:
#' one per combination of values, with a column per field and the count in `freq`.
#'
#' @param result The `result` of pando's JSON answer to a `count`.
#' @param freq The name of the count column.
#' @return A data frame; the query's hits in `attr(, "total")`.
#' @export
pando_flatten_count <- function(result, freq = "freq") {
  fields <- unlist(result$fields)
  if (!length(fields)) stop("not a count result (no fields)", call. = FALSE)
  rows <- list()
  if (!is.null(result$hierarchy)) {
    walk <- function(nodes, path) {
      for (node in nodes) {
        path[[node$field]] <- as.character(node$value %||% NA_character_)
        if (length(node$children)) {
          walk(node$children, path)
        } else {
          rows[[length(rows) + 1]] <<- c(path[fields], stats::setNames(list(node$count), freq))
        }
      }
    }
    walk(result$hierarchy, list())
  } else if (!is.null(result$rows)) {
    if (length(fields) != 1) stop("rows without a hierarchy need exactly one field", call. = FALSE)
    for (r in result$rows) {
      rows[[length(rows) + 1]] <- stats::setNames(list(as.character(r$key %||% NA_character_), r$count), c(fields, freq))
    }
  }
  cols <- lapply(c(fields, freq), function(f) {
    v <- lapply(rows, function(r) r[[f]] %||% NA)
    if (f == freq) as.numeric(unlist(v)) else as.character(unlist(v))
  })
  df <- as.data.frame(stats::setNames(cols, c(fields, freq)), stringsAsFactors = FALSE, check.names = FALSE)
  attr(df, "total") <- result$total_matches
  df
}
