#' Find the pando program
#'
#' In this order: `path`, the `PANDO_BIN` environment variable, `pando` on the `PATH`. A path given
#' (or set in `PANDO_BIN`) is used as it is, without falling back to another one.
#'
#' @param path Path to the `pando` executable, or `NULL`.
#' @param check Run `pando --version` to make sure it works here.
#' @return The full path of the executable.
#' @export
pando_bin <- function(path = NULL, check = TRUE) {
  source <- "path argument"
  if (is.null(path) || !nzchar(path)) {
    path <- Sys.getenv("PANDO_BIN", "")
    source <- "PANDO_BIN"
  }
  if (!nzchar(path)) {
    path <- Sys.which("pando")[[1]]
    source <- "PATH"
    if (!nzchar(path)) {
      stop("pando not found: tried the path argument, PANDO_BIN and the PATH.\n",
           "  Build pando, then add it to the PATH or set e.g.\n",
           "  Sys.setenv(PANDO_BIN = \"/path/to/pando/build/pando\")", call. = FALSE)
    }
  }
  path <- normalizePath(path.expand(path), mustWork = FALSE)
  if (!file.exists(path) || dir.exists(path)) stop("pando does not exist (", source, "): ", path, call. = FALSE)
  if (file.access(path, 1) != 0) stop("pando is not executable (", source, "): ", path, call. = FALSE)
  if (check) {
    out <- suppressWarnings(system2(path, "--version", stdout = TRUE, stderr = TRUE))
    status <- attr(out, "status")
    if (!is.null(status) && status != 0) {
      stop("pando --version failed (", source, "): ", path, "\n  ", paste(out, collapse = "\n  "), call. = FALSE)
    }
  }
  path
}
