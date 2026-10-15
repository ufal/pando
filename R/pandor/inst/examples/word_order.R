# Greenberg's word-order correlations, measured on a multilingual corpus with dependency trees,
# drawn with ggplot2 (the R version of python/examples/word_order_universals.py).
#
# Greenberg (1963) and Dryer (1992): languages where the object precedes the verb (OV) tend to
# have postpositions, VO languages prepositions; the order of adjective and noun is not one of the
# pairs that correlate with OV/VO (Dryer). pando samples the dependency pairs with their corpus
# positions; R counts the orders per language, correlates them across languages and plots them.
#
#   Rscript word_order.R /path/to/index [language field] [output.png]
#   (PANDO_BIN=/path/to/pando/build/pando if pando is not on the PATH)
#
# The language field is a region attribute of the hits: text_lang (TEITOK corpora such as ntrex),
# text_langcode (a UD collection indexed per language), …

library(pandor)
library(ggplot2)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("usage: Rscript word_order.R /path/to/index [language field] [output.png]")
lang_field <- if (length(args) >= 2) args[2] else "text_lang"
out_png <- if (length(args) >= 3) args[3] else "word_order.png"
min_pairs <- 40          # pairs a language needs for every feature
sample_size <- 300000    # pairs sampled per feature

corp <- pando_corpus(args[1])

# feature → query; the head is `h`, the dependent `d`; "first" = the dependent comes first
features <- c(
  OV    = 'h:[upos="VERB"] > d:[deprel="obj"]',
  Postp = 'h:[upos="NOUN" | upos="PROPN" | upos="PRON"] > d:[deprel="case" & upos="ADP"]',
  AdjN  = 'h:[upos="NOUN"] > d:[deprel="amod" & upos="ADJ"]'
)

shares <- lapply(names(features), function(f) {
  t <- pando_table(corp, features[[f]], c(paste0("h.", lang_field), "h.cpos", "d.cpos"),
                   sample = sample_size, seed = 1)
  cat(sprintf("  %-5s %7d pairs sampled of %s\n", f, nrow(t), format(attr(t, "total"), big.mark = ",")))
  first <- t$d.cpos < t$h.cpos
  if (f == "Postp") first <- !first     # a postposition comes after its noun
  agg <- aggregate(first, by = list(lang = t[[1]]), FUN = function(x) c(n = length(x), share = mean(x)))
  data.frame(lang = agg$lang, feature = f, n = agg$x[, "n"], share = agg$x[, "share"])
})
long <- do.call(rbind, shares)
long <- long[long$lang != "_" & long$lang != "", ]

# languages with enough pairs for every feature, one row each
enough <- names(which(table(long$lang[long$n >= min_pairs]) == length(features)))
wide <- reshape(long[long$lang %in% enough, c("lang", "feature", "share", "n")],
                idvar = "lang", timevar = "feature", direction = "wide")
cat(sprintf("\n%d languages with at least %d pairs for every feature\n\n", nrow(wide), min_pairs))

rho <- function(a, b) cor(wide[[paste0("share.", a)]], wide[[paste0("share.", b)]], method = "spearman")
for (p in list(c("OV", "Postp"), c("OV", "AdjN"), c("Postp", "AdjN"))) {
  cat(sprintf("  %-5s ~ %-5s  Spearman rho = %+.2f\n", p[1], p[2], rho(p[1], p[2])))
}

g <- ggplot(wide, aes(share.OV, share.Postp)) +
  annotate("rect", xmin = 0.5, xmax = 1, ymin = 0.5, ymax = 1, alpha = 0.06, fill = "#4e79a7") +
  annotate("rect", xmin = 0, xmax = 0.5, ymin = 0, ymax = 0.5, alpha = 0.06, fill = "#4e79a7") +
  geom_hline(yintercept = 0.5, colour = "grey70", linewidth = 0.3) +
  geom_vline(xintercept = 0.5, colour = "grey70", linewidth = 0.3) +
  geom_point(aes(size = n.OV, colour = share.AdjN), alpha = 0.85) +
  geom_text(aes(label = lang), size = 3, vjust = -1.1, check_overlap = TRUE) +
  scale_x_continuous("Objects before their verb (share of pairs)", limits = c(0, 1), labels = scales::percent) +
  scale_y_continuous("Adpositions after their noun (share of pairs)", limits = c(0, 1.05), labels = scales::percent) +
  scale_size_area("Verb-object pairs", max_size = 7) +
  scale_colour_viridis_c("Adjective before noun", labels = scales::percent, limits = c(0, 1)) +
  labs(title = "OV languages have postpositions, VO languages prepositions",
       subtitle = sprintf("One point per language; Spearman rho = %.2f (OV ~ postpositions), %.2f (OV ~ adjective first)",
                          rho("OV", "Postp"), rho("OV", "AdjN")),
       caption = "Pairs sampled from the dependency trees with pando; Greenberg 1963, Dryer 1992") +
  theme_minimal(base_size = 11) +
  theme(legend.position = "right", plot.title.position = "plot")

ggsave(out_png, g, width = 9, height = 6.5, dpi = 150, bg = "white")
cat("\nPlot written to", out_png, "\n")
