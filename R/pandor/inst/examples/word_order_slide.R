library(pandor); library(ggplot2)
corpus <- pando_corpus(commandArgs(TRUE)[1])

dependent_first <- function(query) {
  t <- pando_table(corpus, query, c("h.text_langcode", "h.cpos", "d.cpos"), sample = 300000, seed = 1)
  n <- table(t[[1]])
  tapply(t$d.cpos < t$h.cpos, t[[1]], mean)[names(n)[n >= 40]]
}
ov    <- dependent_first('h:[upos="VERB"] > d:[deprel="obj"]')
postp <- 1 - dependent_first('h:[upos="NOUN" | upos="PROPN" | upos="PRON"] > d:[deprel="case" & upos="ADP"]')
lang  <- intersect(names(ov), names(postp))

ggplot(data.frame(lang, OV = ov[lang], Postp = postp[lang]), aes(OV, Postp, label = lang)) +
  geom_point() + geom_text(vjust = -0.8, check_overlap = TRUE) +
  labs(x = "objects before the verb", y = "adpositions after the noun")
ggsave("word_order.png", width = 7, height = 5)
