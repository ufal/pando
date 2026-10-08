import sys
import pandas as pd
from pando_cql import Corpus

corpus = Corpus(sys.argv[1])

def dependent_first(query):
    t = corpus.table(query, "h.text_langcode, h.cpos, d.cpos", sample=300000, seed=1).frame()
    first = (t["d.cpos"] < t["h.cpos"]).groupby(t["h.text_langcode"])
    return first.mean()[first.size() >= 40]

orders = pd.DataFrame({
    "OV":    dependent_first('h:[upos="VERB"] > d:[deprel="obj"]'),
    "Postp": 1 - dependent_first('h:[upos="NOUN" | upos="PROPN" | upos="PRON"] > d:[deprel="case" & upos="ADP"]'),
    "AdjN":  dependent_first('h:[upos="NOUN"] > d:[deprel="amod" & upos="ADJ"]'),
}).dropna()

print(orders.corr(method="spearman").round(2))
print("OV + prepositions:", orders.query("OV > .5 and Postp < .5").index.tolist())
print("VO + postpositions:", orders.query("OV < .5 and Postp > .5").index.tolist())
