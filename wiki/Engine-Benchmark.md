# Engine benchmark: pando and other corpus engines

> **Work in progress** (generated 2026-10-30). Measurements are still being added; numbers may change
> when runs are repeated without other load. Plan and method: `dev/bench/ENGINE-BENCHMARK.md` (local).

How pando compares with other corpus query engines on the same data and the same queries: token, sequence and
dependency queries, aggregation (frequency tables), and indexing. Every engine is measured in the set-ups we could
build; the article uses each competitor's most favourable one (marked **article** below).

## Set-up

- **Machine:** Apple M1 Max (10 cores), 32 GB, macOS 14.5; external USB SSD for indexes and Docker.
- **Docker:** Docker Desktop (engine 29.9.2), Linux VM with 20 GB; every engine container limited to 8 CPUs and
  16 GB; indexes in Docker volumes (never in folders mounted from the Mac).
- **Versions:** pando commit `bace06c` (Linux arm64 build); BlackLab 4.1.1 (built from source for arm64);
  PostgreSQL 16.15 with pmltq-server (`6e80931`); PMLTQ 3.0.2, PMLTQ::Commands 2.0.3, PMLTQ::PML2BASE 3.0.1;
  Treex 2.20210102 + ufal/treex `df815f1`; Manatee: manatee-open 2.225.8 natively (FQS's library, fqs-manatee 0.4,
  FQS `c80e7d9`; KonText 0.22.0-alpha's own Manatee runs under amd64 emulation and is not used for comparison);
  CWB 3.5.0 natively; Odinson `9b265c8` (Lucene; our query driver `Bench.scala`, one JVM kept running);
  dep_search (TurkuNLP) `550f5d4` with its LMDB databases.
- **Engine settings:** no result caches anywhere (each timed request does the work); BlackLab: no limits on
  hits retrieved or counted, case-sensitive matching (as pando), 8 threads per search; pando-server:
  `--cache-mb 0 --result-cache 0 --query-threads 8`; PostgreSQL: shared_buffers 4 GB, work_mem 64 MB,
  maintenance_work_mem 2 GB, effective_cache_size 12 GB, 4 parallel workers per query.
- **Timing:** 1 warm-up run, then the median of 5, warm page cache, wall-clock time as the client sees it.
  *first* = time to the first 100 hits; *total* = time to the full number of hits; *own* = the engine's own
  reported time (BlackLab's `countTime`). Odinson pages by sentence: *first* fetches 100 sentences. dep_search
  compiles every query to a C extension before searching and has no separate first page: *dep_search* is the whole
  run, *search only* without the compilation (4–7 s). BlackLab's server re-checks a waiting count every 100 ms, so its wall
  time for a total is never below ~100 ms.
- **Queries:** the same query shapes on every corpus, with lexical slots filled per corpus from its own frequency
  list (rare ≈ 2, mid ≈ 100, frequent ≈ 3,000 per million noun lemma). Hit counts must agree between engines;
  where they do not, the cell is marked ⚠ and explained below.


| corpus | tokens | slots (rare / mid / frequent) | grouped by |
|---|---:|---|---|
| ParlaMint 5.0, all parliaments | 1,549,744,675 | beskatning / ordfører / zakon | G1 = speaker gender, C2 = country |
| ParlaMint LV | 11,522,672 | aborts / pilnvara / lasījums | G1 = speaker gender, M2 = sitting month |
| UD 2.18 (all treebanks) | 38,181,322 | addition / uçuş / společnost | L1 = language, T2 = treebank |
| UD German-HDT | 3,455,580 | Abenteuer / Anschluß / Jahr | — (no metadata) |
| UD English-EWT | 254,820 | government / law / time | — |

**pando versions.** D1 and D7 are timed with a later pando than the other queries: `bace06c` plus two changes of 2026-10-26 (same counts): DEP-CHILD-FASTPATH, which evaluates `child [C]` / `not child [C]` inside a token set-wise (D7, plan path `dep_child`), and a sparse AND such as `[deprel="amod" & head#lemma="R"]` resolved from its rare side instead of the bitmap kernel (D1). **The demo server (KonText on the test VM) runs `bace06c` without them:** there D7 takes about 1.2 s on all of UD and 76 s on ParlaMint, D1 about 0.1 s on ParlaMint. pando's index sizes and indexing times are from `bace06c` plus the memory fix and `--misc-keys none` (same day).

## Retrieval: time to the first page and to the full count (ms)

### ParlaMint LV (11.5M tokens), Docker on SSD

**article:** pando, Manatee, BlackLab, PML-TQ. Manatee and CWB natively (no HTTP); PML-TQ counts sequences within the sentence, the others across sentence boundaries; CWB reports one match per start (S4 differs). ⚠ = count differs (see *Count discrepancies*).

|  | query | hits | pando first | pando total | Manatee first | Manatee total | CWB first | CWB total | BlackLab first | BlackLab total | BlackLab own | PML-TQ first | PML-TQ total | Odinson first | Odinson total | dep_search | dep_search search only |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T1 | rare lemma | 23 | 2.7 | 2.4 | 0.3 | 0.0 | 0.1 | 0.0 | 17 | 120 | 1.0 | 123 | 77 | 2.8 | 3.5 | 3,484 | 72 |
| T2 | mid-frequency lemma | 1,153 | 5.7 | 1.5 | 1.1 | 0.0 | 0.3 | 0.1 | 24 | 121 | 2.0 | 71 | 72 | 2.7 | 4.5 | 3,459 | 92 |
| T3 | frequent lemma | 33,932 | 3.3 | 1.5 | 1.1 | 0.5 | 1.4 | 1.1 | 36 | 123 | 6.0 | 86 | 142 | 2.4 | 17 | 5,087 | 1,758 |
| T4 | ADJ | 564,151 | 4.5 | 2.6 | 0.9 | 7.7 | 19 | 19 | 45 | 224 | 113 | 237 | 1,054 | 1.5 | 154 | 20,351 | 16,736 |
| T5 | NOUN | 3,217,990 | 3.6 | 1.4 | 0.9 | 44 | 105 | 105 | 270 | 655 | 539 | 193 | 7,917 | 1.6 | 322 | 36,719 ⚠ | 33,207 ⚠ |
| S1 | rare lemma + VERB | 8 | 1.7 | 1.7 | 14 | 14 | 0.1 | 0.0 | 12 | 118 | 1.0 | 166 | 105 | 2.8 | 2.7 | — | — |
| S1r | VERB + rare lemma | 6 | 1.5 | 3.1 | 15 | 15 | 98 | 99 | 14 | 114 | 1.0 | 151 | 88 | 2.7 | 3.2 | — | — |
| S2 | VERB + mid lemma | 113 | 4.6 | 1.7 | 14 | 15 | 99 | 98 | 26 | 120 | 3.0 | 140 | 141 | 3.6 | 5.1 | — | — |
| S3 | ADJ NOUN | 397,048 | 5.0 | 3.2 | 1.1 | 46 | 42 | 43 | 63 | 202 | 86 | 343 ⚠ | 4,149 ⚠ | 2.7 ⚠ | 289 ⚠ | — | — |
| S4 | DET, 0–2 tokens, NOUN | 282,265 | 3.2 | 3.2 | 32 | 133 | 23 ⚠ | 22 ⚠ | 148 | 175 | 56 | 313 ⚠ | 4,195 ⚠ | 5.9 ⚠ | 676 ⚠ | — | — |
| S5 | PRON VERB DET NOUN | 7,796 | 4.6 | 2.5 | 2.5 | 63 | 73 | 72 | 34 | 135 | 22 | 1,737 | 20,140 | 3.2 | 86 | — | — |
| D1 | rare lemma > amod | 2 | 2.7 | 2.6 | — | — | — | — | 24 | 126 | 10 | 144 | 87 | 1.8 | 23 | 3,461 | -20.2 |
| D1r | VERB > rare lemma | 8 | 1.8 | 1.6 | — | — | — | — | 394 | 380 | 191 | 132 | 80 | 6.0 | 28 | 3,446 | 49 |
| D2 | mid lemma > amod | 182 | 5.1 | 3.6 | — | — | — | — | 65 | 166 | 56 | 107 | 84 | 3.1 | 26 | 3,558 ⚠ | 85 ⚠ |
| D3 | NOUN > ADJ | 475,057 | 3.8 | 3.2 | — | — | — | — | 871 ⚠ | 13,422 ⚠ | 13,198 ⚠ | 295 | 5,772 | 3.3 | 2,535 | 19,214 ⚠ | 15,324 ⚠ |
| D4 | VERB > nsubj PRON | 275,983 | 4.3 | 3.1 | — | — | — | — | 194 ⚠ | 2,307 ⚠ | 2,184 ⚠ | 395 | 3,208 | 3.8 | 401 | 16,738 ⚠ | 12,944 ⚠ |
| D5 | VERB without nsubj | 1,039,420 | 3.6 | 3.5 | — | — | — | — | 449 | 711 | 533 | 192 | 20,145 | 3.1 | 614 | 27,592 ⚠ | 23,733 ⚠ |
| D6 | VERB >> mid lemma (any depth) | 1,947 | 5.7 | 2.1 | — | — | — | — | — | — | — | 93 | 189 | 11 ⚠ | 52 ⚠ | — | — |
| D7 | VERB with nsubj and obj | 156,998 | 4.3 | 6.6 | — | — | — | — | 241 ⚠ | 1,627 ⚠ | 1,505 ⚠ | 1,153 | 33,056 | 1.7 | 303 | 13,400 | 9,595 |

### ParlaMint 5.0, all parliaments (1.55B tokens)

**article:** all three. pando-server in Docker (8 CPUs, 16 GB, no caches) on SSD; Manatee natively through FQS's library, index on SSD; BlackLab in Docker on SSD, documents of at most 1,000 sentences, so sequences within the sentence. pando and Manatee count sequences across sentence boundaries.

|  | query | hits | pando first | pando total | Manatee first | Manatee total | BlackLab first | BlackLab total |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| T1 | rare lemma | 3,144 | 5.9 | 2.9 | 0.8 | 0.1 | n/m | n/m |
| T2 | mid-frequency lemma | 154,788 | 4.0 | 2.6 | 0.9 | 2.5 | n/m | n/m |
| T3 | frequent lemma | 1,861,691 | 4.8 | 2.2 | 0.9 | 26 | n/m | n/m |
| T4 | ADJ | 118,732,538 | 3.8 | 2.3 | 0.8 | 1,661 | n/m | n/m |
| T5 | NOUN | 316,807,420 | 3.8 | 1.9 | 0.7 | 3,964 | n/m | n/m |
| S1 | rare lemma + VERB | 56 | 6.2 | 3.2 | 1,199 | 1,201 | n/m | n/m |
| S1r | VERB + rare lemma | 514 | 3.9 | 3.5 | 260 | 1,691 | n/m | n/m |
| S2 | VERB + mid lemma | 14,625 | 4.7 | 13 | 242 | 1,678 | n/m | n/m |
| S3 | ADJ NOUN | 64,485,606 | 11 | 46 | 0.8 | 5,931 | n/m | n/m |
| S4 | DET, 0–2 tokens, NOUN | 129,832,692 | 5.3 | 107 | 3,858 | 48,911 | n/m | n/m |
| S5 | PRON VERB DET NOUN | 2,779,647 | 11 | 123 | 1.7 | 8,348 | n/m | n/m |
| D1 | rare lemma > amod | 1,089 | 4.7 | 2.7 | — | — | n/m | n/m |
| D1r | VERB > rare lemma | 1,875 | 8.0 | 5.3 | — | — | n/m | n/m |
| D2 | mid lemma > amod | 20,548 | 24 | 26 | — | — | n/m | n/m |
| D3 | NOUN > ADJ | 85,149,597 | 6.0 | 39 | — | — | n/m | n/m |
| D4 | VERB > nsubj PRON | 39,662,243 | 4.8 | 68 | — | — | n/m | n/m |
| D5 | VERB without nsubj | 100,495,889 | 4.8 | 137 | — | — | n/m | n/m |
| D6 | VERB >> mid lemma (any depth) | 134,576 | 5.2 | 16 | — | — | n/m | n/m |
| D7 | VERB with nsubj and obj | 31,233,364 | 4.2 | 349 | — | — | n/m | n/m |

### UD 2.18 (38.2M tokens) through KonText: first page

What a KonText user waits for before the first page appears (`/view`, 100 lines). Routes as in the next table. pando and Manatee via FQS answer the page first and count after (FQS counts Manatee's total in the background); in KonText's own Manatee the first page comes only after the whole concordance is computed (first ≈ full count).

|  | query | hits | pando via FQS (1 thread) | pando via FQS (8 threads) | Manatee via FQS | Manatee in KonText, mounted | Manatee in KonText, vol. cache | Manatee in KonText, volumes |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| T1 | rare lemma | 77 | 28 | 29 | 27 | 89 | 61 | 35 |
| T2 | mid-frequency lemma | 3,787 | 29 | 32 | 31 | 83 | 63 | 47 |
| T3 | frequent lemma | 21,826 | 38 | 41 | 34 | 74 | 64 | 51 |
| T4 | ADJ | 2,630,105 | 28 | 32 | 31 | 731 | 467 | 395 |
| T5 | NOUN | 8,541,955 | 28 | 36 | 33 | 2,571 | 1,144 | 1,415 |
| S1 | rare lemma + VERB | 0 | 23 | 22 | 34 | 56 | 60 | 37 |
| S1r | VERB + rare lemma | 0 | 25 | 22 | 61 | 83 | 71 | 52 |
| S2 | VERB + mid lemma | 860 | 32 | 40 | 79 | 108 | 104 | 78 |
| S3 | ADJ NOUN | 1,296,236 | 38 | 34 | 38 | 526 | 346 | 358 |
| S4 | DET, 0–2 tokens, NOUN | 2,427,669 | 41 | 38 | 187 | 2,558 | 1,960 | 2,022 |
| S5 | PRON VERB DET NOUN | 32,028 | 41 | 41 | 42 | 235 | 219 | 208 |

### UD 2.18 (38.2M tokens) through KonText: full count

Until KonText reports the concordance as finished (`/get_conc_cache_status` polled every 10 ms). Every route behind the same KonText. pando and Manatee via FQS run natively on the Mac (indexes on a hard disk, warm); *Manatee in KonText* is KonText's own Manatee in Docker, with its index and concordance cache mounted from the Mac (*mounted*), its cache in a volume (*vol. cache*), or both in volumes (*volumes*). **article:** pando via FQS, Manatee via FQS.

|  | query | hits | pando via FQS (1 thread) | pando via FQS (8 threads) | Manatee via FQS | Manatee in KonText, mounted | Manatee in KonText, vol. cache | Manatee in KonText, volumes |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| T1 | rare lemma | 77 | 32 | 29 | 58 | 80 | 61 | 38 |
| T2 | mid-frequency lemma | 3,787 | 54 | 57 | 58 | 73 | 62 | 45 |
| T3 | frequent lemma | 21,826 | 62 | 70 | 65 | 69 | 67 | 49 |
| T4 | ADJ | 2,630,105 | 61 | 61 | 56 | 738 | 417 | 487 |
| T5 | NOUN | 8,541,955 | 55 | 61 | 130 | 2,659 | 1,432 | 1,147 |
| S1 | rare lemma + VERB | 0 | 18 | 21 | 61 | 57 | 50 | 37 |
| S1r | VERB + rare lemma | 0 | 22 | 23 | 108 | 83 | 68 | 51 |
| S2 | VERB + mid lemma | 860 | 54 | 63 | 107 | 103 | 98 | 74 |
| S3 | ADJ NOUN | 1,296,236 | 64 | 57 | 220 | 537 | 349 | 358 |
| S4 | DET, 0–2 tokens, NOUN | 2,427,669 | 67 | 60 | 1,095 | 2,454 | 2,023 | 2,028 |
| S5 | PRON VERB DET NOUN | 32,028 | 67 | 62 | 208 | 227 | 226 | 204 |

### UD 2.18 (38.2M tokens), engines directly, Docker on SSD

Sequences counted within the sentence (`within s`), as PML-TQ does. **article:** pando-server; PML-TQ (one database for all of UD, built with teitok2pmltq.pl).

|  | query | hits | pando first | pando total | Manatee first | Manatee total | CWB first | CWB total | BlackLab first | BlackLab total | PML-TQ first | PML-TQ total | Odinson first | Odinson total | dep_search | dep_search search only |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T1 | rare lemma | 77 | 2.7 | 1.7 | 1.0 | 0.1 | 0.3 | 0.0 | 48 ⚠ | 112 ⚠ | 147 | 78 | 3.4 | 7.0 | 3,381 | 113 |
| T2 | mid-frequency lemma | 3,787 | 2.4 | 1.2 | 1.2 | 0.1 | 0.5 | 0.2 | 75 | 113 | 72 | 78 | 2.0 | 5.9 | 3,468 | 150 |
| T3 | frequent lemma | 21,826 | 2.4 | 2.0 | 1.2 | 0.3 | 1.2 | 0.8 | 23 | 115 | 77 | 132 | 1.7 | 7.1 | 4,263 | 954 |
| T4 | ADJ | 2,630,105 | 3.1 | 1.3 | 1.2 | 37 | 100 | 97 | 82 | 542 | 196 | 47,094 | 2.2 | 746 | 118,432 ⚠ | 114,361 ⚠ |
| T5 | NOUN | 8,541,955 | 2.3 | 1.6 | 1.1 | 106 | 324 | 325 | 136 | 1,576 | 136 | 50,474 | 2.0 | 1,614 | 1,128,148 ⚠ | 1,123,682 ⚠ |
| S1 | rare lemma + VERB | 0 | 1.5 | 3.3 | 16 | 15 | 0.3 | 0.1 | 12 | 114 | 307 | 193 | 3.5 | 9.2 | — | — |
| S1r | VERB + rare lemma | 0 | 1.3 | 1.5 | 39 | 40 | 802 | 782 | 10 | 112 | 312 | 190 | 2.3 | 9.4 | — | — |
| S2 | VERB + mid lemma | 860 | 3.4 | 1.5 | 39 | 39 | 827 | 821 | 25 | 111 | 245 | 300 | 3.1 | 19 | — | — |
| S3 | ADJ NOUN | 1,293,451 | 3.6 | 5.1 | 1.3 | 187 | 539 | 550 | 410 | 981 | 94,279 | 96,310 | 3.1 | 749 | — | — |
| S4 | DET, 0–2 tokens, NOUN | 2,401,964 | 3.6 | 9.7 | 97 ⚠ | 988 ⚠ | 644 ⚠ | 640 ⚠ | 268 | 3,124 | 46,455 | 97,385 | 8.8 ⚠ | 4,622 ⚠ | — | — |
| S5 | PRON VERB DET NOUN | 31,835 | 8.9 | 7.9 | 2.4 | 182 | 440 | 413 | 355 | 492 | 279,588 | 335,623 | 5.0 | 277 | — | — |
| D1 | rare lemma > amod | 11 | 3.4 | 1.9 | — | — | — | — | 28 ⚠ | 134 ⚠ | 304 | 196 | 3.3 | 6.6 | 3,536 | 19 |
| D1r | VERB > rare lemma | 55 | 4.9 | 1.9 | — | — | — | — | 1,162 ⚠ | 723 ⚠ | 134 | 76 | 6.1 | 11 | 3,764 ⚠ | 320 ⚠ |
| D2 | mid lemma > amod | 1,695 | 7.6 | 2.8 | — | — | — | — | 48 ⚠ | 117 ⚠ | 237 | 243 | 3.4 | 10 | 3,505 ⚠ | 53 ⚠ |
| D3 | NOUN > ADJ | 1,883,196 | 3.6 | 5.5 | — | — | — | — | 1,738 ⚠ | 45,320 ⚠ | 240 | 99,167 | 2.8 | 6,274 | 124,988 ⚠ | 119,402 ⚠ |
| D4 | VERB > nsubj PRON | 551,458 | 4.7 | 4.9 | — | — | — | — | 196 ⚠ | 7,927 ⚠ | 611 | 130,768 | 3.2 | 740 | 108,386 ⚠ | 103,381 ⚠ |
| D5 | VERB without nsubj | 2,384,135 | 3.4 | 7.8 | — | — | — | — | 409 | 2,554 | 48,613 | 262,410 | 2.2 | 1,101 | 148,126 ⚠ | 143,376 ⚠ |
| D6 | VERB >> mid lemma (any depth) | 2,608 | 4.5 | 2.1 | — | — | — | — | — | — | 252 | 314 | 7.4 ⚠ | 28 ⚠ | — | — |
| D7 | VERB with nsubj and obj | 727,723 | 2.9 | 11 | — | — | — | — | 158 ⚠ | 5,841 ⚠ | 80,858 | 686,859 | 2.9 | 448 | 102,468 ⚠ | 97,669 ⚠ |

### UD English-EWT (0.25M tokens), Docker on SSD

PML-TQ databases built two ways: with teitok2pmltq.pl (every token attribute indexed; **article**) and the official way (Treex + `pmltq convert`/`load`; no index on lemma, tag or deprel). Sequences within the sentence.

|  | query | hits | pando first | pando total | PML-TQ first | PML-TQ total | PML-TQ official first | PML-TQ official total | Odinson first | Odinson total |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T1 | rare lemma | 80 | 3.9 | 1.8 | 127 | 75 | 219 | 161 | 2.8 | 2.6 |
| T2 | mid-frequency lemma | 55 | 2.3 | 4.3 | 130 | 74 | 214 | 156 | 1.3 | 1.2 |
| T3 | frequent lemma | 558 | 3.0 | 1.4 | 74 | 78 | 101 | 160 | 1.5 | 2.2 |
| T4 | ADJ | 16,789 | 3.8 | 2.6 | 77 | 78 | 104 ⚠ | 156 ⚠ | 1.1 | 6.8 |
| T5 | NOUN | 43,084 | 2.1 | 1.4 | 78 | 79 | 108 | 152 | 1.3 | 14 |
| S1 | rare lemma + VERB | 15 | 1.8 | 1.8 | 137 | 86 | 218 | 165 | 2.8 | 2.0 |
| S1r | VERB + rare lemma | 1 | 1.7 | 2.7 | 132 | 79 | 217 | 165 | 1.7 | 1.6 |
| S2 | VERB + mid lemma | 2 | 1.8 | 1.4 | 131 | 79 | 230 | 161 | 1.4 | 1.6 |
| S3 | ADJ NOUN | 8,649 | 7.6 | 1.8 | 81 | 82 | 117 | 218 | 1.8 | 18 |
| S4 | DET, 0–2 tokens, NOUN | 20,949 | 5.3 | 1.9 | 83 | 82 | 125 | 215 | 3.5 ⚠ | 67 ⚠ |
| S5 | PRON VERB DET NOUN | 833 | 6.2 | 2.9 | 140 | 144 | 122 | 281 | 2.5 | 4.6 |
| D1 | rare lemma > amod | 31 | 3.5 | 2.6 | 126 | 79 | 214 | 166 | 3.3 | 2.5 |
| D1r | VERB > rare lemma | 45 | 3.1 | 2.0 | 137 | 78 | 211 | 184 | 2.9 | 1.8 |
| D2 | mid lemma > amod | 15 | 2.0 | 1.8 | 133 | 74 | 240 | 172 | 1.7 | 1.4 |
| D3 | NOUN > ADJ | 10,569 | 2.8 | 2.1 | 79 | 75 | 116 | 225 | 1.8 | 20 |
| D4 | VERB > nsubj PRON | 9,960 | 2.7 | 1.8 | 77 | 95 | 109 | 160 | 1.6 | 10 |
| D5 | VERB without nsubj | 13,334 | 6.5 | 3.3 | 84 | 79 | 111 ⚠ | 222 ⚠ | 1.5 | 8.4 |
| D6 | VERB >> mid lemma (any depth) | 88 | 3.4 | 2.2 | 143 | 81 | 214 | 161 | 6.9 ⚠ | 4.6 ⚠ |
| D7 | VERB with nsubj and obj | 6,651 | 2.5 | 10 | 81 | 136 | 122 | 215 | 1.2 | 5.6 |

### UD German-HDT (3.5M tokens), Docker on SSD

PML-TQ with teitok2pmltq.pl (**article**). Sequences within the sentence.

|  | query | hits | pando first | pando total | Manatee first | Manatee total | CWB first | CWB total | BlackLab first | BlackLab total | PML-TQ first | PML-TQ total | Odinson first | Odinson total | dep_search | dep_search search only |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| T1 | rare lemma | 7 | 2.3 | 1.8 | 0.1 | 0.0 | 0.0 | 0.0 | 12 | 110 | 127 | 73 | 2.0 | 1.9 | 4,542 | 336 |
| T2 | mid-frequency lemma | 346 | 3.4 | 1.3 | 1.1 | 0.0 | 0.3 | 0.0 | 21 | 110 | 71 | 73 | 3.0 | 1.8 | 4,070 | -707.0 |
| T3 | frequent lemma | 8,408 | 3.5 | 1.7 | 1.2 | 0.1 | 0.6 | 0.3 | 21 ⚠ | 110 ⚠ | 71 | 74 | 1.7 | 9.0 | 5,537 | 809 |
| T4 | ADJ | 262,611 | 3.4 | 2.3 | 1.1 | 3.7 | 9.4 | 9.2 | 21 | 143 | 85 | 404 | 1.4 | 64 | 8,640 ⚠ | 4,635 ⚠ |
| T5 | NOUN | 729,100 | 4.3 | 2.0 | 0.9 | 8.7 | 26 | 26 | 36 | 204 | 83 | 865 | 2.0 | 131 | 9,902 ⚠ | 6,448 ⚠ |
| S1 | rare lemma + VERB | 1 | 1.7 | 1.9 | 2.1 | 2.0 | 0.0 | 0.0 | 16 | 114 | 134 | 77 | 2.6 | 2.4 | — | — |
| S1r | VERB + rare lemma | 0 | 1.7 | 2.1 | 2.4 | 2.4 | 46 | 46 | 6.5 | 109 | 132 | 81 | 2.7 | 2.5 | — | — |
| S2 | VERB + mid lemma | 1 | 1.9 | 1.6 | 2.4 | 2.4 | 46 | 46 | 12 | 112 | 132 | 98 | 3.3 | 3.5 | — | — |
| S3 | ADJ NOUN | 167,806 | 3.9 | 2.0 | 1.1 | 19 | 52 | 50 | 98 | 258 | 461 | 2,032 | 3.1 | 96 | — | — |
| S4 | DET, 0–2 tokens, NOUN | 523,792 | 3.2 | 4.5 | 9.6 | 179 | 111 ⚠ | 112 ⚠ | 116 | 936 | 305 | 2,426 | 6.2 ⚠ | 754 ⚠ | — | — |
| S5 | PRON VERB DET NOUN | 913 | 4.6 | 2.5 | 3.3 | 15 | 21 | 20 | 62 | 134 | 620 | 1,096 | 9.3 | 22 | — | — |
| D1 | rare lemma > amod | 4 | 2.8 | 1.8 | — | — | — | — | 22 | 110 | 134 | 74 | 1.8 | 2.6 | 3,469 ⚠ | 54 ⚠ |
| D1r | VERB > rare lemma | 5 | 1.6 | 1.5 | — | — | — | — | 68 | 173 | 126 | 79 | 2.4 | 2.7 | 3,742 | 101 |
| D2 | mid lemma > amod | 65 | 4.1 | 2.0 | — | — | — | — | 35 | 131 | 134 | 88 | 3.5 | 2.8 | 3,509 ⚠ | 31 ⚠ |
| D3 | NOUN > ADJ | 180,471 | 6.5 | 1.5 | — | — | — | — | 182 ⚠ | 2,926 ⚠ | 360 | 803 | 3.3 | 398 | 7,788 ⚠ | 4,203 ⚠ |
| D4 | VERB > nsubj PRON | 42,984 | 3.8 | 2.0 | — | — | — | — | 62 ⚠ | 520 ⚠ | 79 | 386 | 2.7 | 53 | 5,413 ⚠ | 1,911 ⚠ |
| D5 | VERB without nsubj | 73,572 | 3.1 | 2.2 | — | — | — | — | 113 | 502 | 240 | 1,029 | 2.8 | 49 | 6,417 ⚠ | 2,869 ⚠ |
| D6 | VERB >> mid lemma (any depth) | 426 | 5.3 | 1.7 | — | — | — | — | — | — | 74 | 81 | 8.6 ⚠ | 12 ⚠ | — | — |
| D7 | VERB with nsubj and obj | 105,706 | 2.3 | 3.1 | — | — | — | — | 155 ⚠ | 784 ⚠ | 383 | 1,240 | 1.5 | 30 | 7,084 ⚠ | 3,447 ⚠ |

## Aggregation: time to the full frequency table (ms)

The two metadata attributes grouped by are labelled with their first letter (G1 = speaker gender, L1 = language…): the first with few values, the second with many. The servers compute every group and return the first 100.

### ParlaMint LV (11.5M tokens), Docker on SSD

G1 = speaker gender, M2 = sitting month. **article:** both.

| | aggregation | groups | pando | Manatee | CWB | BlackLab | BlackLab own | PML-TQ |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| G1 | rare lemma by G1 | 2 | 8.7 | 0.0 | 0.6 | 28 | 14 | 137 |
| G2 | mid lemma by G1 | 2 | 9.9 | 0.2 | 0.9 | 236 | 218 | 135 |
| G3 | frequent lemma by G1 | 2 | 3.3 | 4.2 | 6.6 | 127 | 118 | 426 |
| G4 | ADJ by G1 | 2 | 3.5 | 47 | 88 | 1,228 | 1,204 | 30,131 |
| G5 | NOUN by G1 | 2 | 22 | 234 | 468 | 4,384 | 4,354 | 48,522 |
| G5b | NOUN by M2 | 89 | 9.3 | 252 | 286 | 2,095 | 1,953 | 27,821 |
| G5c | NOUN by lemma | 23,344 | 21 | 2,050 | 4,144 | 2,239 | 2,222 | 27,073 |
| G5d | NOUN by G1 × M2 | 178 | 20 | 645 | 595 | 7,657 | 7,466 | 27,335 |
| S3g | ADJ NOUN by both lemmas | 59,376 | 44 | 750 | 534 | 804 | 773 | 14,082 ⚠ |
| D1g | rare lemma > amod, by dependent's lemma | 2 | 5.5 | — | — | 38 | 28 | 149 |
| D3g | NOUN > ADJ, by dependent's lemma | 3,938 | 12 | — | — | 15,682 ⚠ | 15,528 ⚠ | 11,715 |
| D4g | VERB > nsubj PRON, by M2 | 89 | 10 | — | — | 3,248 ⚠ | 2,957 ⚠ | 9,634 |

### ParlaMint 5.0, all parliaments (1.55B tokens)

G1 = speaker gender, C2 = country. Set-up as for retrieval. **article:** all three.

| | aggregation | groups | pando | Manatee | BlackLab |
|---|---|---:|---:|---:|---:|
| G1 | rare lemma by G1 | 3 | 6.0 | 0.8 | n/m |
| G2 | mid lemma by G1 | 3 | 6.5 | 26 | n/m |
| G3 | frequent lemma by G1 | 3 | 12 | 189 | n/m |
| G4 | ADJ by G1 | 5 | 101 | 9,095 | n/m |
| G5 | NOUN by G1 | 5 | 230 | 22,174 | n/m |
| G5b | NOUN by C2 | 29 | 168 | 20,813 | n/m |
| G5c | NOUN by lemma | 1,887,089 | 4,223 | 173,616 | n/m |
| G5d | NOUN by G1 × C2 | 79 | 646 | 47,953 | n/m |
| S3g | ADJ NOUN by both lemmas | 6,902,715 | 10,838 | 126,914 | n/m |
| D1g | rare lemma > amod, by dependent's lemma | 213 | 195 | — | — |
| D3g | NOUN > ADJ, by dependent's lemma | 517,964 | 7,491 | — | — |
| D4g | VERB > nsubj PRON, by C2 | 29 | 1,055 | — | — |

### UD 2.18 (38.2M tokens)

L1 = language, T2 = treebank. pando-server in Docker on SSD; Manatee through FQS's native library (`fqs manatee-worker`) with its index on SSD and no HTTP (which favours it by ~1–2 ms). **article:** both. — = not expressible.

| | aggregation | groups | pando | Manatee (FQS library) | CWB | BlackLab | PML-TQ |
|---|---|---:|---:|---:|---:|---:|---:|
| G1 | rare lemma by L1 | 3 | 3.1 | 0.1 | 0.8 | 25 ⚠ | 137 |
| G2 | mid lemma by L1 | 2 | 2.5 | 0.4 | 1.2 | 22 | 135 |
| G3 | frequent lemma by L1 | 1 | 3.2 | 1.7 | 2.8 | 22 | 244 |
| G4 | ADJ by L1 | 185 | 4.6 | 196 | 251 | 1,071 | 105,143 |
| G5 | NOUN by L1 | 193 | 9.8 | 608 | 846 | 3,799 | 118,810 |
| G5b | NOUN by T2 | 261 | 9.7 | 658 | 864 | 4,718 | 150,322 |
| G5c | NOUN by lemma | 712,305 | 120 | 7,590 | 13,389 | 7,401 | 144,870 |
| G5d | NOUN by L1 × T2 | 353 | 28 | 1,394 | 1,230 | 8,226 | 241,987 |
| S3g | ADJ NOUN by both lemmas | 751,281 | 329 | 4,306 | 5,578 | 2,690 ⚠ | 225,805 ⚠ |
| D1g | rare lemma > amod, by dependent's lemma | 9 | 19 | — | — | 27 ⚠ | 307 |
| D3g | NOUN > ADJ, by dependent's lemma | 189,374 | 75 | — | — | 27,357 ⚠ | 602,966 |
| D4g | VERB > nsubj PRON, by T2 | 256 | 35 | — | — | 2,668 ⚠ | 261,485 |

### UD 2.18 through KonText (query, then frequency table)

What a KonText user waits for: the concordance, then `/freqs`. KonText with pando cannot group by text attributes yet, and groups two-token hits by the first token only.

| | query | grouping | groups | KonText + pando | KonText + Manatee (volumes) |
|---|---|---|---:|---:|---:|
| KG2 | `[lemma="uçuş"]` | `text.langcode 0` | 193 | — | 623 |
| KG3 | `[lemma="společnost"]` | `lemma/e 0` | 1 | 122 | 603 |
| KG4 | `[upos="ADJ"]` | `lemma/e 0` | 241,655 | 943 | 21,306 |
| KG5c | `[upos="NOUN"]` | `lemma/e 0` | 712,305 | 2,739 | 52,102 |
| KG5l | `[upos="NOUN"]` | `text.langcode 0` | 193 | — | 4,867 |
| KG5t | `[upos="NOUN"]` | `text.treebank 0` | 261 | — | 4,066 |
| KS3 | `[upos="ADJ"] [upos="NOUN"]` | `lemma/e 0` | 141,689 | 866 | 15,281 |

### UD German-HDT (3.5M tokens)

No metadata: only aggregations by lemma and by dependent.

| | aggregation | groups | pando | Manatee | CWB | BlackLab | PML-TQ |
|---|---|---:|---:|---:|---:|---:|---:|
| G5c | NOUN by lemma | 91,639 | 16 | 669 | 1,120 | 601 | 5,901 |
| S3g | ADJ NOUN by both lemmas | 94,596 | 30 | 440 | 694 | 328 | 1,887 ⚠ |
| D1g | rare lemma > amod, by dependent's lemma | 3 | 3.9 | — | — | 15 | 139 |
| D3g | NOUN > ADJ, by dependent's lemma | 11,378 | 8.1 | — | — | 4,400 ⚠ | 1,308 |

## Parallel corpora: aligned queries (Europarl, 6 languages, 324M tokens)

The question a parallel corpus answers: *find X in one language where the aligned sentence in another language contains Y* (or does not contain it, or is shown next to the hit). Europarl v8 from OPUS in English, Czech, German, Spanish, French and Dutch, with OPUS's own tags (TreeTagger, hunpos, MElt), 323.9M tokens. The two set-ups:

- **Manatee** as InterCorp and KonText use it: one corpus per language, and alignment files per language pair (de:en, fr:en, cs:en, nl:fr, es:nl, both directions), the query written `[…] within de: […]`.
- **pando**: all six languages in one index, the language a text attribute, aligned sentences sharing a translation-unit id (`s_tuid`). The same question can be asked in four forms (Manatee's own syntax, `within s_tuid having`, a class join `=:s_tuid`, `with … :: a.s_tuid = b.s_tuid`); all give the same counts. **pando-server** is one running process, as KonText uses it; the **CLI** starts a new process per query.

In short:

- **Same answers.** All pando forms agree with each other everywhere. Manatee finds up to 0.01 % more, always for the same reason: its alignment files link groups of sentences (beads), so where one sentence was split or merged in translation, a hit is aligned with a neighbour of its translation too (see the counts below).
- **Full counts: pando-server is faster on every one**, from 2 times (nouns *not* aligned with nouns: 9.7 s → 4.5 s) to 50 times (nouns aligned with a rare word: 266 ms → 5 ms); all nouns aligned with nouns: 16.3 s → 4.5 s, in two languages at once: 27.3 s → 8.2 s, their frequency list: 38.3 s → 7.3 s.
- **First page: Manatee is instant everywhere** (about 1 ms), as it shows hits while it is still searching. pando-server is as fast for rare words, but for frequent words with *not aligned with* or alignment in two languages it finishes the search first (4.5–8 s).
- **A frequency list of frequent words limited by a rare word** (F1) is the one count where Manatee is faster (0.3 s against 2.8 s).
- **Showing the aligned sentence**: pando 1 ms (it reads the aligned sentence for the hits on the page), Manatee 0.01–41 s (`add_aligned` processes every hit of the concordance).
- **The CLI** needs about 1.7 s more on every aligned query, to build the table of sentences whose `s_tuid` holds several ids (Europarl merges some sentences as `a|b`); a server builds it once. Storing it in the index would remove this.

Warm times: median of 3 runs after the first, without process start and corpus open; pando-server times are the client's wall clock (HTTP and JSON included), with no result cache. pando-server column: the fastest of the four forms.

| | question | Manatee query | hits Manatee | hits pando | first page: Manatee | pando-server | all hits: Manatee | pando-server | pando CLI |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| W1 | rare word, rare word aligned | `[lemma="fisherman"] within de: [lemma="Fischer"]` | 2,507 | 2,507 | 0.9 ms | 0.5 ms | 18 ms | 3.1 ms | 1.7 s |
| W2 | all nouns, rare word aligned | `[tree="NN"] within de: [lemma="Fischer"]` | 12,499 | 12,499 | 3.1 ms | 3.6 ms | 266 ms | 4.9 ms | 1.7 s |
| W3 | rare word, nouns aligned | `[lemma="fisherman"] within de: [tree="NN"]` | 2,720 | 2,720 | 5.7 ms | 0.6 ms | 319 ms | 13 ms | 1.7 s |
| W4 | nouns, nouns aligned | `[tree="NN"] within de: [tree="NN"]` | 6,921,775 | 6,921,443 | 0.8 ms | 44 ms | 16.3 s | 4.5 s | 6.2 s |
| W5 | nouns, fr→nl | `[pos="NOM"] within nl: [tree="nounsg"]` | 12,104,352 | 12,104,139 | 0.7 ms | 108 ms | 23.8 s | 2.9 s | 4.5 s |
| W6 | rare word, en→cs | `[lemma="fisherman"] within cs: [word="rybář\|rybáře\|rybáři\|rybářů\|rybářům\|rybářích\|rybářem\|rybářovi"]` | 692 | 692 | 0.9 ms | 0.8 ms | 4.2 ms | 1.4 ms | 1.7 s |
| W7 | nouns, en→cs | `[tree="NN"] within cs: [hun="N_N"]` | 2,254,459 | 2,254,459 | 0.7 ms | 63 ms | 3.7 s | 1.2 s | 2.9 s |
| W8 | rare word, nl→es | `[lemma="visser"] within es: [lemma="pescador"]` | 2,578 | 2,577 | 0.9 ms | 0.7 ms | 23 ms | 3.0 ms | 1.6 s |
| N1 | not aligned with (rare) | `[lemma="fisherman"] !within de: [lemma="Fischer"]` | 638 | 638 | 1.5 ms | 3.3 ms | 14 ms | 3.7 ms | 1.7 s |
| N2 | not aligned with (frequent) | `[tree="NN"] !within de: [tree="NN"]` | 1,595,941 | 1,596,273 | 0.9 ms | 4.5 s | 9.7 s | 4.5 s | 6.2 s |
| C1 | aligned in two languages (rare) | `[lemma="fisherman"] within de: [lemma="Fischer"] within fr: [lemma="pêcheur"]` | 2,387 | 2,387 | 0.9 ms | 5.9 ms | 31 ms | 5.8 ms | 1.7 s |
| C2 | aligned in two languages (frequent) | `[tree="NN"] within de: [tree="NN"] within fr: [pos="NOM"]` | 6,714,731 | 6,714,188 | 0.8 ms | 8.2 s | 27.3 s | 8.2 s | 10.0 s |
| S1 | with aligned sentence (rare) | `[lemma="fisherman"]` | — | — | 10 ms | 1.1 ms | — | — | 1.7 s |
| S2 | with aligned sentence (frequent) | `[tree="NN"]` | — | — | 40.8 s | 1.1 ms | — | — | 1.7 s |
| F1 | frequency list under alignment (rare B) | `[tree="NN"] within de: [lemma="Fischer"]` | 12,499 | 12,499 | — | — | 310 ms | 2.8 s | 4.4 s |
| F2 | frequency list under alignment (frequent) | `[tree="NN"] within de: [tree="NN"]` | 6,921,775 | 6,921,443 | — | — | 38.3 s | 7.3 s | 9.0 s |

Rare = *fisherman*, *Fischer*, *pêcheur*, *visser*, *pescador* (a few thousand hits), frequent = all nouns of the tagger (English `tree="NN"`, German `NN`, French `pos="NOM"`, Dutch `nounsg`, Czech hunpos `N_N`). In pando each query also names the language (`& text_lang="en"`). W6 lists the Czech forms of *rybář*: this manatee-open build (2.225.8, built without PCRE) matches no wildcard pattern (`.*` gives 0 hits); alternatives work. The CLI column is one thread; 8 threads give the same times, since the work is a single pass. Full report with every form and thread count: `dev/bench/parallel/results/europarl/report.md` (local).

**The counts that differ** are all Manatee's beads. The 332 nouns Manatee has more in W4 (aligned) are exactly the 332 pando has more in N2 (not aligned): both sum to the same 8,517,716 English nouns. The alignment files keep 99.999 % of the sentence links (de:en: 19 lost of 2.16M) and add 326–1,558 sentence pairs per language pair through beads.

### Building the indexes

| | input | time | size on disk |
|---|---|---:|---:|
| pando | TEITOK XML, streamed by flexencoder into one index | 1 h 51 min (peak memory 7 GB) | 22 GB |
| Manatee | TEITOK XML → 6 VRT files (flexencoder, 1 h 8 min) → encodevert (51 min) → mkalign (10 × 1.5 s) | 2 h 0 min | 6.6 GB (+ 28 GB VRT) |

pando's index is larger mainly because it keeps every token attribute of all six languages in full-length columns (nine attributes, most of them used by one language only) plus the `s_tuid` equivalence table (2.67M translation units, 348M memberships).


## Indexing

| corpus | engine | input | time | index size |
|---|---|---|---:|---:|
| ParlaMint LV | pando | CoNLL-U | 29 s | 980 MB |
| ParlaMint LV | BlackLab | XML (conversion 1 min 42 s not included) | 5 min 7 s | 1.2 GB |
| ParlaMint LV | Manatee (native encodevert) | VRT | 52 s | 206 MB |
| ParlaMint LV | CWB 3.5 (cwb-encode + cwb-makeall) | VRT | 31 s | 641 MB |
| UD 2.18, all | CWB 3.5 | VRT | 91 s | 2.0 GB |
| UD German-HDT | Manatee (native encodevert) | VRT | 11 s | 66 MB |
| UD German-HDT | CWB 3.5 | VRT | 8 s | 176 MB |
| UD German-HDT | BlackLab | XML, 192 documents of ≤ 1,000 sentences | 1 min 39 s | 360 MB |
| UD English-EWT | PML-TQ (teitok2pmltq.pl) | CoNLL-U → TEITOK VRT | 13 s | 208 MB |
| UD German-HDT | Odinson | CoNLL-U → Odinson JSON docs | 52 s | 170 MB |
| ParlaMint LV | Odinson | CoNLL-U → Odinson JSON docs | 1 min 32 s | 834 MB |
| UD 2.18, all | Odinson | CoNLL-U → Odinson JSON docs | 4 min 53 s | 2.8 GB |
| UD German-HDT | dep_search | CoNLL-U | 7 min 38 s | 1.1 GB |
| ParlaMint LV | dep_search | CoNLL-U | 24 min 6 s | 3.3 GB |
| UD 2.18, all | dep_search | CoNLL-U | 1 h 47 min | 12 GB |
| UD English-EWT | PML-TQ (official) | CoNLL-U → Treex → pmltq convert/load | 17 min 42 s + 10 min 32 s + 36 s | 295 MB |
| UD German-HDT | pando (`--misc-keys none`) | CoNLL-U | 7.5 s | 252 MB |
| UD 2.18, all | pando (`--misc-keys none`) | CoNLL-U | 1 min 55 s | 3.9 GB |
| UD 2.18, all | Manatee (native encodevert) | VRT (conllu2vrt.py) | 2 min 5 s | 966 MB (the index queried: 1.1 GB, built earlier from a VRT with more structure attributes) |
| ParlaMint 5.0, all | pando (native) | CoNLL-U | 1 h 58 min (peak memory 28.7 GB) | 117 GB |
| ParlaMint 5.0, all | Manatee (native encodevert, FD_FGD attributes) | VRT | 1 h 38 min | 29 GB |
| UD German-HDT | PML-TQ (teitok2pmltq.pl) | CoNLL-U → TEITOK VRT | 211 s | 3.0 GB |
| ParlaMint LV | PML-TQ (teitok2pmltq.pl) | CoNLL-U → TEITOK VRT | 1533 s | 10.3 GB |
| UD 2.18, all | PML-TQ (teitok2pmltq.pl) | CoNLL-U → TEITOK VRT | 9028 s | 30.8 GB |

Odinson and dep_search were indexed while other engines were running, so their times are upper bounds. dep_search skips sentences over 256 tokens (`sent_limit`).

pando's German-HDT and UD indexes and Manatee's UD index were timed by building them again (2026-10-26); pando's copies came out the same size as the indexes queried. PML-TQ sizes are the PostgreSQL database as it is now (`pg_database_size`, after loading and analysing).

## Index size and indexing time

| corpus | tokens | pando | Manatee | CWB | BlackLab | PML-TQ | Odinson | dep_search |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| UD German-HDT | 3.5M | 252 MB · 8 s | 66 MB · 11 s | 176 MB · 8 s | 360 MB · 1 min 39 s | 3.0 GB · 3 min 31 s | 170 MB · 18 s | 1.1 GB · 2 min 18 s |
| ParlaMint LV | 11.5M | 980 MB · 29 s | 207 MB · 52 s | 641 MB · 31 s | 1.2 GB · 5 min 7 s | 10.3 GB · 25 min 33 s | 834 MB · 1 min 6 s | 3.3 GB · 9 min 18 s |
| UD 2.18, all | 38.2M | 3.9 GB · 1 min 55 s | 1.1 GB · 2 min 5 s | 2.0 GB · 1 min 31 s | 4.1 GB · 16 min 48 s | 30.8 GB · 2 h 30 min‡ | 2.8 GB · 3 min 5 s | 12.0 GB · 1 h 47 min* |
| ParlaMint 5.0, all | 1.55B | 117.0 GB · 1 h 58 min | 29.0 GB · 1 h 38 min | — | — | — | — | — |

Each cell: index size on disk (decimal MB/GB, directories measured with `du`; PML-TQ: `pg_database_size`) · indexing time from the engine's input format (conversions to that format not included, see the table above; PML-TQ: conversion and loading). \* indexed while other engines were running: an upper bound. † native build on a hard disk (the other engines: SSD), not yet re-timed in the benchmark set-up. ‡ PML-TQ's loader ended with exit code 137 after loading; the database is complete. — not measured.

What the size differences mostly reflect: Manatee and CWB store compressed attribute columns and their reverse indexes, and treat the dependency relation as one more attribute; pando adds the dependency structure itself and precomputed indexes (bitmaps, packed head postings) for dependency and grouping queries; BlackLab (Lucene) indexes every relation as a span and stores the documents for context; PML-TQ is a PostgreSQL database with an index on every token attribute; dep_search keeps a per-sentence database of the trees. pando stores a full-length column (about 4 bytes per token, plus its reverse index) for every token attribute, so its size follows the number of token attributes: 6 in ParlaMint, 7 in German-HDT and 12 in all of UD, where pando-index's default also indexes six MISC keys (tuid, Translit, Vform, Root, CorrectForm, Gloss). The sizes here are without them (`--misc-keys none`): with them UD's index is 5.1 GB instead of 3.9 GB, German-HDT's 269 MB instead of 252 MB. The queries were timed on the indexes with them; no query reads a MISC column.

## Count discrepancies

- **BlackLab 4.1.1, dependency queries:** counts differ from pando's (and from a direct count in the CoNLL-U), for two reasons. (1) Relations are lost when a query runs over several documents unfiltered (the same document gives more hits when the query is limited to it). Reported to the BlackLab developers with a small example (2026-10-28); they reproduced it, found it already fixed in their development branch and backported the fix to the 4.x maintenance branch. The numbers here are from the 4.1.1 release, without the fix; BlackLab is to be measured again with it. (2) A relation query returns the head as the hit, and where a head has two matching dependents it gives sometimes one hit and sometimes two (in the two-document example: 333 hits for 336 head–dependent pairs of 319 heads). Raised with the developers, still open. Times are shown anyway: the work done is comparable.

| | query | pando | BlackLab |
|---|---|---:|---:|
| D3 | NOUN > ADJ | 475,057 | 300,661 |
| D4 | VERB > nsubj PRON | 275,983 | 202,891 |
| D7 | VERB with nsubj and obj | 156,998 | 160,477 |

- **CWB:** CQP reports at most one match per start position (MatchingStrategy `standard`, CWB's default; no strategy reports every variant), so a query with a variable gap (S4) has fewer hits than in pando and Manatee: a difference in counting rule, not an error.

- **Odinson:** keeps one match per start for a variable gap (S4, as CWB), and merges matches that end on the same token, so an any-depth query (D6) counts the dependents reached, not the (verb, dependent) pairs: EWT D6 49 against 88, checked in the CoNLL-U. Every other count equals pando's.

- **dep_search:** a hit is the matching head token (as BlackLab's), and a relation name also matches its subtypes (`nsubj` matches `nsubj:pass`) and, where a treebank has them, enhanced dependencies; no sequences and no any-depth operator.

- **Manatee, `within <s/>`:** drops a valid hit of a variable-gap query in rare cases (UD S4: 17 of 2.4M); reproduced in a 7-token example, report drafted.

- **PML-TQ, teitok2pmltq.pl route:** double quotes in attribute values are lost on loading. The converter doubles them for PostgreSQL's CSV `COPY` but does not enclose the field in quotes, and an unenclosed `""` reads as nothing (`"Firma` is stored as `Firma`). Hit counts of the benchmark queries are not affected; groupings by lemma merge such values (German-HDT G5c: 91,574 groups against 91,639, all 65 explained by quote removal).

- **Aggregations and the sentence:** no aggregation query adds `within s`, so in pando, Manatee, CWB and BlackLab a two-token grouping (S3g) can span a sentence boundary; PML-TQ's cannot (German-HDT S3g: 63 hits fewer).

- **PML-TQ, official route:** Treex keeps UD's empty nodes, pando and teitok2pmltq.pl skip them; a few counts differ by those nodes (e.g. EWT T4: 16,790 against 16,789).

## Not measured yet

- Grew (optional); aggregation in Odinson and dep_search (neither groups results).
- Restriction (R) and regular-expression (X) queries; collocations.
- Larger corpora: ParlaMint NL (74M), UD ×16 (611M); BlackLab on all of ParlaMint (1.55B).
- Cold-cache runs; memory use; FQS against pando directly.
- ParlaMint 5.0 (1.55B) through KonText, pando and Manatee via FQS against KonText's own Manatee: left to the KonText team (setup in kontext-pando).

## Queries used

The forms sent to each engine, with the slots as written in the harness (`pando/dev/bench/harness`); the words and metadata each corpus fills in follow below. Every result file also keeps the exact query of each run. In corpora counted within the sentence (UD), sequence queries get `within s` (pando) or `within <s/>` (Manatee, CWB, BlackLab). Manatee and CWB have no dependency relations.

### Retrieval

- **T1**
  - pando, Manatee, CWB, BlackLab, Odinson: `[lemma="$R"]`
  - PML-TQ: `tok [ lemma = "$R" ]`
  - dep_search: `L=$R`
- **T2**
  - pando, Manatee, CWB, BlackLab, Odinson: `[lemma="$M"]`
  - PML-TQ: `tok [ lemma = "$M" ]`
  - dep_search: `L=$M`
- **T3**
  - pando, Manatee, CWB, BlackLab, Odinson: `[lemma="$H"]`
  - PML-TQ: `tok [ lemma = "$H" ]`
  - dep_search: `L=$H`
- **T4**
  - pando, Manatee, CWB, BlackLab: `[upos="ADJ"]`
  - PML-TQ: `tok [ upos = "ADJ" ]`
  - Odinson: `[tag=ADJ]`
  - dep_search: `ADJ`
- **T5**
  - pando, Manatee, CWB, BlackLab: `[upos="NOUN"]`
  - PML-TQ: `tok [ upos = "NOUN" ]`
  - Odinson: `[tag=NOUN]`
  - dep_search: `NOUN`
- **S1**
  - pando, Manatee, CWB, BlackLab: `[lemma="$R"] [upos="VERB"]`
  - PML-TQ: `tok $a := [ lemma = "$R" ]; tok [ upos = "VERB", same-tree-as $a, ord = $a.ord + 1 ]`
  - Odinson: `[lemma="$R"] [tag=VERB]`
  - dep_search: not expressible (dep_search has no linear sequences)
- **S1r**
  - pando, Manatee, CWB, BlackLab: `[upos="VERB"] [lemma="$R"]`
  - PML-TQ: `tok $a := [ upos = "VERB" ]; tok [ lemma = "$R", same-tree-as $a, ord = $a.ord + 1 ]`
  - Odinson: `[tag=VERB] [lemma="$R"]`
  - dep_search: not expressible (dep_search has no linear sequences)
- **S2**
  - pando, Manatee, CWB, BlackLab: `[upos="VERB"] [lemma="$M"]`
  - PML-TQ: `tok $a := [ upos = "VERB" ]; tok [ lemma = "$M", same-tree-as $a, ord = $a.ord + 1 ]`
  - Odinson: `[tag=VERB] [lemma="$M"]`
  - dep_search: not expressible (dep_search has no linear sequences)
- **S3**
  - pando, Manatee, CWB, BlackLab: `[upos="ADJ"] [upos="NOUN"]`
  - PML-TQ: `tok $a := [ upos = "ADJ" ]; tok [ upos = "NOUN", same-tree-as $a, ord = $a.ord + 1 ]`
  - Odinson: `[tag=ADJ] [tag=NOUN]`
  - dep_search: not expressible (dep_search has no linear sequences)
- **S4**
  - pando, Manatee, CWB, BlackLab: `[upos="DET"] []{0,2} [upos="NOUN"]`
  - PML-TQ: `tok $a := [ upos = "DET" ]; tok [ upos = "NOUN", same-tree-as $a, ord >= $a.ord + 1, ord <= $a.ord + 3 ]`
  - Odinson: `[tag=DET] []{0,2} [tag=NOUN]`
  - dep_search: not expressible (dep_search has no linear sequences)
- **S5**
  - pando, Manatee, CWB, BlackLab: `[upos="PRON"] [upos="VERB"] [upos="DET"] [upos="NOUN"]`
  - PML-TQ: `tok $a := [ upos = "PRON" ]; tok $b := [ upos = "VERB", same-tree-as $a, ord = $a.ord + 1 ]; tok $c := [ upos = "DET", same-tree-as $a, ord = $b.ord + 1 ]; tok [ upos = "NOUN", same-tree-as $a, ord = $c.ord + 1 ]`
  - Odinson: `[tag=PRON] [tag=VERB] [tag=DET] [tag=NOUN]`
  - dep_search: not expressible (dep_search has no linear sequences)
- **D1**
  - pando: `[lemma="$R"] > [deprel="amod"]`
  - BlackLab: `[lemma="$R"] -amod-> _`
  - PML-TQ: `tok [ lemma = "$R", tok [ deprel = "amod" ] ]`
  - Odinson: `[lemma="$R"] >amod []`
  - dep_search: `L=$R >amod _`
- **D1r**
  - pando: `[upos="VERB"] > [lemma="$R"]`
  - BlackLab: `[upos="VERB"] --> [lemma="$R"]`
  - PML-TQ: `tok [ upos = "VERB", tok [ lemma = "$R" ] ]`
  - Odinson: `[tag=VERB] >> [lemma="$R"]`
  - dep_search: `VERB > L=$R`
- **D2**
  - pando: `[lemma="$M"] > [deprel="amod"]`
  - BlackLab: `[lemma="$M"] -amod-> _`
  - PML-TQ: `tok [ lemma = "$M", tok [ deprel = "amod" ] ]`
  - Odinson: `[lemma="$M"] >amod []`
  - dep_search: `L=$M >amod _`
- **D3**
  - pando: `[upos="NOUN"] > [upos="ADJ"]`
  - BlackLab: `[upos="NOUN"] --> [upos="ADJ"]`
  - PML-TQ: `tok [ upos = "NOUN", tok [ upos = "ADJ" ] ]`
  - Odinson: `[tag=NOUN] >> [tag=ADJ]`
  - dep_search: `NOUN > ADJ`
- **D4**
  - pando: `[upos="VERB"] > [deprel="nsubj" & upos="PRON"]`
  - BlackLab: `[upos="VERB"] -nsubj-> [upos="PRON"]`
  - PML-TQ: `tok [ upos = "VERB", tok [ deprel = "nsubj", upos = "PRON" ] ]`
  - Odinson: `[tag=VERB] >nsubj [tag=PRON]`
  - dep_search: `VERB >nsubj PRON`
- **D5**
  - pando: `[upos="VERB"] !> [deprel="nsubj"]`
  - BlackLab: `[upos="VERB"] !-nsubj-> _`
  - PML-TQ: `tok [ upos = "VERB", 0x tok [ deprel = "nsubj" ] ]`
  - Odinson: `[tag=VERB & !outgoing=nsubj]`
  - dep_search: `VERB !>nsubj _`
- **D6**
  - pando: `[upos="VERB"] >> [lemma="$M"]`
  - BlackLab: not expressible (BlackLab 4 has no any-depth (descendant) relation search)
  - PML-TQ: `tok [ upos = "VERB", descendant tok [ lemma = "$M" ] ]`
  - Odinson: `[tag=VERB] >>+ [lemma="$M"]`
  - dep_search: not expressible (no any-depth (descendant) operator found)
- **D7**
  - pando: `[upos="VERB" & child [deprel="nsubj"] & child [deprel="obj"]]`
  - BlackLab: `[upos="VERB"] -nsubj-> _ ; -obj-> _`
  - PML-TQ: `tok [ upos = "VERB", 1+x tok [ deprel = "nsubj" ], 1+x tok [ deprel = "obj" ] ]`
  - Odinson: `[tag=VERB & outgoing=nsubj & outgoing=obj]`
  - dep_search: `VERB >nsubj _ >obj _`

### Aggregation

- **G1**
  - pando: `[lemma="$R"]; count by match.$G1`
  - Manatee (query; criterion): `[lemma="$R"]; $MG1`
  - CWB (query; command): `[lemma="$R"]; group A match $CG1`
  - BlackLab (pattern; group): `[lemma="$R"] $BLW1; $BLG1`
  - PML-TQ (query; grouped by): `tok $n := [ lemma = "$R" ]; $n.$PG1`
- **G2**
  - pando: `[lemma="$M"]; count by match.$G1`
  - Manatee (query; criterion): `[lemma="$M"]; $MG1`
  - CWB (query; command): `[lemma="$M"]; group A match $CG1`
  - BlackLab (pattern; group): `[lemma="$M"] $BLW1; $BLG1`
  - PML-TQ (query; grouped by): `tok $n := [ lemma = "$M" ]; $n.$PG1`
- **G3**
  - pando: `[lemma="$H"]; count by match.$G1`
  - Manatee (query; criterion): `[lemma="$H"]; $MG1`
  - CWB (query; command): `[lemma="$H"]; group A match $CG1`
  - BlackLab (pattern; group): `[lemma="$H"] $BLW1; $BLG1`
  - PML-TQ (query; grouped by): `tok $n := [ lemma = "$H" ]; $n.$PG1`
- **G4**
  - pando: `[upos="ADJ"]; count by match.$G1`
  - Manatee (query; criterion): `[upos="ADJ"]; $MG1`
  - CWB (query; command): `[upos="ADJ"]; group A match $CG1`
  - BlackLab (pattern; group): `[upos="ADJ"] $BLW1; $BLG1`
  - PML-TQ (query; grouped by): `tok $n := [ upos = "ADJ" ]; $n.$PG1`
- **G5**
  - pando: `[upos="NOUN"]; count by match.$G1`
  - Manatee (query; criterion): `[upos="NOUN"]; $MG1`
  - CWB (query; command): `[upos="NOUN"]; group A match $CG1`
  - BlackLab (pattern; group): `[upos="NOUN"] $BLW1; $BLG1`
  - PML-TQ (query; grouped by): `tok $n := [ upos = "NOUN" ]; $n.$PG1`
- **G5b**
  - pando: `[upos="NOUN"]; count by match.$G2`
  - Manatee (query; criterion): `[upos="NOUN"]; $MG2`
  - CWB (query; command): `[upos="NOUN"]; group A match $CG2`
  - BlackLab (pattern; group): `[upos="NOUN"]; $BLG2`
  - PML-TQ (query; grouped by): `tok $n := [ upos = "NOUN" ]; $n.$PG2`
- **G5c**
  - pando: `[upos="NOUN"]; count by match.lemma`
  - Manatee (query; criterion): `[upos="NOUN"]; lemma 0`
  - CWB (query; command): `[upos="NOUN"]; count A by lemma`
  - BlackLab (pattern; group): `[upos="NOUN"]; hit:lemma:s`
  - PML-TQ (query; grouped by): `tok $n := [ upos = "NOUN" ]; $n.lemma`
- **G5d**
  - pando: `[upos="NOUN"]; count by match.$G1, match.$G2`
  - Manatee (query; criterion): `[upos="NOUN"]; $MG1 $MG2`
  - CWB (query; command): `[upos="NOUN"]; group A match $CG1 by match $CG2`
  - BlackLab (pattern; group): `[upos="NOUN"] $BLW1; $BLG1,$BLG2`
  - PML-TQ (query; grouped by): `tok $n := [ upos = "NOUN" ]; $n.$PG1, $n.$PG2`
- **S3g**
  - pando: `a:[upos="ADJ"] b:[upos="NOUN"]; count by a.lemma, b.lemma`
  - Manatee (query; criterion): `[upos="ADJ"] [upos="NOUN"]; lemma 0 lemma 1`
  - CWB (query; command): `[upos="ADJ"] [upos="NOUN"]; count A by lemma`
  - BlackLab (pattern; group): `a:[upos="ADJ"] b:[upos="NOUN"]; capture:lemma:s:a,capture:lemma:s:b`
  - PML-TQ (query; grouped by): `tok $a := [ upos = "ADJ" ]; tok $b := [ upos = "NOUN", same-tree-as $a, ord = $a.ord + 1 ]; $a.lemma, $b.lemma`
- **D1g**
  - pando: `[lemma="$R"] > a:[deprel="amod"]; count by a.lemma`
  - BlackLab (pattern; group): `[lemma="$R"] -amod-> a:_; capture:lemma:s:a`
  - PML-TQ (query; grouped by): `tok [ lemma = "$R", tok $a := [ deprel = "amod" ] ]; $a.lemma`
- **D3g**
  - pando: `[upos="NOUN"] > a:[upos="ADJ"]; count by a.lemma`
  - BlackLab (pattern; group): `[upos="NOUN"] --> a:[upos="ADJ"]; capture:lemma:s:a`
  - PML-TQ (query; grouped by): `tok [ upos = "NOUN", tok $a := [ upos = "ADJ" ] ]; $a.lemma`
- **D4g**
  - pando: `[upos="VERB"] > [deprel="nsubj" & upos="PRON"]; count by match.$G2`
  - BlackLab (pattern; group): `[upos="VERB"] -nsubj-> [upos="PRON"]; $BLG2`
  - PML-TQ (query; grouped by): `tok $n := [ upos = "VERB", tok [ deprel = "nsubj", upos = "PRON" ] ]; $n.$PG2`

PML-TQ's aggregation is the query plus the output filter `>> for KEYS give KEYS, count() sort by COUNT desc`.

### As run on UD 2.18, all treebanks

Read back from the result files. Sequence queries are limited to the sentence; L1 = language (`text_langcode`), T2 = treebank (`text_treebank`). — = not expressible in that engine.

| | what | pando | Manatee | BlackLab | PML-TQ |
|---|---|---|---|---|---|
| T1 | rare lemma | `[lemma="addition"]` | `[lemma="addition"]` | `[lemma="addition"]` | `tok [ lemma = "addition" ]` |
| T2 | mid-frequency lemma | `[lemma="uçuş"]` | `[lemma="uçuş"]` | `[lemma="uçuş"]` | `tok [ lemma = "uçuş" ]` |
| T3 | frequent lemma | `[lemma="společnost"]` | `[lemma="společnost"]` | `[lemma="společnost"]` | `tok [ lemma = "společnost" ]` |
| T4 | ADJ | `[upos="ADJ"]` | `[upos="ADJ"]` | `[upos="ADJ"]` | `tok [ upos = "ADJ" ]` |
| T5 | NOUN | `[upos="NOUN"]` | `[upos="NOUN"]` | `[upos="NOUN"]` | `tok [ upos = "NOUN" ]` |
| S1 | rare lemma + VERB | `[lemma="addition"] [upos="VERB"] within s` | `[lemma="addition"] [upos="VERB"] within <s/>` | `[lemma="addition"] [upos="VERB"] within <s/>` | `tok $a := [ lemma = "addition" ]; tok [ upos = "VERB", same-tree-as $a, ord = $a.ord + 1 ]` |
| S1r | VERB + rare lemma | `[upos="VERB"] [lemma="addition"] within s` | `[upos="VERB"] [lemma="addition"] within <s/>` | `[upos="VERB"] [lemma="addition"] within <s/>` | `tok $a := [ upos = "VERB" ]; tok [ lemma = "addition", same-tree-as $a, ord = $a.ord + 1 ]` |
| S2 | VERB + mid lemma | `[upos="VERB"] [lemma="uçuş"] within s` | `[upos="VERB"] [lemma="uçuş"] within <s/>` | `[upos="VERB"] [lemma="uçuş"] within <s/>` | `tok $a := [ upos = "VERB" ]; tok [ lemma = "uçuş", same-tree-as $a, ord = $a.ord + 1 ]` |
| S3 | ADJ NOUN | `[upos="ADJ"] [upos="NOUN"] within s` | `[upos="ADJ"] [upos="NOUN"] within <s/>` | `[upos="ADJ"] [upos="NOUN"] within <s/>` | `tok $a := [ upos = "ADJ" ]; tok [ upos = "NOUN", same-tree-as $a, ord = $a.ord + 1 ]` |
| S4 | DET, 0–2 tokens, NOUN | `[upos="DET"] []{0,2} [upos="NOUN"] within s` | `[upos="DET"] []{0,2} [upos="NOUN"] within <s/>` | `[upos="DET"] []{0,2} [upos="NOUN"] within <s/>` | `tok $a := [ upos = "DET" ]; tok [ upos = "NOUN", same-tree-as $a, ord >= $a.ord + 1, ord <= $a.ord + 3 ]` |
| S5 | PRON VERB DET NOUN | `[upos="PRON"] [upos="VERB"] [upos="DET"] [upos="NOUN"] within s` | `[upos="PRON"] [upos="VERB"] [upos="DET"] [upos="NOUN"] within <s/>` | `[upos="PRON"] [upos="VERB"] [upos="DET"] [upos="NOUN"] within <s/>` | `tok $a := [ upos = "PRON" ]; tok $b := [ upos = "VERB", same-tree-as $a, ord = $a.ord + 1 ]; tok $c := [ upos = "DET", same-tree-as $a, ord = $b.ord + 1 ]; tok [ upos = "NOUN", same-tree-as $a, ord = $c.ord + 1 ]` |
| D1 | rare lemma > amod | `[lemma="addition"] > [deprel="amod"]` | — | `[lemma="addition"] -amod-> _` | `tok [ lemma = "addition", tok [ deprel = "amod" ] ]` |
| D1r | VERB > rare lemma | `[upos="VERB"] > [lemma="addition"]` | — | `[upos="VERB"] --> [lemma="addition"]` | `tok [ upos = "VERB", tok [ lemma = "addition" ] ]` |
| D2 | mid lemma > amod | `[lemma="uçuş"] > [deprel="amod"]` | — | `[lemma="uçuş"] -amod-> _` | `tok [ lemma = "uçuş", tok [ deprel = "amod" ] ]` |
| D3 | NOUN > ADJ | `[upos="NOUN"] > [upos="ADJ"]` | — | `[upos="NOUN"] --> [upos="ADJ"]` | `tok [ upos = "NOUN", tok [ upos = "ADJ" ] ]` |
| D4 | VERB > nsubj PRON | `[upos="VERB"] > [deprel="nsubj" & upos="PRON"]` | — | `[upos="VERB"] -nsubj-> [upos="PRON"]` | `tok [ upos = "VERB", tok [ deprel = "nsubj", upos = "PRON" ] ]` |
| D5 | VERB without nsubj | `[upos="VERB"] !> [deprel="nsubj"]` | — | `[upos="VERB"] !-nsubj-> _` | `tok [ upos = "VERB", 0x tok [ deprel = "nsubj" ] ]` |
| D6 | VERB >> mid lemma (any depth) | `[upos="VERB"] >> [lemma="uçuş"]` | — | — | `tok [ upos = "VERB", descendant tok [ lemma = "uçuş" ] ]` |
| D7 | VERB with nsubj and obj | `[upos="VERB" & child [deprel="nsubj"] & child [deprel="obj"]]` | — | `[upos="VERB"] -nsubj-> _ ; -obj-> _` | `tok [ upos = "VERB", 1+x tok [ deprel = "nsubj" ], 1+x tok [ deprel = "obj" ] ]` |
| G1 | rare lemma by L1 | `[lemma="addition"]; count by match.text_langcode` | `[lemma="addition"]  (criterion: text.langcode 0)` | `[lemma="addition"]  (group: field:text_langcode)` | `tok $n := [ lemma = "addition" ] >> for $n.text/langcode give $1, count() sort by $2 desc` |
| G2 | mid lemma by L1 | `[lemma="uçuş"]; count by match.text_langcode` | `[lemma="uçuş"]  (criterion: text.langcode 0)` | `[lemma="uçuş"]  (group: field:text_langcode)` | `tok $n := [ lemma = "uçuş" ] >> for $n.text/langcode give $1, count() sort by $2 desc` |
| G3 | frequent lemma by L1 | `[lemma="společnost"]; count by match.text_langcode` | `[lemma="společnost"]  (criterion: text.langcode 0)` | `[lemma="společnost"]  (group: field:text_langcode)` | `tok $n := [ lemma = "společnost" ] >> for $n.text/langcode give $1, count() sort by $2 desc` |
| G4 | ADJ by L1 | `[upos="ADJ"]; count by match.text_langcode` | `[upos="ADJ"]  (criterion: text.langcode 0)` | `[upos="ADJ"]  (group: field:text_langcode)` | `tok $n := [ upos = "ADJ" ] >> for $n.text/langcode give $1, count() sort by $2 desc` |
| G5 | NOUN by L1 | `[upos="NOUN"]; count by match.text_langcode` | `[upos="NOUN"]  (criterion: text.langcode 0)` | `[upos="NOUN"]  (group: field:text_langcode)` | `tok $n := [ upos = "NOUN" ] >> for $n.text/langcode give $1, count() sort by $2 desc` |
| G5b | NOUN by T2 | `[upos="NOUN"]; count by match.text_treebank` | `[upos="NOUN"]  (criterion: text.treebank 0)` | `[upos="NOUN"]  (group: field:text_treebank)` | `tok $n := [ upos = "NOUN" ] >> for $n.text/treebank give $1, count() sort by $2 desc` |
| G5c | NOUN by lemma | `[upos="NOUN"]; count by match.lemma` | `[upos="NOUN"]  (criterion: lemma 0)` | `[upos="NOUN"]  (group: hit:lemma:s)` | `tok $n := [ upos = "NOUN" ] >> for $n.lemma give $1, count() sort by $2 desc` |
| G5d | NOUN by L1 × T2 | `[upos="NOUN"]; count by match.text_langcode, match.text_treebank` | `[upos="NOUN"]  (criterion: text.langcode 0 text.treebank 0)` | `[upos="NOUN"]  (group: field:text_langcode,field:text_treebank)` | `tok $n := [ upos = "NOUN" ] >> for $n.text/langcode, $n.text/treebank give $1, $2, count() sort by $3 desc` |
| S3g | ADJ NOUN by both lemmas | `a:[upos="ADJ"] b:[upos="NOUN"]; count by a.lemma, b.lemma` | `[upos="ADJ"] [upos="NOUN"]  (criterion: lemma 0 lemma 1)` | `a:[upos="ADJ"] b:[upos="NOUN"]  (group: capture:lemma:s:a,capture:lemma:s:b)` | `tok $a := [ upos = "ADJ" ]; tok $b := [ upos = "NOUN", same-tree-as $a, ord = $a.ord + 1 ] >> for $a.lemma, $b.lemma give $1, $2, count() sort by $3 desc` |
| D1g | rare lemma > amod, by dependent's lemma | `[lemma="addition"] > a:[deprel="amod"]; count by a.lemma` | — | `[lemma="addition"] -amod-> a:_  (group: capture:lemma:s:a)` | `tok [ lemma = "addition", tok $a := [ deprel = "amod" ] ] >> for $a.lemma give $1, count() sort by $2 desc` |
| D3g | NOUN > ADJ, by dependent's lemma | `[upos="NOUN"] > a:[upos="ADJ"]; count by a.lemma` | — | `[upos="NOUN"] --> a:[upos="ADJ"]  (group: capture:lemma:s:a)` | `tok [ upos = "NOUN", tok $a := [ upos = "ADJ" ] ] >> for $a.lemma give $1, count() sort by $2 desc` |
| D4g | VERB > nsubj PRON, by T2 | `[upos="VERB"] > [deprel="nsubj" & upos="PRON"]; count by match.text_treebank` | — | `[upos="VERB"] -nsubj-> [upos="PRON"]  (group: field:text_treebank)` | `tok $n := [ upos = "VERB", tok [ deprel = "nsubj", upos = "PRON" ] ] >> for $n.text/treebank give $1, count() sort by $2 desc` |

### Slots per corpus

$R / $M / $H = a rare, a mid-frequency and a frequent lemma; $G1 / $G2 = the metadata grouped by (pando's name; $MG Manatee, $CG CWB, $BLG BlackLab, $PG PML-TQ).

| slot | UD German-HDT | ParlaMint LV | UD 2.18, all | ParlaMint 5.0, all |
|---|---|---|---|---|
| $R | `Abenteuer` | `aborts` | `addition` | `beskatning` |
| $M | `Anschluß` | `pilnvara` | `uçuş` | `ordfører` |
| $H | `Jahr` | `lasījums` | `společnost` | `zakon` |
| $G1 | — | `u_gender` | `text_langcode` | `u_gender` |
| $G2 | — | `text_month` | `text_treebank` | `text_country` |
| $MG1 | — | `u.gender 0` | `text.langcode 0` | `u.gender 0` |
| $MG2 | — | `text.month 0` | `text.treebank 0` | `text.country 0` |
| $CG1 | — | `u_gender` | `text_langcode` | — |
| $CG2 | — | `text_month` | `text_treebank` | — |
| $BLG1 | — | `span-attribute:u:gender:s` | `field:text_langcode` | `span-attribute:u:gender:s` |
| $BLG2 | — | `field:text_month` | `field:text_treebank` | `field:text_country` |
| $BLW1 | — | `within <u/>` | — | `within <u/>` |
| $PG1 | — | `text/gender` | `text/langcode` | — |
| $PG2 | — | `text/month` | `text/treebank` | — |

