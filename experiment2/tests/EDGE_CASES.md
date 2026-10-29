# Edge corpus contract

Use the same stopword file as the main corpus; it must include `a an and in is of the to` for the stopwords-only document. There are four files in `data/edge/`: `edge01.txt` mixes case, repeated words, punctuation, numbers and non-ASCII text; `edge02.txt` repeats some terms across documents; `edge03.txt` contains only these basic stopwords; `edge04.txt` is an empty document.

Tokenization is strictly `[A-Za-z]+`, then lowercase. `data42data` is two `data` tokens, `map-reduce` is `map` and `reduce`, and `Don't` is `don` and `t` before stopword filtering. `café naïve` produces `caf`, `na`, `ve`; Chinese and Greek-only words produce no tokens. This explicitly documents the ASCII-English scope rather than claiming multilingual linguistic correctness.

If `hadoop`, `index`, `data` and `search` are not stopwords, their expected complete lines are:

```text
data	<edge01.txt,2>;<edge02.txt,2>;<total,4>.
hadoop	<edge01.txt,3>;<edge02.txt,1>;<total,4>.
index	<edge01.txt,2>;<edge02.txt,3>;<total,5>.
search	<edge02.txt,2>;<total,2>.
```

Run `python3 tests/verify_index.py data/edge data/stopwords.txt results/edge --summary results/edge-verification.json` against downloaded Hadoop output. It derives all expected postings directly from input bytes, so changing the stopword list does not silently invalidate hard-coded reference values.

Run `python3 tests/test_verifier.py` to test that malformed postings, duplicate documents, invalid totals and incorrect ordering are rejected.
