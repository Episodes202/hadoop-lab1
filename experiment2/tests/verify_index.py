#!/usr/bin/env python3
"""Compare Hadoop postings with an independent per-document Counter reference."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import sys

TOKEN = re.compile(r"[A-Za-z]+")
TERM = re.compile(r"[a-z]+")
POSTING = re.compile(r"<([^<>,;\t\r\n]+),([1-9][0-9]*)>")


def stopwords_from(path: Path) -> set[str]:
    words: set[str] = set()
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        word = line.strip().lower()
        if not word or word.startswith("#"):
            continue
        if TERM.fullmatch(word) is None:
            raise ValueError(f"Invalid stopword at {path.name}:{number}: {word!r}")
        words.add(word)
    return words


def reference_index(input_dir: Path, stopwords: set[str]) -> tuple[dict[str, dict[str, int]], dict]:
    if not input_dir.is_dir():
        raise ValueError(f"Input is not a local directory: {input_dir}")
    entries = sorted(p for p in input_dir.iterdir() if not p.name.startswith((".", "_")))
    if any(p.is_dir() for p in entries):
        raise ValueError("Use a flat input directory with unique document filenames")
    files = [p for p in entries if p.is_file()]
    if not files:
        raise ValueError("Input directory has no visible document files")
    index: dict[str, dict[str, int]] = defaultdict(dict)
    raw_tokens = stopped_tokens = 0
    documents = []
    for path in files:
        if path.name == "total" or any(c in path.name for c in "<>,;\t\r\n"):
            raise ValueError(f"Filename cannot be represented unambiguously: {path.name!r}")
        raw = path.read_bytes()
        tokens = [word.lower() for word in TOKEN.findall(raw.decode("utf-8"))]
        kept = Counter(word for word in tokens if word not in stopwords)
        raw_tokens += len(tokens)
        stopped = sum(word in stopwords for word in tokens)
        stopped_tokens += stopped
        documents.append({"file": path.name, "bytes": len(raw), "raw_tokens": len(tokens),
                          "stopword_tokens": stopped, "indexed_tokens": sum(kept.values()),
                          "sha256": hashlib.sha256(raw).hexdigest()})
        for word, frequency in kept.items():
            index[word][path.name] = frequency
    stats = {"document_count": len(files), "documents": documents,
             "raw_tokens": raw_tokens, "stopword_tokens": stopped_tokens,
             "indexed_tokens": raw_tokens - stopped_tokens,
             "stopword_count": len(stopwords)}
    return dict(index), stats


def parse_line(line: str, location: str) -> tuple[str, dict[str, int], int]:
    if line.count("\t") != 1:
        raise ValueError(f"{location}: expected one TAB between term and postings")
    word, serialized = line.split("\t")
    if TERM.fullmatch(word) is None or not serialized.endswith("."):
        raise ValueError(f"{location}: invalid lowercase term or missing final period")
    pieces = serialized[:-1].split(";")
    if len(pieces) < 2:
        raise ValueError(f"{location}: expected at least one document and total")
    pairs = []
    for piece in pieces:
        match = POSTING.fullmatch(piece)
        if match is None:
            raise ValueError(f"{location}: malformed posting or non-positive frequency: {piece!r}")
        pairs.append((match.group(1), int(match.group(2))))
    if pairs[-1][0] != "total" or any(name == "total" for name, _ in pairs[:-1]):
        raise ValueError(f"{location}: total must appear exactly once at the end")
    names = [name for name, _ in pairs[:-1]]
    if names != sorted(set(names)):
        raise ValueError(f"{location}: document IDs must be unique and lexicographically ordered")
    postings = dict(pairs[:-1])
    total = pairs[-1][1]
    if total != sum(postings.values()):
        raise ValueError(f"{location}: total {total} differs from posting sum {sum(postings.values())}")
    return word, postings, total


def load_output(output: Path) -> tuple[dict[str, dict[str, int]], list[dict]]:
    if output.is_dir():
        parts = sorted(p for p in output.glob("part-r-*") if p.is_file())
    elif output.is_file():
        parts = [output]
    else:
        raise ValueError(f"Output path does not exist: {output}")
    if not parts:
        raise ValueError("No part-r-* output files found")
    actual: dict[str, dict[str, int]] = {}
    output_files = []
    for part in parts:
        raw = part.read_bytes()
        previous = None
        lines = raw.decode("utf-8").splitlines()
        for number, line in enumerate(lines, 1):
            word, postings, _ = parse_line(line, f"{part.name}:{number}")
            if previous is not None and word <= previous:
                raise ValueError(f"{part.name}:{number}: terms are not strictly ascending within partition")
            if word in actual:
                raise ValueError(f"Duplicate term across reducer output: {word}")
            actual[word] = postings
            previous = word
        output_files.append({"file": part.name, "lines": len(lines),
                             "sha256": hashlib.sha256(raw).hexdigest()})
    return actual, output_files


def verify(input_dir: Path, stopwords_file: Path, output: Path, min_documents: int = 0) -> dict:
    stopwords = stopwords_from(stopwords_file)
    expected, stats = reference_index(input_dir, stopwords)
    if stats["document_count"] < min_documents:
        raise ValueError(f"Expected at least {min_documents} documents, found {stats['document_count']}")
    actual, output_files = load_output(output)
    stopped_output = sorted(set(actual) & stopwords)
    if stopped_output:
        raise ValueError(f"Stopwords leaked into output: {stopped_output[:10]}")
    missing, extra = sorted(set(expected) - set(actual)), sorted(set(actual) - set(expected))
    if missing or extra:
        raise ValueError(f"Term mismatch: missing={missing[:10]}, extra={extra[:10]}")
    for word in sorted(expected):
        if actual[word] != expected[word]:
            raise ValueError(f"Postings mismatch for {word!r}: expected={expected[word]}, actual={actual[word]}")
    totals = {word: sum(postings.values()) for word, postings in actual.items()}
    if sum(totals.values()) != stats["indexed_tokens"]:
        raise ValueError("Sum of term totals differs from independently counted indexed tokens")
    top = sorted(totals, key=lambda word: (-totals[word], word))[:20]
    samples = sorted(set(top[:5]) | (set(actual) & {"data", "hadoop", "index", "mapreduce", "search"}))
    return {"verified": True, **stats, "unique_terms": len(actual),
            "document_postings": sum(len(postings) for postings in actual.values()),
            "total_frequency": sum(totals.values()), "output_files": output_files,
            "tokenization": "ASCII [A-Za-z]+, lowercase after token extraction; no stemming",
            "top_terms": [{"term": word, "total": totals[word], "document_frequency": len(actual[word])}
                          for word in top],
            "sample_postings": {word: {"documents": actual[word], "total": totals[word]} for word in samples}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("stopwords", type=Path)
    parser.add_argument("output", type=Path, help="local part-r file or directory containing part-r-* files")
    parser.add_argument("--min-documents", type=int, default=0)
    parser.add_argument("--summary", type=Path, help="write verification JSON to this path")
    args = parser.parse_args()
    try:
        result = verify(args.input_dir, args.stopwords, args.output, args.min_documents)
    except (OSError, ValueError, UnicodeError) as error:
        print(f"VERIFICATION FAILED: {error}", file=sys.stderr)
        return 1
    serialized = json.dumps(result, ensure_ascii=False, indent=2)
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
