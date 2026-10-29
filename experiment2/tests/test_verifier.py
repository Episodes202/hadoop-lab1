#!/usr/bin/env python3
"""Negative tests ensure the verifier rejects plausible-looking wrong results."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from verify_index import parse_line, verify


class PostingParserTests(unittest.TestCase):
    def test_valid_line(self):
        self.assertEqual(parse_line("hadoop\t<a.txt,2>;<b.txt,3>;<total,5>.", "test"),
                         ("hadoop", {"a.txt": 2, "b.txt": 3}, 5))

    def test_wrong_total(self):
        with self.assertRaisesRegex(ValueError, "differs from posting sum"):
            parse_line("hadoop\t<a.txt,2>;<b.txt,3>;<total,6>.", "test")

    def test_duplicate_document(self):
        with self.assertRaisesRegex(ValueError, "unique"):
            parse_line("hadoop\t<a.txt,2>;<a.txt,3>;<total,5>.", "test")

    def test_document_order(self):
        with self.assertRaisesRegex(ValueError, "lexicographically"):
            parse_line("hadoop\t<b.txt,2>;<a.txt,3>;<total,5>.", "test")

    def test_non_positive_frequency(self):
        with self.assertRaisesRegex(ValueError, "non-positive"):
            parse_line("hadoop\t<a.txt,0>;<total,1>.", "test")

    def test_missing_period(self):
        with self.assertRaisesRegex(ValueError, "missing final period"):
            parse_line("hadoop\t<a.txt,1>;<total,1>", "test")

    def test_uppercase_term(self):
        with self.assertRaisesRegex(ValueError, "lowercase"):
            parse_line("Hadoop\t<a.txt,1>;<total,1>.", "test")

    def test_total_position(self):
        with self.assertRaisesRegex(ValueError, "exactly once at the end"):
            parse_line("hadoop\t<total,2>;<a.txt,2>.", "test")


class FullVerifierTests(unittest.TestCase):
    def setUp(self):
        self.owner = Path(__file__).resolve().parent
        self.temporary = TemporaryDirectory(prefix=".verifier-test-", dir=self.owner)
        self.root = Path(self.temporary.name).resolve()
        # Verify the exact recursive-cleanup target is a child of the owned test directory.
        if self.root.parent != self.owner:
            raise RuntimeError("Unexpected temporary test directory")
        self.addCleanup(self.temporary.cleanup)
        self.inputs = self.root / "input"
        self.inputs.mkdir()
        (self.inputs / "a.txt").write_text("Alpha alpha; beta! The.", encoding="utf-8")
        (self.inputs / "b.txt").write_text("BETA", encoding="utf-8")
        self.stops = self.root / "stopwords.txt"
        self.stops.write_text("the\n", encoding="utf-8")
        self.output = self.root / "output"
        self.output.mkdir()
        self.first = self.output / "part-r-00000"
        self.second = self.output / "part-r-00001"
        self.first.write_text("alpha\t<a.txt,2>;<total,2>.\n", encoding="utf-8")
        self.second.write_text("beta\t<a.txt,1>;<b.txt,1>;<total,2>.\n", encoding="utf-8")

    def test_valid_multi_partition_output(self):
        result = verify(self.inputs, self.stops, self.output, 2)
        self.assertEqual((result["document_count"], result["raw_tokens"], result["stopword_tokens"],
                          result["indexed_tokens"], result["unique_terms"], result["document_postings"]),
                         (2, 5, 1, 4, 2, 3))

    def test_plausible_wrong_postings_with_correct_internal_total(self):
        self.second.write_text("beta\t<a.txt,2>;<b.txt,1>;<total,3>.\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Postings mismatch"):
            verify(self.inputs, self.stops, self.output)

    def test_missing_term(self):
        self.second.write_text("", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Term mismatch"):
            verify(self.inputs, self.stops, self.output)

    def test_stopword_leak(self):
        self.second.write_text(self.second.read_text(encoding="utf-8") + "the\t<a.txt,1>;<total,1>.\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Stopwords leaked"):
            verify(self.inputs, self.stops, self.output)

    def test_duplicate_term_across_partitions(self):
        self.second.write_text(self.first.read_text(encoding="utf-8") + self.second.read_text(encoding="utf-8"), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Duplicate term across"):
            verify(self.inputs, self.stops, self.output)

    def test_document_minimum(self):
        with self.assertRaisesRegex(ValueError, "at least 10 documents"):
            verify(self.inputs, self.stops, self.output, 10)


if __name__ == "__main__":
    unittest.main()
