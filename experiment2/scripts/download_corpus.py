"""Download ten Apache Hadoop pages and extract their main English content.

Python standard library only. Existing corpus files are never overwritten.
The checked-in corpus is the authoritative snapshot for reproducing results.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.request import Request, urlopen

BASE = "https://hadoop.apache.org/docs/r3.4.2/"
SOURCES = [
    ("Single Node Setup", "hadoop-project-dist/hadoop-common/SingleCluster.html"),
    ("Cluster Setup", "hadoop-project-dist/hadoop-common/ClusterSetup.html"),
    ("FileSystem Shell", "hadoop-project-dist/hadoop-common/FileSystemShell.html"),
    ("HDFS Architecture", "hadoop-project-dist/hadoop-hdfs/HdfsDesign.html"),
    ("HDFS Users Guide", "hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html"),
    ("HDFS Snapshots", "hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html"),
    ("HDFS Permissions Guide", "hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html"),
    ("HDFS Quotas Guide", "hadoop-project-dist/hadoop-hdfs/HdfsQuotaAdminGuide.html"),
    ("MapReduce Tutorial", "hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html"),
    ("YARN Architecture", "hadoop-yarn/hadoop-yarn-site/YARN.html"),
]


class MainText(HTMLParser):
    BLOCKS = {"p", "div", "li", "tr", "td", "th", "pre", "br", "h1", "h2", "h3", "h4", "dt", "dd"}
    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.hidden = 0
        self.fragments: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if not self.depth and dict(attrs).get("id") == "contentBox":
            self.depth = 1
        elif self.depth and tag not in self.VOID:
            self.depth += 1
        if self.depth:
            if tag in {"script", "style"}:
                self.hidden += 1
            if tag in self.BLOCKS:
                self.fragments.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self.depth:
            if tag in self.BLOCKS:
                self.fragments.append("\n")
            if tag in {"script", "style"}:
                self.hidden -= 1
            if tag not in self.VOID:
                self.depth -= 1

    def handle_data(self, text: str) -> None:
        if self.depth and not self.hidden:
            self.fragments.append(text)

    def text(self) -> str:
        lines = [re.sub(r"\s+", " ", line).strip() for line in "".join(self.fragments).splitlines()]
        return "\n".join(line for line in lines if line) + "\n"


def download(item: tuple[int, tuple[str, str]], output: Path, raw_dir: Path | None) -> dict:
    number, (title, path) = item
    url = BASE + path
    request = Request(url, headers={"User-Agent": "Hadoop-Inverted-Index-Lab/1.0"})
    with urlopen(request, timeout=45) as response:
        raw = response.read()
        status = response.status
    parser = MainText()
    parser.feed(raw.decode("utf-8"))
    text = parser.text()
    assert len(text) > 1000, f"Content extraction failed: {url}"
    name = f"document{number:02d}.txt"
    content = text.encode("utf-8")
    with (output / "corpus" / name).open("xb") as target:
        target.write(content)
    if raw_dir is not None:
        (raw_dir / f"document{number:02d}.html").write_bytes(raw)
    return {"document": name, "title": title, "url": url, "http_status": status,
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "bytes": len(content), "raw_tokens": len(re.findall(r"[A-Za-z]+", text)),
            "sha256": hashlib.sha256(content).hexdigest(), "html_sha256": hashlib.sha256(raw).hexdigest()}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / "data")
    ap.add_argument("--raw-dir", type=Path)
    args = ap.parse_args()
    corpus = args.output / "corpus"
    corpus.mkdir(parents=True, exist_ok=True)
    if any(corpus.iterdir()):
        raise SystemExit("Refusing to overwrite an existing corpus; choose a new --output directory.")
    if args.raw_dir:
        args.raw_dir.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        result = list(pool.map(lambda item: download(item, args.output, args.raw_dir), enumerate(SOURCES, 1)))
    (args.output / "sources.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"documents": len(result), "bytes": sum(x["bytes"] for x in result),
                      "raw_tokens": sum(x["raw_tokens"] for x in result)}, indent=2))


if __name__ == "__main__":
    main()
