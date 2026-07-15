#!/usr/bin/env python3
"""Build the Go-file -> Rust-file mapping from provenance citations.

Every ported Rust module cites its upstream origin ("Go: partSearch.searchBHS",
"port of the upstream lib/mergeset", "part_search.go", ...). This script scans
those citations and prints a mapping table, so an upstream diff touching a Go
file can be routed to the Rust files that port it.

Usage:
    scripts/upstream-map.py            # markdown table to stdout
    scripts/upstream-map.py --tsv      # tab-separated (for joining in shell)
    scripts/upstream-map.py --go-file part_search.go   # reverse lookup
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Crate -> upstream Go package prefixes, used to qualify bare *.go filenames
# found in comments (both lib/mergeset and lib/storage have part_search.go).
CRATE_PACKAGES = {
    "esm-common": ["lib/bytesutil", "lib/fasttime", "lib/fastnum", "lib/decimal",
                   "lib/uint64set", "lib/regexutil", "lib/fs", "lib/filestream",
                   "lib/memory", "lib/cgroup", "lib/logger"],
    "esm-encoding": ["lib/encoding"],
    "esm-mergeset": ["lib/mergeset", "lib/blockcache"],
    "esm-storage": ["lib/storage", "lib/workingsetcache", "lib/lrucache"],
    "esm-protoparser": ["lib/protoparser/influx", "lib/protoparser/common"],
    "esm-metricsql": ["<metricsql repo root>"],
    "esm-promql": ["app/vmselect/promql", "app/vmselect/netstorage"],
    "esm-insert": ["app/vminsert", "lib/writeconcurrencylimiter"],
    "esm-select": ["app/vmselect/prometheus", "app/vmselect/searchutil",
                   "app/vmselect"],
    "esm-http": ["lib/httpserver"],
    "esmetrics": ["app/victoria-metrics", "lib/flagutil"],
}

# Module-level overrides where a crate hosts a file whose origin is another
# package (keep this list short; prefer citing paths in the comments).
MODULE_PACKAGE_OVERRIDES = {
    "crates/esm-common/src/memory.rs": ["lib/memory", "lib/cgroup"],
    "crates/esm-storage/src/index/caches.rs": ["lib/workingsetcache", "lib/lrucache"],
    "crates/esm-mergeset/src/blockcache.rs": ["lib/blockcache"],
}

GO_FILE_RE = re.compile(r"\b([a-z0-9_]+\.go)\b")
GO_PATH_RE = re.compile(r"\b((?:lib|app)/[a-z0-9_/.-]+)\b")


def crate_of(path: Path) -> str:
    parts = path.relative_to(ROOT).parts
    return parts[1] if parts[0] == "crates" and len(parts) > 1 else ""


def scan():
    """Returns {qualified_go_ref: set(rust_files)}."""
    mapping = defaultdict(set)
    for rs in sorted(ROOT.glob("crates/*/src/**/*.rs")):
        rel = str(rs.relative_to(ROOT))
        crate = crate_of(rs)
        packages = MODULE_PACKAGE_OVERRIDES.get(rel) or CRATE_PACKAGES.get(crate, [])
        text = rs.read_text(errors="replace")
        comments = "\n".join(
            line for line in text.splitlines() if line.lstrip().startswith("//")
        )
        # Explicit package paths cited in comments win.
        cited_paths = set(GO_PATH_RE.findall(comments))
        for go_file in set(GO_FILE_RE.findall(comments)):
            qualifier = next(
                (p for p in cited_paths if p.endswith("/" + go_file)), None
            )
            if qualifier:
                mapping[qualifier].add(rel)
            elif packages:
                mapping[f"{packages[0]}/{go_file}"].add(rel)
            else:
                mapping[go_file].add(rel)
        # Package-level citations (whole-package provenance in module docs).
        for p in cited_paths:
            if not p.endswith(".go"):
                mapping[p + "/*"].add(rel)
    return mapping


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv", action="store_true")
    ap.add_argument("--go-file", help="print only Rust files mapping to this Go file/path")
    args = ap.parse_args()

    mapping = scan()

    if args.go_file:
        hits = sorted(
            f for ref, files in mapping.items() if args.go_file in ref for f in files
        )
        print("\n".join(dict.fromkeys(hits)))
        return 0 if hits else 1

    if args.tsv:
        for ref in sorted(mapping):
            for f in sorted(mapping[ref]):
                print(f"{ref}\t{f}")
    else:
        print("| upstream Go source | ported in |")
        print("|---|---|")
        for ref in sorted(mapping):
            print(f"| `{ref}` | {'<br>'.join(sorted(mapping[ref]))} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
