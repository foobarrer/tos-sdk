#!/usr/bin/env python3
"""Validate the pinned official Python SDK inventory checked into the crate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "manifest",
        nargs="?",
        type=Path,
        default=Path("compat/python-v2.9.0-api.json"),
    )
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    methods = manifest["methods"]
    expected = manifest["summary"]["publicMethodCount"]
    names = [method["name"] for method in methods]
    if len(methods) != expected or expected != 106:
        raise SystemExit("Python SDK public method count is not the pinned baseline")
    if len(names) != len(set(names)):
        raise SystemExit("Python SDK manifest contains duplicate public methods")
    print(f"Python {manifest['upstream']['version']}: {expected} public TosClientV2 methods")


if __name__ == "__main__":
    main()
