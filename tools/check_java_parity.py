#!/usr/bin/env python3
"""Validate and summarize the pinned Java compatibility manifest."""

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
        default=Path("compat/java-v2.9.19-api.json"),
    )
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()

    data = json.loads(args.manifest.read_text(encoding="utf-8"))
    declarations = data["declarations"]
    counts = {
        status: sum(item["status"] == status for item in declarations)
        for status in ("complete", "partial", "missing")
    }
    expected = data["summary"]
    if len(declarations) != expected["declarationCount"]:
        raise SystemExit("declaration count does not match manifest summary")
    if len({item["name"] for item in declarations}) != expected["uniqueMethodCount"]:
        raise SystemExit("unique method count does not match manifest summary")
    if counts["complete"] != expected["completeMethodCount"]:
        raise SystemExit("complete count does not match manifest summary")
    if counts["partial"] != expected["partialDeclarationCount"]:
        raise SystemExit("partial count does not match manifest summary")
    if counts["missing"] != expected["missingDeclarationCount"]:
        raise SystemExit("missing count does not match manifest summary")

    print(
        f"Java {data['upstream']['version']}: "
        f"{counts['complete']} complete, {counts['partial']} partial, "
        f"{counts['missing']} missing declarations"
    )
    if args.require_complete and (counts["partial"] or counts["missing"]):
        raise SystemExit("Java SDK parity is incomplete")


if __name__ == "__main__":
    main()
