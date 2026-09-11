#!/usr/bin/env python3
"""Extract the public TosClientV2 API from the pinned official Python SDK."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path


PINNED_VERSION = "2.9.0"
PINNED_COMMIT = "b6b93b6ca5e4c697688452cf04218ef200dd2996"
SOURCE_URL = (
    "https://github.com/volcengine/ve-tos-python-sdk/blob/"
    f"{PINNED_COMMIT}/tos/clientv2.py"
)
EXPECTED_PUBLIC_METHODS = 106


def parameter_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    args = node.args
    names = [arg.arg for arg in (*args.posonlyargs, *args.args)]
    if args.vararg:
        names.append(f"*{args.vararg.arg}")
    names.extend(arg.arg for arg in args.kwonlyargs)
    if args.kwarg:
        names.append(f"**{args.kwarg.arg}")
    return names


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("python_client", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    raw = args.python_client.read_bytes()
    tree = ast.parse(raw.decode("utf-8"))
    client = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "TosClientV2"
    )
    methods = [
        {
            "name": node.name,
            "parameters": parameter_names(node),
            "async": isinstance(node, ast.AsyncFunctionDef),
            "line": node.lineno,
        }
        for node in client.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not node.name.startswith("_")
    ]
    if len(methods) != EXPECTED_PUBLIC_METHODS:
        raise SystemExit(f"unexpected Python API shape: {len(methods)} public methods")

    manifest = {
        "schemaVersion": 1,
        "upstream": {
            "project": "volcengine/ve-tos-python-sdk",
            "version": PINNED_VERSION,
            "commit": PINNED_COMMIT,
            "source": SOURCE_URL,
            "sourceSha256": hashlib.sha256(raw).hexdigest(),
        },
        "summary": {"publicMethodCount": len(methods)},
        "methods": methods,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
