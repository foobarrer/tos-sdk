#!/usr/bin/env python3
"""Extract the public API declared by the pinned Volcengine Java TOS SDK.

The generated manifest is an audit input, not executable code.  It keeps API
parity measurable while the Rust implementations are reviewed operation by
operation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


PINNED_VERSION = "2.9.19"
PINNED_COMMIT = "cd82b2345dbe0c3c15ebee4865ca37cae343512f"
SOURCE_URL = (
    "https://github.com/volcengine/ve-tos-java-sdk/blob/"
    f"{PINNED_COMMIT}/src/main/java/com/volcengine/tos/TOSV2.java"
)

DECLARATION = re.compile(
    r"(?m)^\s*(?:<[^;{]+>\s+)?([\w<>?,.\[\] ]+)\s+(\w+)\s*"
    r"\((.*?)\)\s*(?:throws\s+[\w., ]+)?\s*;",
    re.S,
)

# Existing S3 Desktop facade mappings.  "partial" means the wire operation is
# present but the Java input/output surface is not yet complete.
RUST_MAPPINGS = {
    "createBucket": "create_bucket",
    "deleteBucket": "delete_bucket",
    "listBuckets": "list_buckets",
    "putBucketPolicy": "put_bucket_policy",
    "getBucketPolicy": "get_bucket_policy",
    "deleteBucketPolicy": "delete_bucket_policy",
    "putBucketVersioning": "set_bucket_versioning",
    "getBucketVersioning": "get_bucket_versioning",
    "getObject": "stream_object",
    "getObjectToFile": "download",
    "uploadFile": "upload",
    "downloadFile": "download",
    "headObject": "object_properties",
    "deleteMultiObjects": "delete_objects",
    "putObject": "upload_transfer_stream",
    "putObjectFromFile": "upload",
    "listObjectsType2": "list_objects",
    "copyObject": "copy_object",
    "createMultipartUpload": "upload",
    "uploadPart": "upload",
    "uploadPartFromFile": "upload",
    "completeMultipartUpload": "upload",
    "abortMultipartUpload": "upload",
    "preSignedURL": "presign_get",
    "listObjects": "list_objects_v1",
    "setObjectExpires": "set_object_expires",
}

COMPLETE_LOCAL_MAPPINGS = {
    "createAccessPoint",
    "deleteAccessPoint",
    "getAccessPoint",
    "listAccessPoints",
    "listBindAcceleratorForAccessPoint",
    "listBindAccessPointForAccelerator",
    "listObjects",
    "getBucketNotification",
    "putBucketNotification",
    "setObjectExpires",
}


def compact(value: str) -> str:
    return " ".join(value.split())


def camel_to_snake(value: str) -> str:
    aliases = {
        "changeCredentials": "refresh_credentials",
        "changeRegionAndEndpoint": "refresh_endpoint_region",
    }
    if value in aliases:
        return aliases[value]
    value = value.replace("CORS", "Cors").replace("ACL", "Acl").replace("URL", "Url")
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.lower()


def category(name: str) -> str:
    lower = name.lower()
    if "objectset" in lower:
        return "object-set"
    if "accesspoint" in lower or "accelerator" in lower:
        return "access-point"
    if any(word in lower for word in ("audio", "video", "watermark", "workflow", "animation")):
        return "media"
    if "compress" in lower:
        return "compression"
    if "qos" in lower:
        return "qos"
    if lower.startswith(("change", "refresh")):
        return "client"
    if "multipart" in lower or "uploadpart" in lower:
        return "multipart"
    if "bucket" in lower:
        return "bucket"
    if "object" in lower or "symlink" in lower or "fetch" in lower or "file" in lower:
        return "object"
    if "presign" in lower or "presinged" in lower:
        return "signing"
    return "other"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("java_interface", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--rust-methods",
        type=Path,
        help="newline-delimited Rust operation names provided by the pinned core",
    )
    args = parser.parse_args()

    rust_methods = set()
    if args.rust_methods:
        rust_methods = {
            line.strip()
            for line in args.rust_methods.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        }

    raw = args.java_interface.read_bytes()
    source = raw.decode("utf-8")
    source = re.sub(r"/\*.*?\*/", "", source, flags=re.S)
    source = re.sub(r"//.*", "", source)

    declarations = []
    for match in DECLARATION.finditer(source):
        return_type, name, parameters = match.groups()
        rust_name = camel_to_snake(name)
        if name == "preSingedPolicyURL":
            rust_name = "pre_signed_policy_url"
        has_core_parity = rust_name in rust_methods or name in COMPLETE_LOCAL_MAPPINGS
        declarations.append(
            {
                "name": name,
                "returnType": compact(return_type),
                "parameters": compact(parameters),
                "category": category(name),
                "rustMethod": rust_name if has_core_parity else RUST_MAPPINGS.get(name),
                "status": "complete" if has_core_parity else "partial" if name in RUST_MAPPINGS else "missing",
            }
        )

    unique_names = sorted({item["name"] for item in declarations})
    manifest = {
        "schemaVersion": 1,
        "upstream": {
            "project": "volcengine/ve-tos-java-sdk",
            "version": PINNED_VERSION,
            "commit": PINNED_COMMIT,
            "source": SOURCE_URL,
            "sourceSha256": hashlib.sha256(raw).hexdigest(),
        },
        "summary": {
            "declarationCount": len(declarations),
            "uniqueMethodCount": len(unique_names),
            "completeMethodCount": sum(item["status"] == "complete" for item in declarations),
            "partialDeclarationCount": sum(item["status"] == "partial" for item in declarations),
            "missingDeclarationCount": sum(item["status"] == "missing" for item in declarations),
        },
        "declarations": declarations,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    if len(declarations) != 165 or len(unique_names) != 161:
        raise SystemExit(
            f"unexpected Java API shape: {len(declarations)} declarations, "
            f"{len(unique_names)} unique methods"
        )


if __name__ == "__main__":
    main()
