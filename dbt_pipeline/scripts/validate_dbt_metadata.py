#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def load_manifest(manifest_path: str) -> dict:
    path = Path(manifest_path)
    if not path.exists():
        print(f"[metadata] manifest not found at: {path}", file=sys.stderr)
        sys.exit(1)
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        print(f"[metadata] failed to parse manifest: {exc}", file=sys.stderr)
        sys.exit(1)


def main(manifest_path: str = "target/manifest.json") -> int:
    manifest = load_manifest(manifest_path)
    nodes = manifest.get("nodes", {})

    # Only validate models in this project (exclude packages and non-core layers).
    models = {
        node_id: node
        for node_id, node in nodes.items()
        if node.get("resource_type") == "model"
        and node.get("package_name") == "dbt_pipeline"
        and node.get("config", {}).get("materialized") != "ephemeral"
        # Skip staging/intermediate layers; enforce coverage only on core/marts.
        and not any(
            tag in (node.get("tags") or [])
            for tag in ("layer:staging", "layer:intermediate")
        )
    }

    if not models:
        print("[metadata] no models found to validate; skipping.", file=sys.stderr)
        return 0

    # Collect which models have tests attached (generic or singular).
    tested_models = set()
    for node_id, node in nodes.items():
        if node.get("resource_type") != "test":
            continue
        for dep in node.get("depends_on", {}).get("nodes", []):
            if dep in models:
                tested_models.add(dep)

    missing_docs = []
    missing_tests = []

    for node_id, node in models.items():
        name = node.get("name") or node_id
        description = (node.get("description") or "").strip()

        if not description:
            missing_docs.append(name)

        if node_id not in tested_models:
            missing_tests.append(name)

    ok = True

    if missing_docs:
        ok = False
        print("[metadata] Models missing descriptions:", file=sys.stderr)
        for name in sorted(missing_docs):
            print(f"  - {name}", file=sys.stderr)

    if missing_tests:
        ok = False
        print("\n[metadata] Models missing tests:", file=sys.stderr)
        for name in sorted(missing_tests):
            print(f"  - {name}", file=sys.stderr)

    if not ok:
        print(
            "\n[metadata] Validation failed. "
            "Please add model descriptions and at least one test per model.",
            file=sys.stderr,
        )
        return 1

    print(f"[metadata] Validation passed for {len(models)} models.")
    return 0


if __name__ == "__main__":
    manifest_arg = sys.argv[1] if len(sys.argv) > 1 else "target/manifest.json"
    sys.exit(main(manifest_arg))
