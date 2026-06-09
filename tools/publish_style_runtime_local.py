#!/usr/bin/env python3
"""Publish sanitized Style Runtime packs from Obsidian to local snapshot dir.

Safety rules:
- allowlist only style-runtime-index-v1.json + pack_file entries referenced by it;
- reject obvious PII/raw URLs/CRM URLs/Telegram handles/secrets before copy;
- write versioned snapshot first, then update latest/ with manifest last.

Output layout:
  ~/.hermes/style-runtime-public/
    v1/
      latest/
        manifest.json
        style-runtime-index-v1.json
        packs/<pack_id>.md
      versions/<YYYYMMDDTHHMMSSz>/
        (same structure)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple


class SafetyScanError(RuntimeError):
    pass


class PublishItem(NamedTuple):
    source_path: Path
    publish_path: str
    sha256: str
    content: str
    content_type: str


class PublishPlan(NamedTuple):
    version: str
    items: list[PublishItem]
    manifest: dict


SAFETY_PATTERNS = {
    "email": re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I),
    "raw_url": re.compile(r"https?://", re.I),
    "crm_url": re.compile(r"amocrm\.ru|/leads/detail/|/contacts/detail/|/customers/detail/", re.I),
    "telegram_handle": re.compile(r"@[A-Za-z0-9_]{3,}"),
    "secret": re.compile(r"(api[_-]?key|secret|token|password|aws_access_key_id|aws_secret_access_key)\s*[:=]", re.I),
}
PHONE_CANDIDATE_PATTERN = re.compile(r"\+?\d[\d\s().-]{8,}\d")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def utc_version() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object at {path}")
    return data


def pack_entries(index: dict) -> list[dict]:
    packs = index.get("packs")
    if isinstance(packs, dict):
        return [dict({"pack_id": pack_id}, **(value if isinstance(value, dict) else {})) for pack_id, value in packs.items()]
    if isinstance(packs, list):
        return [p for p in packs if isinstance(p, dict)]
    return []


def safety_scan(path: Path, text: str) -> None:
    hits = []
    if any(len(re.sub(r"\D", "", match.group(0))) >= 10 for match in PHONE_CANDIDATE_PATTERN.finditer(text)):
        hits.append("phone_like")
    for name, pattern in SAFETY_PATTERNS.items():
        if pattern.search(text):
            hits.append(name)
    if hits:
        raise SafetyScanError(f"{path}: unsafe content detected: {', '.join(sorted(hits))}")


def resolve_pack_path(root: Path, runtime_dir: Path, pack_file: str) -> Path:
    candidate = Path(pack_file)
    if candidate.is_absolute():
        return candidate
    root_candidate = root / candidate
    if root_candidate.exists():
        return root_candidate
    runtime_candidate = runtime_dir / candidate
    if runtime_candidate.exists():
        return runtime_candidate
    raise FileNotFoundError(f"pack_file not found: {pack_file}")


def publish_pack_path(pack_id: str, source_path: Path) -> str:
    suffix = source_path.suffix or ".md"
    safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", pack_id).strip("._")
    return f"packs/{safe_id}{suffix}"


def build_publish_plan(runtime_dir: Path, *, root: Path | None = None, version: str | None = None) -> PublishPlan:
    runtime_dir = runtime_dir.expanduser().resolve()
    root = (root.expanduser().resolve() if root else runtime_dir.parents[2])
    version = version or utc_version()
    index_path = runtime_dir / "style-runtime-index-v1.json"
    index_text = index_path.read_text(encoding="utf-8")
    safety_scan(index_path, index_text)
    index = json.loads(index_text)
    if not isinstance(index, dict):
        raise ValueError("style-runtime-index-v1.json must contain a JSON object")

    items = [
        PublishItem(
            source_path=index_path,
            publish_path="style-runtime-index-v1.json",
            sha256=sha256_text(index_text),
            content=index_text,
            content_type="application/json; charset=utf-8",
        )
    ]
    manifest_packs = []
    seen_publish_paths = {"style-runtime-index-v1.json"}
    for entry in pack_entries(index):
        pack_id = entry.get("pack_id")
        pack_file = entry.get("pack_file")
        if not pack_id or not pack_file:
            raise ValueError("every runtime index pack entry must include pack_id and pack_file")
        source_path = resolve_pack_path(root, runtime_dir, str(pack_file)).resolve()
        if not str(source_path).startswith(str(root)):
            raise ValueError(f"pack_file escapes root allowlist: {source_path}")
        content = source_path.read_text(encoding="utf-8")
        safety_scan(source_path, content)
        publish_path = publish_pack_path(str(pack_id), source_path)
        if publish_path in seen_publish_paths:
            raise ValueError(f"duplicate publish path: {publish_path}")
        seen_publish_paths.add(publish_path)
        digest = sha256_text(content)
        items.append(PublishItem(source_path, publish_path, digest, content, "text/markdown; charset=utf-8"))
        manifest_packs.append({"pack_id": pack_id, "path": publish_path, "sha256": digest})

    manifest = {
        "schema_version": "style-runtime-publish-manifest-v1",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "version": version,
        "source": "obsidian_style_engine_runtime",
        "runtime_index": {"path": "style-runtime-index-v1.json", "sha256": items[0].sha256},
        "packs": manifest_packs,
        "privacy": "sanitized_no_raw_messages_no_pii_no_crm_payloads",
        "manual_review_only": True,
        "stable_pack_promotion": "vladimir_manual_approval_required",
    }
    return PublishPlan(version=version, items=items, manifest=manifest)


def write_snapshot(plan: PublishPlan, public_dir: Path) -> dict:
    public_dir = public_dir.expanduser().resolve()
    version_dir = public_dir / "v1" / "versions" / plan.version
    latest_dir = public_dir / "v1" / "latest"
    manifest_text = json.dumps(plan.manifest, ensure_ascii=False, indent=2, sort_keys=True)
    written = []

    for base in (version_dir, latest_dir):
        base.mkdir(parents=True, exist_ok=True)
        (base / "packs").mkdir(exist_ok=True)
        for item in plan.items:
            dest = base / item.publish_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(item.content, encoding="utf-8")
            written.append(str(dest))
        manifest_dest = base / "manifest.json"
        manifest_dest.write_text(manifest_text, encoding="utf-8")
        written.append(str(manifest_dest))

    return {
        "written_count": len(written),
        "version_dir": str(version_dir),
        "latest_dir": str(latest_dir),
        "files": written,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish sanitized style-runtime packs to local snapshot dir")
    parser.add_argument("--runtime-dir", default=os.environ.get("STYLE_RUNTIME_OBSIDIAN_DIR") or os.environ.get("STYLE_RUNTIME_DIR"), help="Obsidian style-engine/runtime directory")
    parser.add_argument("--root", default=None, help="Allowed Obsidian project root; defaults to runtime_dir.parents[2]")
    parser.add_argument("--public-dir", default=os.environ.get("STYLE_RUNTIME_PUBLIC_DIR") or "~/.hermes/style-runtime-public", help="Output snapshot directory")
    parser.add_argument("--version", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Build and print manifest without writing files")
    parser.add_argument("--publish", action="store_true", help="Write versioned snapshot and update latest/")
    args = parser.parse_args()

    if not args.runtime_dir:
        raise SystemExit("--runtime-dir or STYLE_RUNTIME_OBSIDIAN_DIR is required")
    plan = build_publish_plan(Path(args.runtime_dir), root=Path(args.root) if args.root else None, version=args.version)
    if args.dry_run or not args.publish:
        print(json.dumps({
            "dry_run": True,
            "version": plan.version,
            "items": [i.publish_path for i in plan.items],
            "manifest": plan.manifest,
        }, ensure_ascii=False, indent=2))
        return 0
    result = write_snapshot(plan, Path(args.public_dir))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
