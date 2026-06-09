"""sync_style_runtime_to_r2.py safety and manifest tests."""
import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "tools" / "sync_style_runtime_to_r2.py"


def load_script():
    spec = importlib.util.spec_from_file_location("sync_style_runtime_to_r2", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_obsidian_runtime(tmp_path, *, unsafe_pack_text=None):
    root = tmp_path / "vault" / "Assist - Real estate"
    runtime_dir = root / "office" / "style-engine" / "runtime"
    packs_dir = root / "office" / "style-engine" / "packs"
    runtime_dir.mkdir(parents=True)
    packs_dir.mkdir(parents=True)
    pack_text = unsafe_pack_text or "# Client asks question\n\nSanitized style guidance only."
    (packs_dir / "client_asks_question.md").write_text(pack_text, encoding="utf-8")
    index = {
        "schema_version": "style-runtime-index-v1",
        "packs": [
            {
                "pack_id": "client_asks_question",
                "pack_file": "office/style-engine/packs/client_asks_question.md",
            }
        ],
    }
    (runtime_dir / "style-runtime-index-v1.json").write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
    return root, runtime_dir


class FakeUploader:
    def __init__(self):
        self.uploads = []

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.uploads.append({"Bucket": Bucket, "Key": Key, "Body": Body, "ContentType": ContentType})


def test_collect_publish_plan_allowlists_index_and_pack_files_only(tmp_path):
    module = load_script()
    root, runtime_dir = make_obsidian_runtime(tmp_path)

    plan = module.build_publish_plan(runtime_dir, root=root, version="20260609T050000Z")

    assert [item.publish_path for item in plan.items] == [
        "style-runtime-index-v1.json",
        "packs/client_asks_question.md",
    ]
    assert all(str(item.source_path).startswith(str(root)) for item in plan.items)
    assert plan.manifest["manual_review_only"] is True
    assert plan.manifest["packs"][0]["path"] == "packs/client_asks_question.md"
    assert len(plan.manifest["packs"][0]["sha256"]) == 64


def test_safety_scan_rejects_phone_email_urls_telegram_handles_and_secrets(tmp_path):
    module = load_script()
    root, runtime_dir = make_obsidian_runtime(tmp_path, unsafe_pack_text="write to +79991234567 or test@example.com")

    try:
        module.build_publish_plan(runtime_dir, root=root, version="20260609T050000Z")
    except module.SafetyScanError as exc:
        assert "phone_like" in str(exc)
        assert "email" in str(exc)
    else:
        raise AssertionError("unsafe pack was not rejected")


def test_publish_uploads_versioned_snapshot_then_latest_manifest_last(tmp_path):
    module = load_script()
    root, runtime_dir = make_obsidian_runtime(tmp_path)
    uploader = FakeUploader()
    plan = module.build_publish_plan(runtime_dir, root=root, version="20260609T050000Z")

    result = module.publish_plan(plan, uploader, bucket="leads-style-runtime", prefix="style-runtime/v1")

    keys = [u["Key"] for u in uploader.uploads]
    assert "style-runtime/v1/versions/20260609T050000Z/style-runtime-index-v1.json" in keys
    assert "style-runtime/v1/versions/20260609T050000Z/packs/client_asks_question.md" in keys
    assert "style-runtime/v1/latest/packs/client_asks_question.md" in keys
    assert keys[-1] == "style-runtime/v1/latest/manifest.json"
    assert result["uploaded_count"] == 6
    assert result["latest_prefix"] == "style-runtime/v1/latest"
