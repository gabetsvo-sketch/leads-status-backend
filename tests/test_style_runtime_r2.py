"""R2-backed Style Runtime storage tests.

These tests cover production-read storage safety: S3/R2 snapshots are accepted only
when manifest/index/pack hashes match and manifest stays manual-review-only; R2
outages fall back to last known good in-memory snapshot.
"""
import hashlib
import json

BASE_REQUEST = {
    "request_id": "req-style-001",
    "deal_ref": "deal_hash_abc",
    "channel": "whatsapp",
    "client_situation_hint": "price_question",
    "last_client_message_summary": "Клиент спрашивает актуальна ли цена выбранного объекта.",
    "last_vladimir_message_summary": "Владимир ранее отправил подборку объектов.",
    "silence_days": None,
    "deal_stage": "selection",
    "client_last_message_type": "question",
    "facts_available": ["object_ref"],
    "requested_output": "client_reply_draft",
}


class FakeR2Client:
    def __init__(self, objects=None, fail=False):
        self.objects = objects or {}
        self.fail = fail
        self.calls = []

    def get_object(self, Bucket, Key):
        self.calls.append((Bucket, Key))
        if self.fail:
            raise RuntimeError("r2 unavailable")
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": FakeBody(self.objects[Key])}


class FakeBody:
    def __init__(self, text):
        self.text = text

    def read(self):
        return self.text.encode("utf-8")


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def make_snapshot(pack_text="PACK FROM R2", *, manual_review_only=True, bad_pack_hash=False, include_pack=True):
    index = {
        "schema_version": "style-runtime-index-v1",
        "packs": [
            {
                "pack_id": "client_asks_question",
                "pack_file": "packs/client_asks_question.md",
            }
        ],
    }
    index_text = json.dumps(index, ensure_ascii=False, sort_keys=True)
    pack_hash = "0" * 64 if bad_pack_hash else sha(pack_text)
    packs = []
    if include_pack:
        packs.append({"pack_id": "client_asks_question", "path": "packs/client_asks_question.md", "sha256": pack_hash})
    manifest = {
        "schema_version": "style-runtime-publish-manifest-v1",
        "published_at": "2026-06-09T05:00:00+00:00",
        "runtime_index": {"path": "style-runtime-index-v1.json", "sha256": sha(index_text)},
        "packs": packs,
        "manual_review_only": manual_review_only,
        "stable_pack_promotion": "vladimir_manual_approval_required",
    }
    objects = {
        "style-runtime/v1/latest/manifest.json": json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        "style-runtime/v1/latest/style-runtime-index-v1.json": index_text,
    }
    if include_pack:
        objects["style-runtime/v1/latest/packs/client_asks_question.md"] = pack_text
    return objects


def configure_r2(main, monkeypatch, client):
    monkeypatch.setattr(main, "STYLE_RUNTIME_SOURCE", "r2")
    monkeypatch.setattr(main, "STYLE_RUNTIME_R2_ENDPOINT", "https://r2.example.invalid")
    monkeypatch.setattr(main, "STYLE_RUNTIME_R2_BUCKET", "leads-style-runtime")
    monkeypatch.setattr(main, "STYLE_RUNTIME_R2_PREFIX", "style-runtime/v1/latest")
    monkeypatch.setattr(main, "STYLE_RUNTIME_CACHE_TTL_SECONDS", 600)
    monkeypatch.setattr(main, "STYLE_RUNTIME_R2_ACCESS_KEY_ID", "test-access")
    monkeypatch.setattr(main, "STYLE_RUNTIME_R2_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setattr(main, "_STYLE_RUNTIME_R2_CACHE", None)
    monkeypatch.setattr(main, "_STYLE_RUNTIME_R2_LAST_GOOD", None)
    monkeypatch.setattr(main, "_style_runtime_create_r2_client", lambda: client)


def safe_payload():
    return {
        **BASE_REQUEST,
        "request_id": "req-r2-success",
        "client_situation_hint": "followup",
        "last_client_message_summary": "Клиент просит напомнить следующий шаг по подборке.",
        "deal_stage": "question",
        "client_last_message_type": "question",
        "facts_available": ["object_ref", "price_source_ref"],
    }


def test_style_runtime_r2_success_loads_pack_from_hash_valid_snapshot(app_client, office_headers, monkeypatch):
    client, main = app_client
    r2 = FakeR2Client(make_snapshot("R2 pack content for client question"))
    configure_r2(main, monkeypatch, r2)

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=safe_payload())

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["manual_review_only"] is True
    assert body["runtime_source"] == "r2"
    assert body["runtime_pack_loaded"] is True
    assert body["runtime_pack_path"] == "packs/client_asks_question.md"
    assert body["runtime_snapshot_version"] == "2026-06-09T05:00:00+00:00"
    assert body["runtime_block_reason"] is None


def test_style_runtime_r2_unavailable_uses_last_good_snapshot(app_client, office_headers, monkeypatch):
    client, main = app_client
    r2_good = FakeR2Client(make_snapshot("initial good pack"))
    configure_r2(main, monkeypatch, r2_good)
    first = client.post("/style-runtime/v1/draft", headers=office_headers, json=safe_payload())
    assert first.status_code == 200, first.text
    assert first.json()["runtime_pack_loaded"] is True

    r2_down = FakeR2Client(fail=True)
    monkeypatch.setattr(main, "_STYLE_RUNTIME_R2_CACHE", None)
    monkeypatch.setattr(main, "_style_runtime_create_r2_client", lambda: r2_down)

    second = client.post("/style-runtime/v1/draft", headers=office_headers, json=safe_payload())

    assert second.status_code == 200, second.text
    body = second.json()
    assert body["runtime_source"] == "r2_last_good"
    assert body["runtime_pack_loaded"] is True
    assert body["safety_pass"] is True


def test_style_runtime_r2_bad_hash_rejects_snapshot_without_last_good(app_client, office_headers, monkeypatch):
    client, main = app_client
    r2 = FakeR2Client(make_snapshot("tampered pack", bad_pack_hash=True))
    configure_r2(main, monkeypatch, r2)

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=safe_payload())

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["manual_review_only"] is True
    assert body["runtime_pack_loaded"] is False
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "hash" in body["runtime_block_reason"].lower()
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_r2_missing_pack_falls_back_to_guarded_empty_draft(app_client, office_headers, monkeypatch):
    client, main = app_client
    r2 = FakeR2Client(make_snapshot(include_pack=False))
    configure_r2(main, monkeypatch, r2)

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=safe_payload())

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["runtime_pack_loaded"] is False
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "missing pack" in body["runtime_block_reason"].lower()


def test_style_runtime_r2_manifest_without_manual_review_only_is_rejected(app_client, office_headers, monkeypatch):
    client, main = app_client
    r2 = FakeR2Client(make_snapshot(manual_review_only=False))
    configure_r2(main, monkeypatch, r2)

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=safe_payload())

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["runtime_pack_loaded"] is False
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "manual_review_only" in body["runtime_block_reason"]
