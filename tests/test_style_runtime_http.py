"""HTTP-backed Style Runtime storage tests.

Covers: HTTP success, Mac unavailable → last-good fallback, bad sha256 rejected,
manual_review_only missing rejected, missing pack guarded empty draft.
"""
import hashlib
import json
from unittest.mock import MagicMock, patch

BASE_REQUEST = {
    "request_id": "req-http-001",
    "deal_ref": "deal_hash_abc",
    "channel": "whatsapp",
    "client_situation_hint": "followup",
    "last_client_message_summary": "Клиент просит напомнить следующий шаг по подборке.",
    "last_vladimir_message_summary": "Владимир ранее отправил подборку объектов.",
    "silence_days": None,
    "deal_stage": "question",
    "client_last_message_type": "question",
    "facts_available": ["object_ref", "price_source_ref"],
    "requested_output": "client_reply_draft",
}


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def make_snapshot(pack_text="PACK FROM HTTP", *, pack_id="client_asks_question",
                  manual_review_only=True, bad_pack_hash=False, include_pack=True):
    # pack_id берём тот, что реально выбирает роутер для запроса (роутер мог
    # поменяться) — иначе _style_manifest_pack не найдёт пак и снапшот отвергнется.
    pack_path = f"packs/{pack_id}.md"
    index = {
        "schema_version": "style-runtime-index-v1",
        "packs": [{"pack_id": pack_id, "pack_file": pack_path}],
    }
    index_text = json.dumps(index, ensure_ascii=False, sort_keys=True)
    pack_hash = "0" * 64 if bad_pack_hash else sha(pack_text)
    packs = []
    if include_pack:
        packs.append({"pack_id": pack_id, "path": pack_path, "sha256": pack_hash})
    manifest = {
        "schema_version": "style-runtime-publish-manifest-v1",
        "published_at": "2026-06-09T08:00:00+00:00",
        "runtime_index": {"path": "style-runtime-index-v1.json", "sha256": sha(index_text)},
        "packs": packs,
        "manual_review_only": manual_review_only,
        "stable_pack_promotion": "vladimir_manual_approval_required",
    }
    return {
        "manifest": json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        "index": index_text,
        "pack": pack_text if include_pack else None,
    }


def make_urlopen_mock(snapshot, fail=False):
    """Returns a context manager mock for urllib.request.urlopen."""

    def urlopen_side_effect(request, timeout=10):
        if fail:
            raise OSError("connection refused")
        url = request.full_url if hasattr(request, "full_url") else str(request)
        if "manifest.json" in url:
            body = snapshot["manifest"]
        elif "style-runtime-index-v1.json" in url:
            body = snapshot["index"]
        elif "packs/" in url:
            if snapshot["pack"] is None:
                raise OSError("404 not found")
            body = snapshot["pack"]
        else:
            raise OSError("unexpected url")
        cm = MagicMock()
        cm.__enter__ = lambda s: cm
        cm.__exit__ = MagicMock(return_value=False)
        cm.read = MagicMock(return_value=body.encode("utf-8"))
        return cm

    return urlopen_side_effect


def configure_http(main, monkeypatch):
    monkeypatch.setattr(main, "STYLE_RUNTIME_SOURCE", "http")
    monkeypatch.setattr(main, "STYLE_RUNTIME_HTTP_BASE_URL", "https://test.ngrok-free.dev")
    monkeypatch.setattr(main, "STYLE_RUNTIME_HTTP_TOKEN", "")
    monkeypatch.setattr(main, "STYLE_RUNTIME_CACHE_TTL_SECONDS", 600)
    monkeypatch.setattr(main, "_STYLE_RUNTIME_HTTP_CACHE", None)
    monkeypatch.setattr(main, "_STYLE_RUNTIME_HTTP_LAST_GOOD", None)


def test_style_runtime_http_success_loads_pack(app_client, office_headers, monkeypatch):
    client, main = app_client
    configure_http(main, monkeypatch)
    pack_id = main._style_choose_pack(BASE_REQUEST)[0]
    snapshot = make_snapshot("HTTP pack content for followup", pack_id=pack_id)

    with patch("urllib.request.urlopen", side_effect=make_urlopen_mock(snapshot)):
        r = client.post("/style-runtime/v1/draft", headers=office_headers, json=BASE_REQUEST)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["manual_review_only"] is True
    assert body["runtime_source"] == "http"
    assert body["runtime_pack_loaded"] is True
    assert body["runtime_pack_path"] == f"packs/{pack_id}.md"
    assert body["runtime_snapshot_version"] == "2026-06-09T08:00:00+00:00"
    assert body["runtime_block_reason"] is None
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_http_unavailable_uses_last_good(app_client, office_headers, monkeypatch):
    client, main = app_client
    configure_http(main, monkeypatch)
    pack_id = main._style_choose_pack(BASE_REQUEST)[0]
    snapshot = make_snapshot("good pack content", pack_id=pack_id)

    with patch("urllib.request.urlopen", side_effect=make_urlopen_mock(snapshot)):
        first = client.post("/style-runtime/v1/draft", headers=office_headers, json=BASE_REQUEST)
    assert first.status_code == 200
    assert first.json()["runtime_pack_loaded"] is True

    monkeypatch.setattr(main, "_STYLE_RUNTIME_HTTP_CACHE", None)

    with patch("urllib.request.urlopen", side_effect=make_urlopen_mock(snapshot, fail=True)):
        second = client.post("/style-runtime/v1/draft", headers=office_headers, json=BASE_REQUEST)

    assert second.status_code == 200, second.text
    body = second.json()
    assert body["runtime_source"] == "http_last_good"
    assert body["runtime_pack_loaded"] is True
    assert body["safety_pass"] is True


def test_style_runtime_http_bad_sha256_rejects_snapshot(app_client, office_headers, monkeypatch):
    client, main = app_client
    configure_http(main, monkeypatch)
    # Изолируем проверку ОТКЛОНЕНИЯ от availability-фолбэка на вшитый пак:
    # без этого отвергнутый снапшот подменялся бы доверенным локальным паком.
    monkeypatch.setattr(main, "_style_load_local_pack_text", lambda pid: ("", None))
    pack_id = main._style_choose_pack(BASE_REQUEST)[0]
    snapshot = make_snapshot("tampered pack", pack_id=pack_id, bad_pack_hash=True)

    with patch("urllib.request.urlopen", side_effect=make_urlopen_mock(snapshot)):
        r = client.post("/style-runtime/v1/draft", headers=office_headers, json=BASE_REQUEST)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["manual_review_only"] is True
    assert body["runtime_pack_loaded"] is False
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "hash" in body["runtime_block_reason"].lower()
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_http_missing_manual_review_only_rejected(app_client, office_headers, monkeypatch):
    client, main = app_client
    configure_http(main, monkeypatch)
    monkeypatch.setattr(main, "_style_load_local_pack_text", lambda pid: ("", None))
    pack_id = main._style_choose_pack(BASE_REQUEST)[0]
    snapshot = make_snapshot(pack_id=pack_id, manual_review_only=False)

    with patch("urllib.request.urlopen", side_effect=make_urlopen_mock(snapshot)):
        r = client.post("/style-runtime/v1/draft", headers=office_headers, json=BASE_REQUEST)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["runtime_pack_loaded"] is False
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "manual_review_only" in body["runtime_block_reason"]


def test_style_runtime_http_missing_pack_guarded_empty_draft(app_client, office_headers, monkeypatch):
    client, main = app_client
    configure_http(main, monkeypatch)
    monkeypatch.setattr(main, "_style_load_local_pack_text", lambda pid: ("", None))
    pack_id = main._style_choose_pack(BASE_REQUEST)[0]
    snapshot = make_snapshot(pack_id=pack_id, include_pack=False)

    with patch("urllib.request.urlopen", side_effect=make_urlopen_mock(snapshot)):
        r = client.post("/style-runtime/v1/draft", headers=office_headers, json=BASE_REQUEST)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["runtime_pack_loaded"] is False
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "missing pack" in body["runtime_block_reason"].lower()
