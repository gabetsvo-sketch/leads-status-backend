#!/usr/bin/env python3
"""Readonly HTTP server for sanitized style-runtime snapshot.

Serves only from STYLE_RUNTIME_PUBLIC_DIR (~/.hermes/style-runtime-public/).
No directory listing. Binds to 127.0.0.1 only — exposed publicly via ngrok/Cloudflare Tunnel.

Allowed paths:
  GET  /health
  GET  /v1/latest/manifest.json
  GET  /v1/latest/style-runtime-index-v1.json
  GET  /v1/latest/packs/<pack_id>.md
  POST /v1/draft   — generate draft via OpenAI (uses OPENAI_API_KEY)

Env:
  STYLE_RUNTIME_PUBLIC_DIR   default: ~/.hermes/style-runtime-public
  STYLE_RUNTIME_HTTP_PORT    default: 8901
  STYLE_RUNTIME_READ_TOKEN   optional; if set, X-Style-Token header must match
  OPENAI_API_KEY             required for POST /v1/draft
"""
from __future__ import annotations

import http.server
import json
import logging
import mimetypes
import os
import re
import socketserver
import urllib.error
import urllib.request
from pathlib import Path

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger("style_runtime_server")

PUBLIC_DIR = Path(os.environ.get("STYLE_RUNTIME_PUBLIC_DIR", "~/.hermes/style-runtime-public")).expanduser().resolve()
PORT = int(os.environ.get("STYLE_RUNTIME_HTTP_PORT", "8901"))
READ_TOKEN = os.environ.get("STYLE_RUNTIME_READ_TOKEN", "").strip()
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:14b")

ALLOWED_GET_RE = re.compile(
    r"^(/health|/v1/latest/manifest\.json|/v1/latest/style-runtime-index-v1\.json|/v1/latest/packs/[A-Za-z0-9_.-]+\.md)$"
)
PACK_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# --- Поиск похожих ситуаций (RAG) -----------------------------------------
# numpy импортируем лениво: модуль должен грузиться и без него (тестовое окружение
# бэкенда без numpy), а поиск просто отключается, если numpy недоступен.
_NP = None


def _get_np():
    global _NP
    if _NP is None:
        try:
            import numpy as _n
            _NP = _n
        except Exception:
            _NP = False
    return _NP or None


STYLE_RETRIEVAL_DIR = Path(
    os.environ.get("STYLE_RETRIEVAL_DIR", str(Path.home() / ".local/state/style-retrieval"))
)
EMBED_MODEL = os.environ.get("STYLE_EMBED_MODEL", "bge-m3")
# Порог смысловой близости: ниже — считаем, что похожей ситуации нет (общий стиль).
SIMILAR_MIN_SIM = float(os.environ.get("STYLE_SIMILAR_MIN_SIM", "0.60"))
_RETRIEVAL = {"loaded": False, "vectors": None, "meta": None}
_WORD_RE = re.compile(r"[а-яёa-z0-9]{4,}")


def _retrieval_load() -> bool:
    if _RETRIEVAL["loaded"]:
        return _RETRIEVAL["vectors"] is not None
    _RETRIEVAL["loaded"] = True
    np = _get_np()
    if np is None:
        log.warning("retrieval: numpy недоступен — поиск похожих отключён")
        return False
    try:
        vecs = np.load(STYLE_RETRIEVAL_DIR / "vectors.npy")
        meta = [
            json.loads(l)
            for l in (STYLE_RETRIEVAL_DIR / "meta.jsonl").read_text(encoding="utf-8").splitlines()
            if l.strip()
        ]
        if len(meta) == vecs.shape[0] and len(meta) > 0:
            _RETRIEVAL["vectors"] = vecs.astype(np.float32)
            _RETRIEVAL["meta"] = meta
            log.info("retrieval: loaded %d situation->reply vectors", len(meta))
            return True
        log.warning("retrieval: vectors/meta size mismatch")
    except Exception as e:
        log.warning("retrieval: index load failed: %s", e)
    return False


def _retrieval_embed(text: str):
    body = json.dumps({"model": EMBED_MODEL, "prompt": (text or "")[:1200]}).encode("utf-8")
    req = urllib.request.Request(
        f"{OLLAMA_BASE_URL}/api/embeddings", data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())["embedding"]


def _recency_weight(ts: str) -> float:
    try:
        from datetime import datetime, timezone
        d = datetime.fromisoformat((ts or "").replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - d).days
    except Exception:
        return 0.0
    if age <= 90:
        return 0.06
    if age <= 365:
        return 0.03
    if age <= 730:
        return 0.0
    return -0.03


def retrieval_similar(situation: str, scenario: str = "", k: int = 3) -> list:
    """Топ-k похожих (ситуация->ответ): смысл + сценарий + свежесть + повторы + слова."""
    if not (situation or "").strip() or not _retrieval_load():
        return []
    np = _get_np()
    q = np.asarray(_retrieval_embed(situation), dtype=np.float32)
    q = q / (np.linalg.norm(q) or 1.0)
    vecs = _RETRIEVAL["vectors"]
    meta = _RETRIEVAL["meta"]
    sims = vecs @ q
    q_words = set(_WORD_RE.findall(situation.lower()))
    scen = (scenario or "").lower()
    top_idx = np.argsort(-sims)[:40]
    scored = []
    for i in top_idx:
        i = int(i)
        base = float(sims[i])
        if base < SIMILAR_MIN_SIM:
            continue
        m = meta[i]
        s = base
        if scen and scen in [str(x).lower() for x in (m.get("scenarios") or [])]:
            s += 0.08
        s += _recency_weight(m.get("ts") or "")
        s += min(0.04, 0.01 * (int(m.get("repeats") or 1) - 1))
        r_words = set(_WORD_RE.findall((m.get("situation") or "").lower()))
        if q_words and r_words:
            s += 0.10 * (len(q_words & r_words) / len(q_words))
        scored.append((s, base, m))
    scored.sort(key=lambda x: -x[0])
    out, seen = [], set()
    for s, base, m in scored:
        rep = (m.get("reply") or "").strip()
        if rep[:80] in seen:
            continue
        seen.add(rep[:80])
        out.append({
            "reply": rep,
            "score": round(s, 3),
            "similarity": round(base, 3),
            "scenario": (m.get("scenarios") or [None])[0],
            "date": (m.get("ts") or "")[:10],
            "deal_ref": m.get("deal_id") or "",
        })
        if len(out) >= k:
            break
    return out


class StyleRuntimeHandler(http.server.BaseHTTPRequestHandler):
    server_version = "StyleRuntimeServer/1"
    sys_version = ""

    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, code: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, code: int, data: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _check_token(self) -> bool:
        if not READ_TOKEN:
            return True
        return self.headers.get("X-Style-Token", "") == READ_TOKEN

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]

        if not ALLOWED_GET_RE.match(path):
            self._send_json(404, {"error": "not found"})
            return

        if not self._check_token():
            self._send_json(401, {"error": "unauthorized"})
            return

        if path == "/health":
            self._send_json(200, {"ok": True})
            return

        file_path = PUBLIC_DIR / path.lstrip("/")
        try:
            resolved = file_path.resolve()
            resolved.relative_to(PUBLIC_DIR)
        except (ValueError, OSError):
            self._send_json(404, {"error": "not found"})
            return

        if not resolved.is_file():
            self._send_json(404, {"error": "not found"})
            return

        data = resolved.read_bytes()
        content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        if resolved.suffix == ".md":
            content_type = "text/markdown; charset=utf-8"
        elif resolved.suffix == ".json":
            content_type = "application/json; charset=utf-8"
        self._send_text(200, data, content_type)

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0]

        if path == "/v1/similar":
            if not self._check_token():
                self._send_json(401, {"error": "unauthorized"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            try:
                body = json.loads(self.rfile.read(length)) if length else {}
            except Exception:
                self._send_json(400, {"error": "invalid JSON"})
                return
            try:
                results = retrieval_similar(
                    body.get("situation") or "",
                    body.get("scenario") or "",
                    int(body.get("k") or 3),
                )
                self._send_json(200, {"ok": True, "results": results, "count": len(results)})
            except Exception as e:
                log.warning("similar failed: %s", e)
                self._send_json(200, {"ok": False, "results": [], "error": str(e)[:200]})
            return

        if path != "/v1/draft":
            self._send_json(404, {"error": "not found"})
            return

        if not self._check_token():
            self._send_json(401, {"error": "unauthorized"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        if length > 262_144:
            self._send_json(400, {"error": "payload too large"})
            return
        try:
            body = json.loads(self.rfile.read(length))
        except Exception:
            self._send_json(400, {"error": "invalid JSON"})
            return

        pack_id = body.get("pack_id", "")
        if not PACK_ID_RE.match(pack_id):
            self._send_json(400, {"error": "invalid pack_id"})
            return

        payload = body.get("payload") or {}
        provided_pack_text = body.get("pack_text")
        result = self._generate_draft(pack_id, payload, provided_pack_text=provided_pack_text)
        self._send_json(200, result)

    @staticmethod
    def _extract_style_memory_ids(pack_text: str) -> dict:
        example_ids = []
        guard_ids = []
        in_examples = False
        in_guards = False
        for line in (pack_text or "").splitlines():
            stripped = line.strip()
            upper = stripped.upper()
            if upper.startswith("STYLE MEMORY EXAMPLES"):
                in_examples = True
                in_guards = False
                continue
            if upper.startswith("STYLE MEMORY GUARDS"):
                in_examples = False
                in_guards = True
                continue
            match = re.match(r"^-\s+([A-Za-z0-9_.:-]+)", stripped)
            if not match:
                continue
            if in_examples:
                example_ids.append(match.group(1))
            elif in_guards:
                guard_ids.append(match.group(1))
        return {"style_memory_example_ids": example_ids, "style_memory_guard_ids": guard_ids}

    def _generate_draft(self, pack_id: str, payload: dict, provided_pack_text=None) -> dict:
        pack_text_source = "disk"
        if provided_pack_text is not None:
            if not isinstance(provided_pack_text, str) or not provided_pack_text.strip():
                return {"ok": False, "error": "invalid pack_text"}
            pack_text = provided_pack_text
            pack_text_source = "request"
        else:
            pack_path = PUBLIC_DIR / "v1" / "latest" / "packs" / f"{pack_id}.md"
            if not pack_path.is_file():
                return {"ok": False, "error": f"pack not found: {pack_id}"}

            try:
                pack_text = pack_path.read_text(encoding="utf-8")
            except OSError as e:
                return {"ok": False, "error": str(e)}
        style_memory_debug = self._extract_style_memory_ids(pack_text)

        channel = (payload.get("channel") or "app").lower()
        is_messenger = channel in ("whatsapp", "telegram")
        facts = payload.get("facts_available") or []
        has_price_source = "price_source_ref" in facts
        length_note = "1–3 предложения" if is_messenger else "3–5 предложений"
        price_note = (
            "Конкретные цены, проценты, доходность — НЕ упоминать: нет подтверждённого источника."
            if not has_price_source
            else "Конкретные цифры — только из подтверждённого источника, без выдумок."
        )

        parts = []
        if payload.get("last_client_message_summary"):
            parts.append(f"Последнее сообщение клиента: {payload['last_client_message_summary']}")
        if payload.get("last_vladimir_message_summary"):
            parts.append(f"Последнее сообщение Владимира: {payload['last_vladimir_message_summary']}")
        if payload.get("silence_days"):
            parts.append(f"Клиент молчал {payload['silence_days']} дней.")
        if payload.get("deal_stage"):
            parts.append(f"Стадия: {payload['deal_stage']}.")
        if payload.get("client_situation_hint"):
            parts.append(f"Подсказка: {payload['client_situation_hint']}.")
        # Если клиент ранее сказал «не актуально» — safety gate требует, чтобы
        # черновик явно проверял актуальность. Подсказываем модели заранее,
        # чтобы черновик не был заблокирован постфактум.
        snapshot = payload.get("deal_context_snapshot") or {}
        demand = str(((snapshot.get("client_state") or {}).get("demand_status")) or "").lower()
        if demand in ("not_actual", "uncertain") or payload.get("dialogue_transferred"):
            parts.append(
                "Клиент ранее говорил, что вопрос может быть не актуален. "
                "Обязательно мягко уточни, актуален ли вопрос сейчас, и используй слово «актуально» или «актуален»."
            )

        feedback = str(
            payload.get("vladimir_feedback")
            or payload.get("regen_feedback")
            or payload.get("feedback_text")
            or ""
        ).strip()
        user_content = (
            "\n".join(parts)
            + f"\n\nКанал: {channel}. Длина: {length_note}. {price_note}"
        )
        similar = payload.get("similar_examples") or []
        similar = [str(s).strip() for s in similar if str(s).strip()][:3]
        if similar:
            examples_block = "\n".join(f"- «{s}»" for s in similar)
            user_content += (
                "\n\nПОХОЖИЕ СИТУАЦИИ ИЗ ПРОШЛЫХ ДИАЛОГОВ ВЛАДИМИРА (как он отвечал в близких случаях):\n"
                f"{examples_block}\n"
                "Напиши заново в этом ключе - держись тона, структуры и заходов из этих примеров, "
                "но адаптируй под текущую сделку и её факты. Если контекст не позволяет повторить "
                "ход примера - не копируй его, опирайся на общий стиль. Не выдумывай факты из примеров."
            )
        if feedback:
            user_content += (
                "\n\n!!! ГЛАВНОЕ ТРЕБОВАНИЕ. Владимир прочитал прошлый черновик и просит "
                "переписать его так:\n"
                f"«{feedback}»\n"
                "Выполни эту правку буквально и в первую очередь."
            )
        user_content += "\n\nНапиши черновик ответа Владимира. Только текст, без заголовков и пояснений."
        system_prompt = (
            "Ты помогаешь Владимиру — агенту по недвижимости в Пхукете — писать ответы клиентам. "
            "Используй стиль и паттерны из пака ниже: структуру, тон, типичные CTA. "
            "Не копируй фразы дословно — повторяй структуру и тон. "
            "Пиши только по-русски. Никогда не используй длинное тире (—) и среднее тире (–): "
            "только запятая, точка или короткий дефис. "
            "Пиши только текст черновика ответа, без заголовков и пояснений.\n\n"
            + pack_text
        )

        # Ollama local inference — no API key required
        api_body = json.dumps({
            "model": OLLAMA_MODEL,
            "stream": False,
            "options": {"num_predict": 300},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{OLLAMA_BASE_URL}/api/chat",
            data=api_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        # До двух попыток: локальная модель изредка вставляет CJK-текст —
        # такой черновик бракуем и генерируем заново (regenerate-before-block).
        last_error = "empty draft"
        for attempt in (1, 2):
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read())
                    text = (data.get("message") or {}).get("content", "").strip()
            except Exception as e:
                log.warning("style_draft: Ollama error (attempt %d): %s", attempt, e)
                last_error = str(e)[:300]
                continue
            if any("一" <= ch <= "鿿" for ch in text):
                log.warning("style_draft: CJK glitch on attempt %d, retrying", attempt)
                last_error = "CJK glitch in draft"
                continue
            # Жёсткое правило стиля Владимира: длинное/среднее тире — признак AI-текста.
            text = text.replace(" — ", ", ").replace(" – ", ", ").replace("—", "-").replace("–", "-")
            if not text:
                continue
            log.info("style_draft: generated %d chars for pack %s via Ollama/%s (attempt %d)", len(text), pack_id, OLLAMA_MODEL, attempt)
            return {
                "ok": True,
                "draft_text": text,
                "pack_text_source": pack_text_source,
                **style_memory_debug,
            }
        return {"ok": False, "error": last_error}


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def main() -> None:
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    server = ThreadedHTTPServer(("127.0.0.1", PORT), StyleRuntimeHandler)
    log.info("style-runtime server listening on 127.0.0.1:%d, serving %s", PORT, PUBLIC_DIR)
    if READ_TOKEN:
        log.info("token auth: enabled (X-Style-Token required)")
    else:
        log.info("token auth: disabled")
    log.info("draft generation: Ollama/%s at %s (no API key required)", OLLAMA_MODEL, OLLAMA_BASE_URL)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("shutting down")


if __name__ == "__main__":
    main()
