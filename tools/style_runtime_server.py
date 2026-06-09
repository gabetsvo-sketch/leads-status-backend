#!/usr/bin/env python3
"""Readonly HTTP server for sanitized style-runtime snapshot.

Serves only from STYLE_RUNTIME_PUBLIC_DIR (~/.hermes/style-runtime-public/).
No directory listing. Binds to 127.0.0.1 only — exposed publicly via ngrok/Cloudflare Tunnel.

Allowed paths:
  GET /health
  GET /v1/latest/manifest.json
  GET /v1/latest/style-runtime-index-v1.json
  GET /v1/latest/packs/<pack_id>.md

Env:
  STYLE_RUNTIME_PUBLIC_DIR   default: ~/.hermes/style-runtime-public
  STYLE_RUNTIME_HTTP_PORT    default: 8901
  STYLE_RUNTIME_READ_TOKEN   optional; if set, X-Style-Token header must match
"""
from __future__ import annotations

import http.server
import json
import logging
import mimetypes
import os
import re
import socketserver
from pathlib import Path

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger("style_runtime_server")

PUBLIC_DIR = Path(os.environ.get("STYLE_RUNTIME_PUBLIC_DIR", "~/.hermes/style-runtime-public")).expanduser().resolve()
PORT = int(os.environ.get("STYLE_RUNTIME_HTTP_PORT", "8901"))
READ_TOKEN = os.environ.get("STYLE_RUNTIME_READ_TOKEN", "").strip()

ALLOWED_PATH_RE = re.compile(
    r"^(/health|/v1/latest/manifest\.json|/v1/latest/style-runtime-index-v1\.json|/v1/latest/packs/[A-Za-z0-9_.-]+\.md)$"
)


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

        if not ALLOWED_PATH_RE.match(path):
            self._send_json(404, {"error": "not found"})
            return

        if not self._check_token():
            self._send_json(401, {"error": "unauthorized"})
            return

        if path == "/health":
            self._send_json(200, {"ok": True})
            return

        file_path = PUBLIC_DIR / path.lstrip("/")
        # Prevent path traversal: resolved path must stay within PUBLIC_DIR
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
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("shutting down")


if __name__ == "__main__":
    main()
