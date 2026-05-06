"""Этап 2.4: pytest setup для backend.

main.py читает env vars на module-load (TG_API_ID, TG_API_HASH, TG_CHAT_ID,
WIDGET_TOKEN, INTERNAL_TOKEN). Без них — крах. Поэтому conftest подкладывает
тестовые значения ДО первого импорта main.

Файлы состояния (leads/tasks/devices/heartbeat) перенаправляются в
tmp_path — изоляция от prod данных и параллельных тестов.

TestClient используется без `with` — это НЕ триггерит lifespan
(который пытается подключить Telegram клиента). Тесты гоняют только
endpoints, не трогающие глобальный `client`.
"""
import os
import sys
from pathlib import Path

import pytest


# 1. Env vars ДО импорта main (модуль читает их at import-time).
TEST_WIDGET_TOKEN = "test-widget-token"
TEST_INTERNAL_TOKEN = "test-internal-token"


def _set_test_env(tmp_path: Path) -> None:
    os.environ["TG_API_ID"] = "1"
    os.environ["TG_API_HASH"] = "test"
    os.environ["TG_CHAT_ID"] = "1"
    os.environ["WIDGET_TOKEN"] = TEST_WIDGET_TOKEN
    os.environ["INTERNAL_TOKEN"] = TEST_INTERNAL_TOKEN
    os.environ["SESSION_NAME"] = "test_session"
    # Все state-files изолируем в tmp_path — тесты не должны трогать prod.
    os.environ["STATE_FILE"] = str(tmp_path / "state.json")
    os.environ["LEADS_FILE"] = str(tmp_path / "leads.json")
    os.environ["TASKS_FILE"] = str(tmp_path / "tasks.json")
    os.environ["DEVICES_FILE"] = str(tmp_path / "devices.json")
    os.environ["INSTRUCTIONS_FILE"] = str(tmp_path / "instructions.json")
    os.environ["NEWS_FILE"] = str(tmp_path / "news.json")
    os.environ["ANTHROPIC_HEALTH_FILE"] = str(tmp_path / "anthropic_health.json")
    os.environ["SCHEDULER_HEARTBEAT_FILE"] = str(tmp_path / "heartbeat.json")
    os.environ["REFRESH_REQUEST_FILE"] = str(tmp_path / "refresh_request.json")
    os.environ["FEEDBACK_FILE"] = str(tmp_path / "feedback.jsonl")
    os.environ["OFFICE_DRAFTS_FILE"] = str(tmp_path / "office_drafts.json")


@pytest.fixture
def app_client(tmp_path):
    """Свежий клиент с изолированным state per-test. Возвращает (client, main_module)
    чтобы тесты могли читать константы из main без повторного import."""
    _set_test_env(tmp_path)
    # backend/ должен быть на sys.path — conftest сам в backend/tests/.
    backend_dir = Path(__file__).resolve().parent.parent
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    # Перезагружаем main для каждого теста — иначе глобальные пути закешируются.
    if "main" in sys.modules:
        del sys.modules["main"]
    import main  # noqa: E402

    from fastapi.testclient import TestClient
    client = TestClient(main.app)
    return client, main


@pytest.fixture
def widget_headers():
    return {"Authorization": f"Bearer {TEST_WIDGET_TOKEN}"}


@pytest.fixture
def internal_headers():
    return {"Authorization": f"Bearer {TEST_INTERNAL_TOKEN}"}


@pytest.fixture
def office_headers():
    # OFFICE_TOKEN falls back to INTERNAL_TOKEN when not set separately
    return {"Authorization": f"Bearer {TEST_INTERNAL_TOKEN}"}
