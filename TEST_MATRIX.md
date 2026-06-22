# TEST_MATRIX — leads-status/backend

Ключевые сценарии и проверки по компоненту (§23 стандарта VIBE-CODE v0.01.001).
Всего **114 тестов** (Python 3.11.9 как в Render). Прогон: `pytest tests/`. В CI — блокирующий шаг `verify` (см. `RELEASE.md`).

## Слои проверки (§23.1)
| Слой | Где |
|---|---|
| Статический | `tools/ci_version_guard.py` (страж версии §12.4); валидация YAML workflow |
| Модульный / интеграционный | `pytest tests/` (FastAPI TestClient, изоляция состояния в tmp_path) |
| Сценарный / регрессионный | инварианты ниже (incidents-driven) |
| Сборочный | CI собирает на 3.11.9, ставит `requirements.txt` |
| Операционный | `/health`, `/version` после деплоя; Sentry (opt-in) |

## Карта тестов
| Файл | Тестов | Покрывает |
|---|---:|---|
| `test_endpoints.py` | 43 | health (+version), auth/токены, heartbeat↔scheduler, silent_ack, регистрация устройств, office-drafts |
| `test_style_runtime.py` | 18 | движок стиля: роутер→пак→safety-gate, имя клиента, фидбэк, блокировки |
| `test_name_placeholder.py` | 16 | барьер против заглушки обращения «Имя»; санитайзер исходящего черновика |
| `test_read_enrichment.py` | 9 | enrichment на read-пути `/api/tasks/today` (канал, контакт, нормализация полей) |
| `test_change_status.py` | 6 | очередь `crm_action` (complete/reschedule/change_status), причина отказа |
| `test_style_router.py` | 5 | выбор scenario-пака по запросу |
| `test_style_runtime_http.py` | 5 | загрузка пака по HTTP: успех, last-good фолбэк, отклонение (хэш/manual_review/missing) |
| `test_style_runtime_r2.py` | 5 | то же для R2/S3-хранилища |
| `test_memos.py` | 4 | памятки по проектам (push/get) |
| `test_sync_style_runtime_to_r2.py` | 3 | публикация пакетов стиля в R2 |

## Ключевые инварианты (по реальным инцидентам — не ослаблять)
- **`/health` без auth → 200** и содержит `version` (uptime-проверка Render).
- **`/status` требует bearer-токен** (401 без него).
- **`silent_ack` НЕ ставит `pending_status_change`** — иначе Salesbot создаёт лишнюю автозадачу (инцидент Mary Land 2026-05-02).
- **Регистрация устройства идемпотентна** (APNs-токен).
- **Заглушка «Имя» не уходит клиенту** — санитайзер + флаг гейта (`test_name_placeholder`).
- **Полный пуш не стирает готовый черновик** (`SOFT_PRESERVE_IF_EMPTY`).
- **Канал отправки = переписка в CRM, а не доступность ссылки** (`test_read_enrichment`).
- **Движок стиля:** черновик `manual_review_only`; отвергнутый удалённый снапшот (плохой хэш/не-review-only) не подменяет доверенный пак тампером.

## Базовая линия до изменения (§23.2)
Перед серьёзной правкой зафиксировать: версию (`/version`), commit, что зелёно сейчас (`pytest tests/`), известные дефекты.

## Известные дефекты / открытое
- Нет статического линтера/typecheck в CI (только страж версии + pytest). Кандидат на добавление.
- Полноценный E2E с реальным amoCRM/APNs не автоматизирован (нужны живые сессии) — проверяется вручную после деплоя.
