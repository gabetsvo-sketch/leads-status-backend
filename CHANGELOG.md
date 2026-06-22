# CHANGELOG — leads-status-backend

Карта компонента — в `../CLAUDE.md` (общая, для backend + iOS). Здесь — версии backend.

## v0.01.001 — 2026-06-22 (защита выкладки в CI)

Закрыт пробел №1 аудита (`VERSION_AUDIT.md`): деплой шёл push'ом в `main` без проверок. Теперь на каждый push сначала проверка, деплой — только если прошла (§12.4 стандарта VIBE-CODE v0.01.001).

**Что в этом changeset:**
- `.github/workflows/ci.yml` — единый workflow: job `verify` (страж версии + стабильные тесты) → job `deploy` (Render, `needs: verify`). Аварийный деплой — ручной запуск с `force_deploy=true` или кнопка в Render.
- `tools/ci_version_guard.py` — страж §12.4: если в пуше изменён код продукта (корневые `*.py` / `requirements.txt`), `VERSION.json` обязан быть повышен, иначе сборка падает. `tests/`, `tools/`, `*.md`, `.github/` повышения не требуют.
- Прежний `.github/workflows/deploy-render.yml` удалён — деплой переехал под защиту в `ci.yml` (раньше деплоил на любой push без проверок).
- Починен устаревший тест `test_health_no_auth`: с базовой фиксации `/health` отдаёт `version` (тест ждал старый `{ok:true}`).

**Что НЕ менялось:** поведение и endpoints сервиса (`main.py`) не тронуты — изменения только в CI/тестах/версии. Деплой v0.01.001 обновит лишь номер версии в `/version` и `/health`.

**Честно про тесты:** стабильный набор зелёный на Python 3.11 (104 passed). Наборы `tests/test_style_runtime_http.py` и `tests/test_style_runtime_r2.py` (10 тестов) **сейчас красные** — они подменяют env-переменные после импорта `main.py`, который читает конфиг при загрузке (дрейф тестов от кода, не прод-баг). Вынесены в неблокирующий шаг CI и в пробелы `VERSION_AUDIT.md` — отдельная задача, не маскируются.

## v0.01.000 — 2026-06-20 (baseline компонента)

Первая фиксация продуктовой версии существующего боевого backend (FastAPI на Render). До этого backend жил без `VERSION.json` — нельзя было сверить «какая версия работает в Render и с каким iOS-сборкой она согласована». По §11.4 стандарта VIBE-CODE v0.01.001 — это нарушение «один факт — одно нормативное место».

**Что в этом changeset (внедрение стандарта):**
- `VERSION.json {generation:0, milestone:1, revision:0}` → display `v0.01.000`, semver `0.1.0` (без ведущих нулей, §11.4).
- `GET /version` — отдаёт VERSION.json + поле `component: "leads-status-backend"` (для будущей связки с компонентами iOS / scheduler-workers, §11.7).
- `GET /health` теперь возвращает `version` рядом с `ok`.
- `app.version = BACKEND_VERSION["semver"]` — версия попадает в OpenAPI / docs.

**Что НЕ менялось:**
- Поведение всех 60+ endpoints без изменений.
- Запись в CRM, отправка APNs-пушей, работа Telethon timer_loop — без изменений.
- Контракт с Mac-воркерами (regen / crm / priority / newleads / tasks / enrich) — без изменений.

**Что осталось открытым (на следующий changeset):**
- Связка версии backend ↔ версии iOS-сборки в общий release manifest (`leads-status/RELEASE.md`).
- CI-проверка «код изменён → VERSION.json не повышен» в `.github/workflows/backend-tests.yml`.
- Скрипт `tools/bump_version.py` по образцу `realdream-bot-v2`.

См. также: `VERSION_AUDIT.md` (read-only аудит), общая карта продукта — `../CLAUDE.md`.
