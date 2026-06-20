# CHANGELOG — leads-status-backend

Карта компонента — в `../CLAUDE.md` (общая, для backend + iOS). Здесь — версии backend.

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
