# VERSION_AUDIT — leads-status/backend

Аудит по §12.5 стандарта VIBE-CODE v0.01.001. Обновлён 2026-06-22 (read-only сверка с реальным состоянием).

## Текущий статус (после базовой фиксации 2026-06-20)

| Поле | Значение |
|---|---|
| Проект | leads-status/backend (FastAPI-бэкенд приложения LeadsStatus, деплой на Render) |
| Режим | **БОЕВОЙ ПРОДУКТ** (обслуживает iOS-приложение, подключён к CRM, реальные данные клиентов) |
| Репозиторий/ветка | `gabetsvo-sketch/leads-status-backend` · `main`, last commit `4a38571` (2026-06-20 13:54, baseline) · upstream синхронен · working tree чистый |
| Текущая версия | **PASS** — `VERSION.json {0,1,0}` → display `v0.01.000`, semver `0.1.0` |
| Источник версии | **PASS** — `VERSION.json`, читается `_read_version()` в `main.py` |
| Версия видна (эквивалент фронтенда, §12.2) | **PASS** — `GET /version` (отдаёт VERSION.json + `component`) и `GET /health` (с `version`); `app.version` в OpenAPI/docs. У backend нет визуального фронтенда — endpoint удовлетворяет §12.2. |
| Версии компонентов | **PARTIAL** — `/version` отдаёт `component`, но нет общего release manifest, связывающего версии backend ↔ iOS-сборки ↔ Mac-воркеры (§11.7) |
| CI требует bump | **FAIL** — в репозитории только `deploy-render.yml` (триггер деплоя через Render API). Нет прогона `pytest` и нет проверки «код изменён → VERSION.json повышен» (§12.4) |
| Git local | **GIT-3** (commit + push, синхронизация с remote подтверждена) |
| Теги и артефакты | **FAIL** — release tag не используется; Render тянет HEAD ветки `main`, точный commit в проде по тегу не зафиксирован |
| Источники истины | **PARTIAL** — есть `CHANGELOG.md`, `VERSION_AUDIT.md` и общая карта продукта `../CLAUDE.md` (архитектура + карта endpoints + грабли). Нет отдельных `PROJECT.md`, `ARCHITECTURE.md`, `AGENTS.md`, `CURRENT_STATE.md`, `RISK_MAP.md`, `TEST_MATRIX.md`, `RELEASE.md` |

**Статус версионирования: PARTIAL** — базовая фиксация выполнена (версия есть, видна, источник истины единый; Git честный GIT-3). Остаются 4 пробела ниже.

## Что закрыла базовая фиксация (commit `4a38571`, 2026-06-20)
- Заведён `VERSION.json {0,1,0}` → display `v0.01.000`, semver `0.1.0` (без ведущих нулей, §11.4).
- `GET /version` + `version` в `/health`; `app.version` в OpenAPI.
- Заведён `CHANGELOG.md` (карта версий компонента).

## Что осталось (критические пробелы)
1. **CI не защищает версию.** Деплой = push в `main` → Render, без прогона `pytest` и без блокировки «код изменён, а `VERSION.json` не повышен» (§12.4). Высокий приоритет для боевого продукта.
2. **Нет release manifest backend ↔ iOS ↔ воркеры** (§11.7) — нельзя сверить, какая сборка iOS согласована с какой версией backend и с какими Mac-воркерами (regen/crm/priority/newleads/tasks/enrich).
3. **Нет `RISK_MAP.md`, `TEST_MATRIX.md`, `RELEASE.md`** — не зафиксированы письменно: красные зоны (запись в CRM через `crm_actions`, APNs-пуши, токены `WIDGET/INTERNAL/OFFICE`), ключевые регресс-сценарии после деплоя, порядок отката деплоя на Render.
4. **Release tag не используется** — Render тянет HEAD `main`; нет однозначной связки «версия ↔ commit ↔ артефакт в проде» (§11.5).

## Следующий безопасный changeset (предложение — требует решения Владимира)
- **CI-проверка в backend-репо**: на push с изменением кода — `pytest tests/` + проверка «`VERSION.json` повышен относительно `main`, если изменён код» (§12.4). Не меняет поведение продукта, только защищает.
- **Минимальные источники истины**: `RELEASE.md` (порядок деплоя/отката Render), `TEST_MATRIX.md` (ключевые регресс-сценарии: `/health`, авторизация, `/api/tasks/today`, `/style-runtime/v1/draft`, тест-цепочка), `RISK_MAP.md` (красные зоны backend).
- **Release manifest** `leads-status/RELEASE.md` — связка версий backend ↔ iOS ↔ воркеры (§11.7).
- **Переход на release tag** (`v0.01.000`) и указание Render конкретного commit/tag вместо HEAD — **меняет процесс деплоя**, отдельное решение.

> Замечание по версии: следующее сохранённое изменение кода backend ОБЯЗАНО поднять версию (v0.01.000 → v0.01.001), §11.2.
