# VERSION_AUDIT — leads-status/backend

Аудит по §12.5 стандарта VIBE-CODE v0.01.001. Обновлён 2026-06-22.

## Текущий статус

| Поле | Значение |
|---|---|
| Проект | leads-status/backend (FastAPI-бэкенд приложения LeadsStatus, деплой на Render) |
| Режим | **БОЕВОЙ ПРОДУКТ** (обслуживает iOS-приложение, подключён к CRM, реальные данные клиентов) |
| Репозиторий/ветка | `gabetsvo-sketch/leads-status-backend` · `main` · upstream синхронен · working tree чистый |
| Текущая версия | **PASS** — `VERSION.json {0,1,1}` → display `v0.01.001`, semver `0.1.1` |
| Источник версии | **PASS** — `VERSION.json`, читается `_read_version()` в `main.py` |
| Версия видна (эквивалент фронтенда, §12.2) | **PASS** — `GET /version` + `GET /health` (с `version`); `app.version` в OpenAPI. У backend нет визуального фронтенда — endpoint удовлетворяет §12.2 |
| Версии компонентов | **PASS** — манифест в `RELEASE.md` (backend ↔ iOS ↔ Mac-воркеры, §11.7). Машинной авто-версии у iOS/воркеров пока нет — сверка ручная по таблице (отмечено как открытое) |
| CI требует bump | **PASS** — `ci.yml`: `verify` (страж `tools/ci_version_guard.py` §12.4 + полный набор тестов) → `deploy` только если verify прошла И изменён код продукта. Аварийный обход — `force_deploy=true` / кнопка Render |
| Git local | **GIT-3** (commit + push, синхронизация с remote подтверждена) |
| Теги и артефакты | **FAIL** — release tag не используется; Render тянет HEAD ветки `main` (единственный оставшийся пробел) |
| Источники истины | **PASS** (для боевой выкладки) — `VERSION.json`, `CHANGELOG.md`, `VERSION_AUDIT.md`, `RISK_MAP.md`, `TEST_MATRIX.md`, `RELEASE.md` + общая карта `../CLAUDE.md`. Нет отдельных `PROJECT.md`/`ARCHITECTURE.md`/`AGENTS.md`/`CURRENT_STATE.md` — их роль закрывает `../CLAUDE.md` (карта продукта backend+iOS) |

**Статус версионирования: PARTIAL** — остался 1 пробел (release tag). Всё критичное для безопасной выкладки сделано: версия есть/видна, CI блокирует выкладку без bump/тестов, красные зоны и порядок отката зафиксированы письменно, Git честный GIT-3.

## Что сделано
- **Базовая фиксация** (commit `4a38571`, 20.06): `VERSION.json`, `GET /version`+`/health`, `CHANGELOG.md`.
- **Защита выкладки** (`v0.01.001`, 22.06): `ci.yml` (verify→deploy), `tools/ci_version_guard.py` (страж §12.4), деплой только при изменении кода продукта; починен `test_health_no_auth`.
- **Тесты + источники истины** (22.06): починены 10 тестов `style-runtime http/r2` (теперь весь набор зелёный, 114, в блокирующем гейте); заведены `RISK_MAP.md`, `TEST_MATRIX.md`, `RELEASE.md` (с манифестом компонентов §11.7).

## Что осталось (1 пробел)
1. **Release tag не используется** (§11.5) — Render тянет HEAD `main`; нет однозначной связки «версия ↔ commit ↔ артефакт в проде». **Привязка Render к тегу/коммиту вместо HEAD меняет процесс деплоя (красная зона) — требует решения Владимира.**

## Открытое (не блокирует стандарт)
- У iOS-сборки и Mac-воркеров нет машинной версии, связанной с backend (сверка ручная по `RELEASE.md`).
- Нет статического линтера/typecheck в CI (только страж версии + pytest).
