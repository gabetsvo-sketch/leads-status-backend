# VERSION_AUDIT — leads-status/backend

Аудит по §12.5 стандарта VIBE-CODE v0.01.001 (read-only).

| Поле | Значение |
|---|---|
| Проект | leads-status/backend (FastAPI-бэкенд приложения LeadsStatus, деплой на Render) |
| Режим | **БОЕВОЙ ПРОДУКТ** (обслуживает iOS-приложение, подключён к CRM, реальные данные клиентов) |
| Репозиторий/ветка | `gabetsvo-sketch/leads-status-backend` · `main`, last commit `4e8e656` (2026-06-17 15:37) · upstream синхронен (0/0) · working tree чистый |
| Текущая версия | **FAIL** — нет `VERSION.json`, нет другого канонического источника версии |
| Источник версии | **отсутствует** |
| Версия во фронтенде | **FAIL** — нет `/version`-эндпоинта; iOS приложение не получает версию backend для диагностики |
| Версии компонентов | **UNKNOWN** — backend и iOS должны двигаться вместе; нет release manifest, связывающего пару |
| CI требует bump | **FAIL** — нет CI; правка → push в main → Render деплой без проверки версии |
| Git local | **GIT-3** (commit + push, синхронизация с remote подтверждена) |
| Теги и артефакты | **FAIL** — release tag не используется; Render тянет HEAD ветки main |
| Источники истины | **FAIL** — нет PROJECT.md, ARCHITECTURE.md, AGENTS.md, CURRENT_STATE.md, CHANGELOG.md, RISK_MAP.md (только `tests/`, `main.py`, `requirements.txt`, `render.yaml`) |

## Критические пробелы
1. **Боевой проект без VERSION.json и источников истины** — при изменении iOS не понимает, какая версия backend в работе, и нечем сверить.
2. **Деплой = push в main без CI-проверок** — нет блокировки «код изменён → bump не сделан» (§12.4).
3. **Нет TEST_MATRIX.md / RELEASE.md** — не зафиксированы регрессионные сценарии после деплоя на Render и порядок отката.
4. **Сосуществует «зомби-копия» в `Tasks/leads-status-backend/`** (см. отдельную ноту ниже) — два рабочих каталога одного и того же репозитория, риск работы со старой версией.

## Первый безопасный changeset
- Создать `VERSION.json {0, 1, 0}` (v0.01.000 — baseline).
- Добавить `GET /version` в `main.py`, читать из VERSION.json (FastAPI: вставка в каждый ответ через middleware либо отдельный endpoint).
- Завести `PROJECT.md`, `ARCHITECTURE.md`, `AGENTS.md`, `CURRENT_STATE.md`, `CHANGELOG.md`, `RISK_MAP.md` (минимальные шаблоны).
- Удалить дублирующую `Tasks/leads-status-backend/` (после явного решения Владимира — это GIT-3, но устаревший снимок) ИЛИ преобразовать её в `worktree` (§14.4).

**Статус версионирования: FAIL** (Git в порядке, но всё остальное отсутствует).
