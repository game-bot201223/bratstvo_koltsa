# Восстановление прогресса игрока

Цель: не допускать перезаписи облака «старым» или урезанным `state`; при сбоях опираться на серверные артефакты и админ-инструменты.

## 1. Автоснимки (`admin_player_snapshots`)

- Пишутся бэкендом после успешных сохранений (см. `maybe_auto_player_snapshot` в `game_backend.py`).
- Восстановление последнего **авто**-снимка: `admin_snapshot_restore_latest_auto` (только админ).

## 2. Журнал `player_state_events`

- Таблица `player_state_events`: тип действия, endpoint, `state_version_after`, краткий `summary`, `action_payload`.
- Просмотр: `admin_player_state_events_list` (админ, по `target_tg_id`).
- Полезно для аудита: rename (`set_player_name` / `player_patch_v1`), bootstrap (`player_bootstrap_v1`), будущие patch-actions.

## 3. Ручной снимок и откат

- Создать снимок: `admin_snapshot_create` с `target_tg_id` и опциональной заметкой.
- Список: `admin_snapshot_list`.
- Восстановить конкретный: `admin_snapshot_restore` с `snapshot_id`.

## 4. Клиент

- Локальный кэш и `restoreFromLocalBackup()` — только UX; при конфликте с «установленной» строкой на сервере и пустом облачном `state` игра **не** делает bootstrap из локальной копии (см. загрузчик в `index.html`).
- Синхронизация в фоне из игрового цикла: **pull** (`get_player` / `sync_pull_after_conflict`), без `realtime_save_fast`.

## 5. Bootstrap один раз

- `player_bootstrap_v1` разрешён только если в БД **пустой** JSON `state` и строка ещё не считается «установленной» (зеркало `serverRowLooksEstablished`).
- Иначе ответ `ok: true, skipped: true` — клиент обновляет кэш из `player` в ответе.

## Переменные отключения защит (экстренно)

- `DISABLE_STATE_INTEGRITY_CHECK`
- `DISABLE_SHARP_DEGRADATION_GUARD`
- `DISABLE_PATCH_DOWNGRADE_GUARD`

Использовать только осознанно и временно.
