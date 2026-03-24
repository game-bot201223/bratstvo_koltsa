# Групповой бой: серверный state

## Целевая модель (три RPC)

Единый целевой контракт (см. **`docs/IMPLEMENTATION_SEQUENCE.md`**, Фаза 1):

1. **`create_gf_session_v1`** — создание сессии и начального боя на сервере (аналог текущего init).
2. **`gf_action_v1`** — только **действие** (`op` + параметры) и **`battle_epoch`**; тело **не** содержит полный `battle`.
3. **`gf_commit_v1`** — завершение матча и выдача наград на сервере (идемпотентно по `request_id`).

Пока в коде могут использоваться имена `gf_server_battle_init` и `gb_*` внутри `player_game_action_v1` — это переходный слой до переименования и отдельных URL при необходимости.

## Цель

Исключить подделку `battle` с клиента как источника истины: бой создаётся на сервере из `group_fight_entries` + состояния игрока, хранится в `public.player_gf_battle`, а действия применяются к **копии из БД**, а не к произвольному JSON из тела запроса.

## Включение

В окружении `game_backend`:

```bash
GF_SERVER_BATTLE_PRIMARY=1
# Включить имена RPC create_gf_session_v1, gf_action_v1, gf_commit_v1 (при 0 — только legacy gf_server_battle_init / gb_*):
# GF_SERVER_ACTIONS_ENABLED=1
# Опционально: не дополнять бой ботами до 10 участников (по умолчанию как клиент GF_BOTS_TEST):
# GF_SERVER_BATTLE_INJECT_BOTS=0
```

Миграции: `scripts/migrations/003_player_gf_battle.sql`, `004_player_gf_battle_committed_at.sql` (колонка **`committed_at`** для одноразового commit).

## Поток (текущая реализация ↔ целевые имена)

1. **`create_gf_session_v1`** (сейчас: **`gf_server_battle_init`**) — после старта матча клиент шлёт `start_ts_ms`. Сервер проверяет `groupFight.joined` / `startTs`, читает `group_fight_entries`, строит `battle`, пишет в `player_gf_battle` с `battle_epoch = 1`, кладёт бой в `state.groupFight.battle`, в JSON боя — **`_gfEpoch`**.
2. **`gf_action_v1`** (сейчас: **`gb_*`** в `player_game_action_v1`) — клиент передаёт `battle_epoch` и (пока переходно) может дублировать `battle` для legacy; при `GF_SERVER_BATTLE_PRIMARY=1` источник истины — строка в БД.
3. **`gf_commit_v1`** — завершение матча **только по серверному состоянию боя** (строка `player_gf_battle` при `GF_SERVER_BATTLE_PRIMARY=1`, иначе `state.groupFight.battle` из уже залоченного `players.state`). Проверяется терминальное состояние (победа / поражение), считаются **gold** (как `gfBattleComputeReward`), **случайный consumable 1–10** (веса как `CONSUMABLES_DROP_CHANCES`), инкремент **groupFight.wins/losses**, очистка **joined/startTs/battle/pendingReward**. После успеха при primary выставляется **`committed_at`** и очищается JSON боя в БД; повтор commit → **`gf_battle_already_committed`** (409). Идемпотентность по **`request_id`** — как у остальных `player_game_action_v1`. Тело: **`expected_state_version`**, при primary — **`battle_epoch`**; опционально **`outcome`**: `win`/`loss` для перекрёстной проверки.

При `GF_SERVER_BATTLE_PRIMARY=0` поведение прежнее: commit читает бой из **сохранённого state** игрока (без обязательного `battle` в теле запроса).

## Ограничения текущей версии

- Статы на сервере для боя считаются из `gym` + активных `consumablesBuffs` (время — UTC сервера). Бонусы брони/питомцев как на клиенте **пока не клонированы** — возможен небольшой расхождение с локальным превью; при необходимости перенести формулы `armorStatBonus` / `gymBonusPct`.
- В **`rewards_summary`** поля **xp / tooth / rings** пока **0** (как в текущем клиентском GF); расширение — по дизайну.
- Тесты логики: `python scripts/test_gf_commit_unit.py`.

## Клиент

В `index.html`: `GF_USE_SERVER_BATTLE` — при `true` сначала `player_game_action` с `gf_server_battle_init` или (если `GF_USE_SERVER_ACTION_ALIASES`) `create_gf_session_v1`; при ошибке / сети — `gfEnsureBattleLocalFill` (локальная сборка). Оба флага по умолчанию `false`.

## См. также

- `docs/SERVER_ACTIONS_ROADMAP.md`
- `PLAYER_DATA_PROTECTION.md`
