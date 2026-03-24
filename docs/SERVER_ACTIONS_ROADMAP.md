# Серверные действия и дорожная карта (patch-RPC)

## Порядок внедрения (от roadmap к коду)

Зафиксирован поэтапный план (GF → награды → энергия → инвентарь → dice → питомцы → регрессия → аудит → event log): **`docs/IMPLEMENTATION_SEQUENCE.md`**.

## Принцип (целевой)

- Клиент **не** меняет прогресс (валюта, XP, инвентарь, квесты, экипировка и т.д.) сам по себе.
- Клиент отправляет **одно действие** (`action` + параметры + `expected_state_version` + `session_id` при `REQUIRE_SESSION_MATCH_FOR_SAVE=1`).
- Сервер **считает результат**, валидирует state, пишет в БД, при необходимости пишет журнал, возвращает **актуальный `player`**.

Уже близко к этому: босс (claim), `player_game_action_v1` (лавка, арена med/nade, батончики, расходники ГБ, аптечка/граната в ГБ), `player_patch_v1` (имя), `player_bootstrap_v1` (только stub).

## Уже на сервере (кратко)

| Область | Механизм |
|--------|----------|
| Победа над боссом, награда | `boss_fight_claim` → `boss_fight_commit_rewards_sync` |
| Лавка, арена consumables, батончики, предметы 7–10 в ГБ, med/nade в ГБ | `player_game_action_v1` |
| Имя (+ photo) | `player_patch_v1` / `set_player_name` |
| Первичный stub | `player_bootstrap_v1` (не перетирает установленного игрока) |

## Следующий этап: event log с patch/delta (replay)

Сейчас `player_state_events` хранит **аудит** (summary, payload метаданные). **Полный replay** невозможен без детерминированной дельты на каждое действие.

Рекомендуемое расширение (отдельная миграция):

- Колонка например `state_patch jsonb` — JSON Patch (RFC 6902) или `{ "op": "merge", "path": "...", "value": ... }`, генерируемая **сервером** после успешного commit.
- Либо таблица `player_state_patches` с `(tg_id, state_version_after, patch, action_type)`.

До этого recovery = **snapshot + ручной аудит** (`admin_player_recovery_v1`).

---

## Аудит клиента (`index.html`): что ещё мутирует state локально

Список зон и **предлагаемые RPC** (имена условные; можно добавлять как новые `action` в `player_game_action_v1` или отдельные endpoints).

### Уже переведено

- Лавка → `lavka_buy`
- Арена med/nade → `arena_buy_med` / `arena_buy_nade`
- Батончики, предметы 7–10 в ГБ, med/nade в ГБ → соответствующие `action` в `player_game_action_v1`

### Групповой бой (GF)

| Поведение | Целевые RPC (Фаза 1) | Текущий код (переходный) |
|-----------|----------------------|---------------------------|
| Инициализация боя на сервере | **`create_gf_session_v1`** | `gf_server_battle_init` в `player_game_action_v1` при `GF_SERVER_BATTLE_PRIMARY=1` |
| Удар / предмет / med / nade | **`gf_action_v1`** (`op` + params, **без** полного `battle`) | `gb_consumable_item_use`, `gb_arena_med_use`, `gb_arena_nade_use` + `battle_epoch` |
| Конец боя и награды | **`gf_commit_v1`** | Пока локально / в плане |
| Случайный дроп расходника | часть **`gf_commit_v1`** или `gf_drop_roll_v1` | `gfAddRandomConsumableReward` (клиент) |

Детали и миграция имён: `docs/GROUP_FIGHT_SERVER_STATE.md`, порядок работ: `docs/IMPLEMENTATION_SEQUENCE.md`.

### Арена (PvE)

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Победа/поражение, silver, XP | `arena_match_complete_v1` |
| Сброс кулдауна за gold | `arena_cd_reset_v1` |

### Спортзал

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Прокачка стата за silver | `gym_train_stat_v1` |

### Питомцы

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Покупка / продажа | `pet_buy_v1` / `pet_sell_v1` |

### Районы

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Награды, XP, tooth, gold | `district_claim_reward_v1` / `district_task_complete_v1` |
| Страх, лидеры | `district_fear_add_v1` (+ существующие cloud upsert где нужно) |
| Бизнесы | `district_biz_upgrade_v1`, `district_biz_claim_daily_v1` |

### Боссы (дополнительно к claim)

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| `bossLocalClaim` / fallback | Удалить после стабильного server claim |
| Магазин ударов / зубов | `boss_shop_purchase_v1` |

### Кости

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Ставки, награды | `dice_commit_v1` (серверный RNG) |

### Энергия

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Трата / refill gold | `energy_spend_v1`, `energy_refill_gold_v1` |

### Инвентарь / экипировка

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Слоты, предметы | `inventory_set_slot_v1` / `equipment_equip_v1` |
| Броня | `armor_buy_v1`, `armor_equip_v1` |

### Кланы, промо, friend help

| Поведение | Предлагаемый RPC |
|-----------|-------------------|
| Создание клана за gold | `clan_create_v1` (сервер списывает) |
| Промо: только merge ответа `promo_redeem`, без локального += | уже по смыслу |
| Прокачка помощи за tooth | `friend_help_upgrade_v1` |

### Регресс-поиск перед релизом

```bash
# пример: найти оставшиеся прямые начисления (уточнять по проекту)
rg "state\\.(gold|tooth|silver|rings)\\s*[+\\-]=" index.html
rg "addXp\\(" index.html
```

---

## Чеклист после деплоя

1. `python -m py_compile scripts/game_backend.py`
2. Smoke: `python scripts/smoke_test_endpoints.py` (или свой набор curl)
3. Ручной сценарий: логин → `session_start` → бой → **claim босса** → refresh → повторный вход; прогресс и `state_version` согласованы
4. Лавка/расходник: покупка через `player_game_action_v1` → reload → данные на месте

## REQUIRE_SESSION_MATCH_FOR_SAVE=1

- После `session_start` клиент хранит `__serverSessionId` и шлёт `session_id` во всех мутациях.
- При `session_mismatch` / `session_id_required`: повторить `session_start`, затем действие.
- Не вызывать claim босса до гидратации сессии после холодного старта.

---

## Итог

Архитектура по боссам и анти-обнулению — сильная; следующий этап — закрыть таблицу RPC выше и добавить **patch в журнал** для будущего replay.
