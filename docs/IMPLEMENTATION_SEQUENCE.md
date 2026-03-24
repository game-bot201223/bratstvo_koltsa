# Последовательность внедрения серверных действий

Документ фиксирует **порядок работ** от roadmap к коду: сначала высокий риск (чит/рассинхрон), затем экономика и UX.

Связанные файлы: `docs/SERVER_ACTIONS_ROADMAP.md`, `docs/GROUP_FIGHT_SERVER_STATE.md`, `PLAYER_DATA_PROTECTION.md`.

---

## Фаза 1 — Group Fight (GF): серверный state и три RPC

**Цель:** клиент не шлёт полный `battle` как источник истины — только команды; награды только после серверного commit.

| RPC | Назначение | Тело (минимум) |
|-----|------------|----------------|
| **`create_gf_session_v1`** | Создать сессию боя: участники из `group_fight_entries`, статы с сервера, запись в `player_gf_battle` + зеркало в `state.groupFight.battle` | `start_ts_ms`, `expected_state_version`, `request_id`, `session_id` |
| **`gf_action_v1`** | Одно игровое действие: расходник / аптечка / граната / (далее) удар / конец хода | `op`, параметры опа (напр. `item_id`, `target_id`), **`battle_epoch`**, без полного `battle` |
| **`gf_commit_v1`** | Завершение матча: исход с сервера, начисление gold/tooth/consumable, сброс `pending`, идемпотентность по `request_id` | `start_ts_ms` или `session_id`, `request_id` |

### Миграция от текущего кода

- Сейчас (при `GF_SERVER_BATTLE_PRIMARY=1`): логика разбита на `gf_server_battle_init` + `gb_*` внутри `player_game_action_v1`.
- **Шаг 1:** переименовать/дублировать как **`create_gf_session_v1`** (тот же handler или `action: "create_gf_session_v1"` в одном endpoint).
- **Шаг 2:** свести `gb_consumable_item_use` / `gb_arena_med_use` / `gb_arena_nade_use` к **`gf_action_v1`** с полем `op` (внутри — тот же `_pga_mutate_state_for_action`, бой только из БД).
- **Шаг 3:** убрать передачу `battle` с клиента для этих путей полностью.
- **Шаг 4:** **`gf_commit_v1`** — заменить локальный «ЗАБРАТЬ» / `gfAddRandomConsumableReward` / начисления из `pendingReward`.

**HTTP:** либо три маршрута `POST /functions/v1/create_gf_session_v1` и т.д., либо временно те же три значения как `action` в `player_game_action_v1` (единая транзакция/лимиты) — выбрать один стиль и держать его в проде.

---

## Фаза 2 — Награды вне босса

Перевести на server actions (без локального `+=` до save):

- `gfAddRandomConsumableReward` → часть **`gf_commit_v1`** или отдельный `gf_drop_roll_v1` только если нужен дроп до конца боя.
- Квестовые награды → `quest_claim_v1` / `quest_complete_v1`.
- Любые случайные дропы → RPC с серверным RNG + `request_id`.

---

## Фаза 3 — Энергия и таймеры

- `energy_tick_v1` (или серверный расчёт при любом read/mutation — проще один контракт).
- `energy_spend_v1` — списание с проверкой лимита и времени.

Цель: убрать рассинхрон «клиент тикнул / сервер нет».

---

## Фаза 4 — Инвентарь и доспехи

- `equip_item_v1`, `unequip_item_v1`, при необходимости `inventory_move_v1`.
- Любое изменение экипировки только через RPC + merge `player` с сервера.

---

## Фаза 5 — Dice / случайные механики

- `dice_roll_v1` (или `dice_commit_v1`): ставка → серверный бросок → награда в той же транзакции.

---

## Фаза 6 — Питомцы и улучшения

- `pet_upgrade_v1`, `pet_assign_v1` (и покупка/продажа при необходимости отдельно).

---

## Фаза 7 — Регрессия после каждого нового RPC

После **каждого** нового действия:

1. Сценарий: действие → **refresh** → **повторный вход** — прогресс совпадает.
2. Повтор того же **`request_id`** — дубликат, без двойного начисления.
3. Неверный **`expected_state_version`** — 409, клиент подтягивает версию и повторяет осмысленно.
4. Поиск: нет ли после merge снова локальной мутации `state.*` по этому флоу.

При `REQUIRE_SESSION_MATCH_FOR_SAVE=1`: тот же чеклист с валидным `session_id` после `session_start`.

---

## Фаза 8 — Аудит оставшихся локальных мутаций

Систематически (см. roadmap):

```bash
rg "state\\.(gold|tooth|silver|rings)\\s*[+\\-]=" index.html
rg "addXp\\(" index.html
rg "consumablesItems\\[" index.html
```

Каждое вхождение — либо UI-only, либо очередной RPC.

---

## Фаза 9 — Event log (patch/delta, replay)

После стабилизации RPC:

- хранить детерминированную дельту от сервера (отдельная миграция);
- цель — replay / восстановление, не блокер для п. 1–6.

---

## Текущий фокус (рекомендация)

1. Завершить **Фазу 1** по контракту **create_gf_session_v1 / gf_action_v1 / gf_commit_v1** (имена и отсутствие `battle` в теле для действий и commit).
2. Затем **Фаза 2** (награды ГБ в `gf_commit_v1`).
3. Параллельно не раздувать scope: энергию и инвентарь брать отдельными PR после GF+награды стабильны.
