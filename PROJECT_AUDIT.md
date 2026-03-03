# Аудит проекта «Братство Конца» — для отладки и поиска проблем

Документ создан для проверки всех файлов и запоминания связей между компонентами. Используй его при поиске причин ошибок (например, «owner_tg_id is ambiguous», воскрешение босса, урон не применяется).

---

## 1. Файлы, влияющие на боссов и облако

### Фронтенд
| Файл | Назначение |
|------|------------|
| `index.html` | Единственный фронт: UI, логика, вызовы Edge Functions. Константа `BUILD_ID`, массив `BOSSES` (id 1–12, maxHP, reward), `sbFnCall`, `bossFightHitCloud`, `bossFightClaimCloud`, `bossFightsCloudPull`, `bossHelpSend`, отладка `__lastCloudErr` / `bfCloudDbg` / `__dbgFreeze`. |

### Edge Functions (Supabase)
| Функция | Файл | Вызовы RPC/REST |
|---------|------|------------------|
| `boss_fight_hit` | `supabase/functions/boss_fight_hit/index.ts` | **RPC** `apply_boss_damage_v2` (POST). При ошибке возвращает `details` + `debug.step: "rpc_apply_boss_damage"`. |
| `boss_help_send` | `supabase/functions/boss_help_send/index.ts` | **RPC** `apply_boss_damage_v2` для каждого получателя (клановцы/друзья). Возвращает только `resp.ok` — тело ошибки RPC **не пробрасывается** клиенту. При ошибке вставки событий возвращает `details` + `debug.step: "insert_boss_help_events"`. |
| `boss_fight_claim` | `supabase/functions/boss_fight_claim/index.ts` | **RPC** `claim_boss_reward`. |
| `boss_fight_get` | `supabase/functions/boss_fight_get/index.ts` | **REST** GET `player_boss_fights?owner_tg_id=eq.…&boss_id=eq.…`. |
| `boss_fights_list` | `supabase/functions/boss_fights_list/index.ts` | **REST** GET `player_boss_fights?owner_tg_id=eq.…&select=…&order=boss_id.asc`. |
| `boss_help_pull` | `supabase/functions/boss_help_pull/index.ts` | **REST** GET `boss_help_events?to_tg_id=eq.…&consumed=eq.false`, PATCH пометки `consumed`. Не вызывает `apply_boss_damage`. |

### База данных (миграции и схема)
| Файл | Содержание |
|------|------------|
| `supabase/migrations/20260303121000_add_apply_boss_damage_v2.sql` | Создаёт `public.apply_boss_damage_v2` с алиасом `f` в UPDATE/RETURNING. |
| `supabase/migrations/20260303130500_fix_apply_boss_damage_v1.sql` | Исправляет `public.apply_boss_damage` (v1) — те же алиасы `f`. |
| `supabase/remote-schema-latest.sql` | Текущий дамп схемы с БД: `apply_boss_damage`, `apply_boss_damage_v2`, `claim_boss_reward`, таблица `player_boss_fights`, RLS, GRANT. |

**Важно:** В репозитории **нет** отдельной миграции для `claim_boss_reward`; она есть в `remote-schema-latest.sql` (уже применена на удалённой БД). Локально миграции только для apply_boss_damage (v1 и v2).

---

## 2. Цепочки вызовов (кто что дергает)

- **Удар игрока по боссу:**  
  `index.html` → `sbFnCall('boss_fight_hit', …)` → Edge `boss_fight_hit` → RPC `apply_boss_damage_v2`.  
  Ошибка RPC возвращается клиенту в `details` и показывается в `bfCloudDbg`.

- **Помощь соклановцев/друзей:**  
  `index.html` → `bossHelpSend()` → `sbFnCall('boss_help_send', …)` → Edge `boss_help_send` → для каждого получателя RPC `apply_boss_damage_v2`.  
  Если RPC падает, Edge **не** возвращает эту ошибку клиенту (только `resp.ok`). Клиент видит успех, если вставка в `boss_help_events` прошла. Поэтому ошибки вида «ambiguous» от **удара игрока** видны в `bfCloudDbg`, а от **помощи соклановцев** — нет (урон просто не применится).

- **Список боев / один бой / клейм награды:**  
  `boss_fights_list` → REST `player_boss_fights`;  
  `boss_fight_get` → REST `player_boss_fights`;  
  `boss_fight_claim` → RPC `claim_boss_reward`.

---

## 3. Определения боссов (держать в синхроне)

Три места с max_hp / наградами:

1. **index.html** — массив `BOSSES` (стр. ~2591): id 1–12, maxHP, reward (xp, tooth, gold).
2. **boss_fight_hit/index.ts** — `bossDef()`: те же id 1–12, max_hp и reward.
3. **boss_help_send/index.ts** — `bossDef()`: только id и max_hp (без reward).  
4. **boss_fight_claim/index.ts** и **boss_fight_get/index.ts** — свой `bossDef()` с max_hp и reward.

При добавлении/изменении босса нужно обновить все три (или четыре) места.

---

## 4. Отладка на фронте

- `__lastCloudErr` — текст последней ошибки облачного вызова (устанавливается в `sbFnCall` при `!resp.ok || j.ok === false`). В него попадают `error`, `reason`, `details`, `debug` (в т.ч. `step`, `status`, `status_text`).
- `bfCloudDbg` — блок в модалке боя с боссом; показывает тот же текст, что и общий дебаг (`dbgText()`), чтобы ошибка была видна прямо в окне боя.
- `__dbgFreeze` — по клику на `bfCloudDbg` переключается; при `true` обновление `__lastCloudErr` и текста не делается, чтобы можно было скопировать сообщение.
- Полный `details` с сервера добавляется в строку ошибки без обрезания (для копирования полного JSON).

---

## 5. Возможные источники проблем (для вычисления других багов)

1. **RPC 404 «function apply_boss_damage_v2 not found»**  
   Значит на удалённой БД не применена миграция с `apply_boss_damage_v2`. Нужно: `supabase db push` или применить миграции вручную.

2. **«column reference 'owner_tg_id' is ambiguous»**  
   Возвращается из Postgres при вызове RPC. В текущих миграциях и в `remote-schema-latest.sql` у `apply_boss_damage` и `apply_boss_damage_v2` в UPDATE используется алиас `f` и все колонки квалифицированы (`f.owner_tg_id` и т.д.). Если ошибка всё ещё есть — возможно вызывается старая версия функции (кэш/другая схема) или есть ещё один объект в БД (триггер, другое представление), где есть неоднозначность. Проверить: дамп текущей БД и поиск по `owner_tg_id` без префикса.

3. **Урон от соклановцев не применяется, но ошибки нет**  
   Edge `boss_help_send` при падении `apply_boss_damage_v2` не возвращает детали клиенту. Решение: в `boss_help_send` при вызове RPC проверять `resp.ok`, при false — читать `resp.text()` и возвращать в ответе Edge `details`/`debug`, чтобы клиент мог показать в `bfCloudDbg`.

4. **Босс «воскресает»**  
   Обычно из-за того, что клиент после клейма или после ответа сервера сбрасывал HP локально или удалял облачное состояние. Сейчас логика поправлена: не удаляем облачное состояние и не сбрасываем HP после клейма; опираемся на `reward_claimed` и HP с сервера. Если воскрешение остаётся — смотреть порядок вызовов: `bossFightsCloudPull` после хита/клейма и то, что в `state.bosses.cloud[id]` не перезаписывается старыми локальными значениями.

5. **RLS на `player_boss_fights`**  
   В `remote-schema-latest.sql` таблица с RLS включена. Edge Functions вызывают RPC с `service_role`, поэтому RLS обходится. Если появятся прямые запросы с `anon`/`authenticated` к таблице — проверить политики.

6. **Расхождение max_hp**  
   Если в одном месте (фронт / hit / help_send) изменить max_hp, а в других оставить старые значения — возможны некорректные лимиты урона и отображение.

---

## 6. Полный список Edge Functions в проекте (для проверки зависимостей)

- `boss_fight_hit` — apply_boss_damage_v2  
- `boss_help_send` — apply_boss_damage_v2, players, clans, boss_help_events  
- `boss_fight_claim` — claim_boss_reward  
- `boss_fight_get` — REST player_boss_fights  
- `boss_fights_list` — REST player_boss_fights  
- `boss_help_pull` — boss_help_events (GET + PATCH)  
- `upsert_player`, `get_player` — players  
- `list_clans`, `upsert_clan`, `delete_clan` — clans (+ players)  
- `clan_apply`, `clan_cancel_apply`, `clan_accept`, `clan_reject`, `clan_leave` — clans / заявки  
- `clan_chat_send`, `clan_chat_clear` — clan chat  
- `join_group_fight`, `list_group_fight_entries` — групповые бои  
- `find_arena_opponent` — арена  
- `district_leader_upsert`, `district_leaders_list` — лидеры районов  

---

## 7. Краткий чеклист при появлении новой ошибки

- [ ] В `bfCloudDbg` или в ответе сети посмотреть полный `details` (и при необходимости заморозить экран через клик по блоку).
- [ ] Определить, какой вызов упал: `boss_fight_hit` (удар игрока) или `boss_help_send` (помощь) или claim/list/get.
- [ ] Если ошибка от RPC — проверить в дампе БД актуальные тела `apply_boss_damage` и `apply_boss_damage_v2` (алиасы `f` в UPDATE/RETURNING).
- [ ] Убедиться, что на проде задеплоены актуальные Edge Functions и применены миграции (`supabase functions deploy`, `supabase db push`).
- [ ] Сверить константы боссов (BOSSES в index.html и bossDef в boss_fight_hit, boss_help_send, boss_fight_claim, boss_fight_get).

Файл можно дополнять при обнаружении новых связей или типичных ошибок.
