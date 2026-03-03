# Проект «Братство Конца» — список ключевых файлов

Эта папка собрана для того, чтобы было понятно, **какие файлы критичны для работы проекта** и что нужно брать для деплоя/резервной копии.

## Фронтенд (Telegram WebApp + PWA)

- `../index.html` — **главный файл приложения** (вся разметка, стили и JS‑логика игры).
- `../sw.js` — сервис‑воркер для кеша и PWA.
- `../manifest.json` — PWA‑манифест (иконка/название/режим standalone).

При деплое фронта (на любой хостинг) достаточно взять **эти три файла**.

## Supabase (бекенд, Edge Functions, база)

- `../supabase/config.toml` — конфиг локального проекта Supabase.
- `../supabase/functions/**` — **все Edge Functions**, которые использует игра:
  - `boss_fight_hit`, `boss_fight_get`, `boss_fights_list`, `boss_fight_claim` — бои с боссами и награда.
  - `boss_help_send`, `boss_help_pull` — помощь от друзей/клана по боссам.
  - `upsert_player`, `get_player` — сохранение/загрузка игрока.
  - `list_clans`, `upsert_clan`, `delete_clan`, `clan_apply`, `clan_cancel_apply`, `clan_accept`, `clan_reject`, `clan_leave`, `clan_chat_send`, `clan_chat_clear` — кланы и клановый чат.
  - `join_group_fight`, `list_group_fight_entries` — групповые бои.
  - `find_arena_opponent` — поиск соперника на арене.
  - `district_leader_upsert`, `district_leaders_list` — лидеры районов.
- `../supabase/migrations/**` — SQL‑миграции (включая функции `apply_boss_damage_v2`, `claim_boss_reward` и таблицы `player_boss_fights`, `boss_help_events`, `players`, `clans` и т.д.).
- `../supabase/remote-schema*.sql` — дампы схемы базы (для отладки/документации).

## Прочее

- `../README.md` — корневой README репозитория.
- `../.vscode.code-workspace` — настройки рабочей области (необязательно для продакшена, но удобно для разработки).

> Если нужно собрать **минимальный архив для запуска игры**, достаточно взять: `index.html`, `sw.js`, `manifest.json`.  
> Если нужно перенести **весь проект с Supabase‑функциями**, копируй дополнительно всю папку `supabase/`.

