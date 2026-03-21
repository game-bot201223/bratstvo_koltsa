


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA IF NOT EXISTS "public";


DROP FUNCTION IF EXISTS "public"."apply_boss_damage"("text", integer, bigint, bigint, timestamp with time zone);
DROP FUNCTION IF EXISTS "public"."claim_boss_reward"("text", integer);
DROP FUNCTION IF EXISTS "public"."rls_auto_enable"();


ALTER SCHEMA "public" OWNER TO "pg_database_owner";


COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE OR REPLACE FUNCTION "public"."apply_boss_damage"("p_owner_tg_id" "text", "p_boss_id" integer, "p_dmg" bigint, "p_max_hp" bigint, "p_expires_at" timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS TABLE("owner_tg_id" "text", "boss_id" integer, "hp" bigint, "max_hp" bigint, "expires_at" timestamp with time zone, "reward_claimed" boolean)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $$
declare
  v_owner_tg_id text;
  v_boss_id integer;
  v_hp bigint;
  v_max_hp bigint;
  v_expires_at timestamptz;
  v_reward_claimed boolean;
begin
  if p_owner_tg_id is null or length(trim(p_owner_tg_id)) = 0 then
    raise exception 'bad_owner';
  end if;
  if p_boss_id is null or p_boss_id <= 0 then
    raise exception 'bad_boss_id';
  end if;
  if p_max_hp is null or p_max_hp <= 0 then
    raise exception 'bad_max_hp';
  end if;

  insert into public.player_boss_fights (owner_tg_id, boss_id, hp, max_hp, expires_at, reward_claimed)
  values (trim(p_owner_tg_id), p_boss_id, p_max_hp, p_max_hp, p_expires_at, false)
  on conflict (owner_tg_id, boss_id) do nothing;

  update public.player_boss_fights f
  set
    hp = case
      when f.reward_claimed then f.hp
      else greatest(0, f.hp - greatest(0, coalesce(p_dmg, 0)))
    end,
    max_hp = greatest(f.max_hp, p_max_hp),
    expires_at = coalesce(p_expires_at, f.expires_at),
    updated_at = now()
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id
  returning f.owner_tg_id, f.boss_id, f.hp, f.max_hp, f.expires_at, f.reward_claimed
  into v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;

  return query select v_owner_tg_id, v_boss_id, v_hp, v_max_hp, v_expires_at, v_reward_claimed;
end;
$$;


ALTER FUNCTION "public"."apply_boss_damage"("p_owner_tg_id" "text", "p_boss_id" integer, "p_dmg" bigint, "p_max_hp" bigint, "p_expires_at" timestamp with time zone) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."claim_boss_reward"("p_owner_tg_id" "text", "p_boss_id" integer) RETURNS TABLE("ok" boolean, "hp" bigint, "reward_claimed" boolean)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $$
declare
  v_hp bigint;
  v_claimed boolean;
begin
  if p_owner_tg_id is null or length(trim(p_owner_tg_id)) = 0 then
    raise exception 'bad_owner';
  end if;
  if p_boss_id is null or p_boss_id <= 0 then
    raise exception 'bad_boss_id';
  end if;

  select f.hp, f.reward_claimed
  into v_hp, v_claimed
  from public.player_boss_fights f
  where f.owner_tg_id = trim(p_owner_tg_id)
    and f.boss_id = p_boss_id
  for update;

  if v_hp is null then
    return query select false, null::bigint, false;
    return;
  end if;

  if v_claimed then
    return query select false, v_hp, true;
    return;
  end if;

  if v_hp > 0 then
    return query select false, v_hp, false;
    return;
  end if;

  update public.player_boss_fights
  set reward_claimed = true, updated_at = now()
  where owner_tg_id = trim(p_owner_tg_id)
    and boss_id = p_boss_id;

  return query select true, v_hp, true;
end;
$$;


ALTER FUNCTION "public"."claim_boss_reward"("p_owner_tg_id" "text", "p_boss_id" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."rls_auto_enable"() RETURNS "event_trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'pg_catalog'
    AS $$
DECLARE
  cmd record;
BEGIN
  FOR cmd IN
    SELECT *
    FROM pg_event_trigger_ddl_commands()
    WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
      AND object_type IN ('table','partitioned table')
  LOOP
     IF cmd.schema_name IS NOT NULL AND cmd.schema_name IN ('public') AND cmd.schema_name NOT IN ('pg_catalog','information_schema') AND cmd.schema_name NOT LIKE 'pg_toast%' AND cmd.schema_name NOT LIKE 'pg_temp%' THEN
      BEGIN
        EXECUTE format('alter table if exists %s enable row level security', cmd.object_identity);
        RAISE LOG 'rls_auto_enable: enabled RLS on %', cmd.object_identity;
      EXCEPTION
        WHEN OTHERS THEN
          RAISE LOG 'rls_auto_enable: failed to enable RLS on %', cmd.object_identity;
      END;
     ELSE
        RAISE LOG 'rls_auto_enable: skip % (either system schema or not in enforced list: %.)', cmd.object_identity, cmd.schema_name;
     END IF;
  END LOOP;
END;
$$;


ALTER FUNCTION "public"."rls_auto_enable"() OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."boss_help_events" (
    "id" bigint NOT NULL,
    "to_tg_id" "text" NOT NULL,
    "from_tg_id" "text" NOT NULL,
    "from_name" "text" NOT NULL,
    "boss_id" integer NOT NULL,
    "dmg" integer NOT NULL,
    "clan_id" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "consumed" boolean DEFAULT false NOT NULL,
    "consumed_at" timestamp with time zone
);


ALTER TABLE "public"."boss_help_events" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."boss_help_events_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."boss_help_events_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."boss_help_events_id_seq" OWNED BY "public"."boss_help_events"."id";



CREATE TABLE IF NOT EXISTS "public"."clans" (
    "id" "text" NOT NULL,
    "name" "text" NOT NULL,
    "owner_tg_id" "text" NOT NULL,
    "data" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."clans" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."district_leaders" (
    "district_key" "text" NOT NULL,
    "tg_id" "text" NOT NULL,
    "name" "text" NOT NULL,
    "fear" bigint DEFAULT 0 NOT NULL,
    "photo_url" "text",
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."district_leaders" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."group_fight_entries" (
    "start_ts" timestamp with time zone NOT NULL,
    "tg_id" "text" NOT NULL,
    "name" "text" DEFAULT 'Player'::"text" NOT NULL,
    "photo_url" "text" DEFAULT ''::"text" NOT NULL,
    "stats_sum" bigint DEFAULT 0 NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."group_fight_entries" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."player_boss_fights" (
    "owner_tg_id" "text" NOT NULL,
    "boss_id" integer NOT NULL,
    "hp" bigint NOT NULL,
    "max_hp" bigint NOT NULL,
    "expires_at" timestamp with time zone,
    "reward_claimed" boolean DEFAULT false NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."player_boss_fights" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."players" (
    "tg_id" "text" NOT NULL,
    "name" "text" DEFAULT 'Player'::"text" NOT NULL,
    "photo_url" "text" DEFAULT ''::"text" NOT NULL,
    "arena_power" bigint DEFAULT 0 NOT NULL,
    "level" integer DEFAULT 1 NOT NULL,
    "stats_sum" bigint DEFAULT 0 NOT NULL,
    "boss_wins" bigint DEFAULT 0 NOT NULL,
    "state" "jsonb",
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "arena_wins" bigint DEFAULT 0 NOT NULL,
    "arena_losses" bigint DEFAULT 0 NOT NULL
);


ALTER TABLE "public"."players" OWNER TO "postgres";


ALTER TABLE ONLY "public"."boss_help_events" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."boss_help_events_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."boss_help_events"
    ADD CONSTRAINT "boss_help_events_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."clans"
    ADD CONSTRAINT "clans_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."district_leaders"
    ADD CONSTRAINT "district_leaders_pkey" PRIMARY KEY ("district_key");



ALTER TABLE ONLY "public"."group_fight_entries"
    ADD CONSTRAINT "group_fight_entries_pkey" PRIMARY KEY ("start_ts", "tg_id");



ALTER TABLE ONLY "public"."player_boss_fights"
    ADD CONSTRAINT "player_boss_fights_pkey" PRIMARY KEY ("owner_tg_id", "boss_id");



ALTER TABLE ONLY "public"."players"
    ADD CONSTRAINT "players_pkey" PRIMARY KEY ("tg_id");



CREATE INDEX "boss_help_events_created_at_idx" ON "public"."boss_help_events" USING "btree" ("created_at");



CREATE INDEX "boss_help_events_to_unconsumed_idx" ON "public"."boss_help_events" USING "btree" ("to_tg_id", "consumed", "id");



CREATE UNIQUE INDEX "clans_name_lower_uniq" ON "public"."clans" USING "btree" ("lower"("name"));



CREATE INDEX "player_boss_fights_expires_at_idx" ON "public"."player_boss_fights" USING "btree" ("expires_at");



CREATE INDEX "player_boss_fights_owner_idx" ON "public"."player_boss_fights" USING "btree" ("owner_tg_id");



CREATE UNIQUE INDEX "players_name_lower_uniq" ON "public"."players" USING "btree" ("lower"("name")) WHERE ("lower"("name") <> 'player'::"text");



ALTER TABLE "public"."boss_help_events" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."clans" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."district_leaders" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "district_leaders_select_all" ON "public"."district_leaders" FOR SELECT USING (true);



ALTER TABLE "public"."group_fight_entries" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."player_boss_fights" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."players" ENABLE ROW LEVEL SECURITY;


GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";



GRANT ALL ON FUNCTION "public"."apply_boss_damage"("p_owner_tg_id" "text", "p_boss_id" integer, "p_dmg" bigint, "p_max_hp" bigint, "p_expires_at" timestamp with time zone) TO "anon";
GRANT ALL ON FUNCTION "public"."apply_boss_damage"("p_owner_tg_id" "text", "p_boss_id" integer, "p_dmg" bigint, "p_max_hp" bigint, "p_expires_at" timestamp with time zone) TO "authenticated";
GRANT ALL ON FUNCTION "public"."apply_boss_damage"("p_owner_tg_id" "text", "p_boss_id" integer, "p_dmg" bigint, "p_max_hp" bigint, "p_expires_at" timestamp with time zone) TO "service_role";



GRANT ALL ON FUNCTION "public"."claim_boss_reward"("p_owner_tg_id" "text", "p_boss_id" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."claim_boss_reward"("p_owner_tg_id" "text", "p_boss_id" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."claim_boss_reward"("p_owner_tg_id" "text", "p_boss_id" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."rls_auto_enable"() TO "anon";
GRANT ALL ON FUNCTION "public"."rls_auto_enable"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."rls_auto_enable"() TO "service_role";



GRANT ALL ON TABLE "public"."boss_help_events" TO "anon";
GRANT ALL ON TABLE "public"."boss_help_events" TO "authenticated";
GRANT ALL ON TABLE "public"."boss_help_events" TO "service_role";



GRANT ALL ON SEQUENCE "public"."boss_help_events_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."boss_help_events_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."boss_help_events_id_seq" TO "service_role";



GRANT ALL ON TABLE "public"."clans" TO "anon";
GRANT ALL ON TABLE "public"."clans" TO "authenticated";
GRANT ALL ON TABLE "public"."clans" TO "service_role";



GRANT ALL ON TABLE "public"."district_leaders" TO "anon";
GRANT ALL ON TABLE "public"."district_leaders" TO "authenticated";
GRANT ALL ON TABLE "public"."district_leaders" TO "service_role";



GRANT ALL ON TABLE "public"."group_fight_entries" TO "anon";
GRANT ALL ON TABLE "public"."group_fight_entries" TO "authenticated";
GRANT ALL ON TABLE "public"."group_fight_entries" TO "service_role";



GRANT ALL ON TABLE "public"."player_boss_fights" TO "anon";
GRANT ALL ON TABLE "public"."player_boss_fights" TO "authenticated";
GRANT ALL ON TABLE "public"."player_boss_fights" TO "service_role";



GRANT ALL ON TABLE "public"."players" TO "anon";
GRANT ALL ON TABLE "public"."players" TO "authenticated";
GRANT ALL ON TABLE "public"."players" TO "service_role";



ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "service_role";







