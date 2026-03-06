-- Inbox for havchik (energy gifts) sent from one player to another.

CREATE TABLE IF NOT EXISTS public.havchik_inbox (
  id bigserial PRIMARY KEY,
  to_tg_id text NOT NULL,
  from_tg_id text NOT NULL,
  from_name text NOT NULL,
  type_id smallint NOT NULL DEFAULT 0,
  energy integer NOT NULL DEFAULT 5,
  created_at timestamptz NOT NULL DEFAULT now(),
  claimed boolean NOT NULL DEFAULT false,
  claimed_at timestamptz
);

CREATE INDEX IF NOT EXISTS havchik_inbox_to_claimed_id_idx
  ON public.havchik_inbox (to_tg_id, claimed, id);

ALTER TABLE public.havchik_inbox ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE public.havchik_inbox TO anon, authenticated, service_role;
GRANT USAGE, SELECT ON SEQUENCE public.havchik_inbox_id_seq TO anon, authenticated, service_role;
