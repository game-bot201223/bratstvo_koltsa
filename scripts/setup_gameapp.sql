-- Set password at runtime, do not keep secrets in repository.
-- Usage example:
--   psql -v gameapp_password='YOUR_STRONG_SECRET' -f setup_gameapp.sql
do $$
begin
  if not exists (select 1 from pg_roles where rolname='gameapp') then
    execute format('create role gameapp login password %L', :'gameapp_password');
  else
    execute format('alter role gameapp with login password %L', :'gameapp_password');
  end if;
end $$;
grant usage on schema public to gameapp;
grant select,insert,update,delete on all tables in schema public to gameapp;
grant usage,select on all sequences in schema public to gameapp;
alter default privileges in schema public grant select,insert,update,delete on tables to gameapp;
alter default privileges in schema public grant usage,select on sequences to gameapp;
