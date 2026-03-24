# Disaster Recovery Runbook

This server is configured with layered backups:

- `pgdump` every 5 minutes
- WAL archive (for PITR)
- daily `pg_basebackup`
- daily `fullstate` archive (nginx/certs/backend/static/config)
- offsite sync to B2 via `rclone`

## 1) Restore only database (fast path)

Use latest offsite SQL dump:

```bash
/opt/bratstvo_koltsa/scripts/restore_db_from_offsite_latest.sh gamedb
```

Then restart backend:

```bash
systemctl restart game-backend.service
```

## 2) Restore full server state

Use latest offsite fullstate archive:

```bash
/opt/bratstvo_koltsa/scripts/restore_fullstate_from_offsite_latest.sh
```

This restores nginx config, certificates, backend code/env files, static web files and reloads services.

## 3) Verify

```bash
curl -fsS https://bratstvokoltsa.com/health
systemctl is-active postgresql
systemctl is-active game-backend.service
systemctl is-active nginx
```

## 4) Notes

- Run restore commands as `root`.
- Ensure `/opt/bratstvo_koltsa/backups/.env` contains valid `OFFSITE_TARGET`.
- Offsite freshness check runs every 15 minutes and alerts via Telegram on failure.
