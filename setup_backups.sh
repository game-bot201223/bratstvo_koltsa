#!/bin/bash
# Setup automated backups for PostgreSQL + Redis

mkdir -p /opt/gameapi/backups

# Create backup script
cat > /opt/gameapi/backup.sh << 'BKEOF'
#!/bin/bash
BACKUP_DIR="/opt/gameapi/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "$BACKUP_DIR"

# PostgreSQL dump
sudo -u postgres pg_dump gamedb > "$BACKUP_DIR/pg_${TIMESTAMP}.sql" 2>/dev/null
gzip -f "$BACKUP_DIR/pg_${TIMESTAMP}.sql" 2>/dev/null

# Redis RDB snapshot
redis-cli -a 'BrKo1tsaR3d1s2024!' BGSAVE 2>/dev/null
sleep 2
cp /var/lib/redis/dump.rdb "$BACKUP_DIR/redis_${TIMESTAMP}.rdb" 2>/dev/null

# Keep only last 7 days of backups
find "$BACKUP_DIR" -name "pg_*.sql.gz" -mtime +7 -delete 2>/dev/null
find "$BACKUP_DIR" -name "redis_*.rdb" -mtime +7 -delete 2>/dev/null

echo "Backup done: $TIMESTAMP"
BKEOF

chmod +x /opt/gameapi/backup.sh

# Setup cron: every 6 hours
(crontab -l 2>/dev/null | grep -v 'backup.sh'; echo "0 */6 * * * /opt/gameapi/backup.sh >> /opt/gameapi/backups/backup.log 2>&1") | crontab -

# Run first backup now
/opt/gameapi/backup.sh

echo "=== Backup system configured ==="
echo "Schedule: every 6 hours"
echo "Retention: 7 days"
crontab -l | grep backup
ls -la /opt/gameapi/backups/
