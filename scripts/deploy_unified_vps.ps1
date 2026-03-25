# Единый деплой (Windows): backend + миграции + restart + smoke, затем статика.
# Логика как в deploy_unified_vps.sh — сначала бэкенд и проверки, статика только в конце.
#
#   $env:BRATSTVO_VPS = "root@bratstvokoltsa.com"
#   $env:TELEGRAM_BOT_TOKEN = "..."
#   powershell -File .\scripts\deploy_unified_vps.ps1
#
# Секреты: файл .env.deploy в корне репо (см. scripts/deploy_secrets.example) — подхватывается автоматически.
# Опционально: $env:DEPLOY_STRICT_PRODUCTION_START = "1" — STRICT_PRODUCTION_START=1 в backend .env
#   $env:DEPLOY_SKIP_SMOKE = "1" — без TELEGRAM_BOT_TOKEN; без python smoke (остаётся /health)
#
param(
  [string] $VpsHost = "",
  [string] $RemoteWebRoot = "/var/www/game",
  [string] $RemoteBackend = "/opt/bratstvo_koltsa/backend",
  [string] $RemoteDb = "/opt/bratstvo_koltsa/db",
  [string] $RemoteScripts = "/opt/bratstvo_koltsa/scripts",
  [string] $SmokeBaseUrl = "https://bratstvokoltsa.com",
  [string] $GameDbName = "gamedb",
  [switch] $SkipMigrations
)

$ErrorActionPreference = "Stop"
$ibmRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$envDeploy = Join-Path $ibmRoot ".env.deploy"
if (Test-Path -LiteralPath $envDeploy) {
  Get-Content -LiteralPath $envDeploy | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#")) { return }
    $i = $line.IndexOf("=")
    if ($i -lt 1) { return }
    $k = $line.Substring(0, $i).Trim()
    $v = $line.Substring($i + 1).Trim()
    [Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}
$deployBuildId = $env:DEPLOY_GAME_BUILD_ID
if (-not $deployBuildId) {
  $idxPath = Join-Path $ibmRoot "index.html"
  if (Test-Path -LiteralPath $idxPath) {
    $m = Select-String -LiteralPath $idxPath -Pattern "const BUILD_ID = '([^']+)'" | Select-Object -First 1
    if ($m) { $deployBuildId = $m.Matches.Groups[1].Value }
  }
}
if (-not $deployBuildId) { $deployBuildId = "2026-03-25_pendalf_img_v1" }
$SmokeBaseUrl = $SmokeBaseUrl.TrimEnd("/")
$VpsHost = $VpsHost.TrimEnd("/")
$rb = $RemoteBackend.TrimEnd("/")
$rs = $RemoteScripts.TrimEnd("/")
$rd = $RemoteDb.TrimEnd("/")
$rw = $RemoteWebRoot.TrimEnd("/")

if (-not $VpsHost) { $VpsHost = $env:BRATSTVO_VPS }
if (-not $VpsHost) {
  Write-Host "Задайте `$env:BRATSTVO_VPS или -VpsHost user@host" -ForegroundColor Yellow
  exit 1
}
if ($env:DEPLOY_SKIP_SMOKE -ne "1" -and -not $env:TELEGRAM_BOT_TOKEN) {
  Write-Host "Нужен `$env:TELEGRAM_BOT_TOKEN для smoke или `$env:DEPLOY_SKIP_SMOKE='1' (останется /health)." -ForegroundColor Yellow
  exit 1
}

$py = @("game_backend.py", "gf_server_battle_logic.py", "edge_parity.py")
foreach ($f in $py) {
  $src = Join-Path $PSScriptRoot $f
  if (-not (Test-Path -LiteralPath $src)) { throw "Missing file: $src" }
  $dest = "{0}:{1}/{2}" -f $VpsHost, $rb, $f
  Write-Host "scp $f -> $dest"
  scp $src $dest
}

$apply = Join-Path $PSScriptRoot "apply_migrations.sh"
if (-not (Test-Path -LiteralPath $apply)) { throw "Missing apply_migrations.sh" }
$migLocal = Join-Path $ibmRoot "db\migrations"
if (-not (Test-Path -LiteralPath $migLocal)) { throw "Missing db/migrations" }

Write-Host "ssh mkdir"
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new $VpsHost "mkdir -p '$rs' '$rd/migrations'"

Write-Host "scp apply_migrations.sh"
scp $apply "${VpsHost}:${rs}/apply_migrations.sh"

$migRemote = "${VpsHost}:${rd}/migrations/"
Write-Host "scp migrations/*.sql -> $migRemote"
Get-ChildItem -LiteralPath $migLocal -Filter "*.sql" -File | ForEach-Object {
  scp $_.FullName $migRemote
}
if (-not $SkipMigrations) {
  Write-Host "Migrations run before backend restart. Required for boss help: 20260324140000_boss_help_damage_applied.sql (damage_applied)." -ForegroundColor Cyan
}

$patchPy = Join-Path $PSScriptRoot "remote_patch_backend_env.py"
if (-not (Test-Path -LiteralPath $patchPy)) { throw "Missing remote_patch_backend_env.py" }
Write-Host "scp remote_patch_backend_env.py -> /tmp"
scp -o BatchMode=yes -o StrictHostKeyChecking=accept-new $patchPy "${VpsHost}:/tmp/remote_patch_backend_env.py"
Write-Host "ssh: patch $rb/.env (GAME_BUILD_ID, GF_*)"
$patchStrict = $env:DEPLOY_STRICT_PRODUCTION_START
$strictArg = ""
if ($patchStrict -match "^(1|true|yes|on)$") { $strictArg = "1" }
$patchCmd = "python3 /tmp/remote_patch_backend_env.py '$rb' '$deployBuildId' '$strictArg' && chown postgres:postgres '$rb/.env' && chmod 600 '$rb/.env'"
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new $VpsHost $patchCmd
Write-Host "Verify on VPS: grep -E '^GF_SERVER_(BATTLE_PRIMARY|ACTIONS_ENABLED)=' $rb/.env (both =1). Optional STRICT_PRODUCTION_GF=1." -ForegroundColor DarkCyan

$skipMig = if ($SkipMigrations) { "1" } else { "0" }
$remoteBash = @"
set -euo pipefail
chown postgres:postgres '$rb/game_backend.py' '$rb/gf_server_battle_logic.py' '$rb/edge_parity.py'
chmod 644 '$rb/game_backend.py' '$rb/gf_server_battle_logic.py' '$rb/edge_parity.py'
chmod 755 '$rs/apply_migrations.sh'
if [[ '$skipMig' != '1' ]]; then
  bash '$rs/apply_migrations.sh' '$GameDbName'
fi
systemctl restart game-backend.service
sleep 2
systemctl is-active --quiet game-backend.service || { systemctl status game-backend.service --no-pager; exit 1; }
if systemctl list-unit-files --type=service 2>/dev/null | grep -qF 'game-backend-b.service'; then
  systemctl restart game-backend-b.service
  sleep 2
  systemctl is-active --quiet game-backend-b.service || { systemctl status game-backend-b.service --no-pager; exit 1; }
fi

"@
$remoteBash = $remoteBash.Replace("`r`n", "`n").Replace("`r", "")

Write-Host "ssh: migrations + systemctl (via scp temp script, no CRLF pipe)"
$tmpSh = Join-Path ([System.IO.Path]::GetTempPath()) ("bratstvo_restart_" + [Guid]::NewGuid().ToString() + ".sh")
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($tmpSh, $remoteBash, $utf8NoBom)
$remoteSh = "/tmp/bratstvo_restart_backend.sh"
scp -o BatchMode=yes -o StrictHostKeyChecking=accept-new $tmpSh "${VpsHost}:${remoteSh}"
Remove-Item -LiteralPath $tmpSh -Force -ErrorAction SilentlyContinue
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new $VpsHost "chmod 755 '$remoteSh' && bash '$remoteSh'"
if ($LASTEXITCODE -ne 0) {
  Write-Host "DEPLOY_FAIL: remote migrations or systemctl restart failed. See above." -ForegroundColor Red
  exit 1
}

Write-Host "Wait $SmokeBaseUrl/health"
$healthOk = $false
for ($i = 0; $i -lt 60; $i++) {
  try {
    $r = Invoke-WebRequest -Uri "$SmokeBaseUrl/health" -UseBasicParsing -TimeoutSec 30
    if ($r.StatusCode -eq 200) { $healthOk = $true; break }
  } catch { }
  Start-Sleep -Seconds 1
}
if (-not $healthOk) {
  Write-Host "DEPLOY_FAIL: /health not 200. Static not uploaded." -ForegroundColor Red
  exit 1
}

if ($env:DEPLOY_SKIP_SMOKE -ne "1") {
  $env:SMOKE_BASE_URL = $SmokeBaseUrl
  Push-Location $ibmRoot
  try {
    python scripts/smoke_test_endpoints.py
    if ($LASTEXITCODE -ne 0) {
      Write-Host "DEPLOY_FAIL: smoke tests failed. Static not uploaded." -ForegroundColor Red
      exit 1
    }
  } finally {
    Pop-Location
  }
}

foreach ($name in @("index.html", "manifest.json", "sw.js")) {
  $src = Join-Path $ibmRoot $name
  if (-not (Test-Path -LiteralPath $src)) { throw "Missing $name" }
  Write-Host "scp $name"
  scp $src "${VpsHost}:${rw}/$name"
}
Get-ChildItem -Path $ibmRoot -File | Where-Object {
  $ext = $_.Extension.ToLowerInvariant()
  (".png", ".jpg", ".jpeg", ".webp", ".gif") -contains $ext
} | ForEach-Object {
  Write-Host "scp $($_.Name)"
  scp $_.FullName "${VpsHost}:${rw}/$($_.Name)"
}
$icons = Join-Path $ibmRoot "icons"
if (Test-Path -LiteralPath $icons) {
  Write-Host "scp -r icons"
  scp -r $icons "${VpsHost}:${rw}/"
}

Write-Host "DEPLOY_OK" -ForegroundColor Green
