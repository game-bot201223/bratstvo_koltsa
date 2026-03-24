# Run from repo root: powershell -ExecutionPolicy Bypass -File scripts/verify_backend.ps1
$ErrorActionPreference = "Stop"
$here = $PSScriptRoot
$root = Split-Path -Parent $here
Set-Location $root

Write-Host "== py_compile scripts/gf_server_battle_logic.py scripts/game_backend.py =="
py -3 -m py_compile scripts/gf_server_battle_logic.py scripts/game_backend.py 2>$null
if ($LASTEXITCODE -ne 0) {
  python -m py_compile scripts/gf_server_battle_logic.py scripts/game_backend.py
}
if ($LASTEXITCODE -ne 0) {
  Write-Host "FAIL: py_compile"
  exit 1
}
Write-Host "OK"
Write-Host "Also run: python scripts/smoke_test_endpoints.py (with curl + token as configured)"
exit 0
