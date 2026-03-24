# Проверка синтаксиса game_backend.py (Windows / CI без pyenv в PATH).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$target = Join-Path $root "scripts\game_backend.py"
$candidates = @("python", "python3", "py")
foreach ($c in $candidates) {
    try {
        & $c -m py_compile $target 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "OK: $c -m py_compile game_backend.py"
            exit 0
        }
    } catch { }
}
Write-Host "FAIL: no working Python found for py_compile. Install Python 3 or add it to PATH."
exit 1
