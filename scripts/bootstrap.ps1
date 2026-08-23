# Universal 1-Click Bootstrap Installer for Prismatic Agent Hypervisor Kernel on Windows
$ErrorActionPreference = "Stop"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "    Prismatic Agent Hypervisor Universal Bootstrap (Windows)" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# 1. Determine Target Github Directory
$GithubDir = "$env:USERPROFILE\Github"
if (!(Test-Path $GithubDir)) {
    New-Item -ItemType Directory -Path $GithubDir -Force | Out-Null
}

# 2. Locate Python
$pythonCmd = $null
$candidatePythons = @(
    "C:\Users\Michael Gulden\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe",
    (Get-Command python.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source),
    (Get-Command py.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source)
)

foreach ($py in $candidatePythons) {
    if ($py -and (Test-Path $py)) {
        try {
            $ver = & $py -c "import sys; print(sys.version_info[0])" 2>$null
            if ($ver -eq "3") {
                $pythonCmd = $py
                break
            }
        } catch {}
    }
}

if (-not $pythonCmd) {
    Write-Host "[ERROR] Python 3.10+ was not found on this system. Please install Python." -ForegroundColor Red
    exit 1
}

Write-Host "Using Python: $pythonCmd" -ForegroundColor Green

# 3. Clone / Sync All 5 Repositories
$repos = @(
    @{ Name = "swarmlock";   Url = "https://github.com/mbgulden/swarmlock.git" },
    @{ Name = "swarmproof";  Url = "https://github.com/mbgulden/swarmproof.git" },
    @{ Name = "swarmgate";   Url = "https://github.com/mbgulden/swarmgate.git" },
    @{ Name = "swarmsaga";   Url = "https://github.com/mbgulden/swarmsaga.git" },
    @{ Name = "swarmledger"; Url = "https://github.com/mbgulden/swarmledger.git" }
)

foreach ($r in $repos) {
    $repoPath = Join-Path -Path $GithubDir -ChildPath $r.Name
    if (!(Test-Path $repoPath)) {
        Write-Host "Cloning $($r.Name)..." -ForegroundColor Yellow
        git clone $r.Url $repoPath
    } else {
        Write-Host "Updating $($r.Name)..." -ForegroundColor DarkGray
        git -C $repoPath fetch origin
        git -C $repoPath checkout -B main origin/main --force 2>$null
    }
}

# 4. Install All 5 in Editable Mode
Write-Host ""
Write-Host "Installing all 5 primitives in Editable Mode (pip install -e .)..." -ForegroundColor Cyan
$installArgs = @("-m", "pip", "install")
foreach ($r in $repos) {
    $installArgs += "-e"
    $installArgs += (Join-Path -Path $GithubDir -ChildPath $r.Name)
}
& $pythonCmd $installArgs

# 5. Configure Antigravity Hooks
$hookDir = "$env:USERPROFILE\.gemini\antigravity-cli"
if (!(Test-Path $hookDir)) {
    New-Item -ItemType Directory -Path $hookDir -Force | Out-Null
}

$prismaticHookScript = "$GithubDir\Hermes\.agents\hooks\prismatic_hook.py"
if (!(Test-Path $prismaticHookScript)) {
    $prismaticHookScript = "$GithubDir\swarmlock\scripts\prismatic_hook.py"
}

$hooksJson = @"
{
  "hooks": [
    {
      "event": "pre_tool_invocation",
      "tools": ["read_file", "view_file", "view_file_outline", "grep_search", "find_by_name", "write_to_file", "replace_file_content", "run_command"],
      "command": "\"$pythonCmd\" \"$prismaticHookScript\" --stage pre"
    },
    {
      "event": "post_tool_invocation",
      "tools": ["write_to_file", "replace_file_content", "run_command"],
      "command": "\"$pythonCmd\" \"$prismaticHookScript\" --stage post"
    }
  ]
}
"@

[System.IO.File]::WriteAllText("$hookDir\hooks.json", $hooksJson, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "Configured Antigravity Hooks at $hookDir\hooks.json" -ForegroundColor Green

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "    Prismatic Agent Hypervisor Successfully Provisioned!    " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green