#!/usr/bin/env bash
# Universal 1-Click Bootstrap Installer for Prismatic Agent Hypervisor Kernel (Linux / macOS)
set -e

echo "============================================================"
echo "    Prismatic Agent Hypervisor Universal Bootstrap (Unix)   "
echo "============================================================"

GITHUB_DIR="${HOME}/Github"
mkdir -p "${GITHUB_DIR}"
cd "${GITHUB_DIR}"

# 1. Locate Python 3
PYTHON_CMD="python3"
if ! command -v python3 &>/dev/null; then
    echo "[ERROR] python3 is required but not found in PATH."
    exit 1
fi

echo "Using Python: $($PYTHON_CMD --version)"

# 2. Clone / Sync All 5 Repositories
REPOS=("swarmlock" "swarmproof" "swarmgate" "swarmsaga" "swarmledger")
for repo in "${REPOS[@]}"; do
    if [ ! -d "${GITHUB_DIR}/${repo}" ]; then
        echo "Cloning ${repo}..."
        git clone "https://github.com/mbgulden/${repo}.git" "${GITHUB_DIR}/${repo}"
    else
        echo "Updating ${repo}..."
        cd "${GITHUB_DIR}/${repo}"
        git fetch origin
        git checkout -B main origin/main --force 2>/dev/null || true
    fi
done

# 3. Install All 5 in Editable Mode
echo ""
echo "Installing all 5 primitives in Editable Mode (pip install -e .)..."
cd "${GITHUB_DIR}"
$PYTHON_CMD -m pip install -e ./swarmlock -e ./swarmproof -e ./swarmgate -e ./swarmsaga -e ./swarmledger

# 4. Configure Antigravity Hooks
HOOK_DIR="${HOME}/.gemini/antigravity-cli"
mkdir -p "${HOOK_DIR}"
HOOK_SCRIPT="${GITHUB_DIR}/Hermes/.agents/hooks/prismatic_hook.py"

cat << 'EOF' > "${HOOK_DIR}/hooks.json"
{
  "hooks": [
    {
      "event": "pre_tool_invocation",
      "tools": ["read_file", "view_file", "view_file_outline", "grep_search", "find_by_name", "write_to_file", "replace_file_content", "run_command"],
      "command": "python3 /home/ubuntu/.antigravity/hooks/prismatic_hook.py --stage pre"
    },
    {
      "event": "post_tool_invocation",
      "tools": ["write_to_file", "replace_file_content", "run_command"],
      "command": "python3 /home/ubuntu/.antigravity/hooks/prismatic_hook.py --stage post"
    }
  ]
}
EOF

echo "Configured Antigravity Hooks at ${HOOK_DIR}/hooks.json"
echo ""
echo "============================================================"
echo "    Prismatic Agent Hypervisor Successfully Provisioned!    "
echo "============================================================"