#!/usr/bin/env bash
set -euo pipefail

# Local one-shot setup for PhaseE-04 monitor against machine2.
#
# Assumption:
#   The UFlow daemon on machine2 is started separately and is listening on UF_SOCKET.
#
# This script manages everything else from the local machine:
#   1. ensure remote monitor API is running in machine2 container
#   2. ensure local -> host1 -> machine2 API channel exists
#   3. ensure local static monitor UI is running

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UI_DIR="${ROOT_DIR}/ui/uflow-monitor"
RUNTIME_DIR="${UFLOW_MONITOR_RUNTIME_DIR:-/tmp/uflow_phasee04_monitor}"

HOST1_SSH="${UFLOW_HOST1_SSH:-}"
HOST1_KEY="${UFLOW_HOST1_KEY:-}"
HOST2_SSH="${UFLOW_HOST2_SSH:-}"
HOST2_KEY_ON_HOST1="${UFLOW_HOST2_KEY_ON_HOST1:-}"
CONTAINER="${UFLOW_REMOTE_CONTAINER:-openeuler-2403-DS}"
REMOTE_WORKDIR="${UFLOW_REMOTE_WORKDIR:-/home/sj/git/data-service}"
REMOTE_RUNTIME_DIR="${UFLOW_REMOTE_RUNTIME_DIR:-/tmp/uflow_phasee04_monitor}"

UF_SOCKET="${UF_SOCKET:-/tmp/uflow_phasee04.sock}"
REMOTE_API_HOST="${UF_MONITOR_HOST:-127.0.0.1}"
REMOTE_API_PORT="${UF_MONITOR_PORT:-8082}"
LOCAL_API_HOST="${UFLOW_LOCAL_API_HOST:-127.0.0.1}"
LOCAL_API_PORT="${UFLOW_LOCAL_API_PORT:-18082}"
HOST1_FORWARD_PORT="${UFLOW_HOST1_FORWARD_PORT:-28082}"
UI_HOST="${UFLOW_UI_HOST:-127.0.0.1}"
UI_PORT="${UFLOW_UI_PORT:-3000}"
API_BASE="${UFLOW_API_BASE:-http://${LOCAL_API_HOST}:${LOCAL_API_PORT}}"
UI_URL="http://${UI_HOST}:${UI_PORT}/?api=${API_BASE}"

mkdir -p "${RUNTIME_DIR}"
TUNNEL_PID_FILE="${RUNTIME_DIR}/machine2_tunnel.pid"
UI_PID_FILE="${RUNTIME_DIR}/ui.pid"

log() {
  printf '[uflow-monitor] %s\n' "$*"
}

require_remote_config() {
  local missing=0
  for name in UFLOW_HOST1_SSH UFLOW_HOST1_KEY UFLOW_HOST2_SSH UFLOW_HOST2_KEY_ON_HOST1; do
    if [[ -z "${!name:-}" ]]; then
      log "missing required env: ${name}"
      missing=1
    fi
  done
  if [[ "${missing}" -ne 0 ]]; then
    log "set remote SSH env vars before start/reconnect; no personal host/key defaults are stored in this repo"
    exit 2
  fi
}

read_pid() {
  local file="$1"
  [[ -f "${file}" ]] && tr -d '[:space:]' < "${file}" || true
}

is_running() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

http_ok() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fsS --max-time 2 "${url}" >/dev/null 2>&1
  else
    python3 - "${url}" >/dev/null 2>&1 <<'PY'
import sys
from urllib.request import urlopen
with urlopen(sys.argv[1], timeout=2) as resp:
    if resp.status >= 400:
        raise SystemExit(1)
PY
  fi
}

local_port_open() {
  python3 - "$1" "$2" >/dev/null 2>&1 <<'PY'
import socket
import sys
host = sys.argv[1]
port = int(sys.argv[2])
with socket.create_connection((host, port), timeout=1):
    pass
PY
}

local_port_free() {
  python3 - "$1" "$2" >/dev/null 2>&1 <<'PY'
import socket
import sys
host = sys.argv[1]
port = int(sys.argv[2])
s = socket.socket()
try:
    s.bind((host, port))
finally:
    s.close()
PY
}

host1_port_open() {
  ssh -i "${HOST1_KEY}" "${HOST1_SSH}" \
    "python3 - '${HOST1_FORWARD_PORT}' >/dev/null 2>&1 <<'PY'
import socket
import sys
with socket.create_connection(('127.0.0.1', int(sys.argv[1])), timeout=1):
    pass
PY"
}

remote_container_bash() {
  local script="$1"
  local encoded
  encoded="$(printf '%s' "${script}" | base64 | tr -d '\n')"
  ssh -i "${HOST1_KEY}" "${HOST1_SSH}" \
    "source ~/.bashrc; next \"docker exec ${CONTAINER} bash -lc 'echo ${encoded} | base64 -d | bash'\""
}

ensure_remote_monitor_api() {
  require_remote_config
  log "checking remote monitor API on machine2 container ${CONTAINER}"
  remote_container_bash "
set -euo pipefail
cd '${REMOTE_WORKDIR}'
mkdir -p '${REMOTE_RUNTIME_DIR}'
if [[ ! -S '${UF_SOCKET}' ]]; then
  echo 'daemon_socket_missing socket=${UF_SOCKET}'
  echo 'start uf-daemon separately, then rerun this local script'
  exit 20
fi
if python3 - '${REMOTE_API_HOST}' '${REMOTE_API_PORT}' >/dev/null 2>&1 <<'PY'
import socket
import sys
from urllib.request import urlopen
host = sys.argv[1]
port = int(sys.argv[2])
with urlopen(f'http://{host}:{port}/healthz', timeout=2) as resp:
    if resp.status >= 400:
        raise SystemExit(1)
PY
then
  echo 'remote_monitor_api_running url=http://${REMOTE_API_HOST}:${REMOTE_API_PORT}'
  exit 0
fi
echo 'remote_monitor_api_starting url=http://${REMOTE_API_HOST}:${REMOTE_API_PORT}'
export UF_SOCKET='${UF_SOCKET}'
export UF_MONITOR_HOST='${REMOTE_API_HOST}'
export UF_MONITOR_PORT='${REMOTE_API_PORT}'
export UFLOW_API_BASE='${API_BASE}'
export UFLOW_UI_URL='${UI_URL}'
export UFLOW_TUNNEL_PATH='local:${LOCAL_API_PORT} -> host1:${HOST1_FORWARD_PORT} -> machine2:${REMOTE_API_PORT} container:${CONTAINER}'
nohup python3 tools/uflow_monitor_api.py \
  --socket '${UF_SOCKET}' \
  --host '${REMOTE_API_HOST}' \
  --port '${REMOTE_API_PORT}' \
  > '${REMOTE_RUNTIME_DIR}/monitor_api.log' 2>&1 &
echo \$! > '${REMOTE_RUNTIME_DIR}/monitor_api.pid'
for _ in \$(seq 1 30); do
  if python3 - '${REMOTE_API_HOST}' '${REMOTE_API_PORT}' >/dev/null 2>&1 <<'PY'
from urllib.request import urlopen
import sys
host = sys.argv[1]
port = int(sys.argv[2])
with urlopen(f'http://{host}:{port}/healthz', timeout=2) as resp:
    if resp.status >= 400:
        raise SystemExit(1)
PY
  then
    echo 'remote_monitor_api_started pid='\"\$(cat '${REMOTE_RUNTIME_DIR}/monitor_api.pid')\"
    exit 0
  fi
  sleep 0.3
done
echo 'remote_monitor_api_failed log=${REMOTE_RUNTIME_DIR}/monitor_api.log'
tail -n 80 '${REMOTE_RUNTIME_DIR}/monitor_api.log' || true
exit 21
"
}

ensure_tunnel() {
  require_remote_config
  if http_ok "${API_BASE}/healthz"; then
    log "tunnel/channel already healthy api=${API_BASE}"
    return
  fi

  local old_pid
  old_pid="$(read_pid "${TUNNEL_PID_FILE}")"
  if is_running "${old_pid}"; then
    log "managed tunnel pid=${old_pid} exists but health failed; restarting"
    kill "${old_pid}" >/dev/null 2>&1 || true
    rm -f "${TUNNEL_PID_FILE}"
  fi

  if local_port_open "${LOCAL_API_HOST}" "${LOCAL_API_PORT}"; then
    log "local port ${LOCAL_API_HOST}:${LOCAL_API_PORT} is occupied but /healthz is not healthy"
    log "set UFLOW_LOCAL_API_PORT to another port, or stop the occupying process"
    exit 22
  fi
  if ! local_port_free "${LOCAL_API_HOST}" "${LOCAL_API_PORT}"; then
    log "local port ${LOCAL_API_HOST}:${LOCAL_API_PORT} is not available"
    exit 22
  fi

  log "starting local API channel ${LOCAL_API_HOST}:${LOCAL_API_PORT} -> host1:${HOST1_FORWARD_PORT} -> machine2:${REMOTE_API_PORT}"
  if host1_port_open; then
    log "host1 forward port ${HOST1_FORWARD_PORT} already exists; creating local leg only"
    ssh -i "${HOST1_KEY}" \
      -o ExitOnForwardFailure=yes \
      -o ServerAliveInterval=30 \
      -N \
      -L "${LOCAL_API_HOST}:${LOCAL_API_PORT}:127.0.0.1:${HOST1_FORWARD_PORT}" \
      "${HOST1_SSH}" \
      > "${RUNTIME_DIR}/tunnel.log" 2>&1 &
  else
    log "creating nested SSH tunnel via host1"
    ssh -i "${HOST1_KEY}" \
      -o ExitOnForwardFailure=yes \
      -o ServerAliveInterval=30 \
      -L "${LOCAL_API_HOST}:${LOCAL_API_PORT}:127.0.0.1:${HOST1_FORWARD_PORT}" \
      "${HOST1_SSH}" \
      "sudo ssh -i '${HOST2_KEY_ON_HOST1}' -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -N -L 127.0.0.1:${HOST1_FORWARD_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT} '${HOST2_SSH}'" \
      > "${RUNTIME_DIR}/tunnel.log" 2>&1 &
  fi
  echo "$!" > "${TUNNEL_PID_FILE}"

  for _ in $(seq 1 40); do
    if http_ok "${API_BASE}/healthz"; then
      log "tunnel_started pid=$(cat "${TUNNEL_PID_FILE}") api=${API_BASE}"
      return
    fi
    sleep 0.25
  done
  log "tunnel_failed log=${RUNTIME_DIR}/tunnel.log"
  tail -n 80 "${RUNTIME_DIR}/tunnel.log" || true
  exit 23
}

ensure_ui() {
  if http_ok "http://${UI_HOST}:${UI_PORT}/"; then
    log "ui already running url=${UI_URL}"
    return
  fi

  local old_pid
  old_pid="$(read_pid "${UI_PID_FILE}")"
  if is_running "${old_pid}"; then
    log "managed ui pid=${old_pid} exists but health failed; restarting"
    kill "${old_pid}" >/dev/null 2>&1 || true
    rm -f "${UI_PID_FILE}"
  fi

  if [[ ! -d "${UI_DIR}" ]]; then
    log "missing UI dir: ${UI_DIR}"
    exit 24
  fi
  if local_port_open "${UI_HOST}" "${UI_PORT}" && ! http_ok "http://${UI_HOST}:${UI_PORT}/"; then
    log "UI port ${UI_HOST}:${UI_PORT} is occupied by another process"
    exit 24
  fi

  log "starting local monitor UI url=${UI_URL}"
  (
    cd "${UI_DIR}"
    nohup python3 -m http.server "${UI_PORT}" --bind "${UI_HOST}" > "${RUNTIME_DIR}/ui.log" 2>&1 &
    echo "$!" > "${UI_PID_FILE}"
  )
  for _ in $(seq 1 20); do
    if http_ok "http://${UI_HOST}:${UI_PORT}/"; then
      log "ui_started pid=$(cat "${UI_PID_FILE}") url=${UI_URL}"
      return
    fi
    sleep 0.25
  done
  log "ui_failed log=${RUNTIME_DIR}/ui.log"
  tail -n 80 "${RUNTIME_DIR}/ui.log" || true
  exit 25
}

status() {
  log "runtime_dir=${RUNTIME_DIR}"
  log "ui_url=${UI_URL}"
  log "api_base=${API_BASE}"
  local pid
  pid="$(read_pid "${UI_PID_FILE}")"
  if is_running "${pid}"; then
    log "ui=running pid=${pid}"
  else
    log "ui=not-managed-or-stopped"
  fi
  pid="$(read_pid "${TUNNEL_PID_FILE}")"
  if is_running "${pid}"; then
    log "tunnel=running pid=${pid}"
  else
    log "tunnel=not-managed-or-stopped"
  fi
  if http_ok "${API_BASE}/healthz"; then
    log "api=healthy"
    if command -v curl >/dev/null 2>&1; then
      curl -fsS "${API_BASE}/healthz"
      printf '\n'
    fi
  else
    log "api=offline"
  fi
}

stop_local() {
  local pid
  pid="$(read_pid "${UI_PID_FILE}")"
  if is_running "${pid}"; then
    kill "${pid}" >/dev/null 2>&1 || true
    log "ui_stopped pid=${pid}"
  fi
  rm -f "${UI_PID_FILE}"

  pid="$(read_pid "${TUNNEL_PID_FILE}")"
  if is_running "${pid}"; then
    kill "${pid}" >/dev/null 2>&1 || true
    log "tunnel_stopped pid=${pid}"
  fi
  rm -f "${TUNNEL_PID_FILE}"
  log "remote monitor API and uf-daemon are left untouched"
}

case "${1:-start}" in
  start)
    ensure_remote_monitor_api
    ensure_tunnel
    ensure_ui
    status
    ;;
  status)
    status
    ;;
  reconnect)
    pid="$(read_pid "${TUNNEL_PID_FILE}")"
    if is_running "${pid}"; then
      kill "${pid}" >/dev/null 2>&1 || true
      rm -f "${TUNNEL_PID_FILE}"
    fi
    ensure_tunnel
    status
    ;;
  stop-local)
    stop_local
    ;;
  *)
    echo "usage: $0 {start|status|reconnect|stop-local}" >&2
    exit 2
    ;;
esac
