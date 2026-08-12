#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UI_DIR="${ROOT_DIR}/ui/uflow-monitor"
RUNTIME_DIR="${UFLOW_MONITOR_RUNTIME_DIR:-/tmp/uflow_phasee04_monitor}"

REMOTE_SSH="${UFLOW_REMOTE_SSH:-serving-b}"
REMOTE_CONTAINER="${UFLOW_REMOTE_CONTAINER:-openeuler-2403-DS}"
REMOTE_WORKDIR="${UFLOW_REMOTE_WORKDIR:-/home/sj/git/data-service}"
REMOTE_RUNTIME_DIR="${UFLOW_REMOTE_RUNTIME_DIR:-/tmp/uflow_phasee04_monitor}"
UF_SOCKET="${UF_SOCKET:-/tmp/uflow_phasee04.sock}"
REMOTE_API_HOST="${UF_MONITOR_HOST:-127.0.0.1}"
REMOTE_API_PORT="${UF_MONITOR_PORT:-8082}"
LOCAL_API_HOST="${UFLOW_LOCAL_API_HOST:-127.0.0.1}"
LOCAL_API_PORT="${UFLOW_LOCAL_API_PORT:-18082}"
UI_HOST="${UFLOW_UI_HOST:-127.0.0.1}"
UI_PORT="${UFLOW_UI_PORT:-3000}"
API_BASE="${UFLOW_API_BASE:-http://${LOCAL_API_HOST}:${LOCAL_API_PORT}}"
UI_URL="http://${UI_HOST}:${UI_PORT}/?api=${API_BASE}"
OPEN_BROWSER="${UFLOW_OPEN_BROWSER:-1}"

mkdir -p "${RUNTIME_DIR}"
TUNNEL_PID_FILE="${RUNTIME_DIR}/tunnel.pid"
UI_PID_FILE="${RUNTIME_DIR}/ui.pid"

log() {
  printf '[uflow-monitor] %s\n' "$*"
}

read_pid() {
  local file="$1"
  [[ -f "${file}" ]] && tr -d '[:space:]' < "${file}" || true
}

process_command() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] || return 1
  ps -p "${pid}" -o command= 2>/dev/null || true
}

managed_process_running() {
  local pid="${1:-}"
  local expected="$2"
  local command
  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" >/dev/null 2>&1 || return 1
  command="$(process_command "${pid}")"
  [[ "${command}" == *"${expected}"* ]]
}

stop_managed_process() {
  local file="$1"
  local expected="$2"
  local label="$3"
  local pid command
  pid="$(read_pid "${file}")"
  if [[ -z "${pid}" ]]; then
    return
  fi
  command="$(process_command "${pid}")"
  if managed_process_running "${pid}" "${expected}"; then
    kill "${pid}"
    log "${label}_stopped pid=${pid}"
  elif [[ -n "${command}" ]]; then
    log "${label}_pid_not_owned pid=${pid}; command mismatch, process left untouched"
  fi
  rm -f "${file}"
}

http_ok() {
  local url="$1"
  curl -fsS --max-time 3 "${url}" >/dev/null 2>&1
}

local_port_open() {
  python3 - "$1" "$2" >/dev/null 2>&1 <<'PY'
import socket
import sys
with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=1):
    pass
PY
}

remote_container_bash() {
  local script="$1"
  printf '%s\n' "${script}" | ssh "${REMOTE_SSH}" "docker exec -i '${REMOTE_CONTAINER}' bash -s"
}

preflight() {
  log "preflight ssh=${REMOTE_SSH} container=${REMOTE_CONTAINER} repo=${REMOTE_WORKDIR}"
  ssh "${REMOTE_SSH}" \
    "set -e; printf 'ssh_alias=%s\\n' '${REMOTE_SSH}'; printf 'hostname=%s\\n' \"\$(hostname)\"; docker inspect -f 'container_id={{.Id}} image={{.Config.Image}} state={{.State.Status}}' '${REMOTE_CONTAINER}'; docker exec '${REMOTE_CONTAINER}' bash -lc 'set -e; cd \"${REMOTE_WORKDIR}\"; printf \"repo=%s\\n\" \"\$PWD\"; printf \"commit=%s\\n\" \"\$(git -c safe.directory=${REMOTE_WORKDIR} rev-parse HEAD)\"; printf \"dirty_entries=%s\\n\" \"\$(git -c safe.directory=${REMOTE_WORKDIR} status --short | wc -l)\"; uname -r; if [[ -S \"${UF_SOCKET}\" ]]; then echo daemon_socket=ready; else echo daemon_socket=missing; exit 20; fi'"
}

remote_api_ok() {
  remote_container_bash "
python3 - '${REMOTE_API_HOST}' '${REMOTE_API_PORT}' <<'PY'
import sys
from urllib.request import urlopen
with urlopen(f'http://{sys.argv[1]}:{int(sys.argv[2])}/healthz', timeout=3) as response:
    if response.status >= 400:
        raise SystemExit(1)
PY
" >/dev/null 2>&1
}

ensure_remote_monitor_api() {
  if remote_api_ok; then
    log "remote_monitor_api=reused url=http://${REMOTE_API_HOST}:${REMOTE_API_PORT}"
    return
  fi
  log "starting remote monitor API inside ${REMOTE_CONTAINER}"
  remote_container_bash "
set -euo pipefail
cd '${REMOTE_WORKDIR}'
mkdir -p '${REMOTE_RUNTIME_DIR}'
if [[ ! -S '${UF_SOCKET}' ]]; then
  echo 'daemon socket missing: ${UF_SOCKET}' >&2
  exit 20
fi
export UF_SOCKET='${UF_SOCKET}'
export UF_MONITOR_HOST='${REMOTE_API_HOST}'
export UF_MONITOR_PORT='${REMOTE_API_PORT}'
export UFLOW_API_BASE='${API_BASE}'
export UFLOW_UI_URL='${UI_URL}'
export UFLOW_TUNNEL_PATH='local -> ssh ${REMOTE_SSH} -> ${REMOTE_CONTAINER}'
nohup python3 tools/uflow_monitor_api.py \
  --socket '${UF_SOCKET}' \
  --host '${REMOTE_API_HOST}' \
  --port '${REMOTE_API_PORT}' \
  > '${REMOTE_RUNTIME_DIR}/monitor_api.log' 2>&1 &
echo \$! > '${REMOTE_RUNTIME_DIR}/monitor_api.pid'
for _attempt in \$(seq 1 30); do
  if python3 - '${REMOTE_API_HOST}' '${REMOTE_API_PORT}' <<'PY' >/dev/null 2>&1
import sys
from urllib.request import urlopen
with urlopen(f'http://{sys.argv[1]}:{int(sys.argv[2])}/healthz', timeout=2) as response:
    if response.status >= 400:
        raise SystemExit(1)
PY
  then
    echo remote_monitor_api_started pid=\$(cat '${REMOTE_RUNTIME_DIR}/monitor_api.pid')
    exit 0
  fi
  sleep 0.2
done
tail -n 80 '${REMOTE_RUNTIME_DIR}/monitor_api.log' >&2 || true
exit 21
"
}

ensure_tunnel() {
  local pid
  if http_ok "${API_BASE}/healthz"; then
    log "local_api=healthy api_base=${API_BASE}"
    return
  fi
  pid="$(read_pid "${TUNNEL_PID_FILE}")"
  if managed_process_running "${pid}" "${LOCAL_API_HOST}:${LOCAL_API_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT}"; then
    log "managed tunnel is running but health failed; reconnecting"
    stop_managed_process "${TUNNEL_PID_FILE}" "${LOCAL_API_HOST}:${LOCAL_API_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT}" tunnel
  fi
  if local_port_open "${LOCAL_API_HOST}" "${LOCAL_API_PORT}"; then
    log "local API port ${LOCAL_API_HOST}:${LOCAL_API_PORT} is occupied but /healthz is not healthy"
    exit 22
  fi
  ssh \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=20 \
    -o ServerAliveCountMax=3 \
    -N \
    -L "${LOCAL_API_HOST}:${LOCAL_API_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT}" \
    "${REMOTE_SSH}" \
    > "${RUNTIME_DIR}/tunnel.log" 2>&1 &
  echo "$!" > "${TUNNEL_PID_FILE}"
  for _attempt in $(seq 1 40); do
    if http_ok "${API_BASE}/healthz"; then
      log "tunnel=started pid=$(cat "${TUNNEL_PID_FILE}") api_base=${API_BASE}"
      return
    fi
    sleep 0.25
  done
  tail -n 80 "${RUNTIME_DIR}/tunnel.log" >&2 || true
  exit 23
}

ui_ok() {
  curl -fsS --max-time 2 "http://${UI_HOST}:${UI_PORT}/" 2>/dev/null | grep -q 'UFlow Monitor'
}

ensure_ui() {
  local pid
  if ui_ok; then
    log "ui=reused url=${UI_URL}"
    return
  fi
  pid="$(read_pid "${UI_PID_FILE}")"
  if managed_process_running "${pid}" "http.server ${UI_PORT}"; then
    stop_managed_process "${UI_PID_FILE}" "http.server ${UI_PORT}" ui
  fi
  if local_port_open "${UI_HOST}" "${UI_PORT}"; then
    log "UI port ${UI_HOST}:${UI_PORT} is occupied by another process"
    exit 24
  fi
  (
    cd "${UI_DIR}"
    nohup python3 -m http.server "${UI_PORT}" --bind "${UI_HOST}" > "${RUNTIME_DIR}/ui.log" 2>&1 &
    echo "$!" > "${UI_PID_FILE}"
  )
  for _attempt in $(seq 1 20); do
    if ui_ok; then
      log "ui=started pid=$(cat "${UI_PID_FILE}") url=${UI_URL}"
      return
    fi
    sleep 0.2
  done
  tail -n 80 "${RUNTIME_DIR}/ui.log" >&2 || true
  exit 25
}

write_config_snapshot() {
  {
    printf 'UFLOW_REMOTE_SSH=%s\n' "${REMOTE_SSH}"
    printf 'UFLOW_REMOTE_CONTAINER=%s\n' "${REMOTE_CONTAINER}"
    printf 'UFLOW_REMOTE_WORKDIR=%s\n' "${REMOTE_WORKDIR}"
    printf 'UF_SOCKET=%s\n' "${UF_SOCKET}"
    printf 'UF_MONITOR_PORT=%s\n' "${REMOTE_API_PORT}"
    printf 'UFLOW_LOCAL_API_PORT=%s\n' "${LOCAL_API_PORT}"
    printf 'UFLOW_UI_PORT=%s\n' "${UI_PORT}"
    printf 'UFLOW_API_BASE=%s\n' "${API_BASE}"
  } > "${RUNTIME_DIR}/config.env"
}

write_status_snapshot() {
  python3 - "${API_BASE}" "${UI_URL}" "${RUNTIME_DIR}/status.json" <<'PY'
import json
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

base, ui_url, output = sys.argv[1:]
result = {"api_base": base, "ui_url": ui_url, "generated_at": int(time.time()), "endpoints": {}}
for path in ("/healthz", "/v1/config", "/v1/stats", "/v1/transfers?limit=1"):
    try:
        with urlopen(base.rstrip("/") + path, timeout=3) as response:
            result["endpoints"][path] = {"status": response.status, "payload": json.loads(response.read())}
    except Exception as exc:
        result["endpoints"][path] = {"status": None, "error": str(exc)}
with open(output, "w", encoding="utf-8") as stream:
    json.dump(result, stream, indent=2, sort_keys=True)
print(json.dumps(result, indent=2, sort_keys=True))
PY
}

status() {
  local ui_pid tunnel_pid
  ui_pid="$(read_pid "${UI_PID_FILE}")"
  tunnel_pid="$(read_pid "${TUNNEL_PID_FILE}")"
  log "ssh_alias=${REMOTE_SSH} container=${REMOTE_CONTAINER} repo=${REMOTE_WORKDIR}"
  log "ui_url=${UI_URL}"
  log "api_base=${API_BASE}"
  if managed_process_running "${ui_pid}" "http.server ${UI_PORT}"; then
    log "ui=managed-running pid=${ui_pid}"
  elif ui_ok; then
    log "ui=healthy-unmanaged"
  else
    log "ui=offline"
  fi
  if managed_process_running "${tunnel_pid}" "${LOCAL_API_HOST}:${LOCAL_API_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT}"; then
    log "tunnel=managed-running pid=${tunnel_pid}"
  elif http_ok "${API_BASE}/healthz"; then
    log "tunnel=healthy-unmanaged"
  else
    log "tunnel=offline"
  fi
  write_status_snapshot
}

open_browser() {
  if [[ "${OPEN_BROWSER}" != "1" ]]; then
    return
  fi
  if command -v open >/dev/null 2>&1; then
    open "${UI_URL}" >/dev/null 2>&1 || true
  fi
}

stop_local() {
  stop_managed_process "${UI_PID_FILE}" "http.server ${UI_PORT}" ui
  stop_managed_process "${TUNNEL_PID_FILE}" "${LOCAL_API_HOST}:${LOCAL_API_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT}" tunnel
  log "remote monitor API and uf-daemon were left untouched"
}

case "${1:-start}" in
  start)
    write_config_snapshot
    preflight
    ensure_remote_monitor_api
    ensure_tunnel
    ensure_ui
    status
    open_browser
    ;;
  status)
    status
    ;;
  reconnect)
    stop_managed_process "${TUNNEL_PID_FILE}" "${LOCAL_API_HOST}:${LOCAL_API_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT}" tunnel
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
