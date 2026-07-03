#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UI_DIR="${ROOT_DIR}/ui/uflow-monitor"
RUNTIME_DIR="${UFLOW_MONITOR_RUNTIME_DIR:-/tmp/uflow_phasee04_monitor}"
UI_HOST="${UFLOW_UI_HOST:-127.0.0.1}"
UI_PORT="${UFLOW_UI_PORT:-3000}"
LOCAL_API_HOST="${UFLOW_LOCAL_API_HOST:-127.0.0.1}"
LOCAL_API_PORT="${UFLOW_LOCAL_API_PORT:-18082}"
REMOTE_API_HOST="${UF_MONITOR_HOST:-127.0.0.1}"
REMOTE_API_PORT="${UF_MONITOR_PORT:-8082}"
REMOTE_SSH="${UFLOW_REMOTE_SSH:-}"
JUMP_SSH="${UFLOW_JUMP_SSH:-}"
MANAGE_TUNNEL="${UFLOW_MANAGE_TUNNEL:-0}"
API_BASE="${UFLOW_API_BASE:-http://${LOCAL_API_HOST}:${LOCAL_API_PORT}}"

mkdir -p "${RUNTIME_DIR}"

ui_pid_file="${RUNTIME_DIR}/ui.pid"
tunnel_pid_file="${RUNTIME_DIR}/tunnel.pid"

is_running() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

read_pid() {
  local file="$1"
  [[ -f "${file}" ]] && tr -d '[:space:]' < "${file}" || true
}

start_ui() {
  local pid
  pid="$(read_pid "${ui_pid_file}")"
  if is_running "${pid}"; then
    echo "ui_running pid=${pid} url=http://${UI_HOST}:${UI_PORT}/?api=${API_BASE}"
    return
  fi
  local server_pid
  server_pid="$(
    cd "${UI_DIR}"
    nohup python3 -m http.server "${UI_PORT}" --bind "${UI_HOST}" > "${RUNTIME_DIR}/ui.log" 2>&1 &
    echo "$!"
  )"
  echo "${server_pid}" > "${ui_pid_file}"
  echo "ui_started pid=${server_pid} url=http://${UI_HOST}:${UI_PORT}/?api=${API_BASE}"
}

start_tunnel() {
  if [[ "${MANAGE_TUNNEL}" != "1" ]]; then
    echo "tunnel_unmanaged api_base=${API_BASE}"
    return
  fi
  if [[ -z "${REMOTE_SSH}" ]]; then
    echo "UFLOW_REMOTE_SSH is required when UFLOW_MANAGE_TUNNEL=1" >&2
    exit 2
  fi
  local pid
  pid="$(read_pid "${tunnel_pid_file}")"
  if is_running "${pid}"; then
    echo "tunnel_running pid=${pid} api_base=${API_BASE}"
    return
  fi
  local ssh_args=(-N -L "${LOCAL_API_HOST}:${LOCAL_API_PORT}:${REMOTE_API_HOST}:${REMOTE_API_PORT}")
  if [[ -n "${JUMP_SSH}" ]]; then
    ssh_args=(-J "${JUMP_SSH}" "${ssh_args[@]}")
  fi
  ssh "${ssh_args[@]}" "${REMOTE_SSH}" > "${RUNTIME_DIR}/tunnel.log" 2>&1 &
  echo "$!" > "${tunnel_pid_file}"
  echo "tunnel_started pid=$(cat "${tunnel_pid_file}") api_base=${API_BASE}"
}

check_api() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsS "${API_BASE}/healthz" || return 1
  else
    python3 - "${API_BASE}/healthz" <<'PY'
import json
import sys
from urllib.request import urlopen

with urlopen(sys.argv[1], timeout=2) as resp:
    print(resp.read().decode())
PY
  fi
}

status() {
  local ui_pid tunnel_pid
  ui_pid="$(read_pid "${ui_pid_file}")"
  tunnel_pid="$(read_pid "${tunnel_pid_file}")"
  if is_running "${ui_pid}"; then
    echo "ui=running pid=${ui_pid} url=http://${UI_HOST}:${UI_PORT}/?api=${API_BASE}"
  else
    echo "ui=stopped pid=${ui_pid:-none}"
  fi
  if [[ "${MANAGE_TUNNEL}" == "1" ]]; then
    if is_running "${tunnel_pid}"; then
      echo "tunnel=running pid=${tunnel_pid} api_base=${API_BASE}"
    else
      echo "tunnel=stopped pid=${tunnel_pid:-none}"
    fi
  else
    echo "tunnel=unmanaged api_base=${API_BASE}"
  fi
  check_api || true
}

stop_local() {
  local pid
  pid="$(read_pid "${ui_pid_file}")"
  if is_running "${pid}"; then
    kill "${pid}"
    echo "ui_stopped pid=${pid}"
  fi
  rm -f "${ui_pid_file}"
  if [[ "${MANAGE_TUNNEL}" == "1" ]]; then
    pid="$(read_pid "${tunnel_pid_file}")"
    if is_running "${pid}"; then
      kill "${pid}"
      echo "tunnel_stopped pid=${pid}"
    fi
    rm -f "${tunnel_pid_file}"
  fi
}

case "${1:-start}" in
  start)
    start_tunnel
    start_ui
    status
    ;;
  status)
    status
    ;;
  reconnect)
    if [[ "${MANAGE_TUNNEL}" == "1" ]]; then
      pid="$(read_pid "${tunnel_pid_file}")"
      if is_running "${pid}"; then
        kill "${pid}"
      fi
      rm -f "${tunnel_pid_file}"
    fi
    start_tunnel
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
