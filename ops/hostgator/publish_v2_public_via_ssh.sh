#!/usr/bin/env bash
set -euo pipefail

required_env=(
  SSH_HOST
  SSH_USER
  PUBLICATION_ROOT
  PUBLIC_PATH
  BUILD_ID
  PACKAGE_SHA
  PACKAGE_DIR
  SSH_KEY_FILE
  SSH_KNOWN_HOSTS_FILE
)
for name in "${required_env[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "missing required environment variable: $name" >&2
    exit 64
  fi
done

SSH_PORT=${SSH_PORT:-22}
REMOTE_INSTALLER_SH=${REMOTE_INSTALLER_SH:-ops/hostgator/install_v2_public_remote.sh}
PYTHON_INSTALLER=${PYTHON_INSTALLER:-api/v2/install_public_generation.py}
TRANSFER_ATTEMPTS=${TRANSFER_ATTEMPTS:-4}

[[ "$SSH_PORT" =~ ^[0-9]+$ ]] || {
  echo "invalid SSH_PORT" >&2
  exit 64
}
[[ "$TRANSFER_ATTEMPTS" =~ ^[1-9][0-9]*$ ]] || {
  echo "invalid TRANSFER_ATTEMPTS" >&2
  exit 64
}
[[ -d "$PACKAGE_DIR" ]] || {
  echo "missing package directory: $PACKAGE_DIR" >&2
  exit 66
}
[[ -f "$PYTHON_INSTALLER" ]] || {
  echo "missing Python installer: $PYTHON_INSTALLER" >&2
  exit 66
}
[[ -f "$REMOTE_INSTALLER_SH" ]] || {
  echo "missing remote installer: $REMOTE_INSTALLER_SH" >&2
  exit 66
}
[[ -f "$SSH_KEY_FILE" ]] || {
  echo "missing SSH key file" >&2
  exit 66
}
[[ -f "$SSH_KNOWN_HOSTS_FILE" ]] || {
  echo "missing SSH known_hosts file" >&2
  exit 66
}

remote="$SSH_USER@$SSH_HOST"
incoming="$PUBLICATION_ROOT/incoming/$BUILD_ID"
payload_root=$(mktemp -d "${RUNNER_TEMP:-/tmp}/v2-hostgator-payload.XXXXXX")
payload_archive="${RUNNER_TEMP:-/tmp}/${BUILD_ID}.payload.tar.gz"

cleanup_local() {
  rm -rf -- "$payload_root"
  rm -f -- "$payload_archive"
}
trap cleanup_local EXIT

mkdir -p "$payload_root/package"
cp -a "$PACKAGE_DIR"/. "$payload_root/package/"
install -m 600 "$PYTHON_INSTALLER" "$payload_root/install_public_generation.py"
install -m 700 "$REMOTE_INSTALLER_SH" "$payload_root/install_v2_public_remote.sh"
tar -C "$payload_root" -czf "$payload_archive" \
  package install_public_generation.py install_v2_public_remote.sh

ssh_opts=(
  -i "$SSH_KEY_FILE"
  -p "$SSH_PORT"
  -o BatchMode=yes
  -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$SSH_KNOWN_HOSTS_FILE"
  -o GlobalKnownHostsFile=/dev/null
  -o UpdateHostKeys=no
  -o ConnectTimeout=20
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=4
  -o TCPKeepAlive=yes
)
scp_opts=(
  -i "$SSH_KEY_FILE"
  -P "$SSH_PORT"
  -o BatchMode=yes
  -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$SSH_KNOWN_HOSTS_FILE"
  -o GlobalKnownHostsFile=/dev/null
  -o UpdateHostKeys=no
  -o ConnectTimeout=20
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=4
  -o TCPKeepAlive=yes
)

retry() {
  local attempt=1
  local delay=5
  local status=0
  while true; do
    if "$@"; then
      return 0
    else
      status=$?
    fi
    if (( attempt >= TRANSFER_ATTEMPTS )); then
      echo "transport command failed after $attempt attempt(s): $*" >&2
      return "$status"
    fi
    echo "transport command failed on attempt $attempt; retrying in ${delay}s" >&2
    sleep "$delay"
    attempt=$((attempt + 1))
    delay=$((delay * 2))
  done
}

retry ssh "${ssh_opts[@]}" "$remote" \
  "rm -rf '$incoming' && mkdir -p '$incoming'"

# Upload one compressed payload instead of hundreds of small JSON files. Shared
# hosting SSH/SFTP sessions are materially more reliable with one short transfer.
retry scp "${scp_opts[@]}" "$payload_archive" \
  "$remote:$incoming/payload.tar.gz"

retry ssh "${ssh_opts[@]}" "$remote" \
  "cd '$incoming' && tar -xzf payload.tar.gz && rm -f payload.tar.gz && chmod 700 install_v2_public_remote.sh"

# The installer is idempotent for the same build_id/package hash, so retrying
# the command is safe if the SSH client loses the session after the remote
# process has already completed the atomic pointer switch.
retry ssh "${ssh_opts[@]}" "$remote" \
  "bash '$incoming/install_v2_public_remote.sh' \
     '$incoming/package' \
     '$PUBLICATION_ROOT' \
     '$PUBLIC_PATH' \
     '$BUILD_ID' \
     '$PACKAGE_SHA' \
     '$incoming/install_public_generation.py'"

retry ssh "${ssh_opts[@]}" "$remote" "rm -rf '$incoming'"
