#!/usr/bin/env bash
# Run one claimed workload on a pinned, private site SSH Docker host.
# Used by the run_on_nix_host and test_on_docker_host fragments when a site is given.
set +x
set -euo pipefail
umask 077
# Remove credentials from every child environment, including validation tools.
private_key=${SSH_PRIVATE_KEY:-}
google_credentials=${GOOGLE_CREDENTIALS:-}
export -n private_key google_credentials
unset SSH_PRIVATE_KEY SSH_PUB_KEY GOOGLE_CREDENTIALS

# A receipt belongs to this task attempt only. Missing output fails closed.
mkdir -p host-cleanup
rm -f host-cleanup/metadata host-cleanup/metadata.tmp

fail() { echo "SSH Docker host task failed: $1" >&2; exit 1; }
metadata=docker-host/metadata
tasks=$(cd -- "$(dirname -- "$0")" && pwd)
root=
run_dir=
active_pid=
# Set once the claimed metadata is valid, cleared just before the first SSH
# command. Failures in between never touched the host, so they release it.
untouched=

# A background timeout lets Bash handle cancellation immediately while wait
# is interrupted. GNU timeout also bounds descendants after TERM with KILL.
bounded() {
  local duration=$1 status=0
  shift
  timeout --kill-after=15s "$duration" "$@" <&0 &
  active_pid=$!
  wait "$active_pid" || status=$?
  active_pid=
  return "$status"
}

# shellcheck disable=SC2317,SC2329 # Invoked by the EXIT trap.
cleanup() {
  local status=$?
  trap - EXIT
  trap '' HUP INT TERM
  if [[ -n $active_pid ]]; then
    kill -TERM "$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
  fi
  # Only this function may write the receipt; drop anything written earlier
  # (repository code runs in this task before the host is contacted). A path
  # that cannot be removed must not stop the remote cleanup below.
  local confirmed='' stale=''
  rm -rf -- host-cleanup/metadata host-cleanup/metadata.tmp 2>/dev/null || stale=1
  if [[ -n $run_dir ]]; then
    if bounded 60s "${ssh[@]}" "bash -s -- $run_dir" < "$tasks/ssh-host-remote-cleanup.sh"; then
      confirmed=1
    else
      echo 'SSH Docker host cleanup failed; inspect host before reuse' >&2
      [[ $status != 0 ]] || status=1
    fi
  elif [[ -n $untouched ]]; then
    echo 'Failed before contacting the SSH host; releasing the claim' >&2
    confirmed=1
  fi
  if [[ -n $confirmed && -n $stale ]]; then
    echo 'Could not remove an earlier cleanup receipt; retain host claim' >&2
    [[ $status != 0 ]] || status=1
  elif [[ -n $confirmed ]]; then
    if ! { cp "$metadata" host-cleanup/metadata.tmp &&
           mv host-cleanup/metadata.tmp host-cleanup/metadata; }; then
      echo 'Could not record SSH cleanup; retain host claim' >&2
      [[ $status != 0 ]] || status=1
    fi
  fi
  [[ -z $root ]] || rm -rf -- "$root"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
trap 'exit 129' HUP

# Fail before host access when the task image lacks a required client.
for dependency in jq openssl curl ssh rsync timeout envsubst; do
  command -v "$dependency" >/dev/null 2>&1 || fail "task image is missing $dependency"
done

# Validate before extracting: jq -r must not hide trailing newlines or coerce
# nonstrings. Only canonical RFC1918 IPv4 addresses are accepted.
jq -se '
  length == 1 and (.[0] |
  .transport == "ssh" and
  (.docker_host_name | type == "string" and test("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\\z")) and
  (.ssh_host_key | type == "string" and test("^ssh-ed25519 [A-Za-z0-9+/]+={0,2}\\z")) and
  (.ssh_host | type == "string" and test("^(0|[1-9][0-9]{0,2})(\\.(0|[1-9][0-9]{0,2})){3}\\z") and
    (split(".") | map(tonumber) | all(. <= 255) and
      (.[0] == 10 or (.[0] == 172 and .[1] >= 16 and .[1] <= 31) or
       (.[0] == 192 and .[1] == 168)))))
' "$metadata" >/dev/null 2>&1 || fail 'invalid host metadata'
name=$(jq -r '.docker_host_name' "$metadata")
host=$(jq -r '.ssh_host' "$metadata")
host_key=$(jq -r '.ssh_host_key' "$metadata")
echo "Running on SSH Docker host: $name"
untouched=1

# REPO_PATH is removed on the host before each copy: one plain path component.
repo=${REPO_PATH:-}
package=${PACKAGE_DIR-.}
mode=${SSH_HOST_MODE:-}
minutes=${WORKLOAD_TIMEOUT_MINUTES:-}
[[ $repo =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || fail 'invalid workload path'
[[ $package =~ ^[A-Za-z0-9_.][A-Za-z0-9_./-]*$ && /$package/ != */../* ]] || fail 'invalid workload path'
[[ $minutes =~ ^[1-9][0-9]{0,2}$ ]] || fail 'invalid workload timeout'
case $mode in
  nix)
    payload=${CMD:-}
    [[ $payload =~ [^[:space:]] ]] || fail 'missing workload input'
    ;;
  compose)
    payload=${TEST_CONTAINER:-}
    [[ $payload =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || fail 'invalid test container'
    ;;
  *) fail 'unsupported workload mode' ;;
esac
[[ -d $repo ]] || fail 'missing workload input'
[[ -n $private_key && -n $google_credentials ]] || fail 'missing CI credentials'

# Names the transport or the remote supervisor relies on cannot be overridden.
param_keys=()
read -r -a requested_keys <<< "${PARAMS_KEYS:-}"
# The ${a[@]+...} form keeps an empty list valid under set -u in Bash 3.2.
for key in ${requested_keys[@]+"${requested_keys[@]}"}; do
  [[ $key =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail 'invalid PARAMS_KEYS entry'
  case $key in
    PATH|HOME|SHELL|IFS|ENV|BASH_ENV|TMPDIR|LD_*|CI_RUN_DIR|CI_MODE|CI_TIMEOUT|REPO_PATH|PACKAGE_DIR|CMD|TEST_CONTAINER|DOCKER_CONFIG)
      fail "reserved PARAMS_KEYS entry: $key" ;;
  esac
  param_keys+=("$key")
done

# Repository preparation runs on the worker, as on the GCP hosts, and before
# any secret is written to disk. The transport credentials are unset, so they
# are neither inherited nor substituted. Same-user code can still read this
# process's initial environment in /proc, as it could on the GCP path.
if [[ $mode == nix ]]; then
  if [[ -f $repo/.env.ci ]]; then
    envsubst < "$repo/.env.ci" > "$repo/tmp.env.ci"
  fi
else
  (cd "$repo" && make create-tmp-env-ci) || true
fi

root=$(mktemp -d "${TMPDIR:-/tmp}/ssh-host-ci.XXXXXXXX")
# mktemp inherits TMPDIR: reject quotes/backslashes before rsync parses -e.
[[ $root != *[\"\\]* ]] || fail 'unsupported temporary directory'
printf '%s\n' "$private_key" > "$root/key"
printf '%s\n' "$google_credentials" > "$root/google.json"
printf '%s %s\n' "$host" "$host_key" > "$root/known_hosts"
unset private_key google_credentials
# Reject a corrupt secret before SSH. The key's token_uri is not trusted.
jq -e '.type == "service_account" and (.client_email | type == "string") and (.private_key | type == "string")' \
  "$root/google.json" >/dev/null 2>&1 || fail 'invalid service-account key'
email=$(jq -r '.client_email' "$root/google.json")
[[ $email =~ ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$ ]] || fail 'invalid service-account key'
jq -r '.private_key' "$root/google.json" > "$root/sa.pem"
grep -qxF -e '-----BEGIN PRIVATE KEY-----' "$root/sa.pem" || fail 'invalid service-account key'
grep -qxF -e '-----END PRIVATE KEY-----' "$root/sa.pem" || fail 'invalid service-account key'

# Additional job params reach the workload through a private file, never argv.
: > "$root/env"
for key in ${param_keys[@]+"${param_keys[@]}"}; do
  if [[ -n ${!key:-} ]]; then
    printf 'export %s=%q\n' "$key" "${!key}" >> "$root/env"
  fi
done

options=(ssh -i "$root/key" -o BatchMode=yes -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$root/known_hosts"
  -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=3)
ssh=("${options[@]}" "ci@$host")

untouched=
bounded 60s "${ssh[@]}" "bash -s -- $name" < "$tasks/ssh-host-remote-preflight.sh"
bounded 60s "${ssh[@]}" 'umask 077; mkdir -p .ci-run; mktemp -d .ci-run/run.XXXXXXXX' > "$root/run-dir"
candidate=$(cat "$root/run-dir")
[[ $candidate =~ ^\.ci-run/run\.[A-Za-z0-9]{8}$ ]] || fail 'invalid remote run directory'
run_dir=$candidate
# Docker bind mounts can leave root-owned files; the GCP hosts also used sudo.
# Large root-owned build trees can take minutes to delete; same bound as rsync.
bounded 300s "${ssh[@]}" "docker ps -aq | xargs -r docker rm -fv && sudo -n rm -rf -- $repo"
# rsync parses -e with its own shell-like tokenizer, not Bash printf %q.
printf -v rsync_ssh '"%s" ' "${options[@]}"
bounded 300s rsync -avr --delete --exclude='buck-out/**' --exclude='**/node_modules/**' \
  --exclude='dev/.data/**' -e "$rsync_ssh" "$repo/" "ci@$host:$repo" > /dev/null
# Do not use gcloud auth print-access-token. Those stable commands have no
# --scopes flag and mint CLOUDSDK_SCOPES (cloud-platform, compute, appengine,
# sql). GCR pull needs only devstorage.read_only. The JSON key stays on the worker.
now=$(date +%s)
iat=$((now - 60))
exp=$((iat + 3600))
header=$(printf '%s' '{"alg":"RS256","typ":"JWT"}' | openssl base64 -A | tr '+/' '-_' | tr -d '=')
payload_claims=$(jq -nc \
  --arg iss "$email" \
  --arg scope "https://www.googleapis.com/auth/devstorage.read_only" \
  --arg aud "https://oauth2.googleapis.com/token" \
  --argjson iat "$iat" \
  --argjson exp "$exp" \
  '{iss:$iss,scope:$scope,aud:$aud,iat:$iat,exp:$exp}' \
  | openssl base64 -A | tr '+/' '-_' | tr -d '=')
signing="${header}.${payload_claims}"
signature=$(printf '%s' "$signing" | openssl dgst -sha256 -sign "$root/sa.pem" -binary \
  | openssl base64 -A | tr '+/' '-_' | tr -d '=')
printf '%s' "${signing}.${signature}" > "$root/assertion"
bounded 60s curl --fail --silent --show-error --max-time 30 \
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer" \
  --data-urlencode "assertion@${root}/assertion" \
  --output "$root/token.json" \
  "https://oauth2.googleapis.com/token"
token=$(jq -r '.access_token // empty' "$root/token.json")
[[ $token =~ ^[A-Za-z0-9._~+/=-]+$ ]] || fail 'no registry access token'
# Token stays on stdin; the service-account JSON never reaches the guest.
bounded 60s "${ssh[@]}" "docker --config $run_dir/docker login us.gcr.io --username oauth2accesstoken --password-stdin" <<< "$token" > /dev/null
unset token email signing signature
# Job params follow the same stdin path into the private run directory.
bounded 60s "${ssh[@]}" "umask 077; cat > $run_dir/env" < "$root/env"
printf -v remote_command 'bash -s -- %q %q %q %q %q %q' "$run_dir" "$repo" "$package" "$mode" "$minutes" "$payload"
bounded "$((minutes * 60 + 200))s" "${ssh[@]}" "$remote_command" < "$tasks/ssh-host-remote-run.sh"
