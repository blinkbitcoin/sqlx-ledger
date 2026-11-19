#!/usr/bin/env bash
set -euo pipefail
run_dir=$1
[[ $run_dir =~ ^\.ci-run/run\.[A-Za-z0-9]{8}$ ]] || exit 1
# Zombies cannot execute or hold credentials; their parent may reap them later.
# A failed process listing is uncertainty, so it must never prove cleanup.
group_gone() {
  ps -eo pgid=,stat= | awk -v group="$pid" '
    $1 == group && $2 !~ /^Z/ { alive = 1 }
    END { exit alive ? 1 : 0 }'
}
wait_for_group() {
  local attempt
  for ((attempt=0; attempt<10; attempt++)); do
    group_gone && return 0
    sleep 0.2
  done
  group_gone
}
status=0
if test -f "$run_dir/runner.pid"; then
  pid=$(cat "$run_dir/runner.pid")
  if [[ ! $pid =~ ^[1-9][0-9]*$ ]]; then
    status=1
  elif ! group_gone; then
    # Signal only a leader bearing this invocation's marker. If its identity
    # cannot be proven, retain the claim rather than signal a recycled PID.
    if test -r "/proc/$pid/environ" &&
       tr '\0' '\n' < "/proc/$pid/environ" | grep -qxF "CI_RUN_DIR=$HOME/$run_dir"; then
      kill -TERM -- "-$pid" 2>/dev/null || true
      if ! wait_for_group; then
        kill -KILL -- "-$pid" 2>/dev/null || true
        wait_for_group || status=1
      fi
    else
      status=1
    fi
  fi
elif test -f "$run_dir/command.sh"; then
  # The supervisor writes command.sh before setsid. Without a published PID,
  # a detached workload may exist but cannot be identified safely.
  status=1
fi
# Credential and job-param cleanup finishes before any directory is removed.
rm -rf -- "$run_dir/docker" "$run_dir/env"
# Keep the four newest run directories (logs and launch evidence), as the
# blink core GCP host task does. Never remove this attempt's directory here.
# A nullglob array, not compgen: Bash builds without programmable completion
# (nixpkgs' non-interactive bash) lack compgen, which would skip the prune.
shopt -s nullglob
runs=(.ci-run/run.*)
shopt -u nullglob
if [[ ${#runs[@]} -gt 0 ]]; then
  kept=0
  # shellcheck disable=SC2012 # Same newest-first order as the GCP host prune; each name is checked before rm.
  ls -dt -- "${runs[@]}" | while IFS= read -r path; do
    [[ $path =~ ^\.ci-run/run\.[A-Za-z0-9]+$ ]] || continue
    [ -L "$path" ] && continue
    [ -d "$path" ] || continue
    kept=$((kept + 1))
    if [ "$kept" -ge 5 ] && [ "$path" != "$run_dir" ]; then
      rm -rf -- "$path"
    fi
  done
fi
if [[ $status != 0 ]]; then
  echo 'Remote workload termination could not be confirmed' >&2
fi
exit "$status"
