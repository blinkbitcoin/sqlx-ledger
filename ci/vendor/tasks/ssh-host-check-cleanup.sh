#!/usr/bin/env bash
# Release gate for site SSH Docker hosts. It runs in the job's ensure step,
# before the pool release, even when the workload failed. Only a cleanup
# receipt matching the claimed metadata releases the host; anything else keeps
# the claim as quarantine for operator recovery.
set -euo pipefail
metadata=docker-host/metadata
if ! jq -e '.transport == "ssh"' "$metadata" > /dev/null 2>&1; then
  echo 'Unexpected host transport; retain claim' >&2
  exit 1
fi
if ! cmp -s "$metadata" host-cleanup/metadata; then
  echo 'SSH host quarantined: cleanup unconfirmed; retain claim for operator recovery' >&2
  exit 1
fi
