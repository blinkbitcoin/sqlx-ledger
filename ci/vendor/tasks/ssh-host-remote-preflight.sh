#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = "$1"
test -f /var/lib/docker-host/ready
docker info > /dev/null
nix store ping --store daemon
# Inspect the canonical output of containerd, not arbitrary TOML input. Root
# must occur exactly once at top level; both supported quote styles are valid.
containerd config dump | awk '
  /^[[:space:]]*\[/ { section = 1 }
  !section && /^[[:space:]]*root[[:space:]]*=/ {
    count++
    sub(/^[[:space:]]*root[[:space:]]*=[[:space:]]*/, "")
    sub(/[[:space:]]*$/, "")
    if ($0 == "\"/var/lib/docker-host/containerd\"" ||
        $0 == "\047/var/lib/docker-host/containerd\047") valid++
  }
  END { exit !(count == 1 && valid == 1) }
'
test "$(findmnt -n -o TARGET --target /var/lib/docker-host/containerd)" = /var/lib/docker-host
