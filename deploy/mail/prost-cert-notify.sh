#!/bin/sh
# Host-only delivery notification. Mount the certificates volume read-only into
# the named mail services. Each service must support SIGHUP certificate reload.
set -eu
[ "$#" -gt 0 ] || { echo 'Usage: prost-cert-notify CONTAINER...' >&2; exit 2; }
for container in "$@"; do
    case "$container" in
        ''|-*|*[!A-Za-z0-9_.-]*) echo 'Invalid container name' >&2; exit 2 ;;
    esac
done
for container in "$@"; do
    docker kill --signal HUP "$container"
done
