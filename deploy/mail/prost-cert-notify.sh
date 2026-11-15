#!/bin/sh
# Host-only delivery notification. Mount the certificates volume read-only into
# the named mail services. Each service must support SIGHUP certificate reload.
set -eu
[ "$#" -ge 3 ] && [ "$1" = --agent-container ] || { echo 'Usage: prost-cert-notify --agent-container CERTD CONTAINER...' >&2; exit 2; }
agent=$2
shift 2
for container in "$agent" "$@"; do
    case "$container" in
        ''|-*|*[!A-Za-z0-9_.-]*) echo 'Invalid container name' >&2; exit 2 ;;
    esac
done
for container in "$@"; do
    docker kill --signal HUP "$container"
done
docker exec "$agent" prost-certd check --certificates
