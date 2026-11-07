#!/bin/sh
set -eu
if [ "$#" -gt 0 ]; then
    exec "$@"
fi
if [ ! -f /var/lib/prost/prost.toml ]; then
    umask 077
    cp /etc/prost/seed.toml /var/lib/prost/prost.toml
fi
exec prost run --config /var/lib/prost/prost.toml
