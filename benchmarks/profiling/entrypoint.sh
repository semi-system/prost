#!/bin/sh
set -eu
umask 077
cp /etc/prost/seed.toml /var/lib/prost/prost.toml
exec "$@"
