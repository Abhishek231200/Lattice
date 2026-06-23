#!/bin/bash
set -e

# Start postgres_exporter in the background
export DATA_SOURCE_NAME="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}?sslmode=disable"
postgres_exporter &

# Start Postgres (hands off to the official docker-entrypoint.sh)
exec docker-entrypoint.sh "$@"
