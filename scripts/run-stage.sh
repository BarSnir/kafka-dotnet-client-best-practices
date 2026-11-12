#!/bin/sh
set -eu
case "${1:-}" in
    standard|high-throughput|throttle) stage=$1 ;;
    *) echo 'Expected standard, high-throughput or throttle' >&2; exit 2 ;;
esac
cd "$(dirname "$0")/.."
producer="producer-$stage"
consumer="consumer-$stage"
cleanup() {
    docker compose --profile "$stage" stop -t 35 "$producer" "$consumer" &&
        docker compose --profile "$stage" rm -f "$producer" "$consumer"
}
trap cleanup 0
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'Stage %s. Press Ctrl+C to stop its clients.\n' "$stage"
docker compose --profile "$stage" up --build --no-deps --abort-on-container-exit "$producer" "$consumer"
