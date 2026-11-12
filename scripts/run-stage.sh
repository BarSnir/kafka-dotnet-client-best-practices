#!/bin/sh
set -eu
case "${1:-}" in
    standard|high-throughput|throttle) stage=$1 ;;
    *) echo 'Expected standard, high-throughput or throttle' >&2; exit 2 ;;
esac
cd "$(dirname "$0")/.."
producer="producer-$stage"
consumer="consumer-$stage"
tmp=$(mktemp -d)
started=false
followers=
compose() { docker compose --profile "$stage" "$@"; }
quiet() {
    if "$@" >"$tmp/command.log" 2>&1; then return 0; fi
    echo "[$stage] Docker command failed" >&2
    tail -n 20 "$tmp/command.log" >&2
    return 1
}
cleanup() {
    status=$?
    trap - 0
    trap '' INT TERM
    if "$started"; then
        echo "[$stage] Stopping clients"
        quiet compose stop -t 35 "$producer" "$consumer" && quiet compose rm -f "$producer" "$consumer" || status=1
    fi
    for pid in $followers; do kill "$pid" 2>/dev/null || :; done
    wait || :
    rm -rf "$tmp"
    exit "$status"
}
trap cleanup 0
trap 'exit 130' INT
trap 'exit 143' TERM

echo "[$stage] Building clients"
quiet compose build --quiet "$producer" "$consumer"
echo "[$stage] Starting clients (Ctrl+C to stop)"
started=true
quiet compose up -d --no-build --no-deps "$producer" "$consumer"
mkfifo "$tmp/rates"
while IFS= read -r line; do
    case "$line" in
        'throughput client.id='*) printf '[%s] %s\n' "$stage" "$line" ;;
    esac
done <"$tmp/rates" &
followers=$!
producer_ids=$(compose ps -aq "$producer")
consumer_ids=$(compose ps -aq "$consumer")
[ -n "$producer_ids" ] && [ -n "$consumer_ids" ] || { echo "[$stage] Missing client container" >&2; exit 1; }
for id in $producer_ids; do
    docker logs -f "$id" >"$tmp/rates" &
    followers="$followers $!"
done
for id in $consumer_ids; do
    docker logs -f "$id" >/dev/null &
    followers="$followers $!"
done

while :; do
    states=$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}} {{.Name}}' $producer_ids $consumer_ids)
    stopped=$(printf '%s\n' "$states" | awk '$1 != "running" {print; exit}')
    if [ -n "$stopped" ]; then
        echo "[$stage] Client stopped: $stopped" >&2
        exit 1
    fi
    sleep 1
done
