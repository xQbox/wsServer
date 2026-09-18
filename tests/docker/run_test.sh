#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

COMPOSE="docker compose -f $SCRIPT_DIR/compose.yml"

cd "$PROJECT_ROOT"

cleanup() {
    $COMPOSE down \
        --volumes \
        --remove-orphans
}

trap cleanup EXIT

echo "=== Building containers ==="
$COMPOSE build

echo "=== Starting 100 upstreams + aggregator ==="

$COMPOSE up \
    -d \
    mock-upstreams \
    aggregator

echo "=== Waiting for 100 WebSocket connections ==="

for attempt in $(seq 1 60); do
    logs="$($COMPOSE logs --no-color aggregator)"

    open_count="$(
        printf '%s\n' "$logs" \
        | grep -oE 'upstream [0-9]+: WebSocket OPEN' \
        | sort -u \
        | wc -l
    )"

    echo "OPEN: ${open_count}/100"

    if [ "$open_count" -ge 100 ]; then
        break
    fi

    sleep 0.5
done

logs="$($COMPOSE logs --no-color aggregator)"

open_count="$(
    printf '%s\n' "$logs" \
    | grep -oE 'upstream [0-9]+: WebSocket OPEN' \
    | sort -u \
    | wc -l
)"

if [ "$open_count" -ne 100 ]; then
    echo "FAIL: only ${open_count}/100 upstreams connected"
    $COMPOSE logs aggregator
    exit 1
fi

echo "=== Waiting for data from all upstreams ==="

for attempt in $(seq 1 60); do
    logs="$($COMPOSE logs --no-color aggregator)"

    source_count="$(
        printf '%s\n' "$logs" \
        | grep -oE 'upstream [0-9]+: message len=' \
        | sed -E 's/.*upstream ([0-9]+).*/\1/' \
        | sort -u \
        | wc -l
    )"

    echo "DATA: ${source_count}/100"

    if [ "$source_count" -ge 100 ]; then
        break
    fi

    sleep 0.5
done

logs="$($COMPOSE logs --no-color aggregator)"

source_count="$(
    printf '%s\n' "$logs" \
    | grep -oE 'upstream [0-9]+: message len=' \
    | sed -E 's/.*upstream ([0-9]+).*/\1/' \
    | sort -u \
    | wc -l
)"

if [ "$source_count" -ne 100 ]; then
    echo "FAIL: received data from only ${source_count}/100 upstreams"
    exit 1
fi

echo "=== Testing aggregator /feed ==="

$COMPOSE run \
    --rm \
    verifier

echo
echo "========================================"
echo " PASS"
echo " 100/100 WebSockets connected"
echo " 100/100 WebSockets produced data"
echo " /feed produced messages"
echo "========================================"
