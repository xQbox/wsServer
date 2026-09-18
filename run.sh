#!/usr/bin/env bash

args=()

for port in $(seq 9001 9010); do
    args+=(
        --upstream
        "ws://127.0.0.1:${port}"
    )
done

exec ./httpserver \
    -p 33333 \
    --upstream "ws://127.0.0.1:50001"
