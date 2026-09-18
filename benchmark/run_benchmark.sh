#!/bin/bash

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="$APP_DIR/httpserver"
BENCH_DIR="$APP_DIR/benchmark"
RAW_CSV="$BENCH_DIR/raw_results1.csv"
PORT=33334
WRK_THREADS=8
WRK_DURATION=3s
RUNS=10
URL="http://127.0.0.1:$PORT/index.html"

CONFIGS=(
    "3 4"
    "3 8"
)

CONNECTIONS=(20000 40000 80000 100000)

echo "listeners,workers,connections,run,requests_per_sec,avg_latency_ms,transfer_mb_s" > "$RAW_CSV"

for cfg in "${CONFIGS[@]}"; do
    read -r L W <<< "$cfg"
    echo "=== Starting server: listeners=$L, workers=$W ==="

    cd "$APP_DIR"
    mkdir -p logs
    "$SERVER" -p "$PORT" -l "$L" -w "$W" &
    SERVER_PID=$!
    sleep 1

    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Server failed to start for L=$L W=$W"
        continue
    fi

    for CONN in "${CONNECTIONS[@]}"; do
        echo "  --- connections=$CONN ---"
        for run in $(seq 1 $RUNS); do
            OUTPUT=$(wrk -t"$WRK_THREADS" -c"$CONN" -d"$WRK_DURATION" "$URL" 2>&1)

            RPS=$(echo "$OUTPUT" | grep "Requests/sec:" | awk '{print $2}') || true

            TRANSFER_LINE=$(echo "$OUTPUT" | grep "Transfer/sec:")
            TRANSFER_VAL=$(echo "$TRANSFER_LINE" | awk '{print $2}' | sed 's/[A-Za-z]*$//')
            TRANSFER_UNIT=$(echo "$TRANSFER_LINE" | awk '{print $2}' | sed 's/[0-9.]*//')
            case "$TRANSFER_UNIT" in
                GB) TRANSFER=$(awk "BEGIN{printf \"%.2f\", $TRANSFER_VAL*1024}");;
                MB) TRANSFER="$TRANSFER_VAL";;
                KB) TRANSFER=$(awk "BEGIN{printf \"%.4f\", $TRANSFER_VAL/1024}");;
                *)  TRANSFER="$TRANSFER_VAL";;
            esac

            LAT_VAL=$(echo "$OUTPUT" | grep "Latency" | head -1 | awk '{print $2}')
            LAT_UNIT=$(echo "$LAT_VAL" | sed 's/[0-9.]*//')
            LAT_NUM=$(echo "$LAT_VAL" | sed 's/[A-Za-z]*$//')
            case "$LAT_UNIT" in
                us) LAT_MS=$(awk "BEGIN{printf \"%.4f\", $LAT_NUM/1000}");;
                ms) LAT_MS="$LAT_NUM";;
                s)  LAT_MS=$(awk "BEGIN{printf \"%.2f\", $LAT_NUM*1000}");;
                *)  LAT_MS="$LAT_NUM";;
            esac

            [ -z "$RPS" ] && RPS="0"
            [ -z "$LAT_MS" ] && LAT_MS="0"
            [ -z "$TRANSFER" ] && TRANSFER="0"

            echo "$L,$W,$CONN,$run,$RPS,$LAT_MS,$TRANSFER" >> "$RAW_CSV"
            printf "    run %2d/%d: %10s req/s  lat=%sms\n" "$run" "$RUNS" "$RPS" "$LAT_MS"
        done
    done

    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    sleep 1
    echo ""
done

echo "Done. Raw results: $RAW_CSV"
