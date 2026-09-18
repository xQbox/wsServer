#!/bin/bash
# Нагрузочный прогон WS-агрегатора: перебор числа источников и частоты,
# throughput и задержка источник->sink через benchmark/ws_benchmark.py.
set -u

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="$APP_DIR/httpserver"
BENCH_DIR="$APP_DIR/benchmark"
OUT_CSV="$BENCH_DIR/ws_summary.csv"
PORT=33344
DURATION=5

SOURCES_LIST=(1 2 4 8 16)
RATE_LIST=(50 200)

rm -f "$OUT_CSV"

cd "$APP_DIR"
mkdir -p logs

if [ ! -x "$SERVER" ]; then
    echo "Собираю сервер..."
    make >/dev/null
fi

echo "=== Запуск сервера (порт $PORT) ==="
"$SERVER" -p "$PORT" -w 8 -l 2 --hwm 4096 --lwm 2048 &
SERVER_PID=$!
sleep 1

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ОШИБКА: сервер не запустился"
    exit 1
fi

for SOURCES in "${SOURCES_LIST[@]}"; do
    for RATE in "${RATE_LIST[@]}"; do
        echo "--- источников=$SOURCES, частота=$RATE/сек, длительность=${DURATION}с ---"
        python3 "$BENCH_DIR/ws_benchmark.py" \
            --host 127.0.0.1 --port "$PORT" \
            --sources "$SOURCES" --rate "$RATE" --duration "$DURATION" \
            --out "$OUT_CSV"
    done
done

kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true

echo ""
echo "Готово. Результаты: $OUT_CSV"
column -s, -t "$OUT_CSV" 2>/dev/null || cat "$OUT_CSV"
