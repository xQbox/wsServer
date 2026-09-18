#!/usr/bin/env python3
"""
Нагрузочный тест WS-агрегатора: N источников -> один sink, throughput и
задержка источник->sink. Использует tests/ws_client.py (голый socket,
без внешних зависимостей — websocat/python-websockets в окружении нет).

Метка времени кладётся дважды: клиентом — в payload (для полной задержки
источник->sink), сервером — в поле "t" JSON-обёртки (Msg_t.ts,
CLOCK_MONOTONIC на момент приёма) — по разнице между ними считается
задержка "агрегатор->sink" отдельно от сетевой задержки до источника.

Пример:
    python3 benchmark/ws_benchmark.py --host 127.0.0.1 --port 33333 \
        --sources 8 --rate 200 --duration 5 --out benchmark/ws_summary.csv
"""
import argparse
import csv
import json
import os
import re
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
from ws_client import connect, OP_TEXT  # noqa: E402


def source_worker(host, port, src_idx, rate, duration, stop_evt, sent_counter, lock):
    url = "ws://{}:{}/bench{}".format(host, port, src_idx)
    conn, status, _ = connect(url)
    if conn is None:
        print("источник {}: рукопожатие не удалось: {}".format(src_idx, status), file=sys.stderr)
        return

    interval = 1.0 / rate if rate > 0 else 0
    deadline = time.time() + duration
    count = 0
    try:
        while time.time() < deadline and not stop_evt.is_set():
            payload = "%.9f" % time.time()  # чистая метка времени отправки
            conn.send_frame(OP_TEXT, payload.encode())
            count += 1
            if interval:
                time.sleep(interval)
    except (BrokenPipeError, ConnectionError, OSError):
        pass
    finally:
        with lock:
            sent_counter[0] += count
        try:
            conn.sock.close()
        except OSError:
            pass


TAG_RE = re.compile(r'"t":([0-9.]+)')


def sink_worker(host, port, duration, grace, results):
    url = "ws://{}:{}/feed".format(host, port)
    conn, status, _ = connect(url)
    if conn is None:
        print("sink: рукопожатие не удалось: {}".format(status), file=sys.stderr)
        return
    conn.sock.settimeout(1.0)

    deadline = time.time() + duration + grace
    e2e_ms, agg_ms = [], []
    received = 0
    while time.time() < deadline:
        try:
            opcode, payload, _fin = conn.recv_frame()
        except Exception:
            continue
        if opcode != OP_TEXT:
            continue
        now = time.time()
        try:
            obj = json.loads(payload.decode())
            send_ts = float(obj["data"])
            server_ts = float(obj["t"])
        except (ValueError, KeyError, UnicodeDecodeError):
            continue
        received += 1
        e2e_ms.append((now - send_ts) * 1000.0)
        agg_ms.append((now - server_ts) * 1000.0)

    try:
        conn.sock.close()
    except OSError:
        pass

    results["received"] = received
    results["e2e_ms"] = e2e_ms
    results["agg_ms"] = agg_ms


def pct(data, p):
    if not data:
        return float("nan")
    s = sorted(data)
    k = int(round((p / 100.0) * (len(s) - 1)))
    return s[max(0, min(k, len(s) - 1))]


def run_once(host, port, sources, rate, duration):
    stop_evt = threading.Event()
    sent_counter = [0]
    lock = threading.Lock()
    results = {}

    sink_thread = threading.Thread(target=sink_worker, args=(host, port, duration, 2.0, results))
    sink_thread.start()
    time.sleep(0.3)  # дать sink'у подключиться до старта источников

    src_threads = []
    for i in range(sources):
        t = threading.Thread(target=source_worker,
                              args=(host, port, i, rate, duration, stop_evt, sent_counter, lock))
        src_threads.append(t)
        t.start()

    for t in src_threads:
        t.join()
    sink_thread.join()

    sent = sent_counter[0]
    received = results.get("received", 0)
    e2e_ms = results.get("e2e_ms", [])
    agg_ms = results.get("agg_ms", [])

    return {
        "sources": sources,
        "rate_per_source": rate,
        "duration_s": duration,
        "sent": sent,
        "received": received,
        "loss": sent - received,
        "throughput_msg_s": round(received / duration, 2) if duration else 0,
        "e2e_p50_ms": round(pct(e2e_ms, 50), 3),
        "e2e_p99_ms": round(pct(e2e_ms, 99), 3),
        "agg_p50_ms": round(pct(agg_ms, 50), 3),
        "agg_p99_ms": round(pct(agg_ms, 99), 3),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=33333)
    ap.add_argument("--sources", type=int, default=4)
    ap.add_argument("--rate", type=float, default=50, help="сообщений/сек на один источник")
    ap.add_argument("--duration", type=float, default=5.0)
    ap.add_argument("--out", default=None, help="CSV-файл для дозаписи одной строки результата")
    args = ap.parse_args()

    row = run_once(args.host, args.port, args.sources, args.rate, args.duration)
    print(json.dumps(row, ensure_ascii=False, indent=2))

    if args.out:
        exists = os.path.isfile(args.out)
        with open(args.out, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not exists:
                w.writeheader()
            w.writerow(row)


if __name__ == "__main__":
    main()
