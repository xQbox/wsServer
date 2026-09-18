#!/usr/bin/env python3
"""
Тестовый WebSocket-клиент на чистом stdlib (RFC 6455) — для проверки
сервера-агрегатора без внешних зависимостей (websocat/python-websockets
в окружении разработки нет).

Подкоманды:
  handshake <url>              разовое рукопожатие, печатает ответ сервера
  src <url> [--rate N] [--count N] [--seconds S] [--text MSG]
                                подключается как источник, шлёт сообщения
  sink <url> [--count N] [--timeout S]
                                подключается как приёмник (--sink-path сервера),
                                печатает все входящие сообщения
  slow-sink <url> [--hold S]   подключается как приёмник и НЕ читает —
                                для проверки backpressure
  fuzz <url> [--max-frame N]   набор протокольных негативных кейсов (RFC 6455)

Примеры:
  python3 ws_client.py sink ws://127.0.0.1:33333/feed
  python3 ws_client.py src  ws://127.0.0.1:33333/s1 --rate 5
  python3 ws_client.py fuzz ws://127.0.0.1:33333/s1
"""
import argparse
import base64
import hashlib
import os
import socket
import struct
import sys
import time
from urllib.parse import urlparse

MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT, OP_TEXT, OP_BIN, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA


def make_key():
    return base64.b64encode(os.urandom(16)).decode()


def expected_accept(key):
    return base64.b64encode(hashlib.sha1((key + MAGIC).encode()).digest()).decode()


class WSConn:
    """Тонкая обёртка над сокетом: буферизация чтения + кодек кадров клиента."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def _fill(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("peer closed")
            self.buf += chunk

    def _take(self, n):
        if n == 0:
            return b""
        self._fill(n)
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def send_frame(self, opcode, payload=b"", fin=True):
        b0 = (0x80 if fin else 0) | (opcode & 0x0F)
        length = len(payload)
        mask_key = os.urandom(4)
        if length <= 125:
            header = bytes([b0, 0x80 | length])
        elif length <= 0xFFFF:
            header = bytes([b0, 0x80 | 126]) + struct.pack("!H", length)
        else:
            header = bytes([b0, 0x80 | 127]) + struct.pack("!Q", length)
        masked = bytes(c ^ mask_key[i % 4] for i, c in enumerate(payload))
        self.sock.sendall(header + mask_key + masked)

    def send_raw(self, data):
        self.sock.sendall(data)

    def recv_frame(self):
        b0, b1 = self._take(2)
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._take(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._take(8))[0]
        mask_key = self._take(4) if masked else None
        payload = self._take(length)
        if masked:
            payload = bytes(c ^ mask_key[i % 4] for i, c in enumerate(payload))
        return opcode, payload, fin


def raw_client_frame(b0, b1_no_mask_bit, masked_payload=b""):
    """Собирает МАСКИРОВАННЫЙ клиентский кадр напрямую из байт заголовка —
    для fuzz-кейсов, где нужен полный контроль над битами (RSV, opcode)."""
    mask_key = os.urandom(4)
    header = bytes([b0, 0x80 | b1_no_mask_bit])
    masked = bytes(c ^ mask_key[i % 4] for i, c in enumerate(masked_payload))
    return header + mask_key + masked


def parse_url(url):
    u = urlparse(url)
    if u.scheme not in ("ws", "wss"):
        raise ValueError("ожидается ws://host:port/path")
    host = u.hostname
    port = u.port or (443 if u.scheme == "wss" else 80)
    path = u.path or "/"
    if u.query:
        path += "?" + u.query
    return host, port, path


def connect(url, timeout=5):
    host, port, path = parse_url(url)
    sock = socket.create_connection((host, port), timeout=timeout)
    key = make_key()
    req = (
        "GET {path} HTTP/1.1\r\n"
        "Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    ).format(path=path, host=host, port=port, key=key)
    sock.sendall(req.encode())

    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("соединение закрыто во время рукопожатия")
        resp += chunk

    header_end = resp.index(b"\r\n\r\n")
    headers_raw = resp[:header_end].decode(errors="replace")
    tail = resp[header_end + 4:]
    status_line = headers_raw.split("\r\n")[0]

    if " 101 " not in status_line:
        return None, status_line, headers_raw

    accept = None
    for line in headers_raw.split("\r\n")[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            if k.strip().lower() == "sec-websocket-accept":
                accept = v.strip()
    if accept != expected_accept(key):
        raise RuntimeError("неверный Sec-WebSocket-Accept: получено {!r}".format(accept))

    conn = WSConn(sock)
    conn.buf = tail
    return conn, status_line, headers_raw


# ---------------------------------------------------------------- handshake

def cmd_handshake(args):
    conn, status, headers = connect(args.url)
    print(headers)
    if conn is None:
        print("РУКОПОЖАТИЕ НЕ УДАЛОСЬ:", status)
        sys.exit(1)
    print("Рукопожатие OK")
    conn.sock.close()


# ---------------------------------------------------------------------- src

def cmd_src(args):
    conn, status, headers = connect(args.url)
    if conn is None:
        print("рукопожатие не удалось:", status)
        sys.exit(1)
    print("подключён как источник:", status)

    count = 0
    start = time.time()
    interval = 1.0 / args.rate if args.rate > 0 else 0
    try:
        while args.count == 0 or count < args.count:
            msg = args.text if args.text else "msg-{} t={:.6f}".format(count, time.time())
            conn.send_frame(OP_TEXT, msg.encode())
            count += 1
            if args.verbose:
                print("отправлено:", msg)
            if args.seconds and (time.time() - start) >= args.seconds:
                break
            if interval:
                time.sleep(interval)
    except (BrokenPipeError, ConnectionError, OSError) as e:
        print("ошибка соединения:", e)
    finally:
        elapsed = time.time() - start
        print("отправлено {} сообщений за {:.2f}с".format(count, elapsed))
        try:
            conn.sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------- sink

def cmd_sink(args):
    conn, status, headers = connect(args.url)
    if conn is None:
        print("рукопожатие не удалось:", status)
        sys.exit(1)
    print("подключён как приёмник:", status)
    conn.sock.settimeout(args.timeout if args.timeout > 0 else None)

    count = 0
    try:
        while args.count == 0 or count < args.count:
            opcode, payload, _fin = conn.recv_frame()
            if opcode == OP_CLOSE:
                code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                print("получен CLOSE, код={}".format(code))
                break
            if opcode == OP_PING:
                conn.send_frame(OP_PONG, payload)
                continue
            if opcode in (OP_TEXT, OP_BIN):
                count += 1
                text = payload.decode(errors="replace") if opcode == OP_TEXT else repr(payload)
                print("[{}] {}".format(count, text))
    except socket.timeout:
        print("таймаут ожидания сообщений")
    except ConnectionError as e:
        print("соединение закрыто:", e)
    finally:
        try:
            conn.sock.close()
        except OSError:
            pass


# ---------------------------------------------------------------- slow-sink

def cmd_slow_sink(args):
    conn, status, headers = connect(args.url)
    if conn is None:
        print("рукопожатие не удалось:", status)
        sys.exit(1)
    print("подключён как МЕДЛЕННЫЙ приёмник (не читает), {}, держим {}с".format(status, args.hold))
    time.sleep(args.hold)
    conn.sock.close()
    print("slow-sink: соединение закрыто по таймауту удержания")


# -------------------------------------------------------------------- fuzz

def _read_close_or_drop(conn, timeout=2.0):
    conn.sock.settimeout(timeout)
    try:
        while True:
            opcode, payload, _fin = conn.recv_frame()
            if opcode == OP_CLOSE:
                if len(payload) >= 2:
                    return struct.unpack("!H", payload[:2])[0]
                return 1000
            # прочие кадры (например, эхо PONG) игнорируем, ждём именно CLOSE
    except (ConnectionError, socket.timeout, OSError):
        return "dropped"
    finally:
        try:
            conn.sock.close()
        except OSError:
            pass


def cmd_fuzz(args):
    def case_unmasked():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        # Немаскированный TEXT-кадр от "клиента" — запрещено RFC 6455 п.5.1
        conn.send_raw(bytes([0x81, 0x05]) + b"hello")
        return _read_close_or_drop(conn)

    def case_rsv():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_raw(raw_client_frame(0xC1, 2, b"hi"))  # FIN+RSV1+TEXT
        return _read_close_or_drop(conn)

    def case_reserved_opcode():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_raw(raw_client_frame(0x83, 2, b"hi"))  # opcode 0x3, зарезервирован
        return _read_close_or_drop(conn)

    def case_cont_no_open():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_raw(raw_client_frame(0x80, 2, b"hi"))  # FIN + CONT без открытого сообщения
        return _read_close_or_drop(conn)

    def case_new_msg_while_fragmenting():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_raw(raw_client_frame(0x01, 2, b"ab"))  # FIN=0 TEXT — открыли сообщение
        conn.send_raw(raw_client_frame(0x82, 2, b"cd"))  # новый BIN вместо CONT
        return _read_close_or_drop(conn)

    def case_frag_ping():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_raw(raw_client_frame(0x09, 2, b"hi"))  # FIN=0 PING — запрещено
        return _read_close_or_drop(conn)

    def case_close_len1():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_raw(raw_client_frame(0x88, 1, b"x"))
        return _read_close_or_drop(conn)

    def case_bad_close_code():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        payload = struct.pack("!H", 1005)  # запрещённый "на проводе" код
        conn.send_raw(raw_client_frame(0x88, len(payload), payload))
        return _read_close_or_drop(conn)

    def case_normal_close():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        payload = struct.pack("!H", 1000)
        conn.send_raw(raw_client_frame(0x88, len(payload), payload))
        return _read_close_or_drop(conn)

    def case_oversized():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        big = b"x" * (args.max_frame + 1000)
        conn.send_frame(OP_BIN, big)
        return _read_close_or_drop(conn, timeout=5.0)

    def case_bad_utf8():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_raw(raw_client_frame(0x81, 2, b"\xc0\x80"))  # overlong-кодировка NUL
        return _read_close_or_drop(conn)

    def case_fragmented_valid():
        conn, status, _ = connect(args.url)
        if conn is None:
            return "handshake failed: " + status
        conn.send_frame(OP_TEXT, b"Hel", fin=False)
        conn.send_frame(OP_PING, b"ping-between-fragments", fin=True)
        conn.send_frame(OP_CONT, b"lo", fin=True)
        # ждём PONG на ping — если пришёл, кодек фрагментации + control-приоритет работают
        conn.sock.settimeout(2.0)
        try:
            opcode, payload, _fin = conn.recv_frame()
            conn.sock.close()
            return "pong:{}".format(payload == b"ping-between-fragments") if opcode == OP_PONG else "unexpected opcode {}".format(opcode)
        except (ConnectionError, socket.timeout):
            return "dropped"

    cases = [
        ("немаскированный кадр -> 1002", case_unmasked, 1002),
        ("RSV1 установлен -> 1002", case_rsv, 1002),
        ("зарезервированный opcode 0x3 -> 1002", case_reserved_opcode, 1002),
        ("CONT без открытого сообщения -> 1002", case_cont_no_open, 1002),
        ("новое сообщение при открытом предыдущем -> 1002", case_new_msg_while_fragmenting, 1002),
        ("фрагментированный PING (FIN=0) -> 1002", case_frag_ping, 1002),
        ("CLOSE длиной 1 байт -> 1002", case_close_len1, 1002),
        ("CLOSE с запрещённым кодом 1005 -> 1002", case_bad_close_code, 1002),
        ("обычный CLOSE 1000 -> эхо 1000", case_normal_close, 1000),
        ("кадр больше --max-frame -> 1009", case_oversized, 1009),
        ("невалидный UTF-8 (overlong) -> 1007", case_bad_utf8, 1007),
        ("валидная фрагментация + PING между частями -> pong:True", case_fragmented_valid, "pong:True"),
    ]

    passed = 0
    for name, fn, want in cases:
        try:
            got = fn()
        except Exception as e:  # noqa: BLE001 — тестовый скрипт, репортим любую ошибку кейса
            got = "exception: {}".format(e)
        ok = (got == want)
        print("[{}] {}: получено={} ожидалось={}".format("OK" if ok else "FAIL", name, got, want))
        if ok:
            passed += 1
    print("{}/{} fuzz-кейсов пройдено".format(passed, len(cases)))
    sys.exit(0 if passed == len(cases) else 1)


# --------------------------------------------------------------------- main

def build_parser():
    p = argparse.ArgumentParser(description="Тестовый WS-клиент для сервера-агрегатора")
    sub = p.add_subparsers(dest="cmd", required=True)

    ph = sub.add_parser("handshake", help="разовое рукопожатие")
    ph.add_argument("url")
    ph.set_defaults(func=cmd_handshake)

    ps = sub.add_parser("src", help="подключиться как источник и слать сообщения")
    ps.add_argument("url")
    ps.add_argument("--rate", type=float, default=1.0, help="сообщений в секунду (0 = без ограничения)")
    ps.add_argument("--count", type=int, default=0, help="сколько сообщений отправить (0 = по --seconds)")
    ps.add_argument("--seconds", type=float, default=0, help="сколько секунд слать (0 = по --count)")
    ps.add_argument("--text", default=None, help="фиксированный текст вместо автогенерируемого")
    ps.add_argument("--verbose", action="store_true")
    ps.set_defaults(func=cmd_src)

    pk = sub.add_parser("sink", help="подключиться как приёмник и печатать входящее")
    pk.add_argument("url")
    pk.add_argument("--count", type=int, default=0, help="сколько сообщений принять (0 = без ограничения)")
    pk.add_argument("--timeout", type=float, default=0, help="таймаут ожидания сообщения, с (0 = бесконечно)")
    pk.set_defaults(func=cmd_sink)

    pss = sub.add_parser("slow-sink", help="подключиться как приёмник и НЕ читать (backpressure)")
    pss.add_argument("url")
    pss.add_argument("--hold", type=float, default=10.0, help="сколько секунд держать соединение не читая")
    pss.set_defaults(func=cmd_slow_sink)

    pf = sub.add_parser("fuzz", help="набор протокольных негативных кейсов")
    pf.add_argument("url")
    pf.add_argument("--max-frame", type=int, default=65536, dest="max_frame",
                     help="должен совпадать с --max-frame сервера (по умолчанию 65536)")
    pf.set_defaults(func=cmd_fuzz)

    return p


def main():
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
