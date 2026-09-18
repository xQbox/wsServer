#ifndef WS_H
#define WS_H

#include <stdint.h>
#include <stddef.h>

#include "conn.h"   /* Utf8St_t */

/*
 * ws.h — протокол WebSocket (RFC 6455) без единого системного вызова:
 * разбор/кодирование кадров, рукопожатие, валидация UTF-8. Всё, что
 * трогает сокеты и буферы соединений, живёт в http.c/worker.c/agg.c.
 */

#define WS_OP_CONT  0x0
#define WS_OP_TEXT  0x1
#define WS_OP_BIN   0x2
#define WS_OP_CLOSE 0x8
#define WS_OP_PING  0x9
#define WS_OP_PONG  0xA

typedef enum { WS_NEED_MORE = 0, WS_OK = 1, WS_ERR_PROTO = -1 } WsRc_t;

typedef struct WsFrameHdr_t
{
    int      fin;
    int      opcode;
    int      masked;
    uint64_t payloadLen;
    uint8_t  maskKey[4];   /* валиден только если masked */
    size_t   headerLen;    /* байт заголовка до начала payload (включая маску) */
} WsFrameHdr_t;

/*
 * Разбирает заголовок одного кадра из buf[0..avail).
 * WS_NEED_MORE — в avail недостаточно байт даже на заголовок, ждать ещё.
 * WS_ERR_PROTO — нарушение протокола (RSV≠0, зарезервированный opcode,
 *                неминимальная кодировка длины, фрагментированный/большой
 *                управляющий кадр) — вызывающий обязан закрыть соединение
 *                кодом 1002.
 * WS_OK — заголовок разобран; out->headerLen байт можно пропустить, чтобы
 *         добраться до payload, out->payloadLen — сколько payload ещё
 *         должно быть в буфере (может быть больше avail — тогда ждать).
 */
WsRc_t wsParseHeader(const uint8_t *buf, size_t avail, WsFrameHdr_t *out);

/* XOR-демаскирование payload на месте (RFC 6455 п.5.3). */
void wsUnmask(uint8_t *payload, size_t n, const uint8_t key[4]);

/*
 * Кодирует заголовок исходящего кадра в out (буфер должен вмещать 10 байт).
 * FIN всегда 1 и MASK всегда 0 — сервер не фрагментирует и не маскирует
 * исходящие кадры (RFC 6455 п.5.1: маскирует только клиент). Возвращает
 * длину заголовка.
 */
size_t wsEncodeHeader(uint8_t *out, uint8_t opcode, size_t payloadLen);

/*
 * Sec-WebSocket-Accept = base64(sha1(key + magic)) (RFC 6455 п.1.3).
 * out должен вмещать 29 байт (28 base64-символов + NUL).
 * Возвращает 0 при успехе, -1 если keyLen != 24 или key — не валидный base64.
 */
int wsComputeAccept(const char *key, size_t keyLen, char out[29]);

/* Допустимые коды закрытия от клиента (RFC 6455 п.7.4.1), 1004/1005/1006/1015 — запрещены. */
int wsIsValidCloseCode(uint16_t code);

void wsUtf8Reset(Utf8St_t *st);
/* Скармливает очередную порцию байт. Возвращает 0, пока последовательность
 * валидна (даже если разрезана на границе), -1 при первой же ошибке. */
int  wsUtf8Feed(Utf8St_t *st, const uint8_t *p, size_t n);
/* 1, если на конец скормленных данных нет незавершённого символа —
 * проверять в конце собранного TEXT-сообщения. */
int  wsUtf8Done(const Utf8St_t *st);

#endif /* WS_H */
