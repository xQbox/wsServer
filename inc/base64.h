#ifndef BASE64_H
#define BASE64_H

#include <stddef.h>
#include <stdint.h>

/*
 * Кодирование/декодирование base64 (RFC 4648) — нужно для
 * Sec-WebSocket-Accept (кодирование) и проверки Sec-WebSocket-Key (декодирование длины).
 */

/* out должен вмещать base64EncodedLen(n)+1 байт (включая завершающий NUL).
 * Возвращает длину результата без NUL. */
size_t base64Encode(const uint8_t *in, size_t n, char *out);

/* Длина base64-строки (без NUL) для n байт входа. */
size_t base64EncodedLen(size_t n);

/* Декодирует base64-строку длины n в out (буфер должен вмещать base64DecodedLen(n) байт).
 * Возвращает число декодированных байт, или -1 при некорректной строке. */
long base64Decode(const char *in, size_t n, uint8_t *out);

/* Сколько байт даст декодирование строки длины n, БЕЗ фактического декодирования
 * (используется только для быстрой проверки длины Sec-WebSocket-Key: она обязана
 * декодироваться ровно в 16 байт). Возвращает -1, если n невалидна для base64. */
long base64DecodedLen(const char *in, size_t n);

#endif /* BASE64_H */
