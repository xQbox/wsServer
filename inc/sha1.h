#ifndef SHA1_H
#define SHA1_H

#include <stddef.h>
#include <stdint.h>

/*
 * Минимальная реализация SHA-1 (RFC 3174) — нужна только для
 * Sec-WebSocket-Accept при WS-рукопожатии (RFC 6455 п.1.3).
 * Не для криптографии общего назначения.
 */

typedef struct Sha1_t
{
    uint32_t h[5];
    uint64_t totalLen;
    uint8_t  buf[64];
    size_t   bufLen;
} Sha1_t;

void sha1Init(Sha1_t *c);
void sha1Update(Sha1_t *c, const void *data, size_t n);
void sha1Final(Sha1_t *c, uint8_t out[20]);

/* Удобная обёртка: sha1(data, n, out) == Init+Update+Final */
void sha1(const void *data, size_t n, uint8_t out[20]);

#endif /* SHA1_H */
