#include "sha1.h"
#include <string.h>

static uint32_t rotl32(uint32_t x, int n)
{
    return (x << n) | (x >> (32 - n));
}

void sha1Init(Sha1_t *c)
{
    c->h[0] = 0x67452301u;
    c->h[1] = 0xEFCDAB89u;
    c->h[2] = 0x98BADCFEu;
    c->h[3] = 0x10325476u;
    c->h[4] = 0xC3D2E1F0u;
    c->totalLen = 0;
    c->bufLen = 0;
}

/* Обработка одного блока 64 байта */
static void sha1Block(Sha1_t *c, const uint8_t block[64])
{
    uint32_t w[80];
    for (int i = 0; i < 16; ++i)
    {
        w[i] = ((uint32_t)block[i * 4] << 24) | ((uint32_t)block[i * 4 + 1] << 16) |
               ((uint32_t)block[i * 4 + 2] << 8) | ((uint32_t)block[i * 4 + 3]);
    }
    for (int i = 16; i < 80; ++i)
    {
        w[i] = rotl32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }

    uint32_t a = c->h[0], b = c->h[1], cc = c->h[2], d = c->h[3], e = c->h[4];

    for (int i = 0; i < 80; ++i)
    {
        uint32_t f, k;
        if (i < 20)      { f = (b & cc) | ((~b) & d);        k = 0x5A827999u; }
        else if (i < 40) { f = b ^ cc ^ d;                   k = 0x6ED9EBA1u; }
        else if (i < 60) { f = (b & cc) | (b & d) | (cc & d); k = 0x8F1BBCDCu; }
        else             { f = b ^ cc ^ d;                   k = 0xCA62C1D6u; }

        uint32_t temp = rotl32(a, 5) + f + e + k + w[i];
        e = d; d = cc; cc = rotl32(b, 30); b = a; a = temp;
    }

    c->h[0] += a; c->h[1] += b; c->h[2] += cc; c->h[3] += d; c->h[4] += e;
}

void sha1Update(Sha1_t *c, const void *data, size_t n)
{
    const uint8_t *p = data;
    c->totalLen += n;

    if (c->bufLen > 0)
    {
        size_t need = 64 - c->bufLen;
        size_t take = (n < need) ? n : need;
        memcpy(c->buf + c->bufLen, p, take);
        c->bufLen += take;
        p += take;
        n -= take;
        if (c->bufLen == 64)
        {
            sha1Block(c, c->buf);
            c->bufLen = 0;
        }
    }

    while (n >= 64)
    {
        sha1Block(c, p);
        p += 64;
        n -= 64;
    }

    if (n > 0)
    {
        memcpy(c->buf, p, n);
        c->bufLen = n;
    }
}

void sha1Final(Sha1_t *c, uint8_t out[20])
{
    uint64_t bitLen = c->totalLen * 8;
    size_t idx = c->bufLen;

    /* Паддинг: один бит 1 (байт 0x80), затем нули; если после 0x80 в текущем
       блоке не остаётся 8 байт под длину — дополняем блок нулями, считаем его
       и продолжаем паддинг нулями уже в новом блоке. */
    c->buf[idx++] = 0x80;
    if (idx > 56)
    {
        while (idx < 64)
        {
            c->buf[idx++] = 0x00;
        }
        sha1Block(c, c->buf);
        idx = 0;
    }
    while (idx < 56)
    {
        c->buf[idx++] = 0x00;
    }

    for (int i = 0; i < 8; ++i)
    {
        c->buf[56 + i] = (uint8_t)(bitLen >> (56 - 8 * i));
    }
    sha1Block(c, c->buf);
    c->bufLen = 0;

    for (int i = 0; i < 5; ++i)
    {
        out[i * 4]     = (uint8_t)(c->h[i] >> 24);
        out[i * 4 + 1] = (uint8_t)(c->h[i] >> 16);
        out[i * 4 + 2] = (uint8_t)(c->h[i] >> 8);
        out[i * 4 + 3] = (uint8_t)(c->h[i]);
    }
}

void sha1(const void *data, size_t n, uint8_t out[20])
{
    Sha1_t c;
    sha1Init(&c);
    sha1Update(&c, data, n);
    sha1Final(&c, out);
}
