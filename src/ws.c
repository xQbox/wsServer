#include <string.h>

#include "ws.h"
#include "sha1.h"
#include "base64.h"

WsRc_t wsParseHeader(const uint8_t *b, size_t avail, WsFrameHdr_t *h)
{
    if (avail < 2)
    {
        return WS_NEED_MORE;
    }

    uint8_t b0 = b[0];
    uint8_t b1 = b[1];

    if (b0 & 0x70)
    {
        return WS_ERR_PROTO; /* RSV1-3 != 0 — расширения не согласованы */
    }

    h->fin    = (b0 >> 7) & 1;
    h->opcode = b0 & 0x0F;
    h->masked = (b1 >> 7) & 1;

    /* Зарезервированные opcode: 0x3-0x7 (данные), 0xB-0xF (управляющие) */
    if ((h->opcode >= 0x3 && h->opcode <= 0x7) || h->opcode >= 0xB)
    {
        return WS_ERR_PROTO;
    }

    uint64_t len = b1 & 0x7F;
    size_t need = 2;

    if (len == 126)
    {
        need += 2;
        if (avail < need)
        {
            return WS_NEED_MORE;
        }
        len = ((uint64_t)b[2] << 8) | (uint64_t)b[3];
        if (len < 126)
        {
            return WS_ERR_PROTO; /* неминимальная кодировка */
        }
    }
    else if (len == 127)
    {
        need += 8;
        if (avail < need)
        {
            return WS_NEED_MORE;
        }
        len = 0;
        for (int i = 0; i < 8; ++i)
        {
            len = (len << 8) | (uint64_t)b[2 + i];
        }
        if (len & (UINT64_C(1) << 63))
        {
            return WS_ERR_PROTO; /* старший бит обязан быть 0 */
        }
        if (len <= 0xFFFF)
        {
            return WS_ERR_PROTO; /* неминимальная кодировка */
        }
    }

    if (h->masked)
    {
        need += 4;
        if (avail < need)
        {
            return WS_NEED_MORE;
        }
        memcpy(h->maskKey, b + need - 4, 4);
    }

    /* Управляющие кадры: обязателен FIN=1 и длина <= 125 (RFC 6455 п.5.5) */
    if ((h->opcode & 0x8) && (!h->fin || len > 125))
    {
        return WS_ERR_PROTO;
    }

    h->payloadLen = len;
    h->headerLen  = need;
    return WS_OK;
}

void wsUnmask(uint8_t *p, size_t n, const uint8_t key[4])
{
    for (size_t i = 0; i < n; ++i)
    {
        p[i] ^= key[i & 3];
    }
}

size_t wsEncodeHeader(uint8_t *out, uint8_t opcode, size_t payloadLen)
{
    size_t o = 0;
    out[o++] = (uint8_t)(0x80 | (opcode & 0x0F)); /* FIN=1, RSV=0 */

    if (payloadLen <= 125)
    {
        out[o++] = (uint8_t)payloadLen; /* MASK=0 */
    }
    else if (payloadLen <= 0xFFFF)
    {
        out[o++] = 126;
        out[o++] = (uint8_t)(payloadLen >> 8);
        out[o++] = (uint8_t)payloadLen;
    }
    else
    {
        out[o++] = 127;
        for (int i = 7; i >= 0; --i)
        {
            out[o++] = (uint8_t)(payloadLen >> (8 * i));
        }
    }
    return o;
}

int wsComputeAccept(const char *key, size_t keyLen, char out[29])
{
    static const char MAGIC[] = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    if (keyLen != 24 || base64DecodedLen(key, keyLen) != 16)
    {
        return -1;
    }

    char concat[24 + sizeof(MAGIC) - 1];
    memcpy(concat, key, 24);
    memcpy(concat + 24, MAGIC, sizeof(MAGIC) - 1);

    uint8_t digest[20];
    sha1(concat, sizeof concat, digest);
    base64Encode(digest, sizeof digest, out);
    return 0;
}

int wsIsValidCloseCode(uint16_t code)
{
    if (code >= 1000 && code <= 1003) return 1;
    if (code >= 1007 && code <= 1011) return 1;
    if (code >= 3000 && code <= 4999) return 1;
    return 0; /* включая зарезервированные 1004/1005/1006/1015 и всё < 1000 */
}

/* ============================ UTF-8 (RFC 3629) ============================ */

void wsUtf8Reset(Utf8St_t *st)
{
    st->need = 0;
    st->kind = U8_NONE;
}

int wsUtf8Feed(Utf8St_t *st, const uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; ++i)
    {
        uint8_t b = p[i];

        if (st->need == 0)
        {
            if (b <= 0x7F)                    { continue; }
            else if (b >= 0xC2 && b <= 0xDF)  { st->need = 1; st->kind = U8_GEN; }
            else if (b == 0xE0)               { st->need = 2; st->kind = U8_E0;  }
            else if (b >= 0xE1 && b <= 0xEC)  { st->need = 2; st->kind = U8_GEN; }
            else if (b == 0xED)               { st->need = 2; st->kind = U8_ED;  }
            else if (b >= 0xEE && b <= 0xEF)  { st->need = 2; st->kind = U8_GEN; }
            else if (b == 0xF0)               { st->need = 3; st->kind = U8_F0;  }
            else if (b >= 0xF1 && b <= 0xF3)  { st->need = 3; st->kind = U8_GEN; }
            else if (b == 0xF4)               { st->need = 3; st->kind = U8_F4;  }
            else                              { return -1; } /* 0x80-0xBF/0xC0-0xC1/0xF5-0xFF как ведущий */
        }
        else
        {
            int ok;
            switch (st->kind)
            {
                case U8_E0: ok = (b >= 0xA0 && b <= 0xBF); break; /* исключает overlong */
                case U8_ED: ok = (b >= 0x80 && b <= 0x9F); break; /* исключает суррогаты D800-DFFF */
                case U8_F0: ok = (b >= 0x90 && b <= 0xBF); break; /* исключает overlong */
                case U8_F4: ok = (b >= 0x80 && b <= 0x8F); break; /* ограничивает <= U+10FFFF */
                default:    ok = (b >= 0x80 && b <= 0xBF); break;
            }
            if (!ok)
            {
                return -1;
            }
            st->kind = U8_GEN; /* особое ограничение действует только на первый continuation-байт */
            st->need--;
        }
    }
    return 0;
}

int wsUtf8Done(const Utf8St_t *st)
{
    return st->need == 0;
}
