#include "base64.h"

static const char ENC_TABLE[64] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

static int decodeChar(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    if (c == '=') return 64;
    return -1;
}

size_t base64EncodedLen(size_t n) { return ((n + 2) / 3) * 4; }

size_t base64Encode(const uint8_t* in, size_t n, char* out) {
    size_t o = 0;
    size_t i = 0;

    while (i + 3 <= n) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8) | in[i + 2];
        out[o++] = ENC_TABLE[(v >> 18) & 0x3F];
        out[o++] = ENC_TABLE[(v >> 12) & 0x3F];
        out[o++] = ENC_TABLE[(v >> 6) & 0x3F];
        out[o++] = ENC_TABLE[v & 0x3F];
        i += 3;
    }

    size_t rem = n - i;
    if (rem == 1) {
        uint32_t v = (uint32_t)in[i] << 16;
        out[o++] = ENC_TABLE[(v >> 18) & 0x3F];
        out[o++] = ENC_TABLE[(v >> 12) & 0x3F];
        out[o++] = '=';
        out[o++] = '=';
    } else if (rem == 2) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8);
        out[o++] = ENC_TABLE[(v >> 18) & 0x3F];
        out[o++] = ENC_TABLE[(v >> 12) & 0x3F];
        out[o++] = ENC_TABLE[(v >> 6) & 0x3F];
        out[o++] = '=';
    }

    out[o] = '\0';
    return o;
}

static long checkedLen(const char* in, size_t n, int* padOut) {
    if (n == 0) {
        if (padOut) *padOut = 0;
        return 0;
    }
    if (n % 4 != 0) {
        return -1;
    }

    int pad = 0;
    if (in[n - 1] == '=') pad++;
    if (pad == 1 && in[n - 2] == '=') pad++;

    for (size_t i = 0; i < n - (size_t)pad; ++i) {
        int c = decodeChar((unsigned char)in[i]);
        if (c < 0 || c == 64) {
            return -1;
        }
    }
    for (size_t i = n - (size_t)pad; i < n; ++i) {
        if (in[i] != '=') {
            return -1;
        }
    }

    if (padOut) *padOut = pad;
    return (long)((n / 4) * 3 - (size_t)pad);
}

long base64DecodedLen(const char* in, size_t n) { return checkedLen(in, n, NULL); }

long base64Decode(const char* in, size_t n, uint8_t* out) {
    int pad = 0;
    long outLen = checkedLen(in, n, &pad);
    if (outLen < 0) {
        return -1;
    }

    size_t o = 0;
    for (size_t i = 0; i < n; i += 4) {
        int c0 = decodeChar((unsigned char)in[i]);
        int c1 = decodeChar((unsigned char)in[i + 1]);
        int c2 = decodeChar((unsigned char)in[i + 2]);
        int c3 = decodeChar((unsigned char)in[i + 3]);

        uint32_t v = ((uint32_t)c0 << 18) | ((uint32_t)c1 << 12) | ((uint32_t)(c2 & 0x3F) << 6) |
                     (uint32_t)(c3 & 0x3F);

        out[o++] = (uint8_t)(v >> 16);
        if (c2 != 64) out[o++] = (uint8_t)(v >> 8);
        if (c3 != 64) out[o++] = (uint8_t)v;
    }

    return outLen;
}
