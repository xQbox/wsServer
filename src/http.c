#include <string.h>
#include <strings.h>
#include <stdio.h>

#include "http.h"
#include "ws.h"

static const char *findCRLF(const char *p, size_t n)
{
    for (size_t i = 0; i + 1 < n; ++i)
    {
        if (p[i] == '\r' && p[i + 1] == '\n')
        {
            return p + i;
        }
    }
    return NULL;
}

const char *httpFindHeaderEnd(const char *buf, size_t len)
{
    if (len < 4)
    {
        return NULL;
    }
    for (size_t i = 0; i + 4 <= len; ++i)
    {
        if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n')
        {
            return buf + i;
        }
    }
    return NULL;
}

const char *httpFindHeader(const char *buf, size_t len, const char *name, size_t *valLen)
{
    size_t nameLen = strlen(name);

    /* Пропускаем request line — заголовки начинаются после первого \r\n */
    const char *firstNl = findCRLF(buf, len);
    if (!firstNl)
    {
        return NULL;
    }
    size_t pos = (size_t)(firstNl - buf) + 2;

    while (pos < len)
    {
        const char *lineStart = buf + pos;
        const char *nl = findCRLF(lineStart, len - pos);
        size_t lineLen = nl ? (size_t)(nl - lineStart) : (len - pos);

        if (lineLen == 0)
        {
            break; /* пустая строка — конец заголовков */
        }

        if (lineLen > nameLen && lineStart[nameLen] == ':' &&
            strncasecmp(lineStart, name, nameLen) == 0)
        {
            const char *v = lineStart + nameLen + 1;
            size_t vlen = lineLen - nameLen - 1;
            while (vlen > 0 && *v == ' ') { v++; vlen--; }
            while (vlen > 0 && v[vlen - 1] == ' ') { vlen--; }
            if (valLen) *valLen = vlen;
            return v;
        }

        if (!nl)
        {
            break;
        }
        pos += lineLen + 2;
    }
    return NULL;
}

int httpHeaderHasToken(const char *val, size_t len, const char *token)
{
    size_t tokenLen = strlen(token);
    size_t i = 0;

    while (i < len)
    {
        while (i < len && (val[i] == ' ' || val[i] == ',')) i++;
        size_t start = i;
        while (i < len && val[i] != ',') i++;
        size_t end = i;
        while (end > start && val[end - 1] == ' ') end--;

        if (end > start && end - start == tokenLen && strncasecmp(val + start, token, tokenLen) == 0)
        {
            return 1;
        }
    }
    return 0;
}

void httpParseHandshake(const char *buf, size_t len, HandshakeResult_t *out)
{
    memset(out, 0, sizeof *out);
    out->status = HS_BAD_REQUEST;

    if (len < 5 || memcmp(buf, "GET ", 4) != 0)
    {
        return;
    }

    const char *pathStart = buf + 4;
    const char *pathEnd = memchr(pathStart, ' ', len - 4);
    if (!pathEnd)
    {
        return;
    }

    size_t pathLen = (size_t)(pathEnd - pathStart);
    if (pathLen == 0 || pathLen >= sizeof out->path)
    {
        return;
    }
    memcpy(out->path, pathStart, pathLen);
    out->path[pathLen] = '\0';

    size_t upgradeLen, connLen;
    const char *upgrade = httpFindHeader(buf, len, "Upgrade", &upgradeLen);
    const char *connVal = httpFindHeader(buf, len, "Connection", &connLen);

    if (!upgrade || !httpHeaderHasToken(upgrade, upgradeLen, "websocket") ||
        !connVal || !httpHeaderHasToken(connVal, connLen, "Upgrade"))
    {
        out->status = HS_NOT_UPGRADE;
        return;
    }

    size_t verLen;
    const char *ver = httpFindHeader(buf, len, "Sec-WebSocket-Version", &verLen);
    if (!ver || verLen != 2 || memcmp(ver, "13", 2) != 0)
    {
        out->status = HS_BAD_VERSION;
        return;
    }

    size_t keyLen;
    const char *key = httpFindHeader(buf, len, "Sec-WebSocket-Key", &keyLen);
    if (!key || wsComputeAccept(key, keyLen, out->acceptKey) != 0)
    {
        out->status = HS_BAD_REQUEST;
        return;
    }

    out->status = HS_OK;
}

void httpBuildUpgradeResponse(Buf_t *tx, const char *acceptKey)
{
    char hdr[256];
    int n = snprintf(hdr, sizeof hdr,
                      "HTTP/1.1 101 Switching Protocols\r\n"
                      "Upgrade: websocket\r\n"
                      "Connection: Upgrade\r\n"
                      "Sec-WebSocket-Accept: %s\r\n\r\n",
                      acceptKey);
    bufAppend(tx, hdr, (size_t)n);
}

void httpBuildError(Buf_t *tx, int code, const char *reason, const char *extraHeader)
{
    char hdr[256];
    int n = snprintf(hdr, sizeof hdr,
                      "HTTP/1.1 %d %s\r\n"
                      "%s"
                      "Content-Length: 0\r\n"
                      "Connection: close\r\n\r\n",
                      code, reason, extraHeader ? extraHeader : "");
    bufAppend(tx, hdr, (size_t)n);
}
