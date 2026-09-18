#ifndef HTTP_H
#define HTTP_H

#include <stddef.h>
#include "conn.h"

/*
 * http.c — ровно тот минимум HTTP/1.1, который нужен WS-рукопожатию
 * (RFC 6455 п.4): разбор request line + заголовков и сборка ответа
 * (101 / 400 / 426). Раздачи статики в проекте больше нет.
 */

typedef enum
{
    HS_OK,           /* валидный запрос на апгрейд, acceptKey посчитан */
    HS_NOT_UPGRADE,  /* синтаксически нормальный GET, но без Upgrade: websocket */
    HS_BAD_VERSION,  /* Upgrade есть, но Sec-WebSocket-Version отсутствует/не 13 */
    HS_BAD_REQUEST   /* не парсится вообще: не GET, битый путь, битый/отсутствующий ключ */
} HandshakeStatus_t;

typedef struct HandshakeResult_t
{
    HandshakeStatus_t status;
    char path[256];
    char acceptKey[29]; /* валиден только при status == HS_OK */
} HandshakeResult_t;

/* buf[0..len) должен содержать ЦЕЛИКОМ строку запроса и заголовки, включая
 * завершающую "\r\n\r\n" (её позицию находит httpFindHeaderEnd). */
void httpParseHandshake(const char *buf, size_t len, HandshakeResult_t *out);

/* Находит конец блока заголовков — указатель на начало "\r\n\r\n" в buf[0..len),
 * либо NULL, если заголовки ещё не пришли целиком. Работает на бинарных данных
 * (не требует NUL-терминации), в отличие от strstr(). */
const char *httpFindHeaderEnd(const char *buf, size_t len);

/* Ищет заголовок name (регистронезависимо) среди строк buf[0..len) —
 * ровно один блок заголовков, без request line. Возвращает указатель на
 * значение (без ведущих/хвостовых пробелов) и его длину в *valLen, либо
 * NULL, если заголовка нет. */
const char *httpFindHeader(const char *buf, size_t len, const char *name, size_t *valLen);

/* true, если val[0..len) — список токенов через запятую, содержащий token
 * (регистронезависимо, с учётом пробелов вокруг запятых). Нужно для
 * "Connection: keep-alive, Upgrade". */
int httpHeaderHasToken(const char *val, size_t len, const char *token);

/* Дописывают готовый ответ в tx (через bufAppend — растёт сама, без лимита,
 * т.к. это то, что генерирует сам сервер). */
void httpBuildUpgradeResponse(Buf_t *tx, const char *acceptKey);
void httpBuildError(Buf_t *tx, int code, const char *reason, const char *extraHeader /* может быть NULL */);

#endif /* HTTP_H */
