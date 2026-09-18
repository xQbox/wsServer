#define _GNU_SOURCE

#include "ws_client.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "base64.h"
#include "conn.h"
#include "http.h"
#include "logger.h"
#include "ws.h"

#define WS_CLIENT "[WS-CLIENT] "

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
typedef struct WsClientRx_t {
    Buf_t rx;
    Buf_t msg;

    uint8_t msgOpcode;
    uint8_t fragmenting;

    Utf8St_t msgUtf8;
} WsClientRx_t;
typedef enum {
    UPSTREAM_DISCONNECTED,
    UPSTREAM_CONNECTING,
    UPSTREAM_HANDSHAKE,
    UPSTREAM_OPEN,
    UPSTREAM_CLOSING
} UpstreamState_t;

typedef struct UpstreamClient_t {
    uint64_t id;

    int fd;

    char host[256];
    int port;
    char path[256];

    UpstreamState_t state;

    Buf_t rx;
    Buf_t tx;
    Buf_t msg;

    uint8_t msgOpcode;
    uint8_t fragmenting;
    Utf8St_t msgUtf8;

    char expectedAccept[29];

    /* reconnect */
    uint64_t reconnectAtMs;
    unsigned reconnectAttempt;

    /* backpressure */
    Msg_t* pendingMsg;
    int readPaused;

} UpstreamClient_t;

typedef struct UpstreamReactor_t {
    int epollfd;

    ServerConfig_t* conf;

    UpstreamClient_t** clients;
    size_t count;
    size_t capacity;

    uint64_t nextId;

} UpstreamReactor_t;

static int updateEvents(UpstreamReactor_t* r, UpstreamClient_t* c);

static int upstreamConnectComplete(UpstreamClient_t* c);

static void upstreamDisconnect(UpstreamReactor_t* r, UpstreamClient_t* c);

static int queueHandshake(UpstreamReactor_t* r, UpstreamClient_t* c);

static int flushUpstreamTx(UpstreamReactor_t* r, UpstreamClient_t* c);

static int readHandshake(UpstreamReactor_t* r, UpstreamClient_t* c);

static int processHandshakeResponse(UpstreamReactor_t* r, UpstreamClient_t* c);

static int readUpstreamFrames(UpstreamReactor_t* r, UpstreamClient_t* c);

static int processBufferedFrames(UpstreamReactor_t* r, UpstreamClient_t* c);

static int publishReactorMessage(UpstreamReactor_t* r, UpstreamClient_t* c);

static int queueMaskedFrame(UpstreamReactor_t* r, UpstreamClient_t* c, uint8_t opcode, const void* payload,
                            size_t payloadLen);

static int beginClose(UpstreamReactor_t* r, UpstreamClient_t* c, uint16_t code);
int upstreamAdd(UpstreamReactor_t* r, const char* host, int port, const char* path) {
    if (!r || !host || !path || port <= 0 || port > 65535) {
        return -1;
    }

    if (r->count == r->capacity) {
        size_t newCapacity = r->capacity == 0 ? 4 : r->capacity * 2;

        UpstreamClient_t** tmp = (UpstreamClient_t**)realloc(r->clients, newCapacity * sizeof(*tmp));

        if (!tmp) {
            return -1;
        }

        r->clients = tmp;
        r->capacity = newCapacity;
    }

    UpstreamClient_t* c = (UpstreamClient_t*)calloc(1, sizeof(*c));

    if (!c) {
        return -1;
    }

    c->id = r->nextId++;

    c->fd = -1;
    c->port = port;

    snprintf(c->host, sizeof(c->host), "%s", host);

    snprintf(c->path, sizeof(c->path), "%s", path);

    c->state = UPSTREAM_DISCONNECTED;

    /*
     * 0 означает:
     * reactor попробует подключиться сразу.
     */
    c->reconnectAtMs = 0;
    c->reconnectAttempt = 0;

    wsUtf8Reset(&c->msgUtf8);

    r->clients[r->count++] = c;

    logger(LOG_INFO, WS_CLIENT "добавлен upstream %llu: ws://%s:%d%s", (unsigned long long)c->id, c->host,
           c->port, c->path);

    return 0;
}
void upstreamDestroy(UpstreamReactor_t* r) {
    if (!r) {
        return;
    }

    for (size_t i = 0; i < r->count; ++i) {
        UpstreamClient_t* c = r->clients[i];

        if (!c) {
            continue;
        }

        if (c->fd >= 0) {
            epoll_ctl(r->epollfd, EPOLL_CTL_DEL, c->fd, NULL);

            close(c->fd);
        }

        if (c->pendingMsg) {
            msgFree(c->pendingMsg);
        }

        bufFree(&c->rx);
        bufFree(&c->tx);
        bufFree(&c->msg);

        free(c);
    }

    free(r->clients);

    if (r->epollfd >= 0) {
        close(r->epollfd);
    }

    free(r);
}
UpstreamReactor_t* upstreamCreate(ServerConfig_t* conf) {
    UpstreamReactor_t* r = (UpstreamReactor_t*)calloc(1, sizeof(*r));

    if (!r) {
        return NULL;
    }

    r->epollfd = epoll_create1(EPOLL_CLOEXEC);

    if (r->epollfd < 0) {
        free(r);
        return NULL;
    }

    r->conf = conf;

    /* Чтобы не пересекаться с обычными Conn_t id */
    r->nextId = 1000000;

    return r;
}
/* ============================================================
 * Random
 * ============================================================ */

static int randomBytes(uint8_t* buf, size_t len) {
    size_t off = 0;

    while (off < len) {
        ssize_t n = getrandom(buf + off, len - off, 0);

        if (n > 0) {
            off += (size_t)n;
            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        return -1;
    }

    return 0;
}

static uint64_t nowMs(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}
static uint64_t reconnectDelayMs(unsigned attempt) {
    uint64_t delay = 1000ULL;

    while (attempt > 0 && delay < 30000ULL) {
        delay *= 2;
        --attempt;
    }

    if (delay > 30000ULL) {
        delay = 30000ULL;
    }

    return delay;
}
static void upstreamDisconnect(UpstreamReactor_t* r, UpstreamClient_t* c) {
    if (c->fd >= 0) {
        epoll_ctl(r->epollfd, EPOLL_CTL_DEL, c->fd, NULL);

        close(c->fd);

        c->fd = -1;
    }

    bufReset(&c->rx);
    bufReset(&c->tx);
    bufReset(&c->msg);

    c->fragmenting = 0;
    c->readPaused = 0;

    if (c->pendingMsg) {
        msgFree(c->pendingMsg);
        c->pendingMsg = NULL;
    }

    c->state = UPSTREAM_DISCONNECTED;

    uint64_t delay = reconnectDelayMs(c->reconnectAttempt);

    if (c->reconnectAttempt < 10) {
        c->reconnectAttempt++;
    }

    c->reconnectAtMs = nowMs() + delay;

    logger(LOG_INFO, WS_CLIENT "upstream %llu отключён, reconnect через %llu ms", (unsigned long long)c->id,
           (unsigned long long)delay);
}
static int setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0) {
        return -1;
    }

    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
static int upstreamConnect(UpstreamReactor_t* r, UpstreamClient_t* c) {
    char port[16];

    snprintf(port, sizeof port, "%d", c->port);

    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = NULL;

    int rc = getaddrinfo(c->host, port, &hints, &res);

    if (rc != 0) {
        logger(LOG_ERROR, WS_CLIENT "getaddrinfo(%s): %s", c->host, gai_strerror(rc));

        return -1;
    }

    int fd = -1;

    for (struct addrinfo* ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);

        if (fd < 0) {
            continue;
        }

        if (setNonBlocking(fd) != 0) {
            close(fd);
            fd = -1;
            continue;
        }

        rc = connect(fd, ai->ai_addr, ai->ai_addrlen);

        if (rc == 0 || (rc < 0 && errno == EINPROGRESS)) {
            break;
        }

        close(fd);
        fd = -1;
    }

    freeaddrinfo(res);

    if (fd < 0) {
        return -1;
    }

    c->fd = fd;

    /*
     * Даже если connect() сразу вернул 0,
     * можно пропустить fd через CONNECTING:
     * EPOLLOUT + SO_ERROR завершат проверку.
     */
    c->state = UPSTREAM_CONNECTING;

    struct epoll_event ev = {0};

    ev.events = EPOLLOUT | EPOLLRDHUP;

    ev.data.ptr = c;

    if (epoll_ctl(r->epollfd, EPOLL_CTL_ADD, fd, &ev) != 0) {
        close(fd);
        c->fd = -1;

        return -1;
    }

    logger(LOG_INFO, WS_CLIENT "upstream %llu: connect -> %s:%d", (unsigned long long)c->id, c->host,
           c->port);

    return 0;
}
static int upstreamConnectComplete(UpstreamClient_t* c) {
    int err = 0;
    socklen_t len = sizeof(err);

    if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
        return -1;
    }

    if (err != 0) {
        errno = err;
        return -1;
    }

    return 0;
}
static int updateEvents(UpstreamReactor_t* r, UpstreamClient_t* c) {
    if (c->fd < 0) {
        return -1;
    }

    struct epoll_event ev = {0};

    ev.events = EPOLLRDHUP;

    if (!c->readPaused && (c->state == UPSTREAM_HANDSHAKE || c->state == UPSTREAM_OPEN)) {
        ev.events |= EPOLLIN;
    }

    if (c->state == UPSTREAM_CONNECTING || bufLen(&c->tx) > 0) {
        ev.events |= EPOLLOUT;
    }

    ev.data.ptr = c;

    return epoll_ctl(r->epollfd, EPOLL_CTL_MOD, c->fd, &ev);
}
static int publishReactorMessage(UpstreamReactor_t* r, UpstreamClient_t* c) {
    ServerConfig_t* conf = r->conf;

    Msg_t* m = msgNewData(c->id, c->msgOpcode, bufData(&c->msg), bufLen(&c->msg));

    if (!m) {
        /*
         * Как worker.c:
         * одно сообщение потеряли,
         * соединение не рвём.
         */
        return 0;
    }

    if (queueSize(conf->dataQueue) >= conf->highWatermark) {
        c->pendingMsg = m;
        c->readPaused = 1;

        if (updateEvents(r, c) != 0) {
            msgFree(m);

            c->pendingMsg = NULL;
            c->readPaused = 0;

            return -1;
        }

        return 1;
    }

    push(conf->dataQueue, m);

    return 0;
}
static void resumePausedClients(UpstreamReactor_t* r) {
    ServerConfig_t* conf = r->conf;

    if (queueSize(conf->dataQueue) > conf->lowWatermark) {
        return;
    }

    for (size_t i = 0; i < r->count; ++i) {
        UpstreamClient_t* c = r->clients[i];

        if (!c->readPaused) {
            continue;
        }

        /*
         * Не переполняем очередь снова,
         * если paused upstream много.
         */
        if (queueSize(conf->dataQueue) >= conf->highWatermark) {
            break;
        }

        if (c->pendingMsg) {
            push(conf->dataQueue, c->pendingMsg);

            c->pendingMsg = NULL;
        }

        c->readPaused = 0;

        if (updateEvents(r, c) != 0) {
            upstreamDisconnect(r, c);
            continue;
        }

        /*
         * КРИТИЧЕСКИ ВАЖНО:
         *
         * Пока client был paused, следующие
         * frames могли уже находиться в c->rx.
         *
         * Не ждём нового EPOLLIN.
         */
        int rc = processBufferedFrames(r, c);

        if (rc < 0) {
            upstreamDisconnect(r, c);
        }
    }
}

static void reconnectClients(UpstreamReactor_t* r) {
    uint64_t now = nowMs();

    for (size_t i = 0; i < r->count; ++i) {
        UpstreamClient_t* c = r->clients[i];

        if (c->state != UPSTREAM_DISCONNECTED) {
            continue;
        }

        if (now < c->reconnectAtMs) {
            continue;
        }

        if (upstreamConnect(r, c) != 0) {
            uint64_t delay = reconnectDelayMs(c->reconnectAttempt);

            if (c->reconnectAttempt < 10) {
                c->reconnectAttempt++;
            }

            c->reconnectAtMs = now + delay;
        }
    }
}
static void handleUpstreamEvent(UpstreamReactor_t* r, UpstreamClient_t* c, uint32_t events);

void* upstreamReactorThread(void* arg) {
    UpstreamReactor_t* r = (UpstreamReactor_t*)arg;

    struct epoll_event events[64];

    logger(LOG_INFO, WS_CLIENT "Upstream reactor запущен");

    while (!atomic_load(&r->conf->shutdown)) {
        /*
         * Сначала пробуем восстановить
         * отвалившиеся соединения.
         */
        reconnectClients(r);

        resumePausedClients(r);

        int n = epoll_wait(r->epollfd, events, 64, 500);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }

            logger(LOG_ERROR, WS_CLIENT "epoll_wait: %s", strerror(errno));

            break;
        }

        for (int i = 0; i < n; ++i) {
            UpstreamClient_t* c = (UpstreamClient_t*)events[i].data.ptr;

            handleUpstreamEvent(r, c, events[i].events);
        }
    }

    logger(LOG_INFO, WS_CLIENT "Upstream reactor завершён");

    return NULL;
}
static int queueHandshake(UpstreamReactor_t* r, UpstreamClient_t* c) {
    uint8_t nonce[16];

    if (randomBytes(nonce, sizeof(nonce)) != 0) {
        return -1;
    }

    char key[25];

    base64Encode(nonce, sizeof(nonce), key);

    if (wsComputeAccept(key, 24, c->expectedAccept) != 0) {
        return -1;
    }

    char request[1024];

    int n = snprintf(request, sizeof(request),

                     "GET %s HTTP/1.1\r\n"
                     "Host: %s:%d\r\n"
                     "Upgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: %s\r\n"
                     "Sec-WebSocket-Version: 13\r\n"
                     "\r\n",

                     c->path, c->host, c->port, key);

    if (n < 0 || (size_t)n >= sizeof(request)) {
        return -1;
    }

    bufReset(&c->tx);

    bufAppend(&c->tx, request, (size_t)n);

    c->state = UPSTREAM_HANDSHAKE;

    return updateEvents(r, c);
}
static void handleUpstreamEvent(UpstreamReactor_t* r, UpstreamClient_t* c, uint32_t events) {
    /*
     * 0. Ошибка сокета — сразу disconnect/reconnect.
     */
    if (events & EPOLLERR) {
        int err = 0;
        socklen_t len = sizeof(err);

        if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
            err = errno;
        }

        logger(LOG_ERROR, WS_CLIENT "upstream %llu: socket error: %s", (unsigned long long)c->id,
               err ? strerror(err) : "unknown error");

        upstreamDisconnect(r, c);
        return;
    }

    /*
     * 1. Завершение non-blocking connect().
     */
    if ((events & EPOLLOUT) && c->state == UPSTREAM_CONNECTING) {
        if (upstreamConnectComplete(c) != 0) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: connect failed: %s", (unsigned long long)c->id,
                   strerror(errno));

            upstreamDisconnect(r, c);
            return;
        }

        logger(LOG_INFO, WS_CLIENT "upstream %llu: TCP connected", (unsigned long long)c->id);

        /*
         * Формируем HTTP Upgrade request и кладём его в c->tx.
         *
         * queueHandshake() должна:
         *  - подготовить Sec-WebSocket-Key
         *  - сохранить expectedAccept
         *  - положить HTTP request в c->tx
         *  - перевести state в UPSTREAM_HANDSHAKE
         *  - вызвать updateEvents()
         */
        if (queueHandshake(r, c) != 0) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: не удалось сформировать handshake",
                   (unsigned long long)c->id);

            upstreamDisconnect(r, c);
            return;
        }

        /*
         * Handshake лежит в tx.
         * Следующий EPOLLOUT отправит его.
         */
        return;
    }

    /*
     * 2. Сокет готов к записи.
     *
     * Например:
     *   - HTTP Upgrade request
     *   - позже PONG/CLOSE/client WS frames
     */
    if ((events & EPOLLOUT) && c->state != UPSTREAM_CONNECTING) {
        if (flushUpstreamTx(r, c) != 0) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: send failed: %s", (unsigned long long)c->id,
                   strerror(errno));

            upstreamDisconnect(r, c);
            return;
        }

        if (c->state == UPSTREAM_CLOSING && bufLen(&c->tx) == 0) {
            upstreamDisconnect(r, c);
            return;
        }
    }

    /*
     * 3. Получаем HTTP response на WebSocket Upgrade.
     */
    if ((events & EPOLLIN) && c->state == UPSTREAM_HANDSHAKE) {
        if (readHandshake(r, c) != 0) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: WebSocket handshake failed",
                   (unsigned long long)c->id);

            upstreamDisconnect(r, c);
            return;
        }

        /*
         * readHandshake() может здесь перевести:
         *
         * HANDSHAKE -> OPEN
         *
         * Причём после HTTP header в c->rx уже может
         * находиться первый WebSocket frame.
         *
         * Поэтому намеренно НЕ делаем return.
         */
    }

    /*
     * 4. WebSocket уже открыт.
     *
     * Следующим шагом здесь будет:
     *
     * readUpstreamFrames(r, c);
     *
     * Важно: функция должна сначала обработать то,
     * что уже лежит в c->rx, а потом дочитывать сокет.
     */
    if ((events & EPOLLIN) && c->state == UPSTREAM_OPEN) {
        if (readUpstreamFrames(r, c) != 0) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: WebSocket read failed", (unsigned long long)c->id);

            upstreamDisconnect(r, c);
            return;
        }
    }

    /*
     * 5. Peer закрыл соединение.
     *
     * Проверяем ПОСЛЕ EPOLLIN, потому что ядро
     * может вернуть одновременно:
     *
     * EPOLLIN | EPOLLRDHUP
     *
     * Это означает:
     * "данные ещё можно дочитать, но peer уже закрыл запись".
     */
    if (events & (EPOLLHUP | EPOLLRDHUP)) {
        logger(LOG_INFO, WS_CLIENT "upstream %llu: socket closed", (unsigned long long)c->id);

        upstreamDisconnect(r, c);
        return;
    }
}
static int readUpstreamFrames(UpstreamReactor_t* r, UpstreamClient_t* c) {
    /*
     * В c->rx уже могли остаться данные:
     *
     * - после HTTP 101
     * - после предыдущего recv()
     * - после backpressure
     */
    int rc = processBufferedFrames(r, c);

    if (rc < 0) {
        return -1;
    }

    if (rc == 1 || c->state != UPSTREAM_OPEN || c->readPaused) {
        return 0;
    }

    uint8_t tmp[8192];

    for (;;) {
        ssize_t n = recv(c->fd, tmp, sizeof(tmp), 0);

        if (n > 0) {
            bufAppend(&c->rx, tmp, (size_t)n);

            /*
             * Не ждём следующего epoll_wait().
             * Сразу разбираем полученные bytes.
             */
            rc = processBufferedFrames(r, c);

            if (rc < 0) {
                return -1;
            }

            if (rc == 1 || c->readPaused || c->state != UPSTREAM_OPEN) {
                return 0;
            }

            continue;
        }

        /*
         * TCP EOF.
         */
        if (n == 0) {
            return -1;
        }

        if (errno == EINTR) {
            continue;
        }

        /*
         * Всё доступное из kernel socket buffer
         * уже прочитано.
         *
         * Возвращаемся в epoll_wait().
         */
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }

        return -1;
    }
}
static int processBufferedFrames(UpstreamReactor_t* r, UpstreamClient_t* c) {
    ServerConfig_t* conf = r->conf;

    while (bufLen(&c->rx) > 0) {
        WsFrameHdr_t h;

        WsRc_t rc = wsParseHeader((const uint8_t*)bufData(&c->rx), bufLen(&c->rx), &h);

        if (rc == WS_NEED_MORE) {
            return 0;
        }

        if (rc == WS_ERR_PROTO) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: protocol error", (unsigned long long)c->id);

            if (beginClose(r, c, 1002) != 0) {
                return -1;
            }

            return 0;
        }

        /*
         * server -> client frame
         * НЕ должен быть masked.
         */
        if (h.masked) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: server sent masked frame", (unsigned long long)c->id);

            if (beginClose(r, c, 1002) != 0) {
                return -1;
            }

            return 0;
        }

        if (h.payloadLen > (uint64_t)conf->maxFrame) {
            if (beginClose(r, c, 1009) != 0) {
                return -1;
            }

            return 0;
        }

        size_t payloadLen = (size_t)h.payloadLen;

        if (payloadLen > SIZE_MAX - h.headerLen) {
            return -1;
        }

        size_t frameLen = h.headerLen + payloadLen;

        /*
         * Заголовок уже есть,
         * payload ещё пришёл не полностью.
         */
        if (bufLen(&c->rx) < frameLen) {
            return 0;
        }

        const uint8_t* payload = (const uint8_t*)bufData(&c->rx) + h.headerLen;

        /* ========================================
         * CONTROL FRAMES
         * ======================================== */

        if (h.opcode & 0x08) {
            /*
             * Control frame:
             * FIN обязан быть 1,
             * payload <= 125.
             */
            if (!h.fin || payloadLen > 125) {
                if (beginClose(r, c, 1002) != 0) {
                    return -1;
                }

                return 0;
            }

            if (h.opcode == WS_OP_PING) {
                logger(LOG_DEBUG, WS_CLIENT "upstream %llu: PING", (unsigned long long)c->id);

                /*
                 * PONG должен содержать тот же payload.
                 */
                if (queueMaskedFrame(r, c, WS_OP_PONG, payload, payloadLen) != 0) {
                    return -1;
                }
            } else if (h.opcode == WS_OP_CLOSE) {
                /*
                 * CLOSE payload длиной ровно 1 байт
                 * запрещён RFC.
                 */
                if (payloadLen == 1) {
                    if (beginClose(r, c, 1002) != 0) {
                        return -1;
                    }

                    return 0;
                }

                logger(LOG_INFO, WS_CLIENT "upstream %llu: CLOSE received", (unsigned long long)c->id);

                /*
                 * Сначала consume, потому что payload
                 * указывает внутрь c->rx.
                 *
                 * Для ответа надо сохранить payload.
                 */
                uint8_t closePayload[125];

                if (payloadLen > 0) {
                    memcpy(closePayload, payload, payloadLen);
                }

                bufConsume(&c->rx, frameLen);

                c->state = UPSTREAM_CLOSING;

                if (queueMaskedFrame(r, c, WS_OP_CLOSE, closePayload, payloadLen) != 0) {
                    return -1;
                }

                return 0;
            }

            /*
             * PING/PONG обработали.
             */
            bufConsume(&c->rx, frameLen);

            continue;
        }

        /* ========================================
         * DATA FRAMES
         * ======================================== */

        if (h.opcode == WS_OP_CONT) {
            if (!c->fragmenting) {
                if (beginClose(r, c, 1002) != 0) {
                    return -1;
                }

                return 0;
            }
        } else if (h.opcode == WS_OP_TEXT || h.opcode == WS_OP_BIN) {
            if (c->fragmenting) {
                /*
                 * Уже собираем fragmented message,
                 * а сервер начал новое.
                 */
                if (beginClose(r, c, 1002) != 0) {
                    return -1;
                }

                return 0;
            }

            c->msgOpcode = (uint8_t)h.opcode;

            c->fragmenting = 1;

            bufReset(&c->msg);
            wsUtf8Reset(&c->msgUtf8);
        } else {
            if (beginClose(r, c, 1002) != 0) {
                return -1;
            }

            return 0;
        }

        /*
         * Защита maxMessage.
         */
        size_t current = bufLen(&c->msg);

        if (current > conf->maxMessage || payloadLen > conf->maxMessage - current) {
            if (beginClose(r, c, 1009) != 0) {
                return -1;
            }

            return 0;
        }

        /*
         * Проверяем UTF-8 постепенно,
         * включая fragmented TEXT.
         */
        if (c->msgOpcode == WS_OP_TEXT && payloadLen > 0) {
            if (wsUtf8Feed(&c->msgUtf8, payload, payloadLen) != 0) {
                if (beginClose(r, c, 1007) != 0) {
                    return -1;
                }

                return 0;
            }
        }

        bufAppend(&c->msg, payload, payloadLen);

        /*
         * Frame уже скопирован в msg.
         */
        bufConsume(&c->rx, frameLen);

        /*
         * Сообщение ещё продолжается.
         */
        if (!h.fin) {
            continue;
        }

        /* ========================================
         * MESSAGE COMPLETE
         * ======================================== */

        c->fragmenting = 0;

        if (c->msgOpcode == WS_OP_TEXT && !wsUtf8Done(&c->msgUtf8)) {
            if (beginClose(r, c, 1007) != 0) {
                return -1;
            }

            return 0;
        }

        logger(LOG_INFO, WS_CLIENT "upstream %llu: message len=%zu", (unsigned long long)c->id,
               bufLen(&c->msg));

        /*
         * Создаёт Msg_t и отправляет
         * его в dataQueue.
         */
        int publishRc = publishReactorMessage(r, c);

        /*
         * msgNewData() сделал свою копию.
         */
        bufReset(&c->msg);

        if (publishRc < 0) {
            return -1;
        }

        if (publishRc == 1) {
            /*
             * highWatermark.
             *
             * Этот конкретный upstream теперь
             * readPaused.
             *
             * Остальные upstream продолжают работать.
             */
            return 1;
        }
    }

    return 0;
}
static int beginClose(UpstreamReactor_t* r, UpstreamClient_t* c, uint16_t code) {
    uint8_t payload[2] = {(uint8_t)(code >> 8), (uint8_t)code};

    c->state = UPSTREAM_CLOSING;

    return queueMaskedFrame(r, c, WS_OP_CLOSE, payload, sizeof(payload));
}
static int queueMaskedFrame(UpstreamReactor_t* r, UpstreamClient_t* c, uint8_t opcode, const void* payload,
                            size_t payloadLen) {
    uint8_t header[14];
    size_t hlen = 0;

    header[hlen++] = (uint8_t)(0x80 | (opcode & 0x0F));

    uint64_t len = (uint64_t)payloadLen;

    if (len <= 125) {
        header[hlen++] = (uint8_t)(0x80 | len);
    } else if (len <= 0xFFFF) {
        header[hlen++] = 0x80 | 126;

        header[hlen++] = (uint8_t)(len >> 8);

        header[hlen++] = (uint8_t)len;
    } else {
        header[hlen++] = 0x80 | 127;

        for (int i = 7; i >= 0; --i) {
            header[hlen++] = (uint8_t)(len >> (8 * i));
        }
    }

    uint8_t mask[4];

    if (randomBytes(mask, sizeof(mask)) != 0) {
        return -1;
    }

    memcpy(header + hlen, mask, sizeof(mask));

    hlen += sizeof(mask);

    bufAppend(&c->tx, header, hlen);

    if (payloadLen > 0) {
        uint8_t* masked = malloc(payloadLen);

        if (!masked) {
            return -1;
        }

        const uint8_t* src = (const uint8_t*)payload;

        for (size_t i = 0; i < payloadLen; ++i) {
            masked[i] = src[i] ^ mask[i & 3];
        }

        bufAppend(&c->tx, masked, payloadLen);

        free(masked);
    }

    /*
     * tx теперь непустой →
     * updateEvents добавит EPOLLOUT.
     */
    return updateEvents(r, c);
}
static int readHandshake(UpstreamReactor_t* r, UpstreamClient_t* c) {
    char tmp[4096];

    for (;;) {
        ssize_t n = recv(c->fd, tmp, sizeof(tmp), 0);

        if (n > 0) {
            bufAppend(&c->rx, tmp, (size_t)n);

            continue;
        }

        if (n == 0) {
            /*
             * Peer сделал EOF.
             */
            return -1;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }

        return -1;
    }

    return processHandshakeResponse(r, c);
}
static int processHandshakeResponse(UpstreamReactor_t* r, UpstreamClient_t* c) {
    const char* data = bufData(&c->rx);

    size_t len = bufLen(&c->rx);

    const char* end = httpFindHeaderEnd(data, len);

    if (!end) {
        if (len > r->conf->handshakeMax) {
            logger(LOG_ERROR, WS_CLIENT "upstream %llu: handshake слишком большой",
                   (unsigned long long)c->id);

            return -1;
        }

        /*
         * HTTP header ещё пришёл не полностью.
         */
        return 0;
    }

    size_t headerLen = (size_t)(end - data) + 4;

    /*
     * Проверяем:
     *
     * HTTP/1.1 101 Switching Protocols
     */
    if (headerLen < 12 || memcmp(data, "HTTP/1.1 101", 12) != 0) {
        logger(LOG_ERROR, WS_CLIENT "upstream %llu: сервер не вернул HTTP 101", (unsigned long long)c->id);

        return -1;
    }

    /* Upgrade: websocket */

    size_t upgradeLen = 0;

    const char* upgrade = httpFindHeader(data, headerLen, "Upgrade", &upgradeLen);

    if (!upgrade || !httpHeaderHasToken(upgrade, upgradeLen, "websocket")) {
        return -1;
    }

    /* Connection: Upgrade */

    size_t connectionLen = 0;

    const char* connection = httpFindHeader(data, headerLen, "Connection", &connectionLen);

    if (!connection || !httpHeaderHasToken(connection, connectionLen, "Upgrade")) {
        return -1;
    }

    /* Sec-WebSocket-Accept */

    size_t acceptLen = 0;

    const char* accept = httpFindHeader(data, headerLen, "Sec-WebSocket-Accept", &acceptLen);

    if (!accept || acceptLen != 28 || memcmp(accept, c->expectedAccept, 28) != 0) {
        logger(LOG_ERROR, WS_CLIENT "upstream %llu: неверный Sec-WebSocket-Accept",
               (unsigned long long)c->id);

        return -1;
    }

    /*
     * Убираем HTTP header.
     *
     * После него в rx потенциально уже может
     * лежать первый WebSocket frame.
     */
    bufConsume(&c->rx, headerLen);

    c->state = UPSTREAM_OPEN;

    /*
     * Успешно подключились —
     * сбрасываем reconnect backoff.
     */
    c->reconnectAttempt = 0;

    logger(LOG_INFO, WS_CLIENT "upstream %llu: WebSocket OPEN", (unsigned long long)c->id);

    return updateEvents(r, c);
}
static int flushUpstreamTx(UpstreamReactor_t* r, UpstreamClient_t* c) {
    while (bufLen(&c->tx) > 0) {
        ssize_t n = send(c->fd, bufData(&c->tx), bufLen(&c->tx), MSG_NOSIGNAL);

        if (n > 0) {
            bufConsume(&c->tx, (size_t)n);

            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        }

        return -1;
    }

    /*
     * Если tx опустел, updateEvents()
     * уберёт EPOLLOUT.
     */
    if (updateEvents(r, c) != 0) {
        return -1;
    }

    return 0;
}
/* ============================================================
 * TCP
 * ============================================================ */

static int waitConnect(int fd, ServerConfig_t* conf) {
    /*
     * Проверяем connect каждые 250 мс.
     * Максимум ~5 секунд.
     */
    int elapsed = 0;

    while (elapsed < 5000) {
        if (atomic_load(&conf->shutdown)) {
            errno = ECANCELED;
            return -1;
        }

        struct pollfd pfd = {.fd = fd, .events = POLLOUT};

        int rc = poll(&pfd, 1, 250);

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }

        if (rc == 0) {
            elapsed += 250;
            continue;
        }

        int err = 0;
        socklen_t len = sizeof err;

        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
            return -1;
        }

        if (err != 0) {
            errno = err;
            return -1;
        }

        return 0;
    }

    errno = ETIMEDOUT;
    return -1;
}
static int publishUpstreamMessage(WsClientArgs_t* args, WsClientRx_t* st) {
    ServerConfig_t* conf = args->conf;

    Msg_t* m = msgNewData(0, /* временный ID upstream-источника */
                          st->msgOpcode, bufData(&st->msg), bufLen(&st->msg));

    if (!m) {
        logger(LOG_ERROR, WS_CLIENT "Не удалось создать Msg_t");

        /*
         * Аналогично worker.c:
         * потеря одного сообщения не рвёт WS-соединение.
         */
        return 0;
    }

    /*
     * У обычного worker есть Conn_t, поэтому он может
     * parkListPush() и убрать fd из epoll.
     *
     * У ws_client Conn_t нет, поэтому пока используем
     * простой backpressure: перестаём читать upstream,
     * пока очередь не освободится.
     */
    while (queueSize(conf->dataQueue) >= conf->highWatermark) {
        if (atomic_load(&conf->shutdown)) {
            msgFree(m);
            return -1;
        }

        usleep(1000);
    }

    push(conf->dataQueue, m);

    return 0;
}
static int tcpConnect(WsClientArgs_t* args) {
    char port[16];

    snprintf(port, sizeof port, "%d", args->port);

    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* result = NULL;

    int rc = getaddrinfo(args->host, port, &hints, &result);

    if (rc != 0) {
        logger(LOG_ERROR, WS_CLIENT "getaddrinfo(%s): %s", args->host, gai_strerror(rc));

        return -1;
    }

    int fd = -1;

    for (struct addrinfo* ai = result; ai != NULL; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);

        if (fd < 0) {
            continue;
        }

        int flags = fcntl(fd, F_GETFL, 0);

        if (flags < 0) {
            close(fd);
            fd = -1;
            continue;
        }

        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
            close(fd);
            fd = -1;
            continue;
        }

        rc = connect(fd, ai->ai_addr, ai->ai_addrlen);

        if (rc != 0) {
            if (errno != EINPROGRESS || waitConnect(fd, args->conf) != 0) {
                close(fd);
                fd = -1;
                continue;
            }
        }

        /*
         * connect делали non-blocking только чтобы не зависнуть.
         * После подключения возвращаем обычный blocking режим.
         *
         * bufRecv() всё равно использует MSG_DONTWAIT.
         */
        if (fcntl(fd, F_SETFL, flags) != 0) {
            close(fd);
            fd = -1;
            continue;
        }

        break;
    }

    freeaddrinfo(result);

    return fd;
}

static int sendAll(int fd, const void* data, size_t len) {
    const uint8_t* p = data;
    size_t off = 0;

    while (off < len) {
        ssize_t n = send(fd, p + off, len - off, MSG_NOSIGNAL);

        if (n > 0) {
            off += (size_t)n;
            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        return -1;
    }

    return 0;
}

/* ============================================================
 * Исходящие CLIENT WebSocket frames
 *
 * ВАЖНО:
 *
 * client -> server всегда MASK=1.
 * ============================================================ */

static int sendMaskedFrame(int fd, uint8_t opcode, const void* payload, size_t payloadLen) {
    uint8_t header[14];
    size_t hlen = 0;

    header[hlen++] = (uint8_t)(0x80 | (opcode & 0x0F));

    uint64_t len = (uint64_t)payloadLen;

    if (len <= 125) {
        header[hlen++] = (uint8_t)(0x80 | len);
    } else if (len <= 0xFFFF) {
        header[hlen++] = 0x80 | 126;

        header[hlen++] = (uint8_t)(len >> 8);
        header[hlen++] = (uint8_t)len;
    } else {
        header[hlen++] = 0x80 | 127;

        for (int i = 7; i >= 0; --i) {
            header[hlen++] = (uint8_t)(len >> (8 * i));
        }
    }

    uint8_t mask[4];

    if (randomBytes(mask, sizeof mask) != 0) {
        return -1;
    }

    memcpy(header + hlen, mask, sizeof mask);
    hlen += sizeof mask;

    uint8_t* masked = NULL;

    if (payloadLen > 0) {
        masked = malloc(payloadLen);

        if (!masked) {
            return -1;
        }

        const uint8_t* src = payload;

        for (size_t i = 0; i < payloadLen; ++i) {
            masked[i] = src[i] ^ mask[i & 3];
        }
    }

    if (sendAll(fd, header, hlen) != 0) {
        free(masked);
        return -1;
    }

    if (payloadLen > 0 && sendAll(fd, masked, payloadLen) != 0) {
        free(masked);
        return -1;
    }

    free(masked);

    return 0;
}

static int sendCloseCode(int fd, uint16_t code) {
    uint8_t payload[2] = {(uint8_t)(code >> 8), (uint8_t)code};

    return sendMaskedFrame(fd, WS_OP_CLOSE, payload, sizeof payload);
}

/* ============================================================
 * Handshake
 * ============================================================ */

static int readUpgradeResponse(int fd, WsClientArgs_t* args, const char expectedAccept[29], Buf_t* rx) {
    int waited = 0;

    for (;;) {
        if (bufLen(rx) > 0) {
            const char* data = bufData(rx);

            const char* end = httpFindHeaderEnd(data, bufLen(rx));

            if (end) {
                size_t headerLen = (size_t)(end - data) + 4;

                if (headerLen > args->conf->handshakeMax) {
                    logger(LOG_ERROR, WS_CLIENT "слишком большой handshake");

                    return -1;
                }

                /*
                 * Пока поддерживаем HTTP/1.1.
                 */
                if (headerLen < 12 || memcmp(data, "HTTP/1.1 101", 12) != 0) {
                    logger(LOG_ERROR, WS_CLIENT "сервер не вернул 101 Switching Protocols");

                    return -1;
                }

                size_t upgradeLen = 0;

                const char* upgrade = httpFindHeader(data, headerLen, "Upgrade", &upgradeLen);

                if (!upgrade || !httpHeaderHasToken(upgrade, upgradeLen, "websocket")) {
                    logger(LOG_ERROR, WS_CLIENT "нет Upgrade: websocket");

                    return -1;
                }

                size_t connectionLen = 0;

                const char* connection = httpFindHeader(data, headerLen, "Connection", &connectionLen);

                if (!connection || !httpHeaderHasToken(connection, connectionLen, "Upgrade")) {
                    logger(LOG_ERROR, WS_CLIENT "нет Connection: Upgrade");

                    return -1;
                }

                size_t acceptLen = 0;

                const char* accept = httpFindHeader(data, headerLen, "Sec-WebSocket-Accept", &acceptLen);

                if (!accept || acceptLen != 28 || memcmp(accept, expectedAccept, 28) != 0) {
                    logger(LOG_ERROR, WS_CLIENT "неверный Sec-WebSocket-Accept");

                    return -1;
                }

                /*
                 * В rx после HTTP-заголовков уже может лежать
                 * первый WS frame.
                 *
                 * Поэтому НЕ reset, а consume.
                 */
                bufConsume(rx, headerLen);

                return 0;
            }

            if (bufLen(rx) > args->conf->handshakeMax) {
                logger(LOG_ERROR, WS_CLIENT "handshake превысил лимит");

                return -1;
            }
        }

        if (atomic_load(&args->conf->shutdown)) {
            return -1;
        }

        struct pollfd pfd = {.fd = fd, .events = POLLIN};

        int rc = poll(&pfd, 1, 500);

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }

        if (rc == 0) {
            waited += 500;

            if (waited >= 5000) {
                logger(LOG_ERROR, WS_CLIENT "таймаут handshake");

                return -1;
            }

            continue;
        }

        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            return -1;
        }

        char tmp[4096];

        ssize_t n = recv(fd, tmp, sizeof tmp, 0);

        if (n > 0) {
            bufAppend(rx, tmp, (size_t)n);
            continue;
        }

        if (n == 0) {
            return -1;
        }

        if (errno == EINTR) {
            continue;
        }

        return -1;
    }
}

static int performHandshake(int fd, WsClientArgs_t* args, Buf_t* rx) {
    if (!args->path || args->path[0] != '/') {
        logger(LOG_ERROR, WS_CLIENT "path должен начинаться с '/'");

        return -1;
    }

    uint8_t nonce[16];

    if (randomBytes(nonce, sizeof nonce) != 0) {
        logger(LOG_ERROR, WS_CLIENT "не удалось получить random для Sec-WebSocket-Key");

        return -1;
    }

    char key[25];

    base64Encode(nonce, sizeof nonce, key);

    char expectedAccept[29];

    if (wsComputeAccept(key, 24, expectedAccept) != 0) {
        return -1;
    }

    char request[1024];

    int n = snprintf(request, sizeof request,

                     "GET %s HTTP/1.1\r\n"
                     "Host: %s:%d\r\n"
                     "Upgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: %s\r\n"
                     "Sec-WebSocket-Version: 13\r\n"
                     "\r\n",

                     args->path, args->host, args->port, key);

    if (n < 0 || (size_t)n >= sizeof request) {
        return -1;
    }

    if (sendAll(fd, request, (size_t)n) != 0) {
        logger(LOG_ERROR, WS_CLIENT "ошибка отправки handshake");

        return -1;
    }

    return readUpgradeResponse(fd, args, expectedAccept, rx);
}

/* ============================================================
 * WebSocket receive
 * ============================================================ */

static int protocolError(int fd, uint16_t code, const char* reason) {
    logger(LOG_ERROR, WS_CLIENT "%s (close=%u)", reason, code);

    (void)sendCloseCode(fd, code);

    return -1;
}

static void logMessage(WsClientRx_t* st) {
    size_t len = bufLen(&st->msg);

    if (st->msgOpcode == WS_OP_TEXT) {
        size_t previewLen = len > 256 ? 256 : len;

        const char* data = len > 0 ? bufData(&st->msg) : "";

        logger(LOG_INFO, WS_CLIENT "TEXT len=%zu: %.*s%s", len, (int)previewLen, data,
               len > previewLen ? "..." : "");
    } else {
        logger(LOG_INFO, WS_CLIENT "BINARY len=%zu", len);
    }
}

/*
 * return:
 *
 *  0 -> продолжаем
 *  1 -> получен CLOSE
 * -1 -> ошибка
 */
static int processFrames(int fd, WsClientArgs_t* args, WsClientRx_t* st) {
    while (bufLen(&st->rx) > 0) {
        WsFrameHdr_t h;

        WsRc_t rc = wsParseHeader((const uint8_t*)bufData(&st->rx), bufLen(&st->rx), &h);

        if (rc == WS_NEED_MORE) {
            return 0;
        }

        if (rc == WS_ERR_PROTO) {
            return protocolError(fd, 1002, "ошибка заголовка WS frame");
        }

        /*
         * Мы CLIENT.
         *
         * server -> client НЕ должен быть masked.
         */
        if (h.masked) {
            return protocolError(fd, 1002, "сервер прислал masked frame");
        }

        if (h.payloadLen > (uint64_t)args->conf->maxFrame) {
            return protocolError(fd, 1009, "frame слишком большой");
        }

        size_t payloadLen = (size_t)h.payloadLen;

        if (payloadLen > SIZE_MAX - h.headerLen) {
            return protocolError(fd, 1009, "переполнение размера frame");
        }

        size_t frameLen = h.headerLen + payloadLen;

        if (bufLen(&st->rx) < frameLen) {
            return 0;
        }

        const uint8_t* payload = (const uint8_t*)bufData(&st->rx) + h.headerLen;

        /* ================= CONTROL ================= */

        if (h.opcode & 0x8) {
            if (h.opcode == WS_OP_PING) {
                logger(LOG_DEBUG, WS_CLIENT "получен PING");

                if (sendMaskedFrame(fd, WS_OP_PONG, payload, payloadLen) != 0) {
                    return -1;
                }
            } else if (h.opcode == WS_OP_CLOSE) {
                logger(LOG_INFO, WS_CLIENT "получен CLOSE");

                if (payloadLen == 1) {
                    return protocolError(fd, 1002, "CLOSE payload длиной 1");
                }

                /*
                 * Отвечаем CLOSE тем же payload.
                 * Но уже MASKED, потому что мы client.
                 */
                (void)sendMaskedFrame(fd, WS_OP_CLOSE, payload, payloadLen);

                bufConsume(&st->rx, frameLen);

                return 1;
            }

            /*
             * PONG просто игнорируем.
             */
            bufConsume(&st->rx, frameLen);

            continue;
        }

        /* ================= DATA ================= */

        if (h.opcode == WS_OP_CONT) {
            if (!st->fragmenting) {
                return protocolError(fd, 1002, "CONT без начального frame");
            }
        } else {
            if (st->fragmenting) {
                return protocolError(fd, 1002, "новое сообщение до окончания fragment");
            }

            st->msgOpcode = (uint8_t)h.opcode;

            st->fragmenting = 1;

            bufReset(&st->msg);
            wsUtf8Reset(&st->msgUtf8);
        }

        size_t current = bufLen(&st->msg);

        if (current > args->conf->maxMessage || payloadLen > args->conf->maxMessage - current) {
            return protocolError(fd, 1009, "WS message слишком большое");
        }

        if (st->msgOpcode == WS_OP_TEXT && payloadLen > 0) {
            if (wsUtf8Feed(&st->msgUtf8, payload, payloadLen) != 0) {
                return protocolError(fd, 1007, "невалидный UTF-8");
            }
        }

        bufAppend(&st->msg, payload, payloadLen);

        bufConsume(&st->rx, frameLen);

        if (!h.fin) {
            continue;
        }

        st->fragmenting = 0;

        if (st->msgOpcode == WS_OP_TEXT && !wsUtf8Done(&st->msgUtf8)) {
            return protocolError(fd, 1007, "незавершённый UTF-8");
        }
        if (publishUpstreamMessage(args, st) != 0) {
            logger(LOG_ERROR, WS_CLIENT "не удалось передать сообщение агрегатору");

            return -1;
        }
        logMessage(st);
        if (publishUpstreamMessage(args, st) != 0) {
            logger(LOG_ERROR, WS_CLIENT "ошибка публикации upstream сообщения");

            return -1;
        }
        bufReset(&st->msg);
    }

    return 0;
}

/* ============================================================
 * Main receive loop
 * ============================================================ */

static int receiveLoop(int fd, WsClientArgs_t* args, WsClientRx_t* st) {
    for (;;) {
        /*
         * В rx мог остаться первый WS frame,
         * пришедший вместе с HTTP 101.
         */
        int rc = processFrames(fd, args, st);

        if (rc != 0) {
            return rc;
        }

        if (atomic_load(&args->conf->shutdown)) {
            logger(LOG_INFO, WS_CLIENT "shutdown, отправляем CLOSE");

            (void)sendCloseCode(fd, 1000);

            return 0;
        }

        struct pollfd pfd = {.fd = fd, .events = POLLIN};

        rc = poll(&pfd, 1, 500);

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }

        if (rc == 0) {
            continue;
        }

        if (pfd.revents & POLLIN) {
            int rr = bufRecv(fd, &st->rx, 4096, args->conf->maxMessage + args->conf->maxFrame);

            if (rr == -2) {
                return protocolError(fd, 1009, "rx buffer превысил лимит");
            }

            if (rr == -1) {
                logger(LOG_INFO, WS_CLIENT "TCP соединение закрыто");

                return 0;
            }

            int eof = (rr == 2);

            rc = processFrames(fd, args, st);

            if (rc != 0) {
                return rc;
            }

            if (eof) {
                logger(LOG_INFO, WS_CLIENT "EOF");

                return 0;
            }
        }

        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            logger(LOG_INFO, WS_CLIENT "сокет закрыт/error");

            return 0;
        }
    }
}

/* ============================================================
 * Thread
 * ============================================================ */

void* wsClientThread(void* arg) {
    WsClientArgs_t* args = (WsClientArgs_t*)arg;

    logger(LOG_INFO, WS_CLIENT "подключаемся к ws://%s:%d%s", args->host, args->port, args->path);

    int fd = tcpConnect(args);

    if (fd < 0) {
        logger(LOG_ERROR, WS_CLIENT "не удалось подключиться: %s", strerror(errno));

        return NULL;
    }

    logger(LOG_INFO, WS_CLIENT "TCP соединение установлено");

    WsClientRx_t st;
    memset(&st, 0, sizeof st);

    wsUtf8Reset(&st.msgUtf8);

    if (performHandshake(fd, args, &st.rx) != 0) {
        logger(LOG_ERROR, WS_CLIENT "WebSocket handshake завершился ошибкой");

        close(fd);

        bufFree(&st.rx);
        bufFree(&st.msg);

        return NULL;
    }

    logger(LOG_INFO, WS_CLIENT "WebSocket соединение установлено");

    (void)receiveLoop(fd, args, &st);

    close(fd);

    bufFree(&st.rx);
    bufFree(&st.msg);

    logger(LOG_INFO, WS_CLIENT "клиентский поток завершён");

    return NULL;
}
