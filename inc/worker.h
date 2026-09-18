#ifndef WORKER_H
#define WORKER_H

#include <stdint.h>
#include "conn.h"

/*
 * Worker поток: забирает соединения из очереди и обрабатывает их —
 * HTTP-рукопожатие WebSocket (RFC 6455) и, для источников, сами WS-кадры.
 * После обработки возвращает fd в epoll через rearmEpoll() (EPOLLONESHOT),
 * либо закрывает соединение, либо (для sink'а) передаёт его агрегатору.
 */
void *workerThread(void *args);

/* Возвращение fd в epoll после обработки (EPOLLONESHOT). */
void rearmEpoll(Conn_t *c, uint32_t events);

#endif /* WORKER_H */
