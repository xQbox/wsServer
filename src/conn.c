#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "conn.h"
#include "agg.h"   /* msgFree — на случай, если у уничтожаемого соединения остался pendingMsg */

/* ============================ Buf_t ============================ */

static void bufCompact(Buf_t *b)
{
    if (b->off == 0)
    {
        return;
    }
    size_t remain = b->len - b->off;
    if (remain > 0)
    {
        memmove(b->p, b->p + b->off, remain);
    }
    b->len = remain;
    b->off = 0;
}

int bufReserve(Buf_t *b, size_t extra, size_t hardCap)
{
    if (b->cap - b->len >= extra)
    {
        return 0;
    }

    bufCompact(b);
    if (b->cap - b->len >= extra)
    {
        return 0;
    }

    size_t need = b->len + extra;
    size_t newCap = b->cap ? b->cap : 256;
    while (newCap < need)
    {
        newCap *= 2;
    }

    if (hardCap != 0 && newCap > hardCap)
    {
        if (need > hardCap)
        {
            return -1; /* даже максимум не вмещает запрошенное */
        }
        newCap = hardCap;
    }

    char *np = realloc(b->p, newCap);
    if (!np)
    {
        return -1;
    }
    b->p = np;
    b->cap = newCap;
    return 0;
}

void bufConsume(Buf_t *b, size_t n)
{
    b->off += n;
    if (b->off >= b->len)
    {
        b->off = 0;
        b->len = 0;
    }
}

void bufAppend(Buf_t *b, const void *data, size_t n)
{
    if (n == 0)
    {
        return;
    }
    /* hardCap = 0: без ограничения — используется только для буферов, которые
       формирует сам сервер (ответ рукопожатия, control-кадры), их размер мал
       и не зависит от того, что прислал клиент. */
    if (bufReserve(b, n, 0) != 0)
    {
        return; /* OOM на аллокации в несколько сотен байт — восстановиться нечем */
    }
    memcpy(b->p + b->len, data, n);
    b->len += n;
}

void bufReset(Buf_t *b)
{
    b->len = 0;
    b->off = 0;
}

void bufFree(Buf_t *b)
{
    free(b->p);
    b->p = NULL;
    b->cap = 0;
    b->len = 0;
    b->off = 0;
}

int bufRecv(int fd, Buf_t *b, size_t chunk, size_t hardCap)
{
    int got = 0;
    for (;;)
    {
        if (bufReserve(b, chunk, hardCap) != 0)
        {
            return -2;
        }
        ssize_t n = recv(fd, b->p + b->len, b->cap - b->len, MSG_DONTWAIT);
        if (n > 0)
        {
            b->len += (size_t)n;
            got = 1;
            continue;
        }
        if (n == 0)
        {
            /* EOF. Если этим же вызовом что-то уже прочитано (обычный случай
               "прислал кадр и тут же закрыл соединение") — отдаём это на
               разбор (код 2), а не отбрасываем: peer мог отправить FIN сразу
               вслед за валидными данными, и второй edge-triggered EPOLLIN по
               этому fd может никогда не прийти. Закрывать — обязанность
               вызывающего, после того как он разберёт b. */
            return got ? 2 : -1;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return got ? 1 : 0;
        }
        /* Реальная ошибка сокета: та же логика — не терять уже прочитанное. */
        return got ? 2 : -1;
    }
}

int bufSend(int fd, Buf_t *b)
{
    while (bufLen(b) > 0)
    {
        ssize_t n = send(fd, bufData(b), bufLen(b), MSG_DONTWAIT | MSG_NOSIGNAL);
        if (n > 0)
        {
            bufConsume(b, (size_t)n);
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            return 0;
        }
        return -1;
    }
    return 1;
}

/* ============================ Реестр соединений ============================ */

static struct
{
    pthread_mutex_t mu;
    Conn_t *head;
    _Atomic uint64_t nextId;
} g_registry;

void registryInit(void)
{
    pthread_mutex_init(&g_registry.mu, NULL);
    g_registry.head = NULL;
    atomic_init(&g_registry.nextId, 1);
}

static void registryAdd(Conn_t *c)
{
    pthread_mutex_lock(&g_registry.mu);
    c->regPrev = NULL;
    c->regNext = g_registry.head;
    if (g_registry.head)
    {
        g_registry.head->regPrev = c;
    }
    g_registry.head = c;
    pthread_mutex_unlock(&g_registry.mu);
}

static void registryRemove(Conn_t *c)
{
    pthread_mutex_lock(&g_registry.mu);
    if (c->regPrev) c->regPrev->regNext = c->regNext; else g_registry.head = c->regNext;
    if (c->regNext) c->regNext->regPrev = c->regPrev;
    pthread_mutex_unlock(&g_registry.mu);
}

/* Общая часть освобождения памяти Conn_t (fd закрывать должен вызывающий раньше). */
static void connFreeResources(Conn_t *c)
{
    bufFree(&c->rx);
    bufFree(&c->tx);
    bufFree(&c->msg);
    if (c->pendingMsg)
    {
        msgFree(c->pendingMsg);
        c->pendingMsg = NULL;
    }
    free(c);
}

/* ============================ Conn_t ============================ */

Conn_t *connNew(int fd, int epollfd)
{
    Conn_t *c = calloc(1, sizeof *c);
    if (!c)
    {
        return NULL;
    }

    c->tag.kind = EV_CONN;
    c->tag.obj  = c;
    c->fd = fd;
    c->epollfd = epollfd;
    c->id = atomic_fetch_add_explicit(&g_registry.nextId, 1, memory_order_relaxed);
    c->role = ROLE_UNDECIDED;
    c->state = ST_HANDSHAKE_READ;

    registryAdd(c);
    return c;
}

void connDestroy(Conn_t *c)
{
    if (!c)
    {
        return;
    }
    registryRemove(c);
    connFreeResources(c);
}

void connClose(Conn_t *c)
{
    if (!c)
    {
        return;
    }
    if (c->epollfd >= 0)
    {
        epoll_ctl(c->epollfd, EPOLL_CTL_DEL, c->fd, NULL);
    }
    close(c->fd);
    connDestroy(c);
}

void registryDestroyAll(void)
{
    pthread_mutex_lock(&g_registry.mu);
    Conn_t *c = g_registry.head;
    g_registry.head = NULL;
    pthread_mutex_unlock(&g_registry.mu);

    /* Вызывается после join всех потоков, которые могли трогать соединения:
       epollfd листенеров уже закрыт, поэтому epoll_ctl(DEL) не нужен и небезопасен
       (fd мог быть переиспользован) — только close() самого клиентского сокета. */
    while (c)
    {
        Conn_t *next = c->regNext;
        close(c->fd);
        connFreeResources(c);
        c = next;
    }

    pthread_mutex_destroy(&g_registry.mu);
}
