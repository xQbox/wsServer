#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>

#include "agg.h"
#include "conn.h"
#include "ws.h"
#include "queue.h"
#include "server.h"
#include "logger.h"

/* ============================ Msg_t / Frame_t ============================ */

Msg_t *msgNewData(uint64_t srcId, uint8_t opcode, const void *payload, size_t n)
{
    Msg_t *m = malloc(sizeof *m + n);
    if (!m)
    {
        return NULL;
    }
    m->kind = MSG_DATA;
    m->srcId = srcId;
    m->opcode = opcode;
    /* CLOCK_REALTIME, не MONOTONIC: это поле уходит наружу в JSON ("t") —
       внешнему потребителю нужны часы, сравнимые с его собственными
       (time.time() и т.п.), а не отсчёт от произвольной точки в этом
       процессе. Для внутренней периодики (тик статистики) используется
       отдельный timerfd на CLOCK_MONOTONIC — см. aggregatorThread(). */
    clock_gettime(CLOCK_REALTIME, &m->ts);
    m->conn = NULL;
    m->len = n;
    if (n > 0)
    {
        memcpy(m->data, payload, n);
    }
    return m;
}

Msg_t *msgNewSrcDown(uint64_t srcId)
{
    Msg_t *m = malloc(sizeof *m);
    if (!m)
    {
        return NULL;
    }
    m->kind = MSG_SRC_DOWN;
    m->srcId = srcId;
    m->opcode = 0;
    m->ts = (struct timespec){0, 0};
    m->conn = NULL;
    m->len = 0;
    return m;
}

Msg_t *msgNewSinkAdd(Conn_t *c)
{
    Msg_t *m = malloc(sizeof *m);
    if (!m)
    {
        return NULL;
    }
    m->kind = MSG_SINK_ADD;
    m->srcId = 0;
    m->opcode = 0;
    m->ts = (struct timespec){0, 0};
    m->conn = c;
    m->len = 0;
    return m;
}

void msgFree(Msg_t *m)
{
    free(m);
}

Frame_t *frameNew(uint8_t opcode, const void *payload, size_t n)
{
    uint8_t hdr[10];
    size_t hdrLen = wsEncodeHeader(hdr, opcode, n);

    Frame_t *f = malloc(sizeof *f + hdrLen + n);
    if (!f)
    {
        return NULL;
    }
    f->len = hdrLen + n;
    memcpy(f->bytes, hdr, hdrLen);
    if (n > 0)
    {
        memcpy(f->bytes + hdrLen, payload, n);
    }
    return f;
}

void frameFree(Frame_t *f)
{
    free(f);
}

/* ============================ park-лист ============================ */

void parkListPush(Agg_t *a, Conn_t *c)
{
    pthread_mutex_lock(&a->parkLock);
    c->parkNext = NULL;
    if (a->parkTail)
    {
        a->parkTail->parkNext = c;
    }
    else
    {
        a->parkHead = c;
    }
    a->parkTail = c;
    pthread_mutex_unlock(&a->parkLock);
}

Conn_t *parkListPop(Agg_t *a)
{
    pthread_mutex_lock(&a->parkLock);
    Conn_t *c = a->parkHead;
    if (c)
    {
        a->parkHead = c->parkNext;
        if (!a->parkHead)
        {
            a->parkTail = NULL;
        }
        c->parkNext = NULL;
    }
    pthread_mutex_unlock(&a->parkLock);
    return c;
}

/* ============================ Agg_t ============================ */

void aggInit(Agg_t *a, size_t highWatermark, size_t lowWatermark)
{
    memset(a, 0, sizeof *a);
    pthread_mutex_init(&a->parkLock, NULL);
    a->epollfd = -1;
    a->highWatermark = highWatermark;
    a->lowWatermark  = lowWatermark;
}

void aggFreeState(Agg_t *a)
{
    /* В штатном сценарии к моменту остановки агрегатора park-лист уже пуст
       (источники закрываются воркерами раньше). Если что-то всё же осталось —
       сами Conn_t не трогаем (их закроет финальный проход реестра в main),
       освобождаем только повисший pendingMsg, чтобы не потерять память. */
    Conn_t *c;
    while ((c = parkListPop(a)) != NULL)
    {
        if (c->pendingMsg)
        {
            msgFree(c->pendingMsg);
            c->pendingMsg = NULL;
        }
    }
    pthread_mutex_destroy(&a->parkLock);
}

static AggSrc_t *findOrCreateSrc(Agg_t *a, uint64_t id)
{
    for (int i = 0; i < a->srcCount; ++i)
    {
        if (a->src[i].id == id)
        {
            return &a->src[i];
        }
    }
    if (a->srcCount >= AGG_MAX_SOURCES)
    {
        return NULL; /* лимит источников исчерпан — сообщение всё равно обработаем, просто без учёта в статистике */
    }
    AggSrc_t *s = &a->src[a->srcCount++];
    s->id = id;
    s->alive = 1;
    s->msgCount = 0;
    s->byteCount = 0;
    return s;
}

void aggSrcDown(Agg_t *a, uint64_t srcId)
{
    for (int i = 0; i < a->srcCount; ++i)
    {
        if (a->src[i].id == srcId)
        {
            a->src[i].alive = 0;
            return;
        }
    }
}

/* Экранирование строки для JSON: кавычки/бэкслеш/управляющие символы —
 * остальное (включая многобайтовые UTF-8 последовательности) копируется как есть,
 * т.к. TEXT-payload уже провалидирован как корректный UTF-8 на этапе приёма. */
static size_t jsonEscapeInto(char *out, size_t cap, const char *in, size_t n)
{
    size_t o = 0;
    for (size_t i = 0; i < n; ++i)
    {
        unsigned char c = (unsigned char)in[i];
        char esc[8]; /* "\uXXXX" = 6 видимых символов + NUL, с запасом */
        size_t elen;

        switch (c)
        {
            case '"':  memcpy(esc, "\\\"", 2); elen = 2; break;
            case '\\': memcpy(esc, "\\\\", 2); elen = 2; break;
            case '\n': memcpy(esc, "\\n", 2);  elen = 2; break;
            case '\r': memcpy(esc, "\\r", 2);  elen = 2; break;
            case '\t': memcpy(esc, "\\t", 2);  elen = 2; break;
            default:
                if (c < 0x20)
                {
                    snprintf(esc, sizeof esc, "\\u%04x", c);
                    elen = 6;
                }
                else
                {
                    if (o + 1 > cap) return o;
                    out[o++] = (char)c;
                    continue;
                }
                break;
        }
        if (o + elen > cap)
        {
            return o; /* предохранитель — при точном расчёте cap сюда не попадаем */
        }
        memcpy(out + o, esc, elen);
        o += elen;
    }
    return o;
}

Frame_t *aggOnMessage(Agg_t *a, const Msg_t *m)
{
    AggSrc_t *s = findOrCreateSrc(a, m->srcId);
    if (s)
    {
        s->msgCount++;
        s->byteCount += m->len;
        s->lastTs = m->ts;
        s->alive = 1;
    }

    atomic_fetch_add_explicit(&a->statMsgsIn, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&a->statBytesIn, m->len, memory_order_relaxed);

    Frame_t *f;
    if (m->opcode == WS_OP_BIN)
    {
        /* Бинарные данные передаём как есть — тегирование сломало бы формат для потребителя. */
        f = frameNew(WS_OP_BIN, m->data, m->len);
    }
    else
    {
        /* TEXT: оборачиваем в компактный JSON с тегом источника — ровно то место,
           где потом появится реальная агрегация (окно/редьюс/join по ключу):
           см. контракт aggOnMessage()/aggOnTick() в inc/agg.h. Фаза 1 — passthrough. */
        size_t cap = m->len * 6 + 128; /* с запасом на \uXXXX-экранирование каждого байта */
        char *out = malloc(cap);
        if (!out)
        {
            return NULL; /* OOM на разовое сообщение — теряем его, не блокируем поток */
        }

        int n = snprintf(out, cap,
                          "{\"src\":%" PRIu64 ",\"seq\":%" PRIu64 ",\"t\":%ld.%06ld,\"data\":\"",
                          m->srcId, s ? s->msgCount : (uint64_t)0,
                          (long)m->ts.tv_sec, (long)(m->ts.tv_nsec / 1000));
        size_t o = (size_t)n;
        o += jsonEscapeInto(out + o, cap - o - 3, m->data, m->len);
        o += (size_t)snprintf(out + o, cap - o, "\"}");

        f = frameNew(WS_OP_TEXT, out, o);
        free(out);
    }

    if (f)
    {
        atomic_fetch_add_explicit(&a->statMsgsOut, 1, memory_order_relaxed);
    }
    return f;
}

Frame_t *aggOnTick(Agg_t *a, const struct timespec *now)
{
    (void)a;
    (void)now;
    /* Фаза 1: тик используется только для строки статистики в логе (см.
       logStats() ниже) — сам он ничего не отправляет sink'у. Окно по
       таймеру будущей манипуляции начинается ровно здесь: копить в
       AggSrc_t внутри aggOnMessage(), а тут — собрать и вернуть Frame_t. */
    return NULL;
}

/* ============================ поток агрегатора ============================ */

static void sendCloseBestEffort(int fd, uint16_t code)
{
    uint8_t payload[2] = { (uint8_t)(code >> 8), (uint8_t)code };
    Frame_t *f = frameNew(WS_OP_CLOSE, payload, sizeof payload);
    if (!f)
    {
        return;
    }
    size_t off = 0;
    while (off < f->len)
    {
        ssize_t n = send(fd, f->bytes + off, f->len - off, MSG_DONTWAIT | MSG_NOSIGNAL);
        if (n <= 0)
        {
            break; /* best-effort: не ждём и не повторяем при EAGAIN */
        }
        off += (size_t)n;
    }
    frameFree(f);
}

static void closeSink(Agg_t *agg)
{
    if (!agg->sink)
    {
        return;
    }
    connClose(agg->sink); /* epoll_ctl(DEL) + close + освобождение — c->epollfd уже указывает на epoll агрегатора */
    agg->sink = NULL;
    if (agg->pending)
    {
        frameFree(agg->pending);
        agg->pending = NULL;
        agg->pendingOff = 0;
    }
    if (agg->pendingCtrl)
    {
        frameFree(agg->pendingCtrl);
        agg->pendingCtrl = NULL;
        agg->pendingCtrlOff = 0;
    }
}

static void adoptSink(Agg_t *agg, Conn_t *newSink)
{
    if (agg->sink)
    {
        logger(LOG_INFO, AGG "Новый приёмник (id=%" PRIu64 ") замещает предыдущего (id=%" PRIu64 ")",
               newSink->id, agg->sink->id);
        sendCloseBestEffort(agg->sink->fd, 1001); /* going away */
        closeSink(agg);
    }

    newSink->epollfd = agg->epollfd;
    newSink->role = ROLE_SINK;
    agg->sink = newSink;

    struct epoll_event ev = { .events = EPOLLIN, .data.ptr = &newSink->tag };
    epoll_ctl(agg->epollfd, EPOLL_CTL_ADD, newSink->fd, &ev);
    logger(LOG_INFO, AGG "Приёмник подключён (id=%" PRIu64 ")", newSink->id);
}

static void resumeParked(Queue_t *dataQueue, Conn_t *c)
{
    push(dataQueue, c->pendingMsg);
    c->pendingMsg = NULL;

    /* c->epollfd не менялся при парковке (см. worker.c) — тот же ingest-epoll.
       Если за время простоя в tx остался недосланный control-кадр (например,
       PONG на PING, пришедший непосредственно перед парковкой), взводим и
       EPOLLOUT — иначе он рискует зависнуть до следующего входящего кадра. */
    uint32_t events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT;
    if (bufLen(&c->tx) > 0)
    {
        events |= EPOLLOUT;
    }
    struct epoll_event ev = { .events = events, .data.ptr = &c->tag };
    epoll_ctl(c->epollfd, EPOLL_CTL_ADD, c->fd, &ev);
}

/* Пытается дослать один слот. 1 — слот пуст (либо уже был пуст), 0 — EAGAIN (ждём EPOLLOUT), -1 — ошибка сокета. */
static int sinkFlushOne(int fd, Frame_t **slot, size_t *off)
{
    Frame_t *f = *slot;
    if (!f)
    {
        return 1;
    }
    while (*off < f->len)
    {
        ssize_t n = send(fd, f->bytes + *off, f->len - *off, MSG_DONTWAIT | MSG_NOSIGNAL);
        if (n > 0)
        {
            *off += (size_t)n;
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            return 0;
        }
        return -1;
    }
    frameFree(f);
    *slot = NULL;
    *off = 0;
    return 1;
}

/* Пытается дослать всё накопленное для sink'а: сперва control-ответ (приоритет), потом данные.
 * 1 — оба слота пусты, 0 — EAGAIN, ждём EPOLLOUT, -1 — sink нужно закрыть. */
static int sinkFlush(Agg_t *agg)
{
    if (!agg->sink)
    {
        return 1;
    }
    int rc = sinkFlushOne(agg->sink->fd, &agg->pendingCtrl, &agg->pendingCtrlOff);
    if (rc <= 0)
    {
        return rc;
    }
    return sinkFlushOne(agg->sink->fd, &agg->pending, &agg->pendingOff);
}

static void sinkArm(Agg_t *agg)
{
    if (!agg->sink)
    {
        return;
    }
    uint32_t want = EPOLLIN | (agg->pending || agg->pendingCtrl ? EPOLLOUT : 0);
    struct epoll_event ev = { .events = want, .data.ptr = &agg->sink->tag };
    epoll_ctl(agg->epollfd, EPOLL_CTL_MOD, agg->sink->fd, &ev);
}

/* Читает и разбирает то, что прислал приёмник. Нас интересуют только
 * control-кадры (PING -> PONG, CLOSE -> ответ и закрытие); DATA-кадры от
 * sink'а не ожидаются по смыслу задачи и молча отбрасываются.
 * Возвращает 0, если соединение нужно закрыть, 1 — если можно продолжать. */
static int sinkReadAndHandle(Agg_t *agg, Conn_t *c)
{
    /* eof: peer закрыл соединение / ошибка сокета / данные превысили лимит —
       в любом из этих случаев функция вернёт 0 (закрыть) в конце, но СНАЧАЛА
       разберёт то, что успела прочитать этим же вызовом (peer мог прислать
       CLOSE и тут же оборвать TCP одним махом). Регистрация sink'а в epoll —
       level-triggered без EPOLLET (см. adoptSink), поэтому если тут не было
       ничего нового, ложного "зависания" не будет: следующий же epoll_wait
       немедленно перевыдаст то же событие. */
    int eof = 0;
    for (;;)
    {
        if (bufReserve(&c->rx, 4096, DEFAULT_MAX_FRAME + 64) != 0)
        {
            eof = 1;
            break;
        }
        ssize_t n = recv(c->fd, c->rx.p + c->rx.len, c->rx.cap - c->rx.len, MSG_DONTWAIT);
        if (n > 0)
        {
            c->rx.len += (size_t)n;
            continue;
        }
        if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
        {
            eof = 1;
            break;
        }
        break; /* EAGAIN — новых данных прямо сейчас нет, соединение живо */
    }

    for (;;)
    {
        WsFrameHdr_t h;
        WsRc_t rc = wsParseHeader((const uint8_t *)bufData(&c->rx), bufLen(&c->rx), &h);
        if (rc == WS_NEED_MORE)
        {
            break;
        }
        if (rc == WS_ERR_PROTO)
        {
            sendCloseBestEffort(c->fd, 1002);
            return 0;
        }
        if (bufLen(&c->rx) < h.headerLen + h.payloadLen)
        {
            break; /* заголовок разобран, но payload ещё не весь пришёл */
        }

        uint8_t *payload = (uint8_t *)bufData(&c->rx) + h.headerLen;
        if (h.masked)
        {
            wsUnmask(payload, h.payloadLen, h.maskKey);
        }

        if (h.opcode == WS_OP_PING)
        {
            if (!agg->pendingCtrl)
            {
                agg->pendingCtrl = frameNew(WS_OP_PONG, payload, h.payloadLen);
                agg->pendingCtrlOff = 0;
            }
            /* если предыдущий control-ответ ещё не отправлен — этот PING просто пропускаем */
        }
        else if (h.opcode == WS_OP_CLOSE)
        {
            sendCloseBestEffort(c->fd, 1000);
            bufConsume(&c->rx, h.headerLen + h.payloadLen);
            return 0;
        }
        /* WS_OP_PONG и любые DATA-кадры — отбрасываем без реакции */

        bufConsume(&c->rx, h.headerLen + h.payloadLen);
    }
    return eof ? 0 : 1;
}

static void logStats(Agg_t *agg)
{
    int alive = 0;
    for (int i = 0; i < agg->srcCount; ++i)
    {
        if (agg->src[i].alive)
        {
            alive++;
        }
    }
    logger(LOG_INFO, STATS "источников: %d (всего видели: %d), сообщений: вход %" PRIu64
           " / выход %" PRIu64 ", байт на входе: %" PRIu64 ", приёмник: %s",
           alive, agg->srcCount,
           atomic_load_explicit(&agg->statMsgsIn, memory_order_relaxed),
           atomic_load_explicit(&agg->statMsgsOut, memory_order_relaxed),
           atomic_load_explicit(&agg->statBytesIn, memory_order_relaxed),
           agg->sink ? "подключён" : "нет");
}

/* Основной прогресс данных с учётом backpressure: не берёт из dataQueue
 * следующее сообщение, пока не отправит текущее (или пока нет sink'а вовсе) —
 * именно так заполненность dataQueue привязана к реальной скорости sink'а. */
static void aggPump(ServerConfig_t *conf, Agg_t *agg)
{
    for (;;)
    {
        if (!agg->sink)
        {
            return;
        }

        int rc = sinkFlush(agg);
        if (rc < 0)
        {
            logger(LOG_INFO, AGG "Приёмник (id=%" PRIu64 ") отвалился при записи", agg->sink->id);
            closeSink(agg);
            return;
        }
        if (rc == 0)
        {
            sinkArm(agg); /* EAGAIN — взводим EPOLLOUT и ждём */
            return;
        }

        if (queueSize(conf->dataQueue) <= agg->lowWatermark)
        {
            Conn_t *c = parkListPop(agg);
            if (c)
            {
                resumeParked(conf->dataQueue, c);
            }
        }

        void *item;
        if (trypop(conf->dataQueue, &item) != 0)
        {
            sinkArm(agg); /* нечего слать — снимаем EPOLLOUT, если был взведён */
            return;
        }

        Msg_t *m = item;
        Frame_t *f = aggOnMessage(agg, m);
        if (f)
        {
            agg->pending = f;
            agg->pendingOff = 0;
        }
        msgFree(m);
    }
}

/* Финальный проход при остановке: разбирает то, что осталось, без повторных
 * попыток при EAGAIN и без ожидания появления/пробуждения sink'а — иначе
 * join() этого потока мог бы зависнуть навсегда. */
static void aggDrainForShutdown(ServerConfig_t *conf, Agg_t *agg)
{
    void *item;
    while (trypop(conf->ctrlQueue, &item) == 0)
    {
        Msg_t *m = item;
        if (m->kind == MSG_SINK_ADD)
        {
            adoptSink(agg, m->conn);
        }
        else if (m->kind == MSG_SRC_DOWN)
        {
            aggSrcDown(agg, m->srcId);
        }
        msgFree(m);
    }
    while (trypop(conf->dataQueue, &item) == 0)
    {
        Msg_t *m = item;
        if (m->kind == MSG_DATA && agg->sink)
        {
            Frame_t *f = aggOnMessage(agg, m);
            if (f)
            {
                size_t off = 0;
                sinkFlushOne(agg->sink->fd, &f, &off); /* best-effort, один проход, не ждём EAGAIN */
                if (f)
                {
                    frameFree(f); /* не влезло целиком — отбрасываем остаток */
                }
            }
        }
        msgFree(m);
    }
}

void *aggregatorThread(void *arg)
{
    ServerConfig_t *conf = (ServerConfig_t *)arg;
    Agg_t *agg = conf->agg;

    int epollfd = epoll_create1(0);
    if (epollfd == -1)
    {
        logger(LOG_ERROR, AGG "Ошибка epoll_create1");
        return NULL;
    }
    agg->epollfd = epollfd;

    int dataFd = queueEnableNotify(conf->dataQueue);
    int ctrlFd = queueEnableNotify(conf->ctrlQueue);
    if (dataFd == -1 || ctrlFd == -1)
    {
        logger(LOG_ERROR, AGG "Ошибка queueEnableNotify");
        close(epollfd);
        return NULL;
    }

    EvTag_t tagData = { EV_WAKE, conf->dataQueue };
    EvTag_t tagCtrl = { EV_WAKE, conf->ctrlQueue };
    EvTag_t tagTimer = { EV_TIMER, NULL };

    struct epoll_event ev;
    ev.events = EPOLLIN; ev.data.ptr = &tagData;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, dataFd, &ev);
    ev.events = EPOLLIN; ev.data.ptr = &tagCtrl;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, ctrlFd, &ev);

    int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd != -1)
    {
        long ms = conf->tickMs > 0 ? conf->tickMs : DEFAULT_TICK_MS;
        struct itimerspec spec = {0};
        spec.it_value.tv_sec = ms / 1000;
        spec.it_value.tv_nsec = (ms % 1000) * 1000000L;
        spec.it_interval = spec.it_value;
        timerfd_settime(timerfd, 0, &spec, NULL);
        ev.events = EPOLLIN; ev.data.ptr = &tagTimer;
        epoll_ctl(epollfd, EPOLL_CTL_ADD, timerfd, &ev);
    }

    logger(LOG_INFO, AGG "Поток запущен, sink-путь ждём на \"%s\"", conf->sinkPath);

    struct epoll_event events[MAXEVENTS];
    for (;;)
    {
        /* Управляющие сообщения — вне очереди данных, разбираются немедленно */
        void *item;
        while (trypop(conf->ctrlQueue, &item) == 0)
        {
            Msg_t *m = item;
            if (m->kind == MSG_SINK_ADD)
            {
                adoptSink(agg, m->conn);
            }
            else if (m->kind == MSG_SRC_DOWN)
            {
                aggSrcDown(agg, m->srcId);
            }
            msgFree(m);
        }

        aggPump(conf, agg);

        int dataDone = queueIsShutdown(conf->dataQueue) && queueSize(conf->dataQueue) == 0;
        int ctrlDone = queueIsShutdown(conf->ctrlQueue) && queueSize(conf->ctrlQueue) == 0;
        if (dataDone && ctrlDone)
        {
            break;
        }
        if (queueIsShutdown(conf->dataQueue) && !agg->sink)
        {
            /* Продюсеры закончили, приёмника нет — доставить оставшееся некому. */
            aggDrainForShutdown(conf, agg);
            break;
        }

        int nfds = epoll_wait(epollfd, events, MAXEVENTS, 1000);
        if (nfds == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }
            logger(LOG_ERROR, AGG "Ошибка epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; ++i)
        {
            EvTag_t *tag = events[i].data.ptr;
            switch (tag->kind)
            {
                case EV_WAKE:
                {
                    int fd = (tag->obj == conf->dataQueue) ? dataFd : ctrlFd;
                    uint64_t v;
                    while (read(fd, &v, sizeof v) == (ssize_t)sizeof v) { }
                    break; /* сам разбор — в начале следующей итерации внешнего цикла */
                }
                case EV_TIMER:
                {
                    uint64_t exp;
                    while (read(timerfd, &exp, sizeof exp) == (ssize_t)sizeof exp) { }
                    struct timespec now;
                    /* CLOCK_REALTIME — те же часы, что и у Msg_t.ts (AggSrc_t.lastTs),
                       иначе сравнение "now - lastTs" в будущей оконной логике
                       было бы бессмысленным (см. msgNewData). Сам тик таймера
                       по-прежнему на CLOCK_MONOTONIC (timerfd_create выше) —
                       периодике это не мешает. */
                    clock_gettime(CLOCK_REALTIME, &now);
                    Frame_t *f = aggOnTick(agg, &now);
                    if (f)
                    {
                        if (agg->pending)
                        {
                            frameFree(f); /* редкий случай коллизии с ещё не отправленным кадром данных */
                        }
                        else
                        {
                            agg->pending = f;
                            agg->pendingOff = 0;
                        }
                    }
                    logStats(agg);
                    break;
                }
                case EV_CONN:
                {
                    Conn_t *c = tag->obj;
                    if (c != agg->sink)
                    {
                        break; /* событие от уже заменённого/закрытого sink'а */
                    }
                    if (events[i].events & (EPOLLHUP | EPOLLERR))
                    {
                        logger(LOG_INFO, AGG "Приёмник (id=%" PRIu64 ") отключился", c->id);
                        closeSink(agg);
                        break;
                    }
                    if ((events[i].events & EPOLLIN) && !sinkReadAndHandle(agg, c))
                    {
                        logger(LOG_INFO, AGG "Приёмник (id=%" PRIu64 ") закрыт", c->id);
                        closeSink(agg);
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }

    if (agg->sink)
    {
        sendCloseBestEffort(agg->sink->fd, 1001); /* going away */
        closeSink(agg);
    }
    aggFreeState(agg);
    if (timerfd != -1)
    {
        close(timerfd);
    }
    close(epollfd);
    logger(LOG_INFO, AGG "Поток завершён");
    return NULL;
}
