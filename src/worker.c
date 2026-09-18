#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdatomic.h>
#include <sys/epoll.h>

#include "server.h"
#include "logger.h"
#include "worker.h"
#include "conn.h"
#include "queue.h"
#include "http.h"
#include "ws.h"
#include "agg.h"

/*
 * Итог обработки одного события: что worker'у делать с соединением
 * ПОСЛЕ выхода из обработчика состояния. Ровно одна точка выхода на
 * каждый обработчик — здесь то, что раньше в этом файле было fallthrough
 * между READING/SENDING_HEADERS/SENDING_BODY.
 */
typedef enum
{
    DISP_REARM_IN,
    DISP_REARM_OUT,
    DISP_REARM_INOUT,
    DISP_CLOSE,
    DISP_PARKED,   /* соединение уже выведено из epoll внутри обработчика (backpressure) */
    DISP_HANDOFF   /* соединение уже передано агрегатору внутри обработчика (sink) */
} Disposition_t;

/*
 * Возвращение fd в epoll после обработки (EPOLLONESHOT).
 */
void rearmEpoll(Conn_t *c, uint32_t events)
{
    struct epoll_event ev = {0};
    ev.data.ptr = &c->tag;
    ev.events = events | EPOLLET | EPOLLRDHUP | EPOLLONESHOT;
    epoll_ctl(c->epollfd, EPOLL_CTL_MOD, c->fd, &ev);
}

static void applyDisposition(Conn_t *c, Disposition_t disp)
{
    switch (disp)
    {
        case DISP_REARM_IN:    rearmEpoll(c, EPOLLIN); break;
        case DISP_REARM_OUT:   rearmEpoll(c, EPOLLOUT); break;
        case DISP_REARM_INOUT: rearmEpoll(c, EPOLLIN | EPOLLOUT); break;
        case DISP_CLOSE:       connClose(c); break;
        case DISP_PARKED:      break; /* уже сделано в handleWsOpen */
        case DISP_HANDOFF:     break; /* уже сделано в handleHandshakeSend */
    }
}

/* Готовит CLOSE-кадр с заданным кодом в c->tx и помечает: после отправки — закрыть.
 * Не перезаписывает уже принятое решение закрыться (второй CLOSE не нужен). */
static void sendCloseAndMark(Conn_t *c, uint16_t code)
{
    if (c->closeAfterSend)
    {
        return;
    }
    uint8_t payload[2] = { (uint8_t)(code >> 8), (uint8_t)code };
    Frame_t *f = frameNew(WS_OP_CLOSE, payload, sizeof payload);
    if (f)
    {
        bufAppend(&c->tx, f->bytes, f->len);
        frameFree(f);
    }
    c->closeAfterSend = 1;
}

/* Дошлёвывает c->tx (ответ рукопожатия или WS control-кадры), уже
 * скопившиеся в буфере. Общая точка выхода для обработчиков ниже. */
static Disposition_t flushTx(Conn_t *c)
{
    int rc = bufSend(c->fd, &c->tx);
    if (rc == -1)
    {
        return DISP_CLOSE;
    }
    if (rc == 0)
    {
        return DISP_REARM_INOUT; /* не всё ушло — ждём EPOLLOUT тоже */
    }
    if (c->closeAfterSend)
    {
        return DISP_CLOSE; /* наш ответ/CLOSE полностью ушёл — закрываем, не дожидаясь ответа клиента */
    }
    return DISP_REARM_IN;
}

static Disposition_t handleHandshakeSend(ServerConfig_t *conf, Conn_t *c);
static Disposition_t handleWsOpen(ServerConfig_t *conf, Conn_t *c);

/*
 * ST_HANDSHAKE_READ: копим заголовки в c->rx, пока не найдём "\r\n\r\n".
 */
static Disposition_t handleHandshakeRead(ServerConfig_t *conf, Conn_t *c)
{
    int rc = bufRecv(c->fd, &c->rx, 4096, conf->handshakeMax);
    if (rc == -1)
    {
        return DISP_CLOSE;
    }
    if (rc == -2)
    {
        httpBuildError(&c->tx, 431, "Request Header Fields Too Large", NULL);
        c->closeAfterSend = 1;
        c->state = ST_HANDSHAKE_SEND;
        return handleHandshakeSend(conf, c);
    }
    if (rc == 0)
    {
        return DISP_REARM_IN; /* EAGAIN — заголовки ещё не пришли целиком */
    }
    int eof = (rc == 2); /* peer прислал что-то и тут же закрылся — см. bufRecv() */

    const char *end = httpFindHeaderEnd(bufData(&c->rx), bufLen(&c->rx));
    if (!end)
    {
        /* При eof новых данных уже не будет никогда — ждать бессмысленно. */
        return eof ? DISP_CLOSE : DISP_REARM_IN;
    }

    size_t headerLen = (size_t)(end - bufData(&c->rx)) + 4; /* включая завершающие "\r\n\r\n" */

    HandshakeResult_t hr;
    httpParseHandshake(bufData(&c->rx), headerLen, &hr);
    /* Хвост (уже пришедший вместе с заголовками первый WS-кадр, если есть)
       остаётся в rx нетронутым — именно это чинит потерю данных, которая
       была бы при простом обнулении позиции чтения. */
    bufConsume(&c->rx, headerLen);

    switch (hr.status)
    {
        case HS_OK:
            httpBuildUpgradeResponse(&c->tx, hr.acceptKey);
            c->role = (strcmp(hr.path, conf->sinkPath) == 0) ? ROLE_SINK : ROLE_SOURCE;
            c->closeAfterSend = 0;
            logger(LOG_DEBUG, WORKER "[%d] %s: рукопожатие WS, путь=%s, роль=%s",
                   gettid(), c->clientip, hr.path, c->role == ROLE_SINK ? "sink" : "source");
            break;
        case HS_BAD_VERSION:
            httpBuildError(&c->tx, 426, "Upgrade Required", "Sec-WebSocket-Version: 13\r\n");
            c->closeAfterSend = 1;
            break;
        case HS_NOT_UPGRADE:
        case HS_BAD_REQUEST:
        default:
            httpBuildError(&c->tx, 400, "Bad Request", NULL);
            c->closeAfterSend = 1;
            break;
    }

    if (eof)
    {
        /* peer прислал запрос и тут же закрылся, не читая ответ — нет смысла
           переходить в WS_OPEN/передавать sink агрегатору, дошлём ответ
           best-effort и закроем. */
        c->closeAfterSend = 1;
    }

    c->state = ST_HANDSHAKE_SEND;
    return handleHandshakeSend(conf, c);
}

/*
 * ST_HANDSHAKE_SEND: дошлёвываем ответ рукопожатия. При успехе — либо
 * закрываем (ошибка), либо передаём sink агрегатору, либо переходим в
 * открытый WS-режим (источник).
 */
static Disposition_t handleHandshakeSend(ServerConfig_t *conf, Conn_t *c)
{
    Disposition_t disp = flushTx(c);
    if (disp != DISP_REARM_IN)
    {
        /* DISP_CLOSE (ошибка сокета или наш ответ об ошибке отправлен) или
           DISP_REARM_INOUT (частичная отправка, EAGAIN) — решение окончательное. */
        return disp;
    }
    /* tx полностью отправлен и closeAfterSend не выставлен — успешный апгрейд */

    if (c->role == ROLE_SINK)
    {
        /* Передаём соединение агрегатору. DEL из ingest-epoll ДО постановки
           в очередь — соединение выключено ONESHOT'ом с момента, когда его
           взяли из очереди, поэтому DEL не может пересечься ни с каким
           другим событием по этому fd. */
        epoll_ctl(c->epollfd, EPOLL_CTL_DEL, c->fd, NULL);
        c->epollfd = -1;

        Msg_t *m = msgNewSinkAdd(c);
        if (!m)
        {
            return DISP_CLOSE;
        }
        if (push(conf->ctrlQueue, m) != 0)
        {
            msgFree(m);
            return DISP_CLOSE;
        }
        return DISP_HANDOFF;
    }

    /* ROLE_SOURCE: открытый WS-режим на этом же соединении. rx уже может
       содержать начало первого кадра — разбираем сразу, не дожидаясь
       следующего события. */
    c->state = ST_WS_OPEN;
    wsUtf8Reset(&c->msgUtf8);
    return handleWsOpen(conf, c);
}

/*
 * ST_WS_OPEN (только ROLE_SOURCE): читает и разбирает WS-кадры, отвечает на
 * PING/CLOSE, собирает фрагментированные сообщения, отправляет готовые
 * сообщения агрегатору — с приостановкой (backpressure), если тот отстаёт.
 */
static Disposition_t handleWsOpen(ServerConfig_t *conf, Conn_t *c)
{
    /* hardCap здесь — НЕ защита от одного слишком большого кадра (для этого
       есть точная проверка h.payloadLen ниже, сразу после разбора заголовка);
       это общий предохранитель от неограниченного роста буфера, если данные
       прибывают быстрее, чем успевает парситься. Он обязан быть заметно
       больше одного максимального кадра — иначе обычная пачка из нескольких
       ПОЛНОСТЬЮ ВАЛИДНЫХ кадров, пришедшая одним всплеском (частый случай
       при высокой частоте источника), ложно принималась бы за перегруз, а
       разбор ниже даже не запустился бы (closeAfterSend выставляется до
       входа в цикл разбора). */
    int rc = bufRecv(c->fd, &c->rx, 4096, conf->maxMessage + conf->maxFrame);
    if (rc == -2)
    {
        sendCloseAndMark(c, 1009); /* данные накопились сверх предохранителя быстрее, чем их успели разобрать */
    }
    /* eof: peer закрыл соединение/ошибка сокета — при rc==-1 (ничего нового
       не прочитано СЕЙЧАС) это ещё не повод отбрасывать то, что уже могло
       накопиться в c->rx РАНЬШЕ и остаться неразобранным — например, у
       соединения, которое только что разморозили после парковки по
       backpressure: оно вполне могло успеть получить EOF, пока стояло вне
       epoll, но хвост уже прочитанных до парковки байт всё ещё ждёт разбора.
       Поэтому ниже парсим содержимое rx безусловно, а закрываем — только
       после того как разберём всё, что там есть. rc==2 — тот же случай, но
       что-то было прочитано и этим самым вызовом. */
    int eof = (rc == -1 || rc == 2);
    /* rc == 0 (EAGAIN) или 1 (что-то прочитано, соединение живо) — как и
       eof-случаи выше, пробуем разобрать то, что уже накоплено в rx. */

    while (!c->closeAfterSend)
    {
        WsFrameHdr_t h;
        WsRc_t prc = wsParseHeader((const uint8_t *)bufData(&c->rx), bufLen(&c->rx), &h);

        if (prc == WS_NEED_MORE)
        {
            break;
        }
        if (prc == WS_ERR_PROTO)
        {
            sendCloseAndMark(c, 1002);
            break;
        }
        if (!h.masked)
        {
            /* RFC 6455 п.5.1: клиент обязан маскировать все кадры */
            sendCloseAndMark(c, 1002);
            break;
        }
        if (h.payloadLen > conf->maxFrame)
        {
            /* Точная проверка лимита ОДНОГО кадра — до попытки дождаться его
               целиком (иначе пришлось бы копить в rx весь заявленный размер). */
            sendCloseAndMark(c, 1009);
            break;
        }
        if (bufLen(&c->rx) < h.headerLen + h.payloadLen)
        {
            break; /* заголовок разобран, но payload ещё не весь пришёл */
        }

        uint8_t *payload = (uint8_t *)bufData(&c->rx) + h.headerLen;
        wsUnmask(payload, h.payloadLen, h.maskKey);

        if (h.opcode & 0x8)
        {
            /* Управляющий кадр: FIN=1 и len<=125 уже гарантированы wsParseHeader() */
            if (h.opcode == WS_OP_PING)
            {
                Frame_t *f = frameNew(WS_OP_PONG, payload, h.payloadLen);
                if (f)
                {
                    bufAppend(&c->tx, f->bytes, f->len);
                    frameFree(f);
                }
            }
            else if (h.opcode == WS_OP_CLOSE)
            {
                uint16_t code = 1000;
                if (h.payloadLen == 1)
                {
                    sendCloseAndMark(c, 1002);
                }
                else
                {
                    if (h.payloadLen >= 2)
                    {
                        code = (uint16_t)(((uint16_t)payload[0] << 8) | (uint16_t)payload[1]);
                        Utf8St_t rst;
                        wsUtf8Reset(&rst);
                        if (!wsIsValidCloseCode(code))
                        {
                            code = 1002;
                        }
                        else if (wsUtf8Feed(&rst, payload + 2, h.payloadLen - 2) != 0 || !wsUtf8Done(&rst))
                        {
                            code = 1007;
                        }
                    }
                    sendCloseAndMark(c, code);
                }
                bufConsume(&c->rx, h.headerLen + h.payloadLen);
                break; /* дальше не парсим — соединение закрывается */
            }
            /* WS_OP_PONG (в т.ч. непрошеный) — игнорируется */
            bufConsume(&c->rx, h.headerLen + h.payloadLen);
            continue;
        }

        /* DATA-кадр: CONT / TEXT / BIN */
        if (h.opcode == WS_OP_CONT)
        {
            if (!c->fragmenting)
            {
                sendCloseAndMark(c, 1002); /* продолжение без открытого сообщения */
                break;
            }
        }
        else
        {
            if (c->fragmenting)
            {
                sendCloseAndMark(c, 1002); /* новое сообщение при ещё не завершённом предыдущем */
                break;
            }
            c->msgOpcode = (uint8_t)h.opcode;
            c->fragmenting = 1;
            bufReset(&c->msg);
            wsUtf8Reset(&c->msgUtf8);
        }

        if (bufLen(&c->msg) + h.payloadLen > conf->maxMessage)
        {
            sendCloseAndMark(c, 1009);
            break;
        }

        if (c->msgOpcode == WS_OP_TEXT && h.payloadLen > 0 &&
            wsUtf8Feed(&c->msgUtf8, payload, h.payloadLen) != 0)
        {
            sendCloseAndMark(c, 1007);
            break;
        }

        bufAppend(&c->msg, payload, h.payloadLen);
        bufConsume(&c->rx, h.headerLen + h.payloadLen);

        if (!h.fin)
        {
            continue;
        }

        c->fragmenting = 0;
        if (c->msgOpcode == WS_OP_TEXT && !wsUtf8Done(&c->msgUtf8))
        {
            sendCloseAndMark(c, 1007); /* сообщение оборвалось на середине символа */
            break;
        }

        Msg_t *m = msgNewData(c->id, c->msgOpcode, bufData(&c->msg), bufLen(&c->msg));
        bufReset(&c->msg);
        if (!m)
        {
            continue; /* OOM на разовое сообщение — теряем его, соединение не рвём */
        }

        if (queueSize(conf->dataQueue) >= conf->highWatermark)
        {
            /* Backpressure: паркуем соединение целиком — включая то, что уже
               могло накопиться в rx (например, начало следующего сообщения),
               оно спокойно подождёт разморозки в буфере. c->epollfd НЕ
               обнуляем: это тот же ingest-epoll, соединение вернётся туда же
               при разморозке (agg.c: resumeParked), в отличие от передачи
               sink'а агрегатору, где epoll меняется навсегда. */
            c->pendingMsg = m;
            parkListPush(conf->agg, c);
            epoll_ctl(c->epollfd, EPOLL_CTL_DEL, c->fd, NULL);
            return DISP_PARKED;
        }

        push(conf->dataQueue, m);
    }

    Disposition_t disp = flushTx(c);
    if (eof && disp != DISP_CLOSE)
    {
        /* peer уже закрыл соединение — не ждём EPOLLOUT/новый EPOLLIN,
           который может не прийти повторно (edge-triggered epoll уже отдал
           нам это событие один раз). Наш ответ (если был) уже отправлен
           best-effort внутри flushTx() выше. */
        return DISP_CLOSE;
    }
    return disp;
}

/*
 * Worker поток: забирает соединения из очереди и обрабатывает их по состоянию.
 */
void *workerThread(void *args)
{
    ServerConfig_t *conf = (ServerConfig_t *)args;

    logger(LOG_INFO, WORKER "Поток %d запущен", gettid());

    void *item;
    while (pop(conf->queue, &item) == 0)
    {
        Conn_t *c = item;
        atomic_fetch_add_explicit(&total_requests, 1, memory_order_relaxed);

        Disposition_t disp;
        switch (c->state)
        {
            case ST_HANDSHAKE_READ: disp = handleHandshakeRead(conf, c); break;
            case ST_HANDSHAKE_SEND: disp = handleHandshakeSend(conf, c); break;
            case ST_WS_OPEN:        disp = handleWsOpen(conf, c);        break;
            default:                disp = DISP_CLOSE;                  break;
        }

        applyDisposition(c, disp);
    }

    logger(LOG_INFO, WORKER "Поток %d завершён", gettid());
    return NULL;
}
