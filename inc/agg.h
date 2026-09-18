#ifndef AGG_H
#define AGG_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>

#include "conn.h"

/*
 * agg.h — единственное место, где живёт СЕМАНТИКА агрегации, и сам
 * поток-агрегатор (архитектурный близнец listenerThread: тот же паттерн
 * "epoll_create1 + горстка fd + ET-цикл", только вместо accept() —
 * вычитывание очереди сообщений, а вместо N клиентов — один sink).
 *
 * aggOnMessage()/aggOnTick() — швы для будущих манипуляций: сейчас
 * реализован passthrough с тегом источника; окно по таймеру, редьюс,
 * join по ключу добавляются локальным изменением src/agg.c.
 */

/* ---- Сообщение источник -> агрегатор. Владение строго move: создал воркер,
 * push() в очередь, агрегатор забрал и msgFree(). Refcount не нужен —
 * потребитель ровно один. ---- */
typedef enum { MSG_DATA, MSG_SRC_DOWN, MSG_SINK_ADD } MsgKind_t;

typedef struct Msg_t
{
    MsgKind_t kind;
    uint64_t  srcId;           /* MSG_DATA, MSG_SRC_DOWN */
    uint8_t   opcode;          /* MSG_DATA: WS_OP_TEXT | WS_OP_BIN (см. ws.h) */
    struct timespec ts;        /* MSG_DATA: момент приёма, CLOCK_REALTIME — сравнимо с часами
                                   внешнего потребителя (это поле уходит в JSON приёмнику как "t") */
    Conn_t   *conn;            /* MSG_SINK_ADD: передаваемое соединение */
    size_t    len;             /* MSG_DATA: длина payload */
    char      data[];          /* MSG_DATA: payload, FAM */
} Msg_t;

Msg_t *msgNewData(uint64_t srcId, uint8_t opcode, const void *payload, size_t n);
Msg_t *msgNewSrcDown(uint64_t srcId);
Msg_t *msgNewSinkAdd(Conn_t *c);
void   msgFree(Msg_t *m);

/* ---- Уже закодированный исходящий WS-кадр. Единственный писатель — поток
 * агрегатора, единственный потребитель — sink-сокет; refcount не нужен. ---- */
typedef struct Frame_t
{
    size_t  len;
    uint8_t bytes[];
} Frame_t;

Frame_t *frameNew(uint8_t opcode, const void *payload, size_t n);
void     frameFree(Frame_t *f);

/* ---- Состояние по каждому источнику — сюда добавляются аккумуляторы окна /
 * последнее значение для join, когда появится реальная манипуляция. ---- */
#define AGG_MAX_SOURCES 256

typedef struct AggSrc_t
{
    uint64_t id;
    int      alive;
    uint64_t msgCount;
    uint64_t byteCount;
    struct timespec lastTs;
} AggSrc_t;

typedef struct Agg_t
{
    AggSrc_t src[AGG_MAX_SOURCES];
    int      srcCount;

    Conn_t *sink;   /* NULL, если выходной WS ещё не подключён */
    int     epollfd; /* epoll потока агрегатора — удобство для sink-хелперов в agg.c */

    /* Единственный "в полёте" исходящий кадр с данными (от aggOnMessage/aggOnTick)
     * и единственный control-ответ sink'у (PONG/CLOSE) — у него приоритет при отправке.
     * Ровно один слот каждого вида: агрегатор не начинает кодировать следующее
     * сообщение, пока не отправит текущее целиком — это и есть точка, которой
     * backpressure на dataQueue привязана к реальной скорости sink'а. */
    Frame_t *pending;     size_t pendingOff;
    Frame_t *pendingCtrl; size_t pendingCtrlOff;

    /* Запаркованные источники (backpressure), см. Backpressure в плане.
     * Список трогают и воркеры (park), и поток агрегатора (resume) — под одним мьютексом. */
    pthread_mutex_t parkLock;
    Conn_t  *parkHead, *parkTail;

    size_t highWatermark;
    size_t lowWatermark;

    _Atomic uint64_t statMsgsIn;
    _Atomic uint64_t statBytesIn;
    _Atomic uint64_t statMsgsOut;
} Agg_t;

void aggInit(Agg_t *a, size_t highWatermark, size_t lowWatermark);
void aggFreeState(Agg_t *a); /* освобождает то, что осталось в park-листе (Conn_t не закрывает) */

void    parkListPush(Agg_t *a, Conn_t *c);
Conn_t *parkListPop(Agg_t *a); /* NULL, если пусто */

/* --- Шов агрегации: обе функции вызываются ТОЛЬКО из потока агрегатора,
 * последовательно — внутри можно держать состояние без мьютекса. --- */
Frame_t *aggOnMessage(Agg_t *a, const Msg_t *m);
Frame_t *aggOnTick(Agg_t *a, const struct timespec *now);
void     aggSrcDown(Agg_t *a, uint64_t srcId); /* источник отключился — пометить неживым для статистики */

/* Сам поток агрегатора. arg = ServerConfig_t* (см. server.h); тип оставлен
 * void*, чтобы не тянуть server.h в этот заголовок (server.h сам включает agg.h). */
void *aggregatorThread(void *arg);

#endif /* AGG_H */
