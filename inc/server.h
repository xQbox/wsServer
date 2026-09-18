#ifndef SERVER_H
#define SERVER_H

#include <stdint.h>
#include <stdatomic.h>

#include "queue.h"
#include "agg.h"

/* Константы по умолчанию */
#define DEFAULT_PORT        33333
#define DEFAULT_LISTENERS   2
#define DEFAULT_WORKERS     8
#define MAXCON              128
#define MAXEVENTS           128
#define MAX_WORKERS         64
#define MAX_LISTENERS       16

#define DEFAULT_SINK_PATH      "/feed"
#define DEFAULT_HANDSHAKE_MAX  (8u * 1024u)          /* макс. размер HTTP-заголовков рукопожатия */
#define DEFAULT_MAX_FRAME      (64u * 1024u)         /* макс. размер одного WS-кадра */
#define DEFAULT_MAX_MESSAGE    (4u * 1024u * 1024u)  /* макс. размер собранного (возможно фрагментированного) сообщения */
#define DEFAULT_HIGH_WATERMARK 256                   /* backpressure: выше — источники паркуются */
#define DEFAULT_LOW_WATERMARK  128                    /* backpressure: ниже — источники размораживаются */
#define DEFAULT_TICK_MS        1000                   /* период статистики/тика агрегации */

/* Префиксы логов */
#define SERVER      "[СЕРВЕР] "
#define LISTENER    "[LISTENER] "
#define WORKER      "[WORKER] "
#define AGG         "[АГРЕГАТОР] "
#define WARN        "[ВНИМАНИЕ] "
#define STATS       "[СТАТИСТИКА] "

/* Уровни логов */
typedef enum
{
    LOG_DEBUG,
    LOG_INFO,
    LOG_ERROR
} LogLevel;

/* Глобальный счётчик принятых WS-рукопожатий (источники + sink) */
extern _Atomic uint64_t total_requests;

/* Конфигурация сервера */
typedef struct ServerConfig_t
{
    Queue_t *queue;       /* ingest: listener -> worker (элементы — Conn_t*) */
    Queue_t *dataQueue;   /* worker -> агрегатор: Msg_t* с kind == MSG_DATA (подчиняется backpressure) */
    Queue_t *ctrlQueue;   /* worker -> агрегатор: Msg_t* с kind == MSG_SINK_ADD/MSG_SRC_DOWN
                             (низкообъёмный канал, разбирается немедленно, backpressure не подчиняется —
                             иначе новый sink никогда не подключится, пока dataQueue переполнена) */
    Agg_t   *agg;

    _Atomic int shutdown;
    int shutdownfd;       /* eventfd для пробуждения Listener'ов и агрегатора */

    /* Настраиваемые параметры */
    int port;
    int workers;
    int listeners;

    const char *sinkPath;
    size_t handshakeMax;
    size_t maxFrame;
    size_t maxMessage;
    size_t highWatermark;
    size_t lowWatermark;
    int    tickMs;
} ServerConfig_t;

#endif /* SERVER_H */
