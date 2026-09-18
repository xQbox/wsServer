#ifndef CONN_H
#define CONN_H

#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <netinet/in.h>

/*
 * Conn_t — контекст одного TCP-соединения (замена бывшего Task_t).
 * Роль определяется после разбора HTTP-рукопожатия: источник данных
 * (ROLE_SOURCE, остаётся под воркерами) или единственный выходной
 * приёмник (ROLE_SINK, передаётся агрегатору).
 */

/* ---- Тег для epoll: кладётся в ev.data.ptr ВЕЗДЕ, data.fd не используется никогда ---- */
typedef enum
{
    EV_LISTEN,      /* серверный слушающий сокет */
    EV_SHUTDOWN,    /* eventfd сигнала завершения */
    EV_WAKE,        /* eventfd пробуждения агрегатора очередью сообщений */
    EV_TIMER,       /* timerfd статистики/тика агрегации */
    EV_CONN         /* клиентское соединение, obj = Conn_t* */
} EvKind_t;

typedef struct EvTag_t
{
    EvKind_t kind;
    void    *obj;
} EvTag_t;

/* ---- Растущий буфер с сохранением непрочитанного хвоста ---- */
typedef struct Buf_t
{
    char   *p;
    size_t  cap;   /* выделено */
    size_t  len;   /* заполнено (от начала p, включая уже "съеденное") */
    size_t  off;   /* сколько с начала уже обработано/отправлено */
} Buf_t;

int    bufReserve(Buf_t *b, size_t extra, size_t hardCap); /* гарантирует cap-len >= extra; -1 если > hardCap */
void   bufConsume(Buf_t *b, size_t n);                       /* off += n, с компактизацией при необходимости */
void   bufAppend(Buf_t *b, const void *data, size_t n);      /* добавляет в конец (растит буфер сама) */
void   bufReset(Buf_t *b);                                   /* len = off = 0, память не освобождается */
void   bufFree(Buf_t *b);
static inline char   *bufData(const Buf_t *b) { return b->p + b->off; }
static inline size_t  bufLen(const Buf_t *b)  { return b->len - b->off; }

/*
 * Читает всё, что доступно на fd, неблокирующе, добавляя в конец b (сама
 * растит буфер порциями по chunk байт, не давая ему расти сверх hardCap).
 * Возвращает:  1 — прочитано что-то, соединение остаётся открытым;
 *              2 — прочитано что-то, но следом сразу EOF/ошибка сокета —
 *                  РАЗОБРАТЬ содержимое b как обычно, но после разбора
 *                  соединение нужно ЗАКРЫТЬ, не дожидаясь нового события от
 *                  epoll (peer мог прислать данные и тут же FIN одним махом —
 *                  повторного edge-triggered EPOLLIN по этому fd может не быть);
 *              0 — EAGAIN, новых данных нет прямо сейчас, соединение открыто;
 *             -1 — ничего не прочитано, и это EOF/ошибка (закрывать немедленно);
 *             -2 — buffered данные превысили бы hardCap (вызывающий сам
 *                  решает, каким протокольным кодом ответить перед закрытием).
 */
int bufRecv(int fd, Buf_t *b, size_t chunk, size_t hardCap);

/*
 * Досылает bufData(b)/bufLen(b) в fd неблокирующе, потребляя отправленное
 * через bufConsume(). Возвращает 1 — всё отправлено, 0 — EAGAIN (нужно
 * вернуться в epoll на EPOLLOUT), -1 — ошибка сокета (закрывать).
 */
int bufSend(int fd, Buf_t *b);

typedef enum { ROLE_UNDECIDED = 0, ROLE_SOURCE, ROLE_SINK } Role_t;

typedef enum
{
    ST_HANDSHAKE_READ,   /* ждём конца заголовков запроса */
    ST_HANDSHAKE_SEND,   /* дошлёвываем ответ (101 или ошибку) из tx */
    ST_WS_OPEN           /* только ROLE_SOURCE: разбор WS-кадров из rx */
} ConnState_t;

/*
 * Инкрементальное состояние проверки UTF-8 (RFC 3629, для WS TEXT-кадров) —
 * символ может быть разрезан границей WS-фрагмента, поэтому проверка идёт
 * побайтово с переносимым состоянием между вызовами wsUtf8Feed().
 * need — сколько continuation-байт ещё ожидается у текущего символа;
 * kind — особое ограничение на ПЕРВЫЙ ещё не пройденный continuation-байт
 * (для ведущих байт E0/ED/F0/F4 оно ýже общего 0x80-0xBF — так исключаются
 * переполненные кодировки и суррогатный диапазон); после первого
 * continuation-байта kind всегда становится обычным.
 */
typedef enum { U8_NONE = 0, U8_GEN, U8_E0, U8_ED, U8_F0, U8_F4 } Utf8Kind_t;

typedef struct Utf8St_t
{
    int        need;
    Utf8Kind_t kind;
} Utf8St_t;

struct Msg_t; /* см. inc/agg.h */

typedef struct Conn_t
{
    EvTag_t tag;              /* ПЕРВОЕ поле; tag.kind = EV_CONN, tag.obj = this */

    int      fd;
    int      epollfd;         /* epoll текущего владельца; -1 в момент передачи агрегатору */
    uint64_t id;               /* auto-increment, тег источника в сообщениях/логах */
    char     clientip[16];

    Role_t      role;
    ConnState_t state;
    int         closeAfterSend; /* после отправки tx — закрыть, а не переходить в WS/handoff */
    uint32_t    lastEvents;     /* маска, с которой листенер снял событие */

    Buf_t rx;   /* входящие байты: сначала HTTP-заголовки, потом WS-кадры */
    Buf_t tx;   /* исходящие байты рукопожатия/control-кадров (send() по этому буферу) */

    /* --- только ROLE_SOURCE, состояние сборки текущего WS-сообщения --- */
    Buf_t    msg;          /* накопитель фрагментов */
    uint8_t  msgOpcode;    /* opcode первого фрагмента (TEXT/BIN) */
    uint8_t  fragmenting;  /* 1, если сообщение открыто и ждёт CONT/FIN */
    Utf8St_t msgUtf8;      /* валидатор для TEXT, инкрементально по фрагментам */
    uint64_t seq;          /* порядковый номер сообщения от этого источника */
    struct Msg_t   *pendingMsg; /* см. backpressure: собранное сообщение, ждущее места в очереди */
    struct Conn_t  *parkNext;   /* интрузивный список запаркованных источников */

    /* глобальный реестр всех соединений (для чистого shutdown) */
    struct Conn_t *regPrev, *regNext;
} Conn_t;

Conn_t *connNew(int fd, int epollfd);
void    connClose(Conn_t *c);     /* epoll_ctl(DEL) + close(fd) + connDestroy(c); вызывает ТОЛЬКО владелец */
void    connDestroy(Conn_t *c);   /* снять с реестра и освободить память; fd уже должен быть закрыт */

void registryInit(void);
void registryDestroyAll(void);    /* только после join всех потоков, которые могли держать соединения */

#endif /* CONN_H */
