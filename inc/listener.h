#ifndef LISTENER_H
#define LISTENER_H

#include "server.h"

/*
 * Listener поток: socket->bind->listen, epoll_wait для accept и событий клиентов.
 * События от клиентов передаются в очередь для обработки Worker'ами.
 * SO_REUSEPORT позволяет нескольким Listener'ам слушать один порт.
 *
 * Listener НЕ закрывает соединения сам (даже при EPOLLRDHUP/HUP/ERR) —
 * закрывает только владелец (воркер/агрегатор), см. connClose(). И не
 * закрывает свой собственный epollfd при выходе — сохраняет его в
 * ListenerArg_t::epollfd; закрыть обязан main() и только ПОСЛЕ join()
 * всех listener/worker/агрегатор потоков (иначе rearmEpoll() из ещё
 * дорабатывающего воркера попадёт на переиспользованный номер fd).
 */
typedef struct ListenerArg_t
{
    ServerConfig_t *conf;
    int epollfd; /* -1, пока поток не стартовал; иначе валиден до явного close() в main() */
} ListenerArg_t;

void *listenerThread(void *args); /* args = ListenerArg_t* */

#endif /* LISTENER_H */
