#define _GNU_SOURCE
#include "listener.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "conn.h"
#include "logger.h"
#include "queue.h"
#include "server.h"

static int setNonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка fcntl F_GETFL", gettid());
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка fcntl F_SETFL", gettid());
        return -1;
    }
    return 0;
}

static void connectionHandle(int serverfd, int epollfd) {
    while (1) {
        struct sockaddr_in clientAddr = {0};
        socklen_t clientLen = sizeof(clientAddr);
        int clientfd = accept(serverfd, (struct sockaddr*)&clientAddr, &clientLen);

        if (clientfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            logger(LOG_ERROR, LISTENER "[%d] Ошибка accept", gettid());
            break;
        }

        if (setNonblocking(clientfd) != 0) {
            logger(LOG_ERROR, LISTENER "[%d] Невозможно сделать сокет неблокирующим", gettid());
            close(clientfd);
            continue;
        }

        Conn_t* c = connNew(clientfd, epollfd);
        if (!c) {
            logger(LOG_ERROR, LISTENER "[%d] Ошибка выделения памяти для Conn_t", gettid());
            close(clientfd);
            continue;
        }

        inet_ntop(AF_INET, &clientAddr.sin_addr, c->clientip, sizeof c->clientip);

        struct epoll_event ev = {0};
        ev.data.ptr = &c->tag;
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT;

        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, clientfd, &ev) == -1) {
            logger(LOG_ERROR, LISTENER "[%d] Ошибка epoll_ctl", gettid());
            connDestroy(c);
            close(clientfd);
            continue;
        }
    }
}

void* listenerThread(void* args) {
    ListenerArg_t* larg = (ListenerArg_t*)args;
    ServerConfig_t* conf = larg->conf;

    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverfd < 0) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка создания сокета", gettid());
        return NULL;
    }

    int opt = 1;
    if (setsockopt(serverfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == -1) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка setsockopt SO_REUSEPORT", gettid());
        close(serverfd);
        return NULL;
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(conf->port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverfd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка bind", gettid());
        close(serverfd);
        return NULL;
    }

    if (listen(serverfd, MAXCON) == -1) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка listen", gettid());
        close(serverfd);
        return NULL;
    }

    if (setNonblocking(serverfd) != 0) {
        close(serverfd);
        return NULL;
    }

    logger(LOG_INFO, LISTENER "Поток %d слушает порт %d", gettid(), conf->port);

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка epoll_create1", gettid());
        close(serverfd);
        return NULL;
    }
    larg->epollfd = epollfd;

    static const EvTag_t TAG_LISTEN = {EV_LISTEN, NULL};
    static const EvTag_t TAG_SHUTDOWN = {EV_SHUTDOWN, NULL};

    struct epoll_event ev = {0};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = (void*)&TAG_LISTEN;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, serverfd, &ev) == -1) {
        logger(LOG_ERROR, LISTENER "[%d] Ошибка epoll_ctl для serverfd", gettid());
        close(serverfd);
        return NULL;
    }

    ev.events = EPOLLIN;
    ev.data.ptr = (void*)&TAG_SHUTDOWN;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, conf->shutdownfd, &ev);

    struct epoll_event events[MAXEVENTS];
    int stop = 0;

    while (!stop) {
        int nfds = epoll_wait(epollfd, events, MAXEVENTS, 1000);

        if (nfds == -1) {
            if (errno == EINTR) {
                continue;
            }
            logger(LOG_ERROR, LISTENER "[%d] Ошибка epoll_wait", gettid());
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            const EvTag_t* tag = events[i].data.ptr;

            if (tag->kind == EV_SHUTDOWN) {
                logger(LOG_INFO, LISTENER "[%d] Получен сигнал завершения", gettid());
                stop = 1;
                break;
            }

            if (tag->kind == EV_LISTEN) {
                connectionHandle(serverfd, epollfd);
                continue;
            }

            Conn_t* c = (Conn_t*)tag->obj;
            c->lastEvents = events[i].events;

            if (push(conf->queue, c) != 0) {
                logger(LOG_ERROR, LISTENER "[%d] Ошибка добавления задачи в очередь", gettid());
                connClose(c);
            }
        }
    }

    close(serverfd);
    logger(LOG_INFO, LISTENER "[%d] Поток завершён", gettid());
    return NULL;
}
