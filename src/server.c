#define _GNU_SOURCE
#include "server.h"

#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "agg.h"
#include "conn.h"
#include "listener.h"
#include "logger.h"
#include "options.h"
#include "queue.h"
#include "worker.h"
#include "ws_client.h"

_Atomic uint64_t total_requests = 0;

static void initListenerPool(ServerConfig_t* conf, pthread_t* listeners, ListenerArg_t* largs) {
    for (int i = 0; i < conf->listeners; ++i) {
        largs[i].conf = conf;
        largs[i].epollfd = -1;

        if (pthread_create(&listeners[i], NULL, &listenerThread, &largs[i]) != 0) {
            logger(LOG_ERROR, SERVER "Не удалось создать Listener поток");
            listeners[i] = 0;
        }
    }
}

static void initWorkerPool(ServerConfig_t* conf, pthread_t* workers) {
    for (int i = 0; i < conf->workers; ++i) {
        if (pthread_create(&workers[i], NULL, &workerThread, conf) != 0) {
            logger(LOG_ERROR, SERVER "Не удалось создать Worker поток");
            workers[i] = 0;
        }
    }
}

static void freeMsgItem(void* item) { msgFree((Msg_t*)item); }
int main(int argc, char** argv) {
    ServerConfig_t conf = {0};

    int rc = parseArgs(argc, argv, &conf);

    if (rc != 0) {
        free(conf.upstreams);
        return (rc == 1) ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);

    if (init_logger("logs/server.log") != 0) {
        fprintf(stderr, SERVER "Не могу открыть файл для логов ./logs/server.log\n");

        free(conf.upstreams);
        return EXIT_FAILURE;
    }

    /*
     * Показываем upstream'ы, полученные через CLI.
     */
    for (size_t i = 0; i < conf.upstreamCount; ++i) {
        logger(LOG_INFO, SERVER "Upstream[%zu]: ws://%s:%d%s", i, conf.upstreams[i].host,
               conf.upstreams[i].port, conf.upstreams[i].path);
    }

    logger(LOG_INFO, SERVER "Запуск WS-агрегатора на порту %d (sink-путь \"%s\")", conf.port, conf.sinkPath);

    logger(LOG_INFO, SERVER "Listener потоков: %d, Worker потоков: %d", conf.listeners, conf.workers);

    /*
     * Блокируем SIGINT/SIGTERM во всех создаваемых далее потоках.
     * Главный поток будет ждать их через sigwait().
     */
    sigset_t sigset;

    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);

    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL) != 0) {
        logger(LOG_ERROR, SERVER "Ошибка блокировки сигналов");

        free(conf.upstreams);
        close_logger();

        return EXIT_FAILURE;
    }

    registryInit();

    /*
     * Очереди.
     */
    conf.queue = createQueue();
    conf.dataQueue = createQueue();
    conf.ctrlQueue = createQueue();

    atomic_store(&conf.shutdown, 0);

    if (!conf.queue || !conf.dataQueue || !conf.ctrlQueue) {
        logger(LOG_ERROR, SERVER "Ошибка создания очередей");

        if (conf.queue) {
            deleteQueue(conf.queue);
        }

        if (conf.dataQueue) {
            deleteQueue(conf.dataQueue);
        }

        if (conf.ctrlQueue) {
            deleteQueue(conf.ctrlQueue);
        }

        free(conf.upstreams);

        registryDestroyAll();
        close_logger();

        return EXIT_FAILURE;
    }

    /*
     * eventfd для остановки listener'ов.
     */
    conf.shutdownfd = eventfd(0, EFD_NONBLOCK);

    if (conf.shutdownfd == -1) {
        logger(LOG_ERROR, SERVER "Ошибка создания eventfd");

        deleteQueue(conf.queue);
        deleteQueue(conf.dataQueue);
        deleteQueue(conf.ctrlQueue);

        free(conf.upstreams);

        registryDestroyAll();
        close_logger();

        return EXIT_FAILURE;
    }

    /*
     * Aggregator.
     */
    Agg_t agg;

    aggInit(&agg, conf.highWatermark, conf.lowWatermark);

    conf.agg = &agg;

    /*
     * Worker / Listener arrays.
     */
    pthread_t* workers = (pthread_t*)calloc(conf.workers, sizeof(pthread_t));

    pthread_t* listeners = (pthread_t*)calloc(conf.listeners, sizeof(pthread_t));

    ListenerArg_t* largs = (ListenerArg_t*)calloc(conf.listeners, sizeof(ListenerArg_t));

    if (!workers || !listeners || !largs) {
        logger(LOG_ERROR, SERVER "Ошибка выделения памяти для потоков");

        free(workers);
        free(listeners);
        free(largs);

        close(conf.shutdownfd);

        deleteQueue(conf.queue);
        deleteQueue(conf.dataQueue);
        deleteQueue(conf.ctrlQueue);

        free(conf.upstreams);

        registryDestroyAll();
        close_logger();

        return EXIT_FAILURE;
    }

    /*
     * Запускаем worker/listener pool.
     */
    initWorkerPool(&conf, workers);

    initListenerPool(&conf, listeners, largs);

    /*
     * Aggregator thread.
     */
    pthread_t aggThread = 0;

    int aggOk = pthread_create(&aggThread, NULL, &aggregatorThread, &conf) == 0;

    if (!aggOk) {
        logger(LOG_ERROR, SERVER "Не удалось создать поток агрегатора");
    }

    /*
     * ============================================================
     * UPSTREAM REACTOR
     * ============================================================
     *
     * Все --upstream обслуживаются одним reactor thread
     * и одним epollfd.
     */
    pthread_t upstreamThread = 0;
    int upstreamOk = 0;

    UpstreamReactor_t* upstream = NULL;

    if (conf.upstreamCount > 0) {
        upstream = upstreamCreate(&conf);

        if (!upstream) {
            logger(LOG_ERROR, SERVER "Не удалось создать upstream reactor");

        } else {
            size_t added = 0;

            for (size_t i = 0; i < conf.upstreamCount; ++i) {
                UpstreamConfig_t* u = &conf.upstreams[i];

                if (upstreamAdd(upstream, u->host, u->port, u->path) != 0) {
                    logger(LOG_ERROR,
                           SERVER
                           "Не удалось добавить upstream "
                           "ws://%s:%d%s",
                           u->host, u->port, u->path);

                    continue;
                }

                added++;
            }

            if (added == 0) {
                logger(LOG_ERROR, SERVER "Не удалось добавить ни одного upstream");

                upstreamDestroy(upstream);
                upstream = NULL;

            } else {
                upstreamOk = pthread_create(&upstreamThread, NULL, &upstreamReactorThread, upstream) == 0;

                if (!upstreamOk) {
                    logger(LOG_ERROR, SERVER "Не удалось создать upstream reactor thread");

                    upstreamDestroy(upstream);
                    upstream = NULL;
                }
            }
        }
    }

    logger(LOG_INFO, SERVER "Сервер запущен.");

    /*
     * ============================================================
     * WAIT FOR SHUTDOWN
     * ============================================================
     */
    int sig;

    sigwait(&sigset, &sig);

    logger(LOG_INFO, SERVER "Получен сигнал %d, останавливаем сервер...", sig);

    atomic_store(&conf.shutdown, 1);

    /*
     * Будим listener'ы.
     */
    uint64_t val = 1;

    if (write(conf.shutdownfd, &val, sizeof(val)) == -1) {
        logger(LOG_ERROR, SERVER "Ошибка записи в eventfd");
    }

    /*
     * ============================================================
     * STOP UPSTREAM REACTOR
     * ============================================================
     */
    if (upstreamOk) {
        pthread_join(upstreamThread, NULL);
    }

    if (upstream) {
        upstreamDestroy(upstream);
        upstream = NULL;
    }

    logger(LOG_INFO, SERVER "Upstream reactor завершён");

    /*
     * ============================================================
     * STOP LISTENERS
     * ============================================================
     */
    for (int i = 0; i < conf.listeners; ++i) {
        if (listeners[i] != 0) {
            pthread_join(listeners[i], NULL);
        }
    }

    logger(LOG_INFO, SERVER "Все Listener потоки завершены");

    /*
     * ============================================================
     * STOP WORKERS
     * ============================================================
     */
    shutdownQueue(conf.queue);

    for (int i = 0; i < conf.workers; ++i) {
        if (workers[i] != 0) {
            pthread_join(workers[i], NULL);
        }
    }

    logger(LOG_INFO, SERVER "Все Worker потоки завершены");

    /*
     * ============================================================
     * STOP AGGREGATOR
     * ============================================================
     */
    shutdownQueue(conf.dataQueue);

    shutdownQueue(conf.ctrlQueue);

    if (aggOk) {
        pthread_join(aggThread, NULL);
    }

    logger(LOG_INFO, SERVER "Поток агрегатора завершён");

    /*
     * ============================================================
     * CLEANUP
     * ============================================================
     */

    for (int i = 0; i < conf.listeners; ++i) {
        if (largs[i].epollfd >= 0) {
            close(largs[i].epollfd);
        }
    }

    registryDestroyAll();

    free(workers);
    free(listeners);
    free(largs);

    /*
     * Конфигурации, созданные parseArgs().
     */
    free(conf.upstreams);

    conf.upstreams = NULL;
    conf.upstreamCount = 0;
    conf.upstreamCapacity = 0;

    close(conf.shutdownfd);

    deleteQueue(conf.queue);

    deleteQueueWith(conf.dataQueue, freeMsgItem);

    deleteQueueWith(conf.ctrlQueue, freeMsgItem);

    close_logger();

    return EXIT_SUCCESS;
}
