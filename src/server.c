#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <pthread.h>
#include <stdatomic.h>

#include "server.h"
#include "options.h"
#include "logger.h"
#include "listener.h"
#include "worker.h"
#include "agg.h"
#include "conn.h"
#include "queue.h"


_Atomic uint64_t total_requests = 0;



/*
    Создаёт пул Listener потоков.
    SO_REUSEPORT позволяет ядру распределять подключения между Listener'ами.
*/
static void initListenerPool(ServerConfig_t *conf, pthread_t *listeners, ListenerArg_t *largs)
{
    for (int i = 0; i < conf->listeners; ++i)
    {
        largs[i].conf = conf;
        largs[i].epollfd = -1;

        if (pthread_create(&listeners[i], NULL, &listenerThread, &largs[i]) != 0)
        {
            logger(LOG_ERROR, SERVER "Не удалось создать Listener поток");
            listeners[i] = 0;
        }
    }
}



/*
    Создаёт пул Worker потоков для обработки клиентов.
*/
static void initWorkerPool(ServerConfig_t *conf, pthread_t *workers)
{
    for (int i = 0; i < conf->workers; ++i)
    {
        if (pthread_create(&workers[i], NULL, &workerThread, conf) != 0)
        {
            logger(LOG_ERROR, SERVER "Не удалось создать Worker поток");
            workers[i] = 0;
        }
    }
}

static void freeMsgItem(void *item)
{
    msgFree((Msg_t *)item);
}



int main(int argc, char **argv)
{
    ServerConfig_t conf = {0};

    /* Парсинг аргументов командной строки */
    int rc = parseArgs(argc, argv, &conf);
    if (rc != 0)
    {
        return (rc == 1) ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);

    if (init_logger("logs/server.log") != 0)
    {
        fprintf(stderr, SERVER "Не могу открыть файл для логов ./logs/server.log\n");
        return EXIT_FAILURE;
    }

    logger(LOG_INFO, SERVER "Запуск WS-агрегатора на порту %d (sink-путь \"%s\")", conf.port, conf.sinkPath);
    logger(LOG_INFO, SERVER "Listener потоков: %d, Worker потоков: %d", conf.listeners, conf.workers);

    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);

    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL) != 0)
    {
        logger(LOG_ERROR, SERVER "Ошибка блокировки сигналов");
        return EXIT_FAILURE;
    }

    registryInit();

    conf.queue = createQueue();
    conf.dataQueue = createQueue();
    conf.ctrlQueue = createQueue();
    atomic_store(&conf.shutdown, 0);

    if (!conf.queue || !conf.dataQueue || !conf.ctrlQueue)
    {
        logger(LOG_ERROR, SERVER "Ошибка создания очередей");
        return EXIT_FAILURE;
    }

    conf.shutdownfd = eventfd(0, EFD_NONBLOCK);
    if (conf.shutdownfd == -1)
    {
        logger(LOG_ERROR, SERVER "Ошибка создания eventfd");
        deleteQueue(conf.queue);
        deleteQueue(conf.dataQueue);
        deleteQueue(conf.ctrlQueue);
        return EXIT_FAILURE;
    }

    Agg_t agg;
    aggInit(&agg, conf.highWatermark, conf.lowWatermark);
    conf.agg = &agg;

    /* Динамические массивы для потоков */
    pthread_t *workers = calloc(conf.workers, sizeof(pthread_t));
    pthread_t *listeners = calloc(conf.listeners, sizeof(pthread_t));
    ListenerArg_t *largs = calloc(conf.listeners, sizeof(ListenerArg_t));

    if (!workers || !listeners || !largs)
    {
        logger(LOG_ERROR, SERVER "Ошибка выделения памяти для потоков");
        free(workers);
        free(listeners);
        free(largs);
        close(conf.shutdownfd);
        deleteQueue(conf.queue);
        deleteQueue(conf.dataQueue);
        deleteQueue(conf.ctrlQueue);
        return EXIT_FAILURE;
    }

    initWorkerPool(&conf, workers);
    initListenerPool(&conf, listeners, largs);

    pthread_t aggThread = 0;
    int aggOk = (pthread_create(&aggThread, NULL, &aggregatorThread, &conf) == 0);
    if (!aggOk)
    {
        logger(LOG_ERROR, SERVER "Не удалось создать поток агрегатора");
    }

    logger(LOG_INFO, SERVER "Сервер запущен.");

    int sig;
    sigwait(&sigset, &sig);

    logger(LOG_INFO, SERVER "Получен сигнал %d, останавливаем сервер...", sig);

    /* 1. Флаг + пробуждение всех Listener'ов через level-triggered eventfd */
    atomic_store(&conf.shutdown, 1);

    uint64_t val = 1;
    if (write(conf.shutdownfd, &val, sizeof(val)) == -1)
    {
        logger(LOG_ERROR, SERVER "Ошибка записи в eventfd");
    }

    /* 2. Listener'ы перестают принимать новые подключения и выходят.
       Свой epollfd они НЕ закрывают — это сделает main() ниже, уже после
       того, как ни один Worker не сможет обратиться к нему через rearmEpoll(). */
    for (int i = 0; i < conf.listeners; ++i)
    {
        if (listeners[i] != 0)
        {
            pthread_join(listeners[i], NULL);
        }
    }
    logger(LOG_INFO, SERVER "Все Listener потоки завершены");

    /* 3. Очередь задач закрывается — Worker'ы дорабатывают остаток и выходят.
       С этой точки никто больше не пишет в conf.dataQueue/conf.ctrlQueue. */
    shutdownQueue(conf.queue);
    for (int i = 0; i < conf.workers; ++i)
    {
        if (workers[i] != 0)
        {
            pthread_join(workers[i], NULL);
        }
    }
    logger(LOG_INFO, SERVER "Все Worker потоки завершены");

    /* 4. Очереди агрегатора закрываются — он доедает остаток (или обрывает
       недоставляемое, если приёмника нет и не будет) и выходит. */
    shutdownQueue(conf.dataQueue);
    shutdownQueue(conf.ctrlQueue);
    if (aggOk)
    {
        pthread_join(aggThread, NULL);
    }
    logger(LOG_INFO, SERVER "Поток агрегатора завершён");

    /* 5. Теперь безопасно закрыть epollfd Listener'ов и добить всё, что
       осталось в глобальном реестре соединений (например, запаркованные
       по backpressure источники, которых некому было закрыть раньше). */
    for (int i = 0; i < conf.listeners; ++i)
    {
        if (largs[i].epollfd >= 0)
        {
            close(largs[i].epollfd);
        }
    }
    registryDestroyAll();

    /* Очистка ресурсов */
    free(workers);
    free(listeners);
    free(largs);
    close(conf.shutdownfd);
    deleteQueue(conf.queue);
    deleteQueueWith(conf.dataQueue, freeMsgItem);
    deleteQueueWith(conf.ctrlQueue, freeMsgItem);
    close_logger();

    return EXIT_SUCCESS;
}
