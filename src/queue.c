#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include "queue.h"

struct QNode_t
{
    QNode_t *next;
    void    *item;
};

Queue_t *createQueue(void)
{
    Queue_t *q = malloc(sizeof *q);
    if (!q)
    {
        return NULL;
    }
    q->head = NULL;
    q->tail = NULL;
    q->size = 0;
    q->shutdown = 0;
    q->notifyFd = -1;

    if (pthread_mutex_init(&q->mutex, NULL) != 0)
    {
        free(q);
        return NULL;
    }
    if (pthread_cond_init(&q->cond, NULL) != 0)
    {
        pthread_mutex_destroy(&q->mutex);
        free(q);
        return NULL;
    }
    return q;
}

int queueEnableNotify(Queue_t *q)
{
    if (!q)
    {
        return -1;
    }
    int fd = eventfd(0, EFD_NONBLOCK);
    if (fd == -1)
    {
        return -1;
    }
    q->notifyFd = fd;
    return fd;
}

void deleteQueueWith(Queue_t *q, void (*freeItem)(void *item))
{
    if (!q)
    {
        return;
    }

    pthread_mutex_lock(&q->mutex);
    QNode_t *node = q->head;
    while (node)
    {
        QNode_t *next = node->next;
        if (freeItem)
        {
            freeItem(node->item);
        }
        free(node);
        node = next;
    }
    q->head = NULL;
    q->tail = NULL;
    q->size = 0;
    pthread_mutex_unlock(&q->mutex);

    if (q->notifyFd >= 0)
    {
        close(q->notifyFd);
    }
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
    free(q);
}

void deleteQueue(Queue_t *q)
{
    deleteQueueWith(q, NULL);
}

int push(Queue_t *q, void *item)
{
    if (!q)
    {
        return -1;
    }

    QNode_t *n = malloc(sizeof *n);
    if (!n)
    {
        return -1;
    }
    n->item = item;
    n->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail)
    {
        q->tail->next = n;
    }
    else
    {
        q->head = n;
    }
    q->tail = n;
    q->size++;
    pthread_cond_signal(&q->cond);
    int notifyFd = q->notifyFd;
    pthread_mutex_unlock(&q->mutex);

    if (notifyFd >= 0)
    {
        uint64_t one = 1;
        /* eventfd, не FIFO: несколько push() между чтениями просто суммируются
           в счётчике — ни одно пробуждение не теряется. Ошибка записи (например,
           переполнение счётчика) сознательно игнорируется — потребитель всё
           равно дренирует очередь через trypop() до опустошения. */
        ssize_t r = write(notifyFd, &one, sizeof one);
        (void)r;
    }

    return 0;
}

void shutdownQueue(Queue_t *q)
{
    if (!q)
    {
        return;
    }
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->cond);
    int notifyFd = q->notifyFd;
    pthread_mutex_unlock(&q->mutex);

    if (notifyFd >= 0)
    {
        uint64_t one = 1;
        ssize_t r = write(notifyFd, &one, sizeof one);
        (void)r;
    }
}

int queueIsShutdown(Queue_t *q)
{
    if (!q)
    {
        return 1;
    }
    pthread_mutex_lock(&q->mutex);
    int sd = q->shutdown;
    pthread_mutex_unlock(&q->mutex);
    return sd;
}

/* Извлечение головы очереди. Вызывающий держит мьютекс. */
static void *popHeadLocked(Queue_t *q)
{
    QNode_t *n = q->head;
    void *item = n->item;
    q->head = n->next;
    if (!q->head)
    {
        q->tail = NULL;
    }
    q->size--;
    free(n);
    return item;
}

int pop(Queue_t *q, void **item)
{
    if (!q || !item)
    {
        return -1;
    }

    pthread_mutex_lock(&q->mutex);
    while (q->size == 0 && !q->shutdown)
    {
        pthread_cond_wait(&q->cond, &q->mutex);
    }

    if (q->size == 0)
    {
        pthread_mutex_unlock(&q->mutex);
        return -1; /* shutdown и очередь пуста */
    }

    *item = popHeadLocked(q);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

int trypop(Queue_t *q, void **item)
{
    if (!q || !item)
    {
        return -1;
    }

    pthread_mutex_lock(&q->mutex);
    if (q->size == 0)
    {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    *item = popHeadLocked(q);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

size_t queueSize(Queue_t *q)
{
    if (!q)
    {
        return 0;
    }
    pthread_mutex_lock(&q->mutex);
    size_t n = q->size;
    pthread_mutex_unlock(&q->mutex);
    return n;
}
