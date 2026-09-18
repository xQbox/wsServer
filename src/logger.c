#define _GNU_SOURCE
#include "server.h"
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>


static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static FILE *log_file = NULL;

int init_logger(const char *filename)
{
    if ((log_file = fopen(filename, "a")) == NULL)
    {
        return -1;
    }
    return 0;
}

void close_logger(void)
{
    pthread_mutex_lock(&log_mutex);
    if (log_file)
    {
        fclose(log_file);
        log_file = NULL;
    }
    pthread_mutex_unlock(&log_mutex);
}

void logger(LogLevel level, const char *fmt, ...) 
{
    char time_str[32];
    time_t now = time(NULL);
    struct tm tmBuf;
    /* localtime() не потокобезопасен (статический внутренний буфер и
       ленивая инициализация часового пояса через tzset()) — при вызове из
       нескольких потоков без синхронизации это гонка. localtime_r() кладёт
       результат в буфер вызывающего и не требует мьютекса. */
    localtime_r(&now, &tmBuf);
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tmBuf);

    pthread_mutex_lock(&log_mutex);

    va_list args_file, args_stdout;
    va_start(args_file, fmt);
    va_copy(args_stdout, args_file); 
    if (log_file) 
    {
        fprintf(log_file, "[%s] [%d] ", time_str, level);
        vfprintf(log_file, fmt, args_file);
        fprintf(log_file, "\n");
        fflush(log_file);
    }

    if (level >= LOG_INFO) 
    {
        printf("[%s] ", time_str);
        vprintf(fmt, args_stdout);
        printf("\n");
    }

    va_end(args_file);
    va_end(args_stdout);

    pthread_mutex_unlock(&log_mutex);
}
