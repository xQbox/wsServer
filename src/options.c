#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

#include "options.h"

/*
    Выводит справку по использованию программы.
*/
void printUsage(const char *progname)
{
    printf("WebSocket-агрегатор: N входящих источников -> один выходной WebSocket\n\n");
    printf("Использование: %s [ОПЦИИ]\n\n", progname);
    printf("Опции:\n");
    printf("  -p, --port PORT          Порт сервера (по умолчанию: %d)\n", DEFAULT_PORT);
    printf("  -w, --workers NUM        Количество Worker потоков (по умолчанию: %d, макс: %d)\n",
           DEFAULT_WORKERS, MAX_WORKERS);
    printf("  -l, --listeners NUM      Количество Listener потоков (по умолчанию: %d, макс: %d)\n",
           DEFAULT_LISTENERS, MAX_LISTENERS);
    printf("      --sink-path PATH     Путь для выходного WS-соединения (по умолчанию: %s)\n",
           DEFAULT_SINK_PATH);
    printf("      --hwm NUM            Верхний водяной знак backpressure, сообщений (по умолчанию: %d)\n",
           DEFAULT_HIGH_WATERMARK);
    printf("      --lwm NUM            Нижний водяной знак backpressure, сообщений (по умолчанию: %d)\n",
           DEFAULT_LOW_WATERMARK);
    printf("      --max-frame BYTES    Максимальный размер одного WS-кадра (по умолчанию: %u)\n",
           (unsigned)DEFAULT_MAX_FRAME);
    printf("      --max-message BYTES  Максимальный размер собранного сообщения (по умолчанию: %u)\n",
           (unsigned)DEFAULT_MAX_MESSAGE);
    printf("      --tick MS            Период тика статистики/агрегации, мс (по умолчанию: %d)\n",
           DEFAULT_TICK_MS);
    printf("  -h, --help               Показать эту справку\n\n");
    printf("Все источники подключаются к серверу по любому пути, кроме sink-path;\n");
    printf("id источника сервер присваивает сам. Ровно один выходной WS подключается\n");
    printf("к sink-path — новое подключение замещает предыдущее.\n\n");
    printf("Примеры:\n");
    printf("  %s                       Запуск с параметрами по умолчанию\n", progname);
    printf("  %s -p 8080               Запуск на порту 8080\n", progname);
    printf("  %s --sink-path /out      Другой путь для выходного WS\n", progname);
}

/* Парсит положительное целое из строки. Возвращает -1 при ошибке формата. */
static long parsePositive(const char *s)
{
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (end == s || *end != '\0' || v <= 0)
    {
        return -1;
    }
    return v;
}

/*
    Парсит аргументы командной строки.
    Возвращает 0 при успехе, -1 при ошибке, 1 при --help.
*/
int parseArgs(int argc, char **argv, ServerConfig_t *conf)
{
    enum
    {
        OPT_SINK_PATH = 1000,
        OPT_HWM,
        OPT_LWM,
        OPT_MAX_FRAME,
        OPT_MAX_MESSAGE,
        OPT_TICK
    };

    static struct option long_options[] =
    {
        {"port",        required_argument, 0, 'p'},
        {"workers",     required_argument, 0, 'w'},
        {"listeners",   required_argument, 0, 'l'},
        {"sink-path",   required_argument, 0, OPT_SINK_PATH},
        {"hwm",         required_argument, 0, OPT_HWM},
        {"lwm",         required_argument, 0, OPT_LWM},
        {"max-frame",   required_argument, 0, OPT_MAX_FRAME},
        {"max-message", required_argument, 0, OPT_MAX_MESSAGE},
        {"tick",        required_argument, 0, OPT_TICK},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    /* Значения по умолчанию */
    conf->port = DEFAULT_PORT;
    conf->workers = DEFAULT_WORKERS;
    conf->listeners = DEFAULT_LISTENERS;
    conf->sinkPath = DEFAULT_SINK_PATH;
    conf->handshakeMax = DEFAULT_HANDSHAKE_MAX;
    conf->maxFrame = DEFAULT_MAX_FRAME;
    conf->maxMessage = DEFAULT_MAX_MESSAGE;
    conf->highWatermark = DEFAULT_HIGH_WATERMARK;
    conf->lowWatermark = DEFAULT_LOW_WATERMARK;
    conf->tickMs = DEFAULT_TICK_MS;

    int opt;
    int option_index = 0;

    while ((opt = getopt_long(argc, argv, "p:w:l:h", long_options, &option_index)) != -1)
    {
        long v;
        switch (opt)
        {
            case 'p':
            {
                int port = atoi(optarg);
                if (port < 1 || port > 65535)
                {
                    fprintf(stderr, "Ошибка: порт должен быть от 1 до 65535\n");
                    return -1;
                }
                conf->port = port;
                break;
            }
            case 'w':
            {
                int workers = atoi(optarg);
                if (workers < 1 || workers > MAX_WORKERS)
                {
                    fprintf(stderr, "Ошибка: количество Worker'ов должно быть от 1 до %d\n", MAX_WORKERS);
                    return -1;
                }
                conf->workers = workers;
                break;
            }
            case 'l':
            {
                int listeners = atoi(optarg);
                if (listeners < 1 || listeners > MAX_LISTENERS)
                {
                    fprintf(stderr, "Ошибка: количество Listener'ов должно быть от 1 до %d\n", MAX_LISTENERS);
                    return -1;
                }
                conf->listeners = listeners;
                break;
            }
            case OPT_SINK_PATH:
                if (optarg[0] != '/')
                {
                    fprintf(stderr, "Ошибка: sink-path должен начинаться с '/'\n");
                    return -1;
                }
                conf->sinkPath = optarg;
                break;
            case OPT_HWM:
                if ((v = parsePositive(optarg)) < 0)
                {
                    fprintf(stderr, "Ошибка: --hwm должен быть положительным числом\n");
                    return -1;
                }
                conf->highWatermark = (size_t)v;
                break;
            case OPT_LWM:
                if ((v = parsePositive(optarg)) < 0)
                {
                    fprintf(stderr, "Ошибка: --lwm должен быть положительным числом\n");
                    return -1;
                }
                conf->lowWatermark = (size_t)v;
                break;
            case OPT_MAX_FRAME:
                if ((v = parsePositive(optarg)) < 0)
                {
                    fprintf(stderr, "Ошибка: --max-frame должен быть положительным числом\n");
                    return -1;
                }
                conf->maxFrame = (size_t)v;
                break;
            case OPT_MAX_MESSAGE:
                if ((v = parsePositive(optarg)) < 0)
                {
                    fprintf(stderr, "Ошибка: --max-message должен быть положительным числом\n");
                    return -1;
                }
                conf->maxMessage = (size_t)v;
                break;
            case OPT_TICK:
                if ((v = parsePositive(optarg)) < 0)
                {
                    fprintf(stderr, "Ошибка: --tick должен быть положительным числом\n");
                    return -1;
                }
                conf->tickMs = (int)v;
                break;
            case 'h':
                printUsage(argv[0]);
                return 1;
            default:
                printUsage(argv[0]);
                return -1;
        }
    }

    if (conf->lowWatermark >= conf->highWatermark)
    {
        fprintf(stderr, "Ошибка: --lwm (%zu) должен быть меньше --hwm (%zu)\n",
                conf->lowWatermark, conf->highWatermark);
        return -1;
    }

    return 0;
}
