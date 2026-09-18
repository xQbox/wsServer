#ifndef LOGGER_H_
#define LOGGER_H_

#include "server.h"

/*
 * Потокобезопасный логгер: запись в файл и stdout
 */

/* Инициализация логгера (открытие файла) */
int init_logger(const char *filename);

/* Закрытие логгера */
void close_logger(void);

/* Запись лога: LOG_DEBUG — только файл, LOG_INFO/LOG_ERROR — файл + stdout */
void logger(LogLevel level, const char *fmt, ...);

#endif /* LOGGER_H_ */
