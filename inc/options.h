#ifndef OPTIONS_H
#define OPTIONS_H

#include "server.h"

/*
 * Выводит справку по использованию программы.
 */
void printUsage(const char *progname);

/*
 * Парсит аргументы командной строки.
 * Возвращает: 0 - успех, -1 - ошибка, 1 - запрошена справка (--help)
 */
int parseArgs(int argc, char **argv, ServerConfig_t *conf);

#endif /* OPTIONS_H */
