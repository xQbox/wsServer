#ifndef WS_CLIENT_H
#define WS_CLIENT_H

#include "server.h"

typedef struct WsClientArgs_t WsClientArgs_t;
struct WsClientArgs_t {
    ServerConfig_t* conf;
    const char* host;
    int port;
    const char* path;
};

void* wsClientThread(void* arg);
typedef struct UpstreamReactor_t UpstreamReactor_t;

UpstreamReactor_t* upstreamCreate(ServerConfig_t* conf);

int upstreamAdd(UpstreamReactor_t* r, const char* host, int port, const char* path);

void* upstreamReactorThread(void* arg);

void upstreamDestroy(UpstreamReactor_t* r);
#endif  // WS_CLIENT_H
