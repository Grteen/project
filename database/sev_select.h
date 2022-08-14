#pragma once
#include "common.h"
using namespace std;

#define WK_PORT "database"      // port
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX   256
#endif

#define BUFLEN 128
#define QLEN 10

#define SEVLOG  "./log/db_server.log"

void sev_select(int );