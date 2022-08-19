#pragma once
#include "common.h"
using namespace std;

#define WK_PORT "database"      // port
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX   256
#endif

#define QLEN 10

#define SEVLOG  "./log/db_server.log"
#define BPTLOG  "./log/bpt.log"
#define IOLOG   "./log/IO.log"

void sev_select(int );

void build_all_bpt();