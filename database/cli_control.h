#pragma once
#include "common.h"
#include <iostream>   

class client {          // client's class
public:
    int fd;             // client's fd  (-1 is unavailable)
};

class cliarray {        // client's array
public:
    client * arr;       // ptr to malloc'ed array (dynamic alloc)
    int arr_size;       // array size
};

static void client_alloc(void);     // alloc more places in cliarray.arr
int client_add(client *cli , cliarray *cli_array);        // add a new client into array
void client_del(int fd , cliarray *cli_array);           // delete client