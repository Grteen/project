#include "cli_control.h"

#define NALLOC  10      // cliarray to alloc / realloc for every time
extern cliarray cli_array;
FILE * logfp;
FILE * io_logfp;

static void client_alloc(cliarray * cli_array) {
    if (cli_array->arr == NULL)       // first time called
        cli_array->arr = (client *)malloc(NALLOC * sizeof(client ));
    else
        cli_array->arr = (client *)realloc(cli_array->arr , (cli_array->arr_size + NALLOC) * sizeof(client ));       // realloc
    if (cli_array->arr == NULL)
        err_sys("can't alloc for client array");

    // initialize new entries
    for (int i = cli_array->arr_size ; i < cli_array->arr_size + NALLOC ; i++)
        cli_array->arr[i].fd = -1;        // fd -1 means entry availale
    
    cli_array->arr_size += NALLOC;
}

int client_add(client *cli , cliarray *cli_array) {
    if (cli_array->arr == NULL)     // first time called
        client_alloc(cli_array);
    
again:
    for (int i = 0 ; i < cli_array->arr_size ; i++) {       // find available places
        if (cli_array->arr[i].fd == -1) {
            cli_array->arr[i] = *cli;
            return i;       // return index in array
        }
    }

    // array is full , call realloc for more
    client_alloc(cli_array);
    goto again;
}

void client_del(int fd , cliarray *cli_array) {       // delete a client after we're done with it
    for (int i = 0 ; i < cli_array->arr_size ; i++)
        if (cli_array->arr[i].fd == fd) {       // find a match
            cli_array->arr[i].fd = -1;
            return ;
        }
    fprintf(logfp , "can't find client entry for fd %d\n" , fd);
}

