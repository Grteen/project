#include "common.h"
#include <iostream>
#include <ctime>

#define BUFLEN  1024
#define client_log      "./log/client.log"

FILE * clilog;

using namespace std;

int main(int argc ,char * argv[]) {
    struct addrinfo *ailist , *aip;
    struct addrinfo hint;
    int sockfd , err;
    char buf[BUFLEN];
    char sockbuf[BUFLEN];
    clock_t start;
    clock_t end;
    int nrecv;
    int nread;

    if (argc != 2)
        err_quit("usage : client hostname");

    if ((clilog = fopen(client_log , "w+")) == NULL) {
        err_sys("fopen error");
    }
    setvbuf(clilog , NULL , _IOLBF , 0);

    memset(&hint , 0 , sizeof(hint));
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_canonname = NULL;
    hint.ai_addr = NULL;
    hint.ai_next = NULL;
    if ((err = getaddrinfo(argv[1] , "database" , &hint , &ailist)) != 0)
        err_quit("getaddrinfo error : %s" , gai_strerror(err));
    for (aip = ailist ; aip != NULL ; aip = aip->ai_next) {
        if ((sockfd = connect_retry(aip->ai_family , SOCK_STREAM , 0 , aip->ai_addr , aip->ai_addrlen)) < 0) {
            err = errno;
        }
        else {
            // get the sql from terminate
            write(STDOUT_FILENO , "prompt > " , 9);
            while ((nread = read(STDIN_FILENO , buf , BUFLEN)) != -1) {
                start = clock();
                if (send(sockfd , buf , nread , 0) != nread)
                    fprintf(clilog , "send error\n");

                if ((nrecv = recv(sockfd , sockbuf , BUFLEN , 0)) > 0)
                    write(STDOUT_FILENO , sockbuf , nrecv);

                end = clock();

                fprintf(stdout , "time : %f seconds\n" , (double)(end - start) / CLOCKS_PER_SEC);
                write(STDOUT_FILENO , "prompt > " , 9);
            }
            exit(0);
        }
    }
    err_exit(err , "can't connect to %s" , argv[1]);
}
