#include "common.h"

#define BUFLEN  128

int main(int argc ,char * argv[]) {
    struct addrinfo *ailist , *aip;
    struct addrinfo hint;
    int sockfd , err;

    if (argc != 2)
        err_quit("usage : client hostname");
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
            send(sockfd , "slect * from tb_user" , 10 , 0);
            exit(0);
        }
    }
    err_exit(err , "can't connect to %s" , argv[1]);
}
