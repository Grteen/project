#include "cli_control.h"
#include "sev_select.h"

bool debug = false;     // if true is debug mode (non daemonize)
cliarray cli_array;
int n , err , sockfd;
char * host;
struct addrinfo hint;
struct addrinfo *ailist , *aip;

int main(int argc , char * argv[]) {

    // if (mkdir("log" , 0777) == -1)
    //     err_sys("mkdir error");

    if (argc != 1)
        err_quit("usage : database");
    if ((n = sysconf(_SC_HOST_NAME_MAX)) < 0)
        n = HOST_NAME_MAX;
    if ((host = (char *)malloc(n)) == NULL)
        err_sys("malloc error");
    if (gethostname(host , n) < 0)
        err_sys("gethostname error");

    // check the debug flag
    if (debug == true)
        daemonize("db_server");         // become a daemonize
    
    memset(&hint , 0 , sizeof(NULL));
    hint.ai_flags = AI_CANONNAME;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_canonname = NULL;
    hint.ai_addr = NULL;
    hint.ai_next = NULL;
    
    if ((err = getaddrinfo(host , WK_PORT , &hint ,&ailist)) != 0){
        syslog(LOG_ERR , "database : getaddrinfo error : %s" , gai_strerror(err));
        exit(1);
    }
    for(aip = ailist ; aip != NULL ; aip = aip->ai_next) {
        // init the server
        if ((sockfd = initserver(SOCK_STREAM , aip->ai_addr , aip->ai_addrlen , QLEN)) >= 0) {
            // start work
            sev_select(sockfd);
            exit(0);
       }
    }  
    exit(1);
}