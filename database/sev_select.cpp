#include "sev_select.h"
#include "cli_control.h"
#include "thread_pool.h"
#include "DDL.h"

extern cliarray cli_array;
extern FILE * logfp;
extern FILE * io_logfp;
extern FILE * bptlog;
extern condition_variable cond;
thread_pool tp;   

void sql_hanlder(task * tk) {
    tp.put_into_tasks(tk);
}

void dealwith_sql() {
    while (true) {
        if (!tp.get_empty()) {          // there are tasks in queue
            int busy = 1;
            task * tk = new task();   
            if (tp.get_task(tk) == -1)
                fprintf(logfp , "get_task error : thread_pool queue is empty");
            for (int i = 0 ; i < thread_max ; i++) {
                if (tp.check_free(tp._thrvec[i]) == true) {     // free
                    tp._thrvec[i]->change_fd(tk->fd);
                    tp._thrvec[i]->change_sql(tk->sql);
                    tp._thrvec[i]->change_allow(true);
                    cond.notify_all();
                    busy = 0;
                    break;
                }
            }
            if (busy)
                fprintf(logfp , "thread_pool busy");
        }
    }
}

void sev_select(int listenfd) {
    int maxfd , maxi , n , clfd , i , nread;
    fd_set allset;      // all client's set
    fd_set rset;        // allset's copy
    char buf[MAXLINE];  
    char stdoutbuf[MAXLINE];
    
    tp.init();
    std::thread t(dealwith_sql);
    t.detach();
    memset(&buf , 0 , sizeof(buf));
    logfp = fopen(SEVLOG , "w+");         // open the log
    io_logfp = fopen(IOLOG , "w+");        // open the IO log
    bptlog = fopen(BPTLOG , "w+");          // open the bpt log
    build_all_bpt();
    setvbuf(stdout , stdoutbuf , _IOLBF , MAXLINE);   
    setvbuf(logfp , NULL , _IOLBF , 0);
    setvbuf(io_logfp , NULL , _IOLBF , 0);
    FD_ZERO(&allset);

    // obtain fd to listen for client requests on
    FD_SET(listenfd , &allset);
    maxfd = listenfd;       // the biggest fd
    maxi = -1;          // the biggest index in cli_array  

    init_returnval();
    for (; ;) {
        rset = allset;      // copy
        if ((n = select(maxfd + 1 , &rset , NULL , NULL , NULL)) < 0) {
            fprintf(logfp , "select error\n");
        }
        else {
            if (FD_ISSET(listenfd , &rset)) {       // listenfd is ok that means a new client comes
                if ((clfd = accept(listenfd , NULL , NULL)) < 0) {
                    syslog(LOG_ERR , "database : accept error : %s" , strerror(errno));
                    exit(1);
                }
                client * cli;
                if ((cli = new client()) == NULL)
                    fprintf(logfp , "malloc error\n");
                cli->fd = clfd;
                i = client_add(cli , &cli_array);
                FD_SET(clfd , &allset);
                if (clfd > maxfd)
                    maxfd = clfd;           // max fd for select
                if (i > maxi)
                    maxi = i;       // max index in cli_array
                fprintf(logfp, "new connection : fd %d\n" , clfd);
                continue;
            }
            for (int j = 0 ; j <= maxi ; j++) {         // go through cli_array
                if ((clfd = cli_array.arr[j].fd) < 0)       // invalid client
                    continue;
                if (FD_ISSET(clfd , &rset)) {
                    // read sql from client
                    if ((nread = recv(clfd , buf , MAXLINE , 0)) < 0) {
                        fprintf(logfp , "read error on fd %d\n" , clfd);
                    }
                    else if (nread == 0) {          // client is closed
                        fprintf(logfp , "closed : fd %d\n" , clfd);
                        client_del(clfd , &cli_array);      
                        FD_CLR(clfd , &allset);
                        close(clfd);
                    }
                    else {          // process client's sql
                        task *tk = new task();
                        tk->fd = clfd;
                        std::string sql(buf , nread);
                        tk->sql = sql; 
                        sql_hanlder(tk);
                    }
                }
            }
        }
    }
}

void build_all_bpt() {
    DIR *dp;
    char fullpath[PATH_MAX];
    
    strcpy(fullpath , "./" DTATBASEL "/");
    string * db;
    int dbsize = 0;
    db = show_databases(&dbsize);

    for (int i = 0 ; i < dbsize ; i++) {
        use_db(db[i]);
        string * tb;
        int tbsize = 0;
        tb = show_tables(&tbsize);
        for (int k = 0 ; k < tbsize ; k++) {
            string * index;
            int idxsize = 0;
            index = show_index(tb[k] , &idxsize);
            string trueindex[idxsize];
            for (int m = 0 ; m < idxsize ; m++) {
                for (int t = 0 ; t < index[i].size() ; t++) {
                    if (index[m][t] == '.')
                        break;
                    else
                        trueindex[m] = trueindex[m] + index[m][t];
                }
            }
            for (int n = 0 ; n < idxsize ; n++) {
                table * tab = read_tables(tb[k]);
                create_index(tab , trueindex[n]);
                create_bpt(tb[k] , trueindex[n]);
                delete tab;
            }
            delete []index;
        }
        delete []tb;
    }
}