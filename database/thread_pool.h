#include "common.h"
#include <thread>
#include <mutex>
#include <queue>
#include <vector>
#include <condition_variable>
#include <string>
#include <iostream>

#define thread_max 8        // the maxnum of thread in thread_pool
#define thread_arg 2        // the number of arguments in threadinfo


class threadinfo {          // thread and its infomation
private:
    unsigned int _argsize;       // the num of arguments of this thread
    std::string _sql;        // the processing sql in this thread
    bool _allow;             // allow to process
    bool _free;             // if this threadinfo is free
    int _fd;                // client's fd
public:
    std::mutex thrmu;       // the mutex that protect the free condition
    threadinfo();
    ~threadinfo();
    void init();
    int change_sql(std::string);        // change the _sql
    int change_allow(bool);         // change the allow
    bool get_allow();           // get the _allow
    std::string get_sql();           // get the _sql
    bool get_free();            // get the free condition   true means free
    int change_free(bool);          // change the free condition
    int get_fd();               // get the client's fd
    int change_fd(int );            // change the client's fd
};

class task {
public:
    std::string sql;
    int fd;
};

class thread_pool {
private:
    unsigned short _size;        // the number of thread in thread_pool    
    std::queue<task *> _tasks;        // task queue
    
public:
    std::vector<threadinfo *> _thrvec;    
    std::mutex _tkmu;           // task queue's mutex
    thread_pool();
    ~thread_pool();
    void init();
    bool check_free(threadinfo *);          // check if this threadinfo is free
    void put_into_tasks(task *);            // put new task into queue
    int get_task(task *);                // get task from queue
    bool get_empty();           // get the empty condition of queue
};

void Prosql(threadinfo *);         // the function that thread called
