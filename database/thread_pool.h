#pragma once
#include "common.h"
#include <thread>
#include <mutex>
#include <queue>
#include <vector>
#include <condition_variable>
#include <string>
#include <iostream>

#include "DDL.h"
#include "DML.h"
#include "DQL.h"

#define thread_max 8        // the maxnum of thread in thread_pool
#define thread_arg 2        // the number of arguments in threadinfo

#define SQLMAX      1024        // the max capcity of a sql 
#define SQLENDFLAG     ';'         // the end flag of a sql
#define SQLSEP      ' '         // the sepator of sql

using namespace std;

static string returnval[1024];          // return val of threadinfo::analyzer

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
    int analyzer();       // analyze the sql and call deal function
    int show_sql(string * sqlseg , int segnum);
    int use_sql(string * sqlseg , int segnum);
    int create_sql(string * sqlseg , int segnum);
    int drop_sql(string * sqlseg , int segnum);
    int desc_sql(string * sqlseg , int segnum);
    int alter_sql(string * sqlseg , int segnum);
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

void init_returnval();      // init the return_val

string print_table(table * tb);     // change the table to string which is easy to print
