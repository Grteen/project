#include "thread_pool.h"

std::condition_variable cond;

threadinfo::threadinfo() {}
threadinfo::~threadinfo() {}

void Prosql(threadinfo * thrinfo) {
    while (1) {
        std::unique_lock<std::mutex> locker(thrinfo->thrmu , std::defer_lock);
        locker.lock();
        thrinfo->change_free(true);         // free
        cond.wait(locker , [=](){return thrinfo->get_allow();});        // wait tasks
        locker.unlock();
        thrinfo->change_free(false);        // busy 
        // process function ...
        std::cout << thrinfo->get_sql() << std::endl; 
        // process function ...
        thrinfo->change_allow(false);           // process done , so next time will be not allowed and waiting to be changed
    }
}

void threadinfo::init() {
    this->_allow = false; 
    this->_argsize = thread_arg;
    this->_free = true;
}

bool threadinfo::get_allow() {
    return this->_allow;
}

int threadinfo::change_allow(bool allow) {
    this->_allow = allow;
    return 0;       // change success
}

int threadinfo::change_sql(std::string sql) {
    this->_sql = sql;
    return 0;       // change sucess
}

std::string threadinfo::get_sql() {
    return this->_sql;
}

bool threadinfo::get_free() {
    return this->_free;
}

int threadinfo::change_free(bool free) {
    this->_free = free;
    return 1;       // change success
}

int threadinfo::get_fd() {
    return this->_fd;
}

int threadinfo::change_fd(int fd) {
    this->_fd = fd;
    return 0;       // change success
}

thread_pool::thread_pool() {}
thread_pool::~thread_pool() {}

void thread_pool::init() {
    this->_size = thread_max;
    for (int i = 0 ; i < thread_max ; i++) {
        threadinfo *thr = new threadinfo();
        thr->init();
        this->_thrvec.push_back(thr);
        std::thread t(Prosql , thr);        // push the threadinfo into process function
        t.detach();
    }
}

bool thread_pool::check_free(threadinfo * tf) {
    std::unique_lock<std::mutex> locker(this->_tkmu);
    bool res = tf->get_free();
    return res;
}

void thread_pool::put_into_tasks(task *tk) {
    std::unique_lock<std::mutex> locker(this->_tkmu);
    this->_tasks.push(tk);
}

int thread_pool::get_task(task * target) {
    std::unique_lock<std::mutex> locker(this->_tkmu);
    if (this->_tasks.empty()) 
        return -1;
    *target = *(this->_tasks.back());            // get the task
    this->_tasks.pop();
    return 0;
}

bool thread_pool::get_empty() {
    return this->_tasks.empty();
}
