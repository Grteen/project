#pragma once
#include <bits/types/struct_sigstack.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <signal.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <syslog.h>
#include <sys/resource.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <sys/msg.h>
#include <sys/poll.h>
#include <sys/un.h>
#include <stddef.h>
#include <sys/uio.h>
#include <termios.h>
#include <dirent.h>
#include <stdlib.h>
#include <limits.h>

#define MAXLINE 1024

typedef void Sigfunc(int);
void err_doit(int , int , const char * , va_list);

// print a message and return
void err_ret(const char *fmt , ...);

// Fatal error related to a system call.
// Print a message and terminate
void err_sys(const char * fmt , ...);

// Nonfatal error unrelated tot a system call
// Error code passed as explict parameter
// Print a message and return
void err_cont(int error , const char *fmt , ...);

// Fatal error unrelated to a system call
// Error code passed as explict parameter
// Print a message and terminate
void err_exit(int error , const char *fmt , ...);

// Fatal error related to a system call
// Print a message , dump core , and terminate
void err_dump(const char *fmt , ...);

// Nonfatal error unrelated to a system call
// Print a message and return
void err_msg(const char * fmt , ...);

//Fatal error unrelated to a system call
//Print a message and terminate
void err_quit(const char *fmt , ...);

void daemonize(const char * cmd);

// init the tcp server
int initserver(int type , const struct sockaddr *addr , socklen_t alen , int qlen);

// set close-on-exec
int set_cloexec(int fd);

// connect (if error auto retry)
int connect_retry(int domain , int type , int protocol , const struct sockaddr *addr , socklen_t alen);

#define read_lock(fd , offset , whence , len) \
		lock_reg((fd) , F_SETLK , F_RDLCK , (offset) , (whence) , (len))
#define readw_lock(fd , offset , whence , len) \
		lock_reg((fd) , F_SETLKW , F_RDLCK , (offset) , (whence) , (len))
#define write_lock(fd , offset , whence , len) \
		lock_reg((fd) , F_SETLK , F_WRLCK , (offset) , (whence) , (len))
#define writew_lock(fd , offset , whence , len) \
		lock_reg((fd) , F_SETLKW , F_WRLCK , (offset) , (whence) , (len))
#define un_lock(fd , offset , whence , len) \
		lock_reg((fd) , F_SETLK , F_UNLCK , (offset) , (whence) , (len))

#define is_read_lockable(fd , offset , whence , len) \
		(lock_test ((fd) , F_RDLCK , (offset) , (whence) , (len)) == 0)
#define is_write_lockable(fd , offset , whence , len) \
		(lock_test ((fd) , F_WRLCK , (offset) , (whence) , (len)) == 0)

int lock_reg(int fd , int cmd , int type , off_t offset , int whence , off_t len);