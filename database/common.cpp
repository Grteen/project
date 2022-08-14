#include "common.h"

void err_doit(int errnoflag , int error , const char *fmt , va_list ap){
	char buf[MAXLINE];
	vsnprintf(buf , MAXLINE - 1 , fmt , ap);
	if (errnoflag)
		snprintf(buf + strlen(buf) , MAXLINE - strlen(buf) - 1 , ": %s" , strerror(error));
	strcat(buf , "\n");
	fflush(stdout);
	fputs(buf , stderr);
	fflush(NULL);
}

void err_ret(const char *fmt , ...){
	va_list ap;
	va_start(ap , fmt);
	err_doit(1 , errno , fmt , ap);
	va_end(ap);
}

void err_sys(const char *fmt , ...){
	va_list ap;
	va_start(ap , fmt);
	err_doit(1 , errno , fmt , ap);
	va_end(ap);
	exit(1);
}

void err_cont(int error , const char * fmt , ...){
	va_list ap;
	va_start(ap , fmt);
	err_doit(1 , error , fmt , ap);
	va_end(ap);
}

void err_exit(int error , const char *fmt , ...){
	va_list ap;
	va_start(ap , fmt);
	err_doit(1 , error , fmt , ap);
	va_end(ap);
	exit(1);
}

void err_dump(const char *fmt , ...){
	va_list ap;
	va_start(ap , fmt);
	err_doit(1 , errno , fmt , ap);
	va_end(ap);
	abort();
	exit(1);
}

void err_msg(const char *fmt , ...){
	va_list ap;
	va_start(ap , fmt);
	err_doit(0 , 0 , fmt , ap);
	va_end(ap);
}

void err_quit(const char *fmt , ...){
	va_list ap;
	va_start(ap , fmt);
	err_doit(0 , 0 , fmt , ap);
	va_end(ap);
	exit(1);
} 

int lock_reg(int fd , int cmd , int type , off_t offset , int whence , off_t len){
	struct flock lock;
	lock.l_type = type;
	lock.l_start = offset;
	lock.l_whence = whence;
	lock.l_len = len;
	
	return(fcntl(fd , cmd , &lock));
}

pid_t lock_test(int fd , int type , off_t offset , int whence , off_t len){
	struct flock lock;
	lock.l_type = type;
	lock.l_start = offset;
	lock.l_whence = whence;
	lock.l_len = len;

	if (fcntl(fd , F_GETLK , &lock) < 0){
		printf("fcntl error");
		exit(-1);
	}

	if (lock.l_type == F_UNLCK)
		return 0;

	return lock.l_pid;
}

void daemonize(const char * cmd){
	int i , fd0 , fd1 , fd2;
	pid_t pid;
	struct rlimit rl;
	struct sigaction sa;

	umask(0);		// clear file creation mask
	
	if (getrlimit(RLIMIT_NOFILE , &rl) < 0)		// get maximun number of file descrptions.
		exit(-1);
	
	if ((pid = fork()) < 0)
		exit(-1);
	else if (pid != 0)		// parent
		exit(0);

	setsid();

	sa.sa_handler = SIG_IGN;		// ensure future opens won't allocate controlling TTYS
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	
	if (sigaction(SIGHUP , &sa , NULL) < 0)
		exit(-1);
	if ((pid = fork()) < 0)
		exit(-1);
	else if (pid != 0)		// parent
		exit(0);

	// change the current working dirctory to the root so we won't prevent file systems from being unmounted
	
	if (chdir("/") < 0)
		exit(-1);

	// close all open file descritions
	
	if (rl.rlim_max == RLIM_INFINITY)
		rl.rlim_max = 1024;
	for (i = 0 ; i < rl.rlim_max ; i++)
		close(i);

	// attach file descritors to /dev/null
	
	fd0 = open("/dev/null" , O_RDWR);
	fd1 = dup(0);
	fd2 = dup(0);

	// initialize the log file
	openlog(cmd , LOG_CONS , LOG_DAEMON);
	if (fd0 != 0 || fd1 != 1 || fd2 != 2){
		exit(1);
	}
}

int initserver(int type , const struct sockaddr *addr , socklen_t alen , int qlen){
	int fd;
	int err = 0;

	if ((fd = socket(addr->sa_family , type , 0)) < 0)
		return -2;
	if (bind(fd , addr , alen) < 0){
		err = errno;
		close(fd);
		errno = err;
		err_sys("bind error");
		return -1;
	}

	if (type == SOCK_STREAM || type == SOCK_SEQPACKET){
		if (listen(fd , qlen) < 0){
			err = errno;
			close(fd);
			errno = err;
			return -1;
		}
	}
	return fd;
}


int set_cloexec(int fd){
	int val;

	if ((val = fcntl(fd , F_GETFD , 0)) < 0)
		return -1;

	val |= FD_CLOEXEC;

	return (fcntl(fd , F_SETFD , val));
}

int connect_retry(int domain , int type , int protocol , const struct sockaddr *addr , socklen_t alen){
	int numsec , fd;
	int MAXSLEEP = 128;
	for (numsec = 1 ; numsec <= MAXSLEEP ; numsec <<= 1){
		if ((fd = socket(domain , type , protocol)) < 0)
			return -1;
		if (connect(fd , addr , alen) == 0)
			return fd;
		close(fd);

		if (numsec <= MAXSLEEP / 2)
			sleep(numsec);
	}
	return -1;
}