/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#if defined(__linux__)
#include <spawn.h>
#endif
#include <time.h>

#include <boost/lexical_cast.hpp>
#include <boost/shared_array.hpp>
#include <boost/bind.hpp>

#include "system/ib_system.h"

using namespace std;
using namespace boost;

_int64 GetPid()
{
	return getpid();
}

std::string GetErrorMessage(int error_id, const char* prefix/*=0*/)
{
    char buf[256];
    std::string result;
    result = prefix ? prefix : "";
    result += strerror_r(error_id, buf, sizeof(buf));
    return result;
}

/****************************
 * IBMutex
 ****************************/
IBMutex::IBMutex(bool count_time) : m_count_time(count_time), m_time_counter(0)
{
    Init();
}

IBMutex::~IBMutex()
{
    Uninit();
}

void IBMutex::Init()
{
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init( &mattr);
    pthread_mutexattr_settype(& mattr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &mattr);
    pthread_mutexattr_destroy( &mattr);
}

void IBMutex::Uninit()
{
    pthread_mutex_destroy(& mutex);
}

void IBMutex::Lock() throw(IBSysException)
{
    int ret = pthread_mutex_lock(& mutex);
    if(ret)
    	throw IBSysException(errno);
}

int IBMutex::TryTimedLock( int timeout) throw (IBSysException)
{
    int ret;
#if defined(__linux__) || defined(__OpenBSD__)
    struct timespec deltatime;
    deltatime.tv_sec = timeout / 1000;
    deltatime.tv_nsec = (timeout % 1000) * 1000000;
    ret = pthread_mutex_timedlock( & mutex, & deltatime);
#else
    while (timeout > 0) {
        ret = pthread_mutex_trylock( & mutex);
        if (ret == EBUSY) {
            usleep(10);
            timeout -= 10;
        } else
            break;
    }
#endif
    if (ret) {
        if (ret == EBUSY) return 0;
        throw IBSysException(errno);
    }
    return 1;
}

int IBMutex::TryLock() throw(IBSysException)
{
    int ret = pthread_mutex_trylock(& mutex);
    if(ret == EBUSY) return 0;
    if(ret)
    	throw IBSysException(errno);
    return 1;
}

void IBMutex::Unlock() throw(IBSysException)
{
    int ret = pthread_mutex_unlock(& mutex);
    if(ret)
    	throw IBSysException(errno);
}


/****************************
 * IBCond
 ****************************/
IBCond::IBCond()
{
    pthread_cond_init(& cond, 0);
}

IBCond::~IBCond()
{
    pthread_cond_destroy(& cond);
}

void IBCond::Lock()
{
    mutex.Lock();
}

void IBCond::Unlock()
{
    mutex.Unlock();
}

void IBCond::Wait()
{
    int ret = pthread_cond_wait(& cond, & mutex.mutex);
    if(ret)
    	throw IBSysException(errno);
}

void IBCond::Wait(IBMutex& m)
{
    int ret = pthread_cond_wait(& cond, & m.mutex);
    if(ret)
    	throw IBSysException(errno);
}

void IBCond::Signal()
{
    int ret = pthread_cond_signal(& cond);
    if(ret)
    	throw IBSysException(errno);
}

void IBCond::Broadcast()
{
    int ret = pthread_cond_broadcast(& cond);
    if(ret)
    	throw IBSysException(errno);
}


/****************************
 * IBThread
 ****************************/
IBThread::IBThread()
  : thread(0)
{

}

IBThread::~IBThread()
{
    if(thread) Join();
}

void IBThread::Start(ThreadFunc func, void* param) throw(IBSysException)
{
    int ret = pthread_create(& thread, 0, func, param);
    if(ret)
    	throw IBSysException(errno);
}

void IBThread::Join(void** result) throw(IBSysException)
{
    if(thread!=0)  {// throw IBSysException(EINVAL);
		int ret = pthread_join(thread, result);
		thread = 0;
		if(ret)
			throw IBSysException(errno);  // TODO: signal interrupt ???
    }
}

THREADVAR IBThread::GetThreadID()
{
    return thread;
}

void IBThread::SleepNow(int msec) throw(IBSysException)
{
	struct timespec ts;
	if( msec >= 1000 )
		ts.tv_sec = msec/1000;
	else
		ts.tv_sec = 0;
	ts.tv_nsec = (msec % 1000) * 1000000;
	nanosleep( &ts, NULL );
    //usleep(msec * 1000);
}

THREADVAR IBThread::GetSelf() throw(IBSysException)
{
    return pthread_self();
}

int IBThread::IsRunning()
{
    return (thread > 0);
}

/****************************
 * IBProcess
 ****************************/
IBProcess::IBProcess()
  : process(0)
{

}

IBProcess::~IBProcess()
{
     // should I stay or should I go?
}

void IBProcess::Start(const char* path) throw(IBSysException)
{
    Start(path, 0, 0);
}

void IBProcess::Start(const char* path, const char* arg) throw(IBSysException)
{
    Start(path, 1, & arg);
}

void IBProcess::Start(const char* path, const char* arg, const char* arg2) throw(IBSysException)
{
    const char* pargs[] = { arg, arg2 };
    Start(path, 2, pargs);
}

void IBProcess::Start(const char* path, const char* arg, const char* arg2, const char* arg3) throw(IBSysException)
{
    const char* pargs[] = { arg, arg2, arg3 };
    Start(path, 3, pargs);
}

void IBProcess::Start(const char* path, const std::vector<std::string>& args) throw (IBSysException)
{
	shared_array<const char*> pargs(new const char * [args.size()]);
	transform(args.begin(), args.end(), pargs.get(), bind(&string::c_str, _1));
	Start(path, (int)args.size(), pargs.get());
}

#ifdef __sun__
extern char** environ;
#endif /* #ifdef __sun__ */

void IBProcess::Start(const char* path, int args_count, const char* args[]) throw(IBSysException)
{
    if((path == 0) ||(args_count < 0))
    	throw IBSysException(EINVAL);

#ifdef __sun__
    const int MAX_ARGC = 256;
    if (args_count >= MAX_ARGC)
    	throw IBSysException(EINVAL);

    char* argv[MAX_ARGC];
    memset (argv, 0, sizeof(argv));
    argv[0] = const_cast<char*>(path);
    for (int i=0; i < args_count; i++) {
        argv[i+1] = const_cast<char*>(args[i]);
    }

    int ret = posix_spawn( & process,
                           path,
                           NULL,   // const posix_spawn_file_actions_t *file_actions,
                           NULL,   // const posix_spawnattr_t *restrict attrp,
                           argv,   // char *const argv[restrict],
                           environ // char *const envp[restrict]);
                         );
    if (ret)
    	throw IBSysException(errno);
#else
    char** pargs = (char**)malloc(sizeof(char*) * (args_count + 2));
	pargs[0] = strdup(path);

	for(int i = 0; i < args_count; i++)
		pargs[i + 1] = strdup(args[i]);

	pargs[args_count + 1] = 0;

	process = vfork(); // VFORK !!!!
	if(process == 0) {
		execv(path, pargs);
		/* error situation, make it negative to differentiate system error from internal error of the executable */
		_exit((-1)*errno);
	}

	for(int i = 0; i <= args_count; i++)
		free(pargs[i]);
	free(pargs);

	if(process < 0)
		throw IBSysException(errno);
#endif
}

void IBProcess::Wait(int& return_code, int msec) throw(IBSysException)
{
    if(process <= 0) throw IBSysException(EINVAL);
    int options =(msec > 0) ? WNOHANG : 0;
    int status;
    int ret = waitpid(process, & status, options);
    if(ret == 0) {
        IBThread::SleepNow(msec);
    }
    else if(ret == process) {
        process = 0;
        /* check if it was signalled (e.g. SIGKILL) */
        if (WIFSIGNALED(status) != 0)
            throw IBSysException(EINTR);
        return_code = (char)WEXITSTATUS(status);
        /* system error code was intentionally made negative in the Start function
         * (to differentiate error from executable), look at the line after the execv call.
         */
        if (return_code < 0)
            throw IBSysException((-1)*return_code);
    }
    else {
        throw IBSysException(errno);
    }
}

PROCESS IBProcess::GetProcess()
{
    return process;
}

void IBProcess::Terminate()
{
    int ret = kill(process, SIGKILL);
    if(ret)
    	throw IBSysException(errno);
}

PROCESS IBProcess::GetSelf()
{
    return getpid();
}


/****************************
 * IBThreadStorage
 ****************************/
template<class T>
IBThreadStorage<T>::IBThreadStorage()
{
    pthread_key_create(&key, NULL);
}

template<class T>
IBThreadStorage<T>::~IBThreadStorage()
{
    pthread_key_delete(key);
}

template<class T>
void IBThreadStorage<T>::Set(T& obj) throw (IBSysException)
{
    if(pthread_setspecific(key, &obj))
    	throw IBSysException(errno);
}

template<class T>
T& IBThreadStorage<T>::Get() throw (IBSysException)
{
	void* ptr = pthread_getspecific(key);
	if(!ptr)
		throw IBSysException(errno);
	return *static_cast<T*>(ptr);
}

template<class T>
bool IBThreadStorage<T>::IsValid()
{
	void* ptr = pthread_getspecific(key);
	if(!ptr)
		return false;
	return true;
}

template<class T>
void IBThreadStorage<T>::Release() throw (IBSysException)
{
	int error = pthread_setspecific(key, NULL);
	if(error)
		throw IBSysException(error);
}

template class IBThreadStorage<ConnectionInfo>;
