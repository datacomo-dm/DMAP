#ifndef IBCompress_test_assist_def_h
#define IBCompress_test_assist_def_h

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "IBCompress.h"


#ifndef TIMEOFDAY
#define TIMEOFDAY CLOCK_REALTIME
#endif

#ifndef MYTIMER_CLASS
#define MYTIMER_CLASS
class mytimer {
#ifdef WIN32 
	LARGE_INTEGER fcpu,st,ct;
#else
	struct timespec st,ct;
#endif
public:
	mytimer() {
#ifdef WIN32
	 QueryPerformanceFrequency(&fcpu);
	 QueryPerformanceCounter(&st);
	 ct.QuadPart=0;
#else
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
#endif
	}
	void Clear() {
#ifdef WIN32
		ct.QuadPart=0;
#else
		memset(&ct,0,sizeof(timespec));
#endif
	}
	void Start() {
#ifdef WIN32
	 QueryPerformanceCounter(&st);
#else
	 clock_gettime(TIMEOFDAY,&st);
#endif
	}
	void Stop() {
#ifdef WIN32
	 LARGE_INTEGER ed;
	 QueryPerformanceCounter(&ed);
	 ct.QuadPart+=(ed.QuadPart-st.QuadPart);
	 st.QuadPart=ed.QuadPart;
#else
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec+=ed.tv_sec-st.tv_sec;
	ct.tv_nsec+=ed.tv_nsec-st.tv_nsec;
	st.tv_sec=ed.tv_sec;st.tv_nsec=ed.tv_nsec;
#endif
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
#ifdef WIN32
		return (double)ct.QuadPart/fcpu.QuadPart;
#else
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
#endif
	}
};
#endif

#ifndef dbg_printf
#define dbg_printf printf
#endif


#define FULL_PACK_OBJ  65536
#define NULL_BUFF_LEN  FULL_PACK_OBJ/8	



#endif
