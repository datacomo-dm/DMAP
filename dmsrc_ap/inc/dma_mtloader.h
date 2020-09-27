#ifndef DMA_MTLOADER
#define DMA_MTLOADER

#include "mt_worker.h"
#include <time.h>
 #ifndef mSleep
 #define mSleep(msec) \
 { \
   struct timespec req,rem;\
   req.tv_sec=msec/1000;\
   req.tv_nsec=msec%1000*1000*1000;\
   nanosleep(&req, &rem);\
 }
 #endif
 
class dma_mtloader: public worker
{
public :
	dma_mtloader():worker() {};
	virtual ~dma_mtloader() {};
	int work();
};

#endif