#ifndef DMA_MTLOADER
#define DMA_MTLOADER

#include "core/mt_worker.h"

 
class dma_mtloader: public worker
{
public :
	dma_mtloader():worker() {};
	virtual ~dma_mtloader() {};
	int work();
};

#endif

