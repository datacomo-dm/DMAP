#ifndef DMA_MTLOADER
#define DMA_MTLOADER

#include "mt_worker.h"

 
class dma_mtreader: public worker
{
public :
	dma_mtreader():worker() {};
	virtual ~dma_mtreader() {};
	int work();
};

#endif

