#ifndef __BLOOM_H__
#define __BLOOM_H__

#include <stdlib.h>

typedef unsigned int (*hashfunc_t)(const char *,unsigned int len);

class bloom {
	size_t asize;
	unsigned char *data;
	size_t nfuncs;
	hashfunc_t *funcs;
public:
	bloom(size_t size, size_t nfuncs, ...);
	
  //BLOOM *bloom_create(size_t size, size_t nfuncs, ...);
  ~bloom();
  int add(const char *s ,unsigned int len);
  int check(const char *s ,unsigned int len);
  int write(const char *filename);
};

#endif

