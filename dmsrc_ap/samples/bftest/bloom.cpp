#include<limits.h>
#include<stdarg.h>
#include <stdio.h>
#include "bloom.h"
#include "hash.h"
#include <string.h>

#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define GETBIT(a, n) (a[n/CHAR_BIT] & (1<<(n%CHAR_BIT)))


bloom::bloom(size_t size, size_t _nfuncs, ...)
{
	
	va_list l;
	int n;
	funcs = NULL;
	data = NULL;
	if(!(data=(unsigned char *)calloc((size+CHAR_BIT-1)/CHAR_BIT, sizeof(char)))) {
		throw "alloc memory failed!";
	}
	if(!(funcs=(hashfunc_t*)malloc(_nfuncs*sizeof(hashfunc_t)))) {
		free(data);
		throw "alloc memory failed!";
	}

	va_start(l, _nfuncs);
	for(n=0; n<_nfuncs; ++n) {
		funcs[n]=va_arg(l, hashfunc_t);
	}
	va_end(l);

    
	nfuncs=_nfuncs;
	asize=size;
}

bloom::~bloom()
{
  if(data != NULL)
  {
     free(data);
     data = NULL;
  }
  
  if(funcs != NULL)
  { 
  	 free(funcs);
  	 funcs = NULL;
  }
}

int bloom::write(const char *filename) 
{
	FILE *fp=fopen(filename,"w+b");
	fwrite(data,(asize+CHAR_BIT-1)/CHAR_BIT,1,fp);
	fclose(fp);
}
int bloom::add(const char *s,unsigned int len)
{
	size_t n;

	for(n=0; n<nfuncs; ++n) {
		unsigned int hv=funcs[n](s,len)%asize;
		SETBIT(data, hv);
	}
	return 0;
}

// 0 for not found
// 1 for found
int bloom::check(const char *s,unsigned int len)
{
	size_t n;

	for(n=0; n<nfuncs; ++n) {
		unsigned int hv=funcs[n](s,len)%asize;
		if(!(GETBIT(data, hv))) return 0;
	}

	return 1;
}

