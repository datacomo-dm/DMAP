#ifndef DT_GLOBAL
#define DT_GLOBAL


#ifndef boolxx_defined
#define boolxx_defined
#if (defined(MYSQL_SERVER) && defined(WIN32)) 
#define bool BOOL
#endif
#endif

#ifndef mbyte
typedef unsigned char mbyte;
#endif

#define MAX_STMT_LEN 20001

#ifndef LONG64
#ifndef WIN32
 typedef long long LONG64;
#else
  typedef __int64 LONG64;
#endif
#endif

#ifndef LLNY
#ifndef WIN32
 #define LLNY  10000000000ll
 #define LLHMS	100000000ll
#else
  #define LLNY 10000000000l
  #define LLHMS	100000000l
 #endif
#endif

#define MAX_ROWS_LIMIT  2l*1024*1024*1024            // 最大记录条数2G的限制

#ifndef LL
 #define LL(A) (LONG64)(A)
#endif
// V as *double 
#ifndef WORDS_BIGENDIAN
 // for system like x86
 #define revDouble(V) 
 #define rev8B revDouble
 #define revInt(V)   
 #define revShort(V)
#else
 #define revDouble(V)   { char def_temp[8];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
                          ((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(double)); }
//			  *((double *)(V)) = def_temp; }
 #define rev8B revDouble

 #define revShort(V)   { short def_temp;\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[0];\
                          *((short *)(V))=def_temp;}
 
 #define revInt(V)   { int def_temp;\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[0];\
			  *(int *)(V) = def_temp; }
#endif

#endif


