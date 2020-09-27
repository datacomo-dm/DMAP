#include "wdbi.h"
#ifdef __unix
extern struct timespec interval = {0, 20000};
#endif
 
extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];

extern OCIEnv *envhp;
extern OCIError *errhp;

extern char errfile[MAX_PATH]="\x0";
extern char lgfile[MAX_PATH]="\x0";
extern bool __output_to_console=false;



