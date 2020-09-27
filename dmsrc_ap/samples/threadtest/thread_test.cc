#include <stdio.h>
#include <stdlib.h>
#include <exception>
#include <string>
#include "DMThreadGroup.h"

class RCException : public std::runtime_error
{
public:
	RCException(std::string const& msg) throw() : std::runtime_error(msg) {}
};

class DatabaseRCException : public RCException
{
public:
	DatabaseRCException(std::string const& msg) throw() : RCException(msg) {}
};

void CommitSaveSessionThread(DMThreadData *pl) {
    void **params=pl->GetParams();
    int *th_index = (int*)params[0];
    printf("enter thread %d....\n",*th_index);
    //throw 1;
    throw DatabaseRCException("CommitSaveSessionThread throw");
}
//--------------------------------------------------------------------------------------------------------------
// compile cmd:
// gcc -std=c++0x -g -o thread_test thread_test.cc DMThreadGroup.cpp  -DNOBOOST_TLIST -lm -lstdc++ -lrt -lpthread
//--------------------------------------------------------------------------------------------------------------
int main(){
    try{        
      	DMThreadGroup ptlist;
        int index[20];
    	for(int i = 0; i < 19; i++){
            void* params[2];
            index[i] = i+1;
            params[0] = &index[i];
    	    ptlist.LockOrCreate()->Start(params,2,CommitSaveSessionThread);
    	}
    	ptlist.WaitAllEnd();
    }catch(DatabaseRCException & e){
        printf("------get exception 01.\n");
    }catch(std::exception & e){
        printf("------get exception 02.\n");
    }catch(...){
        printf("------get exception 03.\n");
    }

    return 0;
}
