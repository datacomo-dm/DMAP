#include <stdio.h>
#include <time.h>
typedef long long int _int64;

#include "apindex.h"

DMMutex apindex::fork_lock;

#ifndef MYTIMER_CLASS
#define MYTIMER_CLASS
#ifndef TIMEOFDAY
#define TIMEOFDAY CLOCK_REALTIME
#endif
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
	double GetCurTime() {
		Stop();
		return GetTime();
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


apindex::DBPool apindex::ap_pool;
//leveldb::Options DBEntry::options_n;
//leveldb::Options DBEntry::options_b;
//bool DBEntry::isinit=false;
// copy index data from a bloomfilter enabled db to a bloomfilter disabled db
void testCopyNBF(const char *srcpath,const char *srcdb,const char *dstpath,const char *dstdb) {
	printf("Test--copy bf db to normal db:%s.%s->%s.%s.\n",srcpath,srcdb,dstpath,dstdb);
	apindex pi(srcdb,srcpath,true);
	apindex po(dstdb,dstpath,false);
	po.ClearDB();
	po.SetAppendMode(true);
	printf("Copy index...\n");
	po.Import(pi);
	printf("suc.\n");
}

// copy index data from a bloomfilter enabled db to a bloomfilter disabled db
void testCopyBF(const char *srcpath,const char *srcdb,const char *dstpath,const char *dstdb) {
	printf("Test--copy bf db to bf db:%s.%s->%s.%s.\n",srcpath,srcdb,dstpath,dstdb);
    apindex pi(srcdb,srcpath,true);
	apindex po(dstdb,dstpath,true);
	po.ClearDB();
	po.SetAppendMode(true);
	printf("Copy index...\n");
	po.Import(pi);
	printf("suc.\n");
}

// merge index1,index2 to index
void testDoMergeIndex(const char *srcpath1,const char *srcdb1,
		const char *srcpath2,const char *srcdb2,
        const char *dstpath,const char *dstdb)
{
    printf("test-- merge leveldb index1[%s/%s],index2[%s/%s] to index[%s/%s].\n",
         srcpath1,srcdb1,srcpath2,srcdb2,dstpath,dstdb);
         
	apindex src_index1(srcdb1,srcpath1,true); 
	apindex src_index2(srcdb2,srcpath2,true);
	apindex mergeidx(dstdb,dstpath);
	//mergeidx.Merge(src_index1,src_index2);
}

void testImportDB(const char *srcdb,const char *dstdb) {
	printf("test--import data from db '%s' to db '%s'.\n",dstdb,srcdb);
	mytimer tm;
	tm.Start();
	apindex src(srcdb,".",true); 
	apindex dst(dstdb,".",true);
	src.Import(dst);
	tm.Stop();
	printf("time:%.4f.\n",tm.GetTime());
}

void testMerge() {
	apindex src_db("msrcdb","/tmp",true); 
	apindex new_db("mnewdb","/tmp",true); 
	src_db.ClearDB();
	new_db.ClearDB();
	src_db.Put("13011111234",1);
	src_db.Put("13011111235",2);
	new_db.Put("13011111234",3);
	new_db.Put("13011111235",4);
	src_db.BatchWrite();
	new_db.BatchWrite();
	
	apindex mergedb("mergedb","/tmp",true);
	mergedb.ClearDB();
	//mergedb.Merge(src_db,new_db);
	std::vector<int> packs;
	mergedb.Get("13011111234",packs);
	if(packs.size()==2 && packs[0]==1 && packs[1]==3)
		printf("Test Merge OK!\n");
	else
		printf("Test Merge failed!\n");
}

void testCompact(const char *srcpath,const char *srcdb) {
	printf("Test--compact db %s.%s.\n",srcpath,srcdb);
	apindex pi(srcdb,srcpath,true);
	pi.Compact();
}

void testDBStat(const char *srcpath,const char *srcdb) {
	printf("Test--index db info: %s.%s\n",srcpath,srcdb);
	apindex pi(srcdb,srcpath,true);
	int rowsnum=0,itemsnum=0;
	printf("Scanning....\n");
	pi.ScanDB(rowsnum,itemsnum);
	printf("Keys:%d,Items:%d.\n",rowsnum,itemsnum);
}

void testDBQuery(const char *srcpath,const char *srcdb,const char *key) {
	printf("Test query--index db info: %s.%s\n",srcpath,srcdb);
	apindex pi(srcdb,srcpath,true);
	std::vector<int> packs;
	pi.GetRange(key,key,packs);
	printf("Key '%s' got %d packs.\n",key,packs.size());
        for(int i=0;i<packs.size();i++) {
         printf("%02d,  ",packs[i]);
         if((i+1)%15 == 0) printf("\n");
	}
	printf("\n");
}

void testDBReplace(const char *srcdb,const char *dstpath,const char *dstdb) {
	printf("Test replace--%s.%s replaced by %s.%s\n",dstpath,dstdb,dstpath,srcdb);
	//apindex pi(srcdb,srcpath,true);
	apindex po(dstdb,dstpath,true);
	po.ReplaceDB(srcdb);
}

// export leveldb (key type: varchar)
void testDBExport(const char *srcpath,const char *srcdb,const char *exp_path,int keytype) {
        printf("Test db export --index db info: %s.%s\n",srcpath,srcdb);
        apindex pi(srcdb,srcpath,true);
 	pi.DumpDB(exp_path,keytype);
}
void dumpMallInfo(const char *msg) {
 struct mallinfo m = mallinfo();
 printf("%s \n    alloc: = %d\n     free: = %d\n", msg,m.uordblks, m.fordblks);
}

void testMLeak() {
	mytimer tm;
	dumpMallInfo("Before extra open db ");
	apindex pi0("16_p1",".",true);
	dumpMallInfo("Before open db ");
	{
	apindex pi("16_p1",".",true);
	dumpMallInfo("Before open db_w "); 
	{
	apindex piw("16_p1_w",".",true);
	dumpMallInfo("Before LoadMap ");
	tm.Restart();
	piw.LoadMap();
	tm.Stop();
	printf("LoadMap takes %.4f sec.\n",tm.GetTime());
	dumpMallInfo("Before MergetoMap ");
	tm.Restart();
	piw.MergeSrcDbtoMap(pi);
	tm.Stop();
	printf("MergeToMap takes %.4f sec.\n",tm.GetTime());
	dumpMallInfo("Before ImportHash ");
	tm.Restart();
	pi.ImportHash(piw);
	tm.Stop();
	printf("ImportHash takes %.4f sec.\n",tm.GetTime());
	//dumpMallInfo("Before ClearMap ");
	//piw.ClearMap();
	dumpMallInfo("Before piw destory ");
  }
	dumpMallInfo("Before ReloadDB ");
	tm.Restart();
	pi.ReloadDB();
	tm.Stop();
	printf("Reload takes %.4f sec.\n",tm.GetTime());
	dumpMallInfo("Before ReloadDB again");
	tm.Restart();
	pi.ReloadDB();
	tm.Stop();
	printf("Reload again takes %.4f sec.\n",tm.GetTime());
	dumpMallInfo("Before pi destory ");
  }
	dumpMallInfo("destory all");
}


void testMergeMap(){	
	mytimer tm;
	/*
	printf("test merge hash map operation \n");
	{
		apindex pi("appdb_1",".",true);
		pi.ClearDB();	
		char str[300];
		for(int i=0;i<(2<<20)-1;i++) {
			sprintf(str,"%010ld",i);
			pi.Put(str,(i>>16)+2);
		
			if(!pi.Get(str,(i>>16)+2)){
			   assert(0);	
			}
		}
		tm.Start();
		pi.WriteMap();
		tm.Stop();
    }*/
	
	/*tm.Start();
	pi.LoadMap();
	tm.Stop();
	pi.BatchWrite();
	pi.LoadMap();
	printf("Load map %ld times,time:%.4f.\n",2l<<20,tm.GetTime());
	apindex pim("appdb_2",".",true);
	pim.ClearDB();
	tm.Start();
	pim.ImportHash(pi);
	tm.Stop();
	printf("Import map %ld times,time:%.4f.\n",2l<<20,tm.GetTime());
	
	
	//---------------------------
	apindex pi3("appdb_1",".",true);
	pi3.LoadMap();	
	
	apindex pi4("1_20130110","/tmp",true);
	tm.Restart();
    pi3.MergeSrcDbtoMap(pi4);
    tm.Stop();
    */

	// MergeFromHashMap test    
    {
    	// create db_1 and db_1 map info
    	dumpMallInfo("Before put map");
    	{
		    apindex pi("appdb_1",".",true);
			pi.ClearDB();	
			char str[300];
			for(int i=0;i<(2<<20)-1;i++) {
				sprintf(str,"%010ld",i);
				pi.Put(str,(i>>16)+2);
				
				if(!pi.Get(str,(i>>16)+2)){
				   assert(0);	
				}
			}
			tm.Restart();
			pi.WriteMap();
			tm.Stop();
			printf("write map use time:%.4f.\n",tm.GetTime());
		}
    	dumpMallInfo("After put map && release map object");
		apindex *pi_1x=new apindex("appdb_1",".",true);
		apindex &pi_1=*pi_1x;//("appdb_1",".",true);
		tm.Restart();
		pi_1.LoadMap();
		tm.Stop();
		printf("LoadMerge use time:%.4f.\n",tm.GetTime());
    	dumpMallInfo("After LoadMap");
		
		
		// create db src
		apindex *pi_3x=new apindex("appdb_3",".",true);
		apindex &pi_3=*pi_3x;//("appdb_3",".",true);
		pi_3.ClearDB();	
		char str[300];
		for(int i=0;i<(2<<20)-1;i++) {
			sprintf(str,"%010ld",i);
			pi_3.Put(str,(i>>16)+2);
				
			if(!pi_3.Get(str,(i>>16)+2)){
			   assert(0);	
			}
		}
		pi_3.BatchWrite();
		dumpMallInfo("After build db3");
		pi_1.MergeSrcDbtoMap(pi_3);
		dumpMallInfo("After Mergesrc");
		pi_3.ImportHash(pi_1);
    	dumpMallInfo("After Import hash");
        pi_1.ClearMap();
    	dumpMallInfo("After Clear map");
    	delete pi_1x;
        dumpMallInfo("After destory map object");
    	
        pi_3.ClearMap();
    	dumpMallInfo("After Clear pi3 map");   	
    	
    	pi_3.ReloadDB();
    	dumpMallInfo("After reload pi3 object");
    	
    	delete pi_3x;
    	dumpMallInfo("After destory pi3 db object");
    	pi_3x=new apindex("appdb_3",".",true);
    	dumpMallInfo("After reopen pi3 db object");
    	delete pi_3x;
    	dumpMallInfo("After destory pi3 db again object");
		// create merge db from map
		//apindex pi_4("appdb_mrg",".",true);
		//pi_4.ClearDB();
		//tm.Restart();
		//pi_4.MergeFromHashMap(pi_3,pi_1);
		//tm.Stop();
		//printf("MergeFromHashMap use time:%.4f.\n",tm.GetTime());		
	}
}

#if 1
const char* pdb_name="16_P1";
const char* pdb_name_w="16_P1_w";
#else 
const char* pdb_name="1_P1";
const char* pdb_name_w="1_P1_w";
#endif

void testCommitMap()
{
	printf("test commit map operation\n");

	// READ_FILE,READ_LEVELDB,PUT_BATCHWRITE,WRITE_BATCHWRITE
	mytimer tm;
	#if 0
	{
		apindex pi_w(pdb_name_w,"/data/dt2/app/dma/leveldb_test_data",true);
		apindex pi(pdb_name,"/data/dt2/app/dma/leveldb_test_data",true);
		
		printf("Test read time \n");
		tm.Restart();
		//pi_w.CommitMap_test(pi,apindex::READ_FILE);
		tm.Stop();
		printf("Read hash map bin file use time:%.4f.\n",tm.GetTime());
	}	
	
	{
		apindex pi_w(pdb_name_w,"/data/dt2/app/dma/leveldb_test_data",true);
		apindex pi(pdb_name,"/data/dt2/app/dma/leveldb_test_data",true);
	
		printf("Test read leveldb time \n");
		tm.Restart();
		//pi_w.CommitMap_test(pi,apindex::READ_LEVELDB);
		tm.Stop();
		printf("Read hash map bin file and Read leveldb use time:%.4f.\n",tm.GetTime());
	}
	
	{
		apindex pi_w(pdb_name_w,"/data/dt2/app/dma/leveldb_test_data",true);
		apindex pi(pdb_name,"/data/dt2/app/dma/leveldb_test_data",true);
	
		printf("Test Put leveldb write batch time \n");
		tm.Restart();
		//pi_w.CommitMap_test(pi,apindex::PUT_BATCHWRITE);
		tm.Stop();
		printf("Read hash map bin file,Read leveldb and Put leveldb WRITEBATCH use time:%.4f.\n",tm.GetTime());
	}	
	
	{
		tm.Restart();
		apindex pi_w(pdb_name_w,"/data/dt2/app/dma/leveldb_test_data",true);
		apindex pi(pdb_name,"/data/dt2/app/dma/leveldb_test_data",true);
		tm.Stop();
		printf("Open and load db use time:%.4f.\n",tm.GetTime());
	
		printf("CommitMap all time \n");
		tm.Restart();
		pi_w.CommitMap_test(pi,apindex::WRITE_BATCHWRITE);
		tm.Stop();
		printf("CommitMap all use time:%.4f.\n",tm.GetTime());
	}
	
	#endif
	
	#if 0
	{
		tm.Restart();
		apindex pi_w(pdb_name_w,"/data/dt2/app/dma/leveldb_test_data",true);
		apindex pi(pdb_name,"/data/dt2/app/dma/leveldb_test_data",true);
		apindex pi_new("new_db","/data/dt2/app/dma/leveldb_test_data",true);
		tm.Stop();
		printf("Open and load db use time:%.4f.\n",tm.GetTime());
		
		tm.Restart();
		pi_w.LoadMap();
		tm.Stop();
		printf("Load db to map use time:%.4f.\n",tm.GetTime());
		
		printf("CommitMap all time \n");
		tm.Restart();
		pi_w.CommitMap_test_2(pi,pi_new);
		tm.Stop();
		printf("CommitMap all use time:%.4f.\n",tm.GetTime());
	}
	#endif
	
	
	for(int i=0;i<10;i++)
	{
		printf("---------- test times [%d] -----------\n",i+1);
		char cmd[300];
		
		strcpy(cmd,"rm /home/dm6/dmsrc/samples/leveldb/16_p1 -r");
		printf("run cmd:%s \n",cmd);
		system(cmd);
		
		// copy
		sprintf(cmd,"cp %s %s -r","/home/dm6/dmsrc/samples/leveldb/16_p1_bak","/home/dm6/dmsrc/samples/leveldb/16_p1");
		printf("run cmd:%s \n",cmd);
		system(cmd);		
		
		{
			std::string dbname=std::string("/home/dm6/dmsrc/samples/leveldb")+ "/" + std::string("16_p1");
			std::string tmpreaddb=dbname+"_fork";
			
			// forkdb	
			printf("fork db ..\n");
			apindex::ForkDB(dbname.c_str(),tmpreaddb.c_str());			
			apindex sindex(tmpreaddb,"");
			sindex.ReloadDB();
					
			// _w DB	
			printf("generate _w db ..\n");
			apindex pi_w("16_p1_w","/home/dm6/dmsrc/samples/leveldb",true);
			pi_w.ClearDB();	
			char str[300];
			for(int i=0;i<(2<<20)-1;i++) {
				sprintf(str,"%010ld",i);
				pi_w.Put(str,(i>>16)+2);
			}
						
			// merge_db
			char mergedbname[300];
			apindex::GetMergeIndexPath(dbname.c_str(),mergedbname);
			apindex mergeidx(mergedbname,"");	
			
			// merge to mrg_db
			printf("MergeFromHashMap db .. \n");
			mergeidx.MergeFromHashMap(sindex,pi_w);
			mergeidx.ReloadDB();					
			sindex.ClearDB(false);
			
		}
	}
}

void testAppend() {
	mytimer tm;
	tm.Start();
	printf("Test append index to empty db.\n");
	apindex pi("appdb",".",true);
	pi.ClearDB();
	printf("Simple hashmap append...\n");
	std::unordered_map<int,int> smap;
	std::unordered_map<int,int>::iterator iter;
	for(int i=0;i<1<<20;i++) {
		iter=smap.find(i);
		smap[i]=i>>16;
	}
	tm.Stop();
	printf("%ld times,time:%.4f.\n",2l<<20,tm.GetTime());
	tm.Restart();
	
	//---------------------------------------------------------
	printf("test unordered_map<leveldb::Slice,string> append...\n");
	#define STRSIZE 32
	char *pkeybuf = new char[(2l<<20)*STRSIZE];
	char *pkey=pkeybuf;
	for(int i=0;i<1<<20;i++){
		sprintf(pkey+STRSIZE,"%010d",i);
		pkey+=STRSIZE;
	}
	tm.Restart();
	std::unordered_map<leveldb::Slice,std::string> char_smap;
	std::unordered_map<leveldb::Slice,std::string>::iterator char_iter;
	pkey=pkeybuf;
	for(int i=0;i<1<<20;i++) {
		leveldb::Slice slicekey(pkey,strlen(pkey));
		std::string value;	
		int npack = i>>16;
		value.append((const char*)&npack,4);
		char_smap[slicekey] = value;
		
		//char_iter = char_smap.find(slicekey);
		//if(char_iter == char_smap.end()){
		//	printf("find error£¡");
		//	break;
		//}
		pkey+=STRSIZE;
	}
	tm.Stop();
	printf("%ld times,time:%.4f.\n",2l<<20,tm.GetTime());
	
	delete [] pkeybuf;
	pkeybuf = NULL;
	tm.Restart();
	//---------------------------------------------------------
		
	
	printf("test unordered_map<string,string> append...\n");
	std::unordered_map<std::string,std::string> string_smap;
	std::unordered_map<std::string,std::string>::iterator string_iter;
	for(int i=0;i<1<<20;i++) {
		std::string value,key;
		key.append((const char*)(&i),4);
		int npack = i>>16;
		value.append((const char*)(&npack),4);
		string_iter=string_smap.find(key);
		string_smap[key]=value;
	}
	tm.Stop();
	printf("%ld times,time:%.4f.\n",2l<<20,tm.GetTime());
	tm.Restart();
	
	char str[100];
	printf("String convert...\n");
	for(int i=0;i<1<<20;i++) {
		sprintf(str,"%10ld",i);
	}
	tm.Stop();
	printf("time:%.4f.\n",tm.GetTime());
	tm.Restart();
	printf("Append 2M differ record...\n");
	pi.Put("xxx123",1);
	pi.Put("xxx123",3);
	assert(pi.Get("xxx123",1) && pi.Get("xxx123",3));
	for(int i=0;i<(1<<20)-1;i++) {
		sprintf(str,"%10ld",i);
		pi.Put(str,(i>>16)+2);
	
		if(!pi.Get(str,(i>>16)+2)){
		   assert(0);	
		}
	}
	double tm1=tm.GetCurTime();
	pi.BatchWrite();
	tm.Stop();
	printf("time:%.4f(fill hash:%.4f).\n",tm.GetTime(),tm1);
	
	//-------------------------------------------------------------------
	// ²âÊÔºÏ²¢²Ù×÷
	//-------------------------------------------------------------------
	apindex pim("appdbm",".",true);
	pim.ClearDB();
	tm.Restart();
	printf("Import db ...\n");
	for(int i=0;i<(1<<20)-1;i++) {
		sprintf(str,"%10ld",i);
		//str[0]='1';
		pim.Put(str,(i>>16));
	}
	double tm2=tm.GetCurTime();
	pim.BatchWrite();
	double tm3=tm.GetCurTime();
	pim.Import(pi);
	double tm4=tm.GetCurTime();
	pim.BatchWrite();
	double tm5=tm.GetCurTime();
	tm.Stop();
	printf("time:%.4f(fill hash:%.4f,write1:%.4f,import:%.4f,write2:%.4f).\n",tm.GetTime(),tm2,tm3-tm2,tm4-tm3,tm5-tm4);
	//-------------------------------------------------------------------
	// Merge
	//-------------------------------------------------------------------
	tm.Restart();
	printf("Merge db ...\n");
	pi.SetAppendMode(true);
	pi.Import(pim);
	tm.Stop();
	printf("time:%.4f.\n",tm.GetTime());
	//-------------------------------------------------------------------
	// Reopen to compact log
	//-------------------------------------------------------------------
	tm.Restart();
	printf("Reopen db ...\n");
	pi.ReloadDB();
	pim.ReloadDB();
	tm.Stop();
	printf("time:%.4f.\n",tm.GetTime());
}

void testCreateDB() {
			printf("create test db [/tmp/pi_test_1]\n");
			apindex pi("pi_test_1","/tmp",true);
			pi.BatchBegin();
			int i = 0;
			
			for(i=0;i<1000;i++){
			   pi.Put(i,i+100);	
			}
			pi.BatchWrite();
			printf("create test db [/tmp/pi_test_2]\n");
			apindex po("pi_test_2","/tmp",true);
			po.ClearDB();
}

int main(int argc,char **argv) {
	if(argc!=2 && argc!=3 && argc != 5 && argc != 4 && argc !=6 ) {
	 printf("Usage pitest <cmd> <path>.\n"
	 				"   cmd: ceb  <srcdb> copy to bloomfilter enabled(/tmp/tpibf).\n"
	 				"        cmp  <srcdb> compact db.\n"
	 				"        stat <srcdb> get stats info.\n"
	 				"        cdb  <srcdb> copy to bloomfilter disabled(/tmp/tpi).\n"
	 				"        rep  <srcdb> repace db by <db>_m.\n"
	 				"        merge <srcpath> <srcdb> <dstpath> <dstdb> merge db1,db2 to db(/tmp/mgr_db).\n"
					"        lst  <srcdb> <keyvalue> list packindex by key.\n"
					"        imp  <srcdb> <dstdb> import data from dstdb to srcdb(based on current directory).\n"
					"        exp  <srcdb> <dstfile> <type: 1,int; 2,long ;3,double ;4,varchar> export leveldb .\n"
					"        testsuite do test suite.\n");
         return -1;
	}
	if(argc ==2 ) {
		if(strcmp(argv[1],"testsuite")==0) {
			//testMerge(); 
			//testCreateDB();
			// testAppend();
			//testMergeMap();
			//testMLeak();
			//dumpMallInfo("Exit from testsuite");
			testCommitMap();
		}
		else
			printf("Invalid command :%s.\n",argv[1]);
  }
	else if(argc == 3){
		if(strcmp(argv[1],"stat")==0)
			testDBStat("",argv[2]);
		else if(strcmp(argv[1],"cdb")==0)
			testCopyNBF("",argv[2],"/tmp","tpi");
		else if(strcmp(argv[1],"ceb")==0)
			testCopyBF("",argv[2],"/tmp","tpibf");
		else if(strcmp(argv[1],"cmp")==0)
			testCompact("",argv[2]);
		else if(strcmp(argv[1],"rep")==0) {
			char srcdb[300];
			sprintf(srcdb,"%s_m",argv[2]);
			testDBReplace(srcdb,"",argv[2]);
		}
		else
			printf("Invalid command :%s.\n",argv[1]);
	}
  else if(argc == 4){
         if(strcmp(argv[1],"lst")==0)
                testDBQuery("",argv[2],argv[3]);
 	     else
         	if(strcmp(argv[1],"imp")==0)
                testImportDB(argv[2],argv[3]);
         else
                printf("Invalid command :%s.\n",argv[1]);
  }
  else if(argc == 5){
             if(strcmp(argv[1],"exp")==0) {
                  int keytype = atoi(argv[4]);
                  if(keytype < 1 || keytype > 4)
                     printf("keytype %d error .",keytype);
                  testDBExport("",argv[2],argv[3],keytype);
             }
             else 
                  printf("Invalid command :%s.\n",argv[1]);
  }
  else 
	{
		if(strcmp(argv[1],"merge")==0){
			testDoMergeIndex(argv[2],argv[3],argv[4],argv[5],"/tmp","mgr_db");		
		}
		else{
			printf("Invalid command :%s.\n",argv[1]);
		}
	}
	return 1;
}
