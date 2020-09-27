#include "IBCompress_test_assist.h"

//-------------------------------------------------------------
// 完整非空包压缩测试
// packn 压缩测试
int ibcprs_full_packn_test(ValueType value_type,uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 解压缩测试
int ibdcprs_full_packn_test(uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 压缩解压缩测试
int ib_full_packn_test(ValueType value_type);

//-------------------------------------------------------------
// 部分记录为null包压缩测试
// packn 压缩测试
int ibcprs_nulls_packn_test(ValueType value_type,uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 解压缩测试
int ibdcprs_nulls_packn_test(uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 压缩解压缩测试
int ib_nulls_packn_test(ValueType value_type);

//-------------------------------------------------------------
// 全部记录为null包压缩测试
// packn 压缩测试
int ibcprs_empty_packn_test(ValueType value_type,uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 解压缩测试
int ibdcprs_empty_packn_test(uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 压缩解压缩测试
int ib_empty_packn_test(ValueType value_type);


//-------------------------------------------------------------
// 不满64k条记录包压缩测试
// packn 压缩测试
int ibcprs_some_packn_test(ValueType value_type,uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 解压缩测试
int ibdcprs_some_packn_test(uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode);
// packn 压缩解压缩测试
int ib_some_packn_test(ValueType value_type);

// 日志输出
int ib_displsy_packn_data(ValueType value_type,void* origin_data,int no_obj,int displsy_num,uint * nulls,int no_nulls,char* title);

int main(int argv,char* args[]){
	ValueType value_type;
	
	//test full pack
	printf("\n---------------------------------------------------\n");
	value_type=UCHAR;
	ib_full_packn_test(value_type);

	value_type=USHORT;
	ib_full_packn_test(value_type);
	
	value_type=UINT;
	ib_full_packn_test(value_type);
	
	value_type=UINT64;
	ib_full_packn_test(value_type);

	//test some full pack
	printf("\n---------------------------------------------------\n");
	value_type=UCHAR;
	ib_nulls_packn_test(value_type);
	
	value_type=USHORT;
	ib_nulls_packn_test(value_type);
	
	value_type=UINT;
	ib_nulls_packn_test(value_type);
	
	value_type=UINT64;
	ib_nulls_packn_test(value_type);
	
	//test empty(all nulls) pack
	printf("\n---------------------------------------------------\n");
	value_type=UCHAR;
	ib_empty_packn_test(value_type);
	
	value_type=USHORT;
	ib_empty_packn_test(value_type);
	
	value_type=UINT;
	ib_empty_packn_test(value_type);
	
	value_type=UINT64;
	ib_empty_packn_test(value_type);
	

	//test some(no_obj < 64k) pack
	printf("\n---------------------------------------------------\n");
	value_type=UCHAR;
	ib_some_packn_test(value_type);
	
	value_type=USHORT;
	ib_some_packn_test(value_type);
	
	value_type=UINT;
	ib_some_packn_test(value_type);
	
	value_type=UINT64;
	ib_some_packn_test(value_type);
	
	return 0;
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 输出信息
int ib_displsy_packn_data(ValueType value_type,void* origin_data,int no_obj,int displsy_num,uint * nulls,int no_nulls,char* title)
{
	if(no_obj < displsy_num){
		dbg_printf("ib_displsy_data input error\n");	
		return -1;
	}
	dbg_printf("-------------------------------------------------------------------------------\n");
	dbg_printf("-------%s--------\n",title);
	uchar *pc;
	ushort *ps;
	uint *pi;
	_uint64 *pl;
	int index = 0;
	switch(value_type)
	{
		case UCHAR:
			pc = (uchar*)origin_data;
			for(int i=0;i<displsy_num;i++)
			{
				if(ib_cprs_assist::IsNull(nulls,no_nulls,no_obj,i)){
					dbg_printf("NULL  ");
				}
				else{
					dbg_printf("%02x   ",pc[i]);
				}
				if((i+1)%15 == 0){
					dbg_printf("\n");	
				}
			}
			break;			
		case USHORT:
			ps = (ushort*)origin_data;
			for(int i=0;i<displsy_num;i++)
			{
				if(ib_cprs_assist::IsNull(nulls,no_nulls,no_obj,i)){
					dbg_printf("NULL  ");
				}
				else{
					dbg_printf("%04x   ",ps[i]);
				}
				if((i+1)%16 == 0){
					dbg_printf("\n");	
				}
			}
			break;			
		case UINT:
			pi = (uint*)origin_data;
			for(int i=0;i<displsy_num;i++)
			{
				if(ib_cprs_assist::IsNull(nulls,no_nulls,no_obj,i)){
					dbg_printf("NULL  ");
				}
				else{
					dbg_printf("%08x   ",pi[i]);
				}				
				if((i+1)%16 == 0){
					dbg_printf("\n");	
				}
			}
			break;
		case UINT64:
			pl = (_uint64*)origin_data;
			for(int i=0;i<displsy_num;i++)
			{
				if(ib_cprs_assist::IsNull(nulls,no_nulls,no_obj,i)){
					dbg_printf("NULL  ");
				}
				else{
					dbg_printf("%016x   ",pl[i]);
				}
				if((i+1)%8 == 0){
					dbg_printf("\n");	
				}
			}
			break;
		default:
			dbg_printf("ValueType error\n");
			return -1;
	}	
	dbg_printf("\n-------------------------------------------------------------------------------\n\n");
	
	return 0;
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int ibcprs_full_packn_test(ValueType value_type,uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ;
	uchar *pc;
	ushort *ps;
	uint *pi;
	_uint64 *pl;
	switch(value_type)
	{
		case UCHAR:
			data_full =(uchar*)malloc(FULL_PACK_OBJ*value_type+1);
			pc = (uchar*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				*pc = (uchar)(i%0xff)+1;
				if(maxv < *pc ){
					maxv = *pc ;
				}			
				pc++;	
			}
			break;			
		case USHORT:
			data_full = (ushort*)malloc(FULL_PACK_OBJ*value_type+1);	
			ps = (ushort*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				*ps = (i%0xffff)+0xff+1;
				if(maxv <*ps ){
					maxv = *ps;
				}				
				ps++;
			}
			break;			
		case UINT:
			data_full = (uint*)malloc(FULL_PACK_OBJ*value_type+1);
			pi = (uint*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				*pi = (i%0xffffffff)+0xffff+1;
				if(maxv < *pi){
					maxv = *pi ;
				}				
				pi++;
			}
			break;
		case UINT64:
			data_full = (_uint64*)malloc(FULL_PACK_OBJ*value_type+1);
			pl = (_uint64*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				*pl = (_uint64)(i%0xffffffffffffffff)+1;
				*pl += 0xefffffff;
				if(maxv <*pl ){
					maxv = *pl;
				}				
				pl++;
			}
			break;
		default:
			dbg_printf("ValueType error\n");
			return -1;
	}

	assert(compressed_buf_len>=no_obj*value_type);
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"before compress data,the data is following ");
	
	int ret = 0;
	
	dbg_printf("ib_packn_compress begin: src_data_len = %d .\n",no_obj*value_type);
	ret = ib_packn_compress(no_nulls,nulls,data_full,no_obj,compressed_buf,compressed_buf_len,optimal_mode,maxv);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	dbg_printf("ib_packn_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data_full);
	data_full = NULL;
	
	return ret;
}

int ibdcprs_full_packn_test(ValueType value_type,const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ;

	data_full = (uchar*)malloc(FULL_PACK_OBJ*value_type);
		
	int ret = 0;
	ret = ib_packn_decompress(compressed_buf,compressed_buf_len,optimal_mode,nulls,no_nulls,data_full,no_obj);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packn_decompress success!\n");
	
finish:
	free(data_full);
	data_full = NULL;
	return ret;
}

int ib_full_packn_test(ValueType value_type)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;
		
	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);
		
	int ret = 0;
	compressed_buf_len = value_type * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	ret = ibcprs_full_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0){
		goto finish;
	}

	ret = ibdcprs_full_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 部分包为null的测试
int ibcprs_nulls_packn_test(ValueType value_type,uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ;
	uchar *pc;
	ushort *ps;
	uint *pi;
	_uint64 *pl;
	int mod=17;
	switch(value_type)
	{
		case UCHAR:
			data_full =(uchar*)malloc(FULL_PACK_OBJ*value_type+1);
			pc = (uchar*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*pc = (uchar)0;
				}
				else{
					*pc = (uchar)(i%0xff)+1;
					if(maxv < *pc ){
						maxv = *pc ;
					}	
				}		
				pc++;	
			}
			break;			
		case USHORT:
			data_full = (ushort*)malloc(FULL_PACK_OBJ*value_type+1);	
			ps = (ushort*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*ps = (ushort)0;
				}
				else{
					*ps = (ushort)(i%0xffff)+0xff+1;
					if(maxv < *ps ){
						maxv = *ps ;
					}	
				}	
				ps++;
			}
			break;			
		case UINT:
			data_full = (uint*)malloc(FULL_PACK_OBJ*value_type+1);
			pi = (uint*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*pi = (uint)0;
				}
				else{
					*pi = (uint)(i%0xffffffff)+0xffff+1;
					if(maxv < *pi ){
						maxv = *pi;
					}	
				}	
				pi++;
			}
			break;
		case UINT64:
			data_full = (_uint64*)malloc(FULL_PACK_OBJ*value_type+1);
			pl = (_uint64*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*pl = (_uint64)0;
				}
				else{
					*pl = (_uint64)(i%0xffffffffffffffff)+1;
					*pl += 0xefffffff;
					if(maxv < *pl ){
						maxv = *pl ;
					}	
				}		
				pl++;
			}
			break;
		default:
			dbg_printf("ValueType error\n");
			return -1;
	}

	assert(compressed_buf_len>=no_obj*value_type);
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"before compress data,the data is following ");
	
	int ret = 0;
	
	dbg_printf("ib_packn_compress begin: src_data_len = %d .\n",no_obj*value_type);
	ret = ib_packn_compress(no_nulls,nulls,data_full,no_obj,compressed_buf,compressed_buf_len,optimal_mode,maxv);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	dbg_printf("ib_packn_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data_full);
	data_full = NULL;
	
	return ret;
}

int ibdcprs_nulls_packn_test(ValueType value_type,const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ;

	data_full = (uchar*)malloc(FULL_PACK_OBJ*value_type);
		
	int ret = 0;
	ret = ib_packn_decompress(compressed_buf,compressed_buf_len,optimal_mode,nulls,no_nulls,data_full,no_obj);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packn_decompress success!\n");
	
finish:
	free(data_full);
	data_full = NULL;
	return ret;
}

int ib_nulls_packn_test(ValueType value_type)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;

	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);

	int ret = 0;
	compressed_buf_len = value_type * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	ret = ibcprs_nulls_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0){
		goto finish;
	}

	ret = ibdcprs_nulls_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//-------------------------------------------------------------
// 全部记录为null包压缩测试
int ibcprs_empty_packn_test(ValueType value_type,uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ;
	uchar *pc;
	ushort *ps;
	uint *pi;
	_uint64 *pl;
	switch(value_type)
	{
		case UCHAR:
			data_full =(uchar*)malloc(FULL_PACK_OBJ*value_type+1);
			pc = (uchar*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
				*pc = (uchar)0;
				
				pc++;	
			}
			break;			
		case USHORT:
			data_full = (ushort*)malloc(FULL_PACK_OBJ*value_type+1);	
			ps = (ushort*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{				
				ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
				*ps = (ushort)0;
				
				ps++;
			}
			break;			
		case UINT:
			data_full = (uint*)malloc(FULL_PACK_OBJ*value_type+1);
			pi = (uint*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
				*pi = (uint)0;
				
				pi++;
			}
			break;
		case UINT64:
			data_full = (_uint64*)malloc(FULL_PACK_OBJ*value_type+1);
			pl = (_uint64*)data_full;
			for(int i=0;i<FULL_PACK_OBJ;i++)
			{
				ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
				*pl = (_uint64)0;
				
				pl++;
			}
			break;
		default:
			dbg_printf("ValueType error\n");
			return -1;
	}

	assert(compressed_buf_len>=no_obj*value_type);
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"before compress data,the data is following ");
	
	int ret = 0;
	
	dbg_printf("ib_packn_compress begin: src_data_len = %d .\n",no_obj*value_type);
	ret = ib_packn_compress(no_nulls,nulls,data_full,no_obj,compressed_buf,compressed_buf_len,optimal_mode,maxv);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	dbg_printf("ib_packn_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data_full);
	data_full = NULL;
	
	return ret;
}

int ibdcprs_empty_packn_test(ValueType value_type,const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ;

	data_full = (uchar*)malloc(FULL_PACK_OBJ*value_type);
		
	int ret = 0;
	ret = ib_packn_decompress(compressed_buf,compressed_buf_len,optimal_mode,nulls,no_nulls,data_full,no_obj);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packn_decompress success!\n");
	
finish:
	free(data_full);
	data_full = NULL;
	return ret;
}

int ib_empty_packn_test(ValueType value_type)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;

	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);

	int ret = 0;
	compressed_buf_len = value_type * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	ret = ibcprs_empty_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0){
		goto finish;
	}

	ret = ibdcprs_empty_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}



///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//-------------------------------------------------------------
// 不满64K条记录的情况测试
int ibcprs_some_packn_test(ValueType value_type,uchar *compressed_buf, uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ/9;
	uchar *pc;
	ushort *ps;
	uint *pi;
	_uint64 *pl;
	int mod=17;
	switch(value_type)
	{
		case UCHAR:
			data_full =(uchar*)malloc(no_obj*value_type+1);
			pc = (uchar*)data_full;
			for(int i=0;i<no_obj;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*pc = (uchar)0;
				}
				else{
					*pc = (uchar)(i%0xff)+1;
					if(maxv < *pc ){
						maxv = *pc ;
					}	
				}		
				pc++;	
			}
			break;			
		case USHORT:
			data_full = (ushort*)malloc(no_obj*value_type+1);	
			ps = (ushort*)data_full;
			for(int i=0;i<no_obj;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*ps = (ushort)0;
				}
				else{
					*ps = (ushort)(i%0xffff)+0xff+1;
					if(maxv < *ps ){
						maxv = *ps ;
					}	
				}	
				ps++;
			}
			break;			
		case UINT:
			data_full = (uint*)malloc(no_obj*value_type+1);
			pi = (uint*)data_full;
			for(int i=0;i<no_obj;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*pi = (uint)0;
				}
				else{
					*pi = (uint)(i%0xffffffff)+0xffff+1;
					if(maxv < *pi ){
						maxv = *pi;
					}	
				}	
				pi++;
			}
			break;
		case UINT64:
			data_full = (_uint64*)malloc(no_obj*value_type+1);
			pl = (_uint64*)data_full;
			for(int i=0;i<no_obj;i++)
			{
				if((i+1)%mod == 0){ // add null
					ib_cprs_assist::SetPacknNull(nulls,no_nulls,i);
					*pl = (_uint64)0;
				}
				else{
					*pl = (_uint64)(i%0xffffffffffffffff)+1;
					*pl += 0xefffffff;
					if(maxv < *pl ){
						maxv = *pl ;
					}	
				}		
				pl++;
			}
			break;
		default:
			dbg_printf("ValueType error\n");
			return -1;
	}

	assert(compressed_buf_len>=no_obj*value_type);
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,no_obj,64,nulls,no_nulls,(char*)"before compress data,the data is following ");
	
	int ret = 0;
	
	dbg_printf("ib_packn_compress begin: src_data_len = %d .\n",no_obj*value_type);
	ret = ib_packn_compress(no_nulls,nulls,data_full,no_obj,compressed_buf,compressed_buf_len,optimal_mode,maxv);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	dbg_printf("ib_packn_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data_full);
	data_full = NULL;
	
	return ret;
}

int ibdcprs_some_packn_test(ValueType value_type,const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode)
{
	void * data_full = NULL;
	_uint64 maxv = 0;
	int no_obj = FULL_PACK_OBJ/9;

	data_full = (uchar*)malloc(no_obj*value_type);
		
	int ret = 0;
	ret = ib_packn_decompress(compressed_buf,compressed_buf_len,optimal_mode,nulls,no_nulls,data_full,no_obj);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packn_compress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
	ib_displsy_packn_data(value_type,(void*)data_full,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packn_decompress success!\n");
	
finish:
	free(data_full);
	data_full = NULL;
	return ret;
}

int ib_some_packn_test(ValueType value_type)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;

	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);

	int ret = 0;
	compressed_buf_len = value_type * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	ret = ibcprs_some_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0){
		goto finish;
	}

	ret = ibdcprs_some_packn_test(value_type,compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}
