#include "IBCompress_test_assist.h"
#include <cr_class/cr_class.h>
#include "msgIBRiak.pb.h"

std::string _str_tmp;
std::string _str_tmp_zlib;

//-------------------------------------------------------------
// 完整非空包压缩测试
// packs 压缩测试
int ibcprs_full_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len);
// packs 解压缩测试
int ibdcprs_full_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len);
// packs 压缩解压缩测试
int ib_full_packs_test(const int max_item_len);

//-------------------------------------------------------------
// 部分非空包压缩测试
// packs 压缩测试
int ibcprs_nulls_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len);
// packs 解压缩测试
int ibdcprs_nulls_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len);
// packs 压缩解压缩测试
int ib_nulls_packs_test(const int max_item_len);

//-------------------------------------------------------------
// 全空完整包压缩解压缩测试
// packs 压缩测试
int ibcprs_empty_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len);
// packs 解压缩测试
int ibdcprs_empty_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len);
// packs 压缩解压缩测试
int ib_empty_packs_test(const int max_item_len);

//-------------------------------------------------------------
// 部分非空包压缩测试
// packs 压缩测试
int ibcprs_some_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len);
// packs 解压缩测试
int ibdcprs_some_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len);
// packs 压缩解压缩测试
int ib_some_packs_test(const int max_item_len);


// 日志输出
int ib_displsy_packs_data(char* origin_data,const uint origin_data_len,const uint* lens,const int no_obj,int displsy_num,uint * nulls,int no_nulls,char* title);


int main(int argv,char* args[]){
	int max_item_len = 0;
	
	//test full pack
	printf("\n---------------------------------------------------\n");
	max_item_len = 11;
	ib_full_packs_test(max_item_len);
	
	max_item_len = 50;
	ib_full_packs_test(max_item_len);
	
	max_item_len = 500;
	ib_full_packs_test(max_item_len);


	//test nulls pack
	printf("\n---------------------------------------------------\n");
	max_item_len = 11;
	ib_nulls_packs_test(max_item_len);
	
	max_item_len = 50;
	ib_nulls_packs_test(max_item_len);
	
	max_item_len = 500;
	ib_nulls_packs_test(max_item_len);
		
	//test empty pack
	printf("\n---------------------------------------------------\n");
	max_item_len = 11;
	ib_empty_packs_test(max_item_len);
	
	max_item_len = 50;
	ib_empty_packs_test(max_item_len);
	
	max_item_len = 500;
	ib_empty_packs_test(max_item_len);
	

	//test some pack
	printf("\n---------------------------------------------------\n");
	max_item_len = 11;
	ib_some_packs_test(max_item_len);
	
	max_item_len = 50;
	ib_some_packs_test(max_item_len);
	
	max_item_len = 500;
	ib_some_packs_test(max_item_len);
	
	return 0;
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 输出信息
int ib_displsy_packs_data(char* origin_data,const uint origin_data_len,const uint* lens,const int no_obj,int displsy_num,uint * nulls,int no_nulls,char* title)
{
	if(no_obj < displsy_num){
		dbg_printf("ib_displsy_data input error\n");	
		assert(0);
		return -1;
	}
	dbg_printf("-------------------------------------------------------------------------------\n");
	dbg_printf("-------%s--------\n",title);
	
	uint max_len = 0;
	uint sum_len = 0;
	for(int i=0;i<no_obj;i++)
	{
		if(lens[i] > max_len){
			max_len = lens[i];
		}		
		sum_len += lens[i];
	}
	
	if(sum_len > origin_data_len)
	{
		dbg_printf("ib_displsy_data input error\n");	
		assert(0);
		return -1;	
	}
	
	char *temp_val = NULL;
	temp_val = (char*)malloc(max_len+2048);
	uint start_pos = 0;

	for(int i=0;i<displsy_num;i++)
	{
		if(ib_cprs_assist::IsNull(nulls,no_nulls,no_obj,i)){
			dbg_printf("NULL  ");
		}
		else{
			memcpy(temp_val,origin_data+start_pos,lens[i]);
			temp_val[lens[i]] = 0;
			dbg_printf("%s  ",temp_val);
		}
		if((i+1)%9 == 0){
			dbg_printf("\n");	
		}
		
		start_pos += lens[i];
	}
	dbg_printf("\n-------------------------------------------------------------------------------\n\n");
	
	if(temp_val != NULL)
	{
		free(temp_val);
		temp_val = NULL;	
	}
	
	return 0;
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int ibcprs_full_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len)
{
	void * data = NULL;
	uint data_full_byte_size = 0;
	int no_obj = FULL_PACK_OBJ;
	uint lens[FULL_PACK_OBJ];
	uint16_t lens_s[FULL_PACK_OBJ];
	msgRCAttr_packS msgPack;
	std::string strPack;
	double in_time, proc_time;

	memset(lens,0,FULL_PACK_OBJ*sizeof(uint));	

	data =(uchar*)malloc(FULL_PACK_OBJ*max_item_len+1);
	char _tmp_val[max_item_len];
	char _tmp_format[100];
	uint _tmp_len = 0;
	for(int i=0;i<FULL_PACK_OBJ;i++)
	{
		sprintf(_tmp_val,"138%08d", i%1000 + 123000);
		_tmp_len = strlen(_tmp_val);
		// 设置值
		memcpy((char*)data+data_full_byte_size,_tmp_val,_tmp_len);
		lens[i] = _tmp_len;
		lens_s[i] = _tmp_len;
		data_full_byte_size+= _tmp_len;
	}

	dbg_printf("ib_packs_compress begin: src_data_len = %d .\n",data_full_byte_size);

	msgPack.set_no_obj(no_obj);
	msgPack.set_no_nulls(no_nulls);
	msgPack.set_optimal_mode(0);
	msgPack.set_decomposer_id(0);
	msgPack.set_no_groups(0);
	msgPack.set_data_full_byte_size(data_full_byte_size);
	msgPack.set_ver(0);

	msgPack.set_lens((char*)lens_s, sizeof(lens_s));
	msgPack.set_data((char*)data, data_full_byte_size);

	msgPack.SerializeToString(&strPack);

	in_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

	CR_Class_NS::compressx(strPack, _str_tmp);

	proc_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - in_time;

	DPRINTF("lzma compress size=%llu, used second:%f\n", _str_tmp.size(), proc_time);

	in_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

	CR_Class_NS::compressx_zlib(strPack, _str_tmp_zlib);

	proc_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - in_time;

	DPRINTF("zlib compress size=%llu, used second:%f\n", _str_tmp_zlib.size(), proc_time);

	in_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

	assert(compressed_buf_len>=data_full_byte_size);

	// 输出日志
//	ib_displsy_packs_data((char*)data,data_full_byte_size,lens,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"before compress data,the data is following ");

	int ret = 0;

	//ib_packs_compress(const void*, uint, int, const uint*, const uint*, int, uchar*, int&, uchar&)
	ret = ib_packs_compress((void*)data,data_full_byte_size,no_obj,lens,nulls,(int)no_nulls,compressed_buf,(int&)compressed_buf_len,optimal_mode);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_compress error: %s\n",msg);
		goto finish;
	}

	proc_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - in_time;

	DPRINTF("ib compress size=%u, used second:%f\n", compressed_buf_len, proc_time);

	dbg_printf("ib_packs_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data);
	data = NULL;

	return ret;
}

int ibdcprs_full_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len)
{
	void * data = NULL;
	int no_obj = FULL_PACK_OBJ;
	double in_time, proc_time;
	msgRCAttr_packS msgPack;
	std::string strPack;

	in_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

	CR_Class_NS::decompressx(_str_tmp, strPack);

	if (!msgPack.ParseFromString(strPack)) {
		DPRINTF("lzma decompress, parse failed!\n");
	}

	proc_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - in_time;

	DPRINTF("lzma decompress size=%llu, used second:%f\n", strPack.size(), proc_time);

	in_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC);

	CR_Class_NS::decompressx(_str_tmp_zlib, strPack);

	if (!msgPack.ParseFromString(strPack)) {
		DPRINTF("zlib decompress, parse failed!\n");
	}

	proc_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - in_time;

	DPRINTF("zlib decompress size=%llu, used second:%f\n", strPack.size(), proc_time);

	uint data_len = 0;
	data_len = FULL_PACK_OBJ*max_item_len;
	data = (uchar*)malloc(data_len);
	uint lens[FULL_PACK_OBJ];
	memset(lens,0x0,FULL_PACK_OBJ*sizeof(uint));
		
	int ret = 0;
	
	ret = ib_packs_decompress(compressed_buf,compressed_buf_len,no_obj,optimal_mode,data,data_len,lens,nulls,no_nulls);

	proc_time = CR_Class_NS::clock_gettime(CLOCK_MONOTONIC) - in_time;

	DPRINTF("ib decompress used second:%f\n", proc_time);

	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_decompress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
//	ib_displsy_packs_data((char*)data,data_len,lens,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packs_decompress success!\n");
	
finish:
	free(data);
	data = NULL;

	return ret;
}

int ib_full_packs_test(const int max_item_len)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;
		
	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);
		
	int ret = 0;
	compressed_buf_len = max_item_len * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	
	ret = ibcprs_full_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0){
		goto finish;
	}

	ret =  ibdcprs_full_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int ibcprs_nulls_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len)
{
	void * data = NULL;
	uint data_full_byte_size = 0;
	int no_obj = FULL_PACK_OBJ;
	
	uint lens[FULL_PACK_OBJ];
	memset(lens,0,FULL_PACK_OBJ*sizeof(uint));	

	data =(uchar*)malloc(FULL_PACK_OBJ*max_item_len+1);
	char _tmp_val[max_item_len];
	char _tmp_format[100];
	uint _tmp_len = 0;
	int _mod = 13;
	for(int i=0;i<FULL_PACK_OBJ;i++)
	{
		if((i+1)%_mod == 0){
			ib_cprs_assist::SetPacksNull(nulls,no_nulls,i);
			lens[i] = 0;		
		}
		else{
			sprintf(_tmp_format,"152%%0%dd",((i+1)%(max_item_len/2))+2);
			sprintf(_tmp_val,_tmp_format,i%0xffffffff +1);
			_tmp_len = strlen(_tmp_val);
			
			// 设置值
			memcpy((char*)data+data_full_byte_size,_tmp_val,_tmp_len);
			
			lens[i] = _tmp_len;
			data_full_byte_size+= _tmp_len;
		}
	}
		
	assert(compressed_buf_len>=data_full_byte_size);
	
	// 输出日志
	ib_displsy_packs_data((char*)data,data_full_byte_size,lens,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"before compress data,the data is following ");
	
	int ret = 0;
	
	dbg_printf("ib_packs_compress begin: src_data_len = %d .\n",data_full_byte_size);
	//ib_packs_compress(const void*, uint, int, const uint*, const uint*, int, uchar*, int&, uchar&)
	ret = ib_packs_compress((void*)data,data_full_byte_size,no_obj,lens,nulls,(int)no_nulls,compressed_buf,(int&)compressed_buf_len,optimal_mode);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_compress error: %s\n",msg);
		goto finish;
	}
	dbg_printf("ib_packs_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data);
	data = NULL;
	
	return ret;
}

int ibdcprs_nulls_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len)
{
	void * data = NULL;
	int no_obj = FULL_PACK_OBJ;

	uint data_len = 0;
	data_len = FULL_PACK_OBJ*max_item_len;
	data = (uchar*)malloc(data_len);
	uint lens[FULL_PACK_OBJ];
	memset(lens,0x0,FULL_PACK_OBJ*sizeof(uint));
		
	int ret = 0;
	
	ret = ib_packs_decompress(compressed_buf,compressed_buf_len,no_obj,optimal_mode,data,data_len,lens,nulls,no_nulls);
	
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_decompress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
	ib_displsy_packs_data((char*)data,data_len,lens,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packs_decompress success!\n");
	
finish:
	free(data);
	data = NULL;
	return ret;
}

int ib_nulls_packs_test(const int max_item_len)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;
		
	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);
		
	int ret = 0;
	compressed_buf_len = max_item_len * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	
	ret = ibcprs_nulls_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0){
		goto finish;
	}

	ret =  ibdcprs_nulls_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}



///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int ibcprs_empty_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len)
{
	void * data = NULL;
	uint data_full_byte_size = 0;
	int no_obj = FULL_PACK_OBJ;
	
	uint lens[FULL_PACK_OBJ];
	memset(lens,0,FULL_PACK_OBJ*sizeof(uint));	

	data =(uchar*)malloc(FULL_PACK_OBJ*max_item_len+1);
	char _tmp_val[max_item_len];
	char _tmp_format[100];
	uint _tmp_len = 0;
	for(int i=0;i<FULL_PACK_OBJ;i++)
	{
		ib_cprs_assist::SetPacksNull(nulls,no_nulls,i);
		lens[i] = 0;		
	}
		
	assert(compressed_buf_len>=data_full_byte_size);
	
	// 输出日志
	ib_displsy_packs_data((char*)data,data_full_byte_size,lens,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"before compress data,the data is following ");
	
	int ret = 0;
	
	dbg_printf("ib_packs_compress begin: src_data_len = %d .\n",data_full_byte_size);
	//ib_packs_compress(const void*, uint, int, const uint*, const uint*, int, uchar*, int&, uchar&)
	ret = ib_packs_compress((void*)data,data_full_byte_size,no_obj,lens,nulls,(int)no_nulls,compressed_buf,(int&)compressed_buf_len,optimal_mode);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_compress error: %s\n",msg);
		goto finish;
	}
	dbg_printf("ib_packs_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data);
	data = NULL;
	
	return ret;
}

int ibdcprs_empty_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len)
{
	void * data = NULL;
	int no_obj = FULL_PACK_OBJ;

	uint data_len = 0;
	data_len = FULL_PACK_OBJ*max_item_len;
	data = (uchar*)malloc(data_len);
	uint lens[FULL_PACK_OBJ];
	memset(lens,0x0,FULL_PACK_OBJ*sizeof(uint));
		
	int ret = 0;
	
	ret = ib_packs_decompress(compressed_buf,compressed_buf_len,no_obj,optimal_mode,data,data_len,lens,nulls,no_nulls);
	
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_decompress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
	ib_displsy_packs_data((char*)data,data_len,lens,FULL_PACK_OBJ,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packs_decompress success!\n");
	
finish:
	free(data);
	data = NULL;
	return ret;
}

int ib_empty_packs_test(const int max_item_len)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;
		
	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);
		
	int ret = 0;
	compressed_buf_len = max_item_len * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	
	ret = ibcprs_empty_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0){
		goto finish;
	}

	ret =  ibdcprs_empty_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int ibcprs_some_packs_test(uchar *compressed_buf,uint& compressed_buf_len,uint* nulls,int &no_nulls,uchar& optimal_mode,const int max_item_len)
{
	void * data = NULL;
	uint data_full_byte_size = 0;
	int no_obj = FULL_PACK_OBJ/9;
	
	uint lens[FULL_PACK_OBJ/9];
	memset(lens,0,(FULL_PACK_OBJ/9)*sizeof(uint));	

	data =(uchar*)malloc(no_obj*max_item_len+1);
	char _tmp_val[max_item_len];
	char _tmp_format[100];
	uint _tmp_len = 0;
	int _mod = 13;
	for(int i=0;i<no_obj;i++)
	{
		if((i+1)%_mod == 0){
			ib_cprs_assist::SetPacksNull(nulls,no_nulls,i);
			lens[i] = 0;		
		}
		else{
			sprintf(_tmp_format,"152%%0%dd",((i+1)%(max_item_len/2))+2);
			sprintf(_tmp_val,_tmp_format,i%0xffffffff +1);
			_tmp_len = strlen(_tmp_val);
			
			// 设置值
			memcpy((char*)data+data_full_byte_size,_tmp_val,_tmp_len);
			
			lens[i] = _tmp_len;
			data_full_byte_size+= _tmp_len;
		}
	}
		
	assert(compressed_buf_len>=data_full_byte_size);
	
	// 输出日志
	ib_displsy_packs_data((char*)data,data_full_byte_size,lens,no_obj,64,nulls,no_nulls,(char*)"before compress data,the data is following ");
	
	int ret = 0;
	
	dbg_printf("ib_packs_compress begin: src_data_len = %d .\n",data_full_byte_size);
	//ib_packs_compress(const void*, uint, int, const uint*, const uint*, int, uchar*, int&, uchar&)
	ret = ib_packs_compress((void*)data,data_full_byte_size,no_obj,lens,nulls,(int)no_nulls,compressed_buf,(int&)compressed_buf_len,optimal_mode);
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_compress error: %s\n",msg);
		goto finish;
	}
	dbg_printf("ib_packs_compress success,compressed_buff_len = %d .\n",compressed_buf_len);

finish:
	free(data);
	data = NULL;
	
	return ret;
}

int ibdcprs_some_packs_test(const uchar *compressed_buf, const uint  compressed_buf_len,uint* nulls,int &no_nulls,const uchar optimal_mode,const int max_item_len)
{
	void * data = NULL;
	int no_obj = FULL_PACK_OBJ/9;

	uint data_len = 0;
	data_len = no_obj*max_item_len;
	data = (uchar*)malloc(data_len);
	uint lens[FULL_PACK_OBJ/9];
	memset(lens,0x0,no_obj*sizeof(uint));
		
	int ret = 0;
	
	ret = ib_packs_decompress(compressed_buf,compressed_buf_len,no_obj,optimal_mode,data,data_len,lens,nulls,no_nulls);
	
	if(ret !=0){
		char msg[3000];
		ib_cprs_message(ret,msg,3000);
		dbg_printf("ib_packs_decompress error: %s\n",msg);
		goto finish;
	}
	
	// 输出日志
	ib_displsy_packs_data((char*)data,data_len,lens,no_obj,64,nulls,no_nulls,(char*)"after decompress data,the data is following ");
	
	dbg_printf("ib_packs_decompress success!\n");
	
finish:
	free(data);
	data = NULL;
	return ret;
}

int ib_some_packs_test(const int max_item_len)
{	
	uchar *compressed_buf = NULL;
	uint compressed_buf_len = 0;

	uchar optimal_mode = 0xff;
		
	int no_nulls = 0;
	uint nulls[NULL_BUFF_LEN];
	memset(nulls,0,NULL_BUFF_LEN);
		
	int ret = 0;
	compressed_buf_len = max_item_len * FULL_PACK_OBJ + 1024;
	compressed_buf = new uchar[compressed_buf_len];
	
	
	ret = ibcprs_some_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0){
		goto finish;
	}

	ret =  ibdcprs_some_packs_test(compressed_buf,compressed_buf_len,nulls,no_nulls,optimal_mode,max_item_len);
	if(ret != 0)
	{
		goto finish;
	}
finish:
	delete [] compressed_buf;
	compressed_buf = NULL;
	
	return ret;
}

