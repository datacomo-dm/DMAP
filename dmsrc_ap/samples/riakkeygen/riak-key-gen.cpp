#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include<string>
using namespace std;

#ifndef uint_
typedef unsigned int uint;
#define uint_
#endif

#ifndef uchar_
typedef unsigned char uchar;
#define uchar_
#endif

#ifndef ushort_
typedef unsigned short ushort;
#define ushort_
#endif


#ifndef _uint64
#ifdef __GNUC__
typedef unsigned long long _uint64;
#else
typedef unsigned __int64 _uint64;
#endif
#endif

enum AttrPackType {PackUnknown = 0, PackN, PackT, PackB, PackS};
const static int PF_NULLS_ONLY	= -1; // Special value pack_file=-1 indicates "nulls only".
const static int PF_NO_OBJ		= -2; // Special value pack_file=-2 indicates "no objects".
const static int PF_NOT_KNOWN	= -3; // Special value pack_file=-3 indicates "number not determined yet".
const static int PF_END			= -4; // Special value pack_file=-4 indicates the end of numbers list.
const static int PF_DELETED		= -5; // Special value pack_file=-5 indicates that the datapack has been deleted

enum AttributeType	{	RC_STRING,				// string treated either as dictionary value or "free" text
						RC_VARCHAR,				// as above (discerned for compatibility with SQL)
						RC_INT,					// integer 32-bit
						RC_NUM,					// numerical: decimal, up to DEC(18,18)
						RC_DATE,				// numerical (treated as integer in YYYYMMDD format)
						RC_TIME,				// numerical (treated as integer in HHMMSS format)
						RC_BYTEINT,				// integer 8-bit
						RC_SMALLINT,			// integer 16-bit
						RC_BIN,					// free binary (BLOB), no encoding
						RC_BYTE,				// free binary, fixed size, no encoding
						RC_VARBYTE,				// free binary, variable size, no encoding
						RC_REAL,				// double (stored as non-interpreted _int64, null value is NULL_VALUE_64)
						RC_DATETIME,
						RC_TIMESTAMP,
						RC_DATETIME_N,
						RC_TIMESTAMP_N,
						RC_TIME_N,
						RC_FLOAT,
						RC_YEAR,
						RC_MEDIUMINT,
						RC_BIGINT,
						RC_UNKNOWN = 255
					};

	inline bool IsInteger32Type(AttributeType attr_type)
	{
		return attr_type == RC_INT 	|| attr_type == RC_BYTEINT || attr_type == RC_SMALLINT || attr_type == RC_MEDIUMINT;
	}
	inline bool IsNumericType(AttributeType attr_type)
	{
		return IsInteger32Type(attr_type) || attr_type == RC_BIGINT || attr_type == RC_NUM || attr_type == RC_FLOAT || attr_type == RC_REAL;
	}
	inline bool IsDateTimeType(AttributeType attr_type)
	{
		return attr_type == RC_DATE || attr_type == RC_TIME || attr_type == RC_YEAR || attr_type == RC_DATETIME
				|| attr_type == RC_TIMESTAMP;
	}

/******************************************
 * ABSwitch implementation
 ******************************************/
typedef enum ABSwtich_tag { ABS_A_STATE, ABS_B_STATE }  ABSwitch;

#define INVALID_TRANSACTION_ID   0xFFFFFFFF

#define AB_SWITCH_FILE_NAME   "ab_switch"

class ABSwitcher
{
public:
    ABSwitch GetState(std::string const& path);

    static const char* SwitchName(ABSwitch value );

private:
    void GenerateName(std::string& name, std::string const& path);

};
/******************************************
 * ABSwitcher implementation
 ******************************************/

ABSwitch ABSwitcher::GetState(std::string const&  path)
{
	std::string name;
    GenerateName(name, path);
	struct stat st;
	if(stat(name.c_str(),&st)){ // 成功返回0
		return ABS_A_STATE;
	}	
    return ABS_B_STATE;
}

void ABSwitcher::GenerateName(std::string& name, std::string const& path)
{
	name = path;
	name += "/";
	name += AB_SWITCH_FILE_NAME;
}

/*static*/
const char* ABSwitcher::SwitchName( ABSwitch value )
{
    if (value == ABS_A_STATE) return "ABS_A_STATE";
    return "ABS_B_STATE";
}

// ftype {0:read,1:write}
// path: $DATAMERGER_HOME/var/dbname/tbname.bht
std::string AttrFileName(const std::string path,const int attr_number) 
{
	char fnm[] = { "TA00000.ctb" };

	bool oppositeName =false;
	int ftype = 0;
	
	if(ftype == 1) {
		fnm[1] = 'S'; // save file:   TS000...
	} else {
		ABSwitcher absw;
		ABSwitch cur_ab_switch_state = absw.GetState(path);

		if(oppositeName) {
			if(cur_ab_switch_state == ABS_A_STATE)
				fnm[1] = 'B';
		} else {
			if(cur_ab_switch_state == ABS_B_STATE)
				fnm[1] = 'B';
		}
	}

	fnm[6]=(char)('0'+attr_number%10);
	fnm[5]=(char)('0'+(attr_number/10)%10);
	fnm[4]=(char)('0'+(attr_number/100)%10);
	fnm[3]=(char)('0'+(attr_number/1000)%10);
	fnm[2]=(char)('0'+(attr_number/10000)%10);
	string filename(path);
	filename +="/";
	filename += fnm;
	return filename;
}


// path: $DATAMERGER_HOME/var/dbname/tbname.bht
std::string DpnFileName(const std::string path,const int attr_number) 
{
	char fnm[] = { "TA00000DPN.ctb" };
	fnm[6]=(char)('0'+attr_number%10);
	fnm[5]=(char)('0'+(attr_number/10)%10);
	fnm[4]=(char)('0'+(attr_number/100)%10);
	fnm[3]=(char)('0'+(attr_number/1000)%10);
	fnm[2]=(char)('0'+(attr_number/10000)%10);
	string filename(path);
	filename += "/";
	filename +=fnm;
	return filename;
}


/////////////////////////////////////////////////////////////////////////////////////
// 生成riak-key
std::string _get_riak_key(const int tbno,const int colno,const int packno)
{
	char riak_key_buf[32]; 
	std::string ret; 
	snprintf(riak_key_buf, sizeof(riak_key_buf), "T%08X-A%08X-P%08X", tbno, colno, packno);
	ret = riak_key_buf; 
	return ret;
}

// 获取表id和列数目
int get_table_columns(const char* pbasepth,const char* pdbname,const char* ptbname,int& tabno,int& colno)
{
	char pth[300];
	// ---> /data/dt2/app/dma/var/zkb/test_continu_dump.bht/Table.ctb
	sprintf(pth,"%s/var/%s/%s.bht/Table.ctb",pbasepth,pdbname,ptbname);
	
	FILE  *pFile  = fopen(pth,"rb");
	if(!pFile){
		printf("文件%s打开失败!\n",pth);	
		return -1;
	}
	// get colno
	char buf[500] = {0};
	int fsize = fread(buf,1,33,pFile);
	if (fsize < 33 || strncmp(buf, "RScT", 4) != 0 || buf[4] != '2'){
		printf("Bad format of table definition in %s\n",pth);
		return -1;		
	}
	colno=*((int*)(buf+25));
	
	// get tabno
	fseek(pFile,-4,SEEK_END);
	int _tabid = 0;
	fread(&_tabid,4,1,pFile);
	fclose(pFile);

	tabno = _tabid;
	
	return 0;
}

// 获取pack_no
// TA00000DPN.ctb 大小除37 就可以
int get_packno(const char* pbasepth,const char* pdbname,const char* ptbname,const int attr_number,std::vector<int> & packlist)
{
	char pth[300];
	// ---> /data/dt2/app/dma/var/zkb/test_continu_dump.bht
	sprintf(pth,"%s/var/%s/%s.bht",pbasepth,pdbname,ptbname);

	int no_pack = 0;
	AttributeType type;
	std::string strAttrName = AttrFileName(pth,attr_number);
	FILE  *pFile  = fopen(strAttrName.c_str(),"rb");
	if(!pFile){
		printf("文件%s打开失败!\n",strAttrName.c_str());	
		return -1;
	}
	// get col_number
	char a_buf[500] = {0};
	int fsize = fread(a_buf,1,34,pFile);
	if (fsize < 34){
		printf("Bad format of table definition in %s\n",pth);
		return -1;		
	}
		if ((uchar)a_buf[8] < 128) {
		type = (AttributeType)a_buf[8];
	} else {
		type = (AttributeType)((uchar)a_buf[8]-128);
	}

	// get column pack type 
	AttrPackType packtype;
	if (IsNumericType(type) ||IsDateTimeType(type))
		packtype = PackN;
	else
		packtype = PackS;

	// get pack_no
	no_pack = *((int*)(a_buf + 30));
	fclose(pFile);
	pFile = NULL;

	// 获取包的编号
	std::string strDpnName = DpnFileName(pth,attr_number);
	const int conDPNSize = 37;

	// 最后一个包读取
	// 0:37 bytes :  ABS_A_STATE
	// 37:37 bytes :  ABS_B_STATE
	
	// ABS_A_STATE ---> A
	// ABS_B_STATE ---> B
	
	ABSwitcher absw;
	ABSwitch cur_ab_switch_state = absw.GetState(pth);
	
	int _pack_no = 0; // 当前读取到的pack_no
	char dpn_buf[38];
	pFile = fopen(strDpnName.c_str(),"rb");
	int ret = 0;
	if(no_pack >= 2){
		fseek(pFile,2*conDPNSize,SEEK_SET);
	}
	while(_pack_no < no_pack)
	{
		if(_pack_no == no_pack - 1)
		{ 	
			// 没有switch_ab文件的时候，最后一个包读取17-34字节
			if(cur_ab_switch_state == ABS_A_STATE){
				fseek(pFile,conDPNSize,SEEK_SET);
			}
			else{
				fseek(pFile,0,SEEK_SET);
			}
		}
		ret =fread(dpn_buf,conDPNSize,1,pFile);
		bool is_stored = true;
		if(ret == 1)// 读取成功，判断是否是有效包
		{
			char* buf=dpn_buf;
			int pack_file = *((int*) buf);
			int pack_addr = *((uint*) (buf + 4));
			_uint64 local_min = *((_uint64*) (buf + 8));
			_uint64 local_max = *((_uint64*) (buf + 16));
			_uint64 sum_size = 0;
			if(packtype == PackN)
				sum_size = *((_uint64*) (buf + 24));
			else
				sum_size = ushort(*((_uint64*) (buf + 24)));
			ushort no_objs = *((ushort*) (buf + 32));
			ushort no_nulls = *((ushort*) (buf + 34));
			if(pack_file == PF_NULLS_ONLY)
				no_nulls = no_objs + 1;
			if(pack_file == PF_NULLS_ONLY || (packtype == PackN && local_min == local_max && no_nulls == 0)) {
				is_stored = false;
			} else if(pack_file == PF_NO_OBJ) {
				is_stored = false;
			} else {
				is_stored = true;
			}

			if(is_stored){
				packlist.push_back(_pack_no);
			}
		}
		_pack_no ++;
		
	}
	return 0;
}

#define RIAK_CLUSTER_DP_BUCKET_NAME	"IB_DATAPACKS"

// riak-key-gen dbname tbname key-gen-file
int main(int argc,char *argv[])
{
	if(argc != 4){
		printf("input error \n");
		printf("riak-key-gen dbname tbname key-gen-file \n");
		printf("example: riak-key-gen wap table_01 /tmp/a.txt \n");
		return -1;	
	}
	int table_no = 0;
	int colnum_no = 0;
	int pack_no = 0;
	const char* poutfile=argv[3];
	
	const char* pbasepth = getenv("DATAMERGER_HOME");
	if(0 != get_table_columns(pbasepth,argv[1],argv[2],table_no,colnum_no))
	{
		return -1;	
	}
	
	// 生成到文件中
	FILE* pfile = NULL;
	pfile = fopen(poutfile,"w+t");
	if(pfile == NULL){
		printf("open file %s error\n",poutfile);	
		return -1;
	}
	for(int i=0;i<colnum_no;i++){
		printf("生成第%d列的riak-key值\n",i);
		std::vector<int> vec_list;
		get_packno(pbasepth,argv[1],argv[2],i,vec_list);
		for(int j=0;j<vec_list.size();j++){
			fprintf(pfile,"%s\n",_get_riak_key(table_no,i,vec_list[j]).c_str());
		}
		vec_list.clear();
	}
	printf("所有riak-key值,生成完成到文件:%s.\n",poutfile);
	fclose(pfile);
	pfile=NULL;
	return 0;		
}

