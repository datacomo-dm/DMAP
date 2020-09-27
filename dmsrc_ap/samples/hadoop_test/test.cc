#include "hdfs.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <iostream>

using namespace std;

#define LISTFILE  "./file.lst"

// 将hdfs hdfsListDirectory 出来的文件进行获取文件名称
// file:/tmp/pth/tmp/data_2013010206_14.txt --> /tmp/pth/tmp/data_2013010206_14.tx
std::string _get_file_name(const char* ls_hdfs_file)
{
	printf("begin:--> %s \n",ls_hdfs_file);
	const char *p=ls_hdfs_file;
	std::string _hdfs_file(p+5);
		
	printf("end:--> %s \n",_hdfs_file.c_str());
	return _hdfs_file;	
}

void listHdfs(const char* nodename,const int port,const char* path,const char* filepatt)
{
	hdfsFS hdfsclient = hdfsConnect(nodename,port);
	if(hdfsclient == NULL){
		printf("连接hdfs文件系统[nodename=%s,port=%s] 失败.\n",nodename,port);
	}
	char hdfs_path[200];
	strcpy(hdfs_path,path);  // 重新获取文件大小使用
	
	// api : hadoop fs -ls /tmp/*.cpp | ls  -1
	char path_pattern[300] = {0};
	sprintf(path_pattern,"%s/%s",path,filepatt);
	int hdfs_num_entries = 0;
	hdfsFileInfo * phdfsFileInfoAry = hdfsListDirectory(hdfsclient,path_pattern,&hdfs_num_entries);
	if( NULL == phdfsFileInfoAry){
		printf(" file_man::listHdfs  hdfsListDirectory(%p,%s,%p) return NULL\n",hdfsclient,path_pattern,&hdfs_num_entries);
		return ;
	}
	else{
		// 获取文件信息存入list文件中
		FILE *fp=fopen(LISTFILE,"wt");
		if(fp==NULL) {
			printf("结果文件%s打开失败.",LISTFILE);
		}
		//GLOB_NOSORT  Don’t sort the returned pathnames.  The only reason to do this is to save processing time.  By default, the returned pathnames are sorted.
		for(int i=0;i<hdfs_num_entries;i++){
			if(phdfsFileInfoAry[i].mKind == kObjectKindFile){
				std::string _hdfs_file = _get_file_name(phdfsFileInfoAry[i].mName);
				fprintf(fp,"%s\n",_hdfs_file.c_str());
			}
		}
		fclose(fp);
	}
	
	printf("获取文件列表 ...");
	FILE *flist=fopen(LISTFILE,"rt");
	if(flist==NULL) printf("文件列表失败.打开文件列表%s失败.");
	fclose(flist);flist=NULL;
	printf("list hdfs sec.\n");
	
	hdfsFreeFileInfo(phdfsFileInfoAry,hdfs_num_entries);
	
	printf("\n\n");
	
}

int main(int argc, char **argv) {
	listHdfs("default", 0,"/tmp/pth/tmp/","data_2013010206*.txt");
	
	listHdfs("default", 0,"/tmp/pth/tmp/","");

	return 0;
}
