#include <stdio.h>
#define PATH_LEN 300
#include "dmncftp.h"
#include "dt_common.h"
#include <unistd.h>

// simple test simulator function
#define errprintf printf

int TestCommonFTP() {
		try {
		// test gunzip
	printf("gunzip...\n");
	gunzipfile("/tmp/test.gz","/tmp/test.ori");
		// test gzip
	printf("gzip...\n");
	gzipfile("/tmp/test.ori","/tmp/test2.gz");
	// no new filename
	printf("gzip local ...\n");
	gzipfile("/tmp/test.ori","/tmp/test3.gz");
	gunzipfile("/tmp/test3.gz");
	// list local file
	printf("list local file...\n");
	listfile("/tmp/*.tar.gz","/tmp/testlstfile.txt");
	// test initialize env
	printf("init...\n");
	FTPClient ftpclient;
	// test connect;
	printf("connect...\n");
	ftpclient.SetContext("118.145.13.134","dma","dma");
	// test list:
	printf("List remote file ...\n");
	ftpclient.ListFile("test/*.gz","/tmp/ncftptest.lst");
	printf("Get remote file size ... \n");
	long size=ftpclient.FileSize("test/SmsLog_201211210720_0604.dat.gz");
        printf("file size:%ld.\n",size); 
	//test put file
	printf("Put file to remote... \n");
  ftpclient.PutFile("/tmp/ncftp-3.2.5-src.tar.gz","test/abc.tar.gz");
  // test get file
	printf("Get file from remote ...\n");
  ftpclient.GetFile("test/abc.tar.gz","/tmp/testftp.tar.gz");
	printf("Get file from remote1 ...\n");
  ftpclient.GetFile("test/abc.tar.gz","/tmp/ftp2");
	unlink("/tmp/testftp.tar.gz");
	printf("delete file on remote ...\n");
	ftpclient.Delete("test/abc.tar.gz");
	printf("Close conn.\n");
        // destructor close connection.
	}
       catch(char *msg) {
	 printf("Error:%s.\n",msg);
	}
	return 1;
}

int TestHuaWei() {
		try {
	printf("init...\n");
	FTPClient ftpclient;
	// test connect;
	printf("connect...\n");
	ftpclient.SetContext("118.145.13.134","dma","dma");
	// test list:
	printf("List remote file (root)...\n");
	ftpclient.ListFile("/tmp/test/*","/tmp/ncftptest.lst");
	printf("List remote file (root:%%2f)...\n");
	ftpclient.ListFile("/tmp/test/*","/tmp/ncftptest1.lst");
	printf("List remote file (current dir)...\n");
	ftpclient.ListFile("test/*","/tmp/ncftptest2.lst");
	printf("Close conn.\n");
        // destructor close connection.
	}
       catch(char *msg) {
	 printf("Error:%s.\n",msg);
	}
	return 1;
}
int main() {
	//TestCommonFTP();
	TestHuaWei();
	return 1;

}


// simple library dependency
 void ThrowWith(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	static char msg[300000];
	vsprintf(msg,format,vlist);
	errprintf(msg);
	va_end(vlist);
	throw msg;
}
