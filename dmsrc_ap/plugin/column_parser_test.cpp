#include "column_parser.h"

int test_get_keyword_from_http(const char* http);
int test_get_download_from_http(const char* http);

int main(int argc,char* argv[]){
    WOCIInit("column_parser_test/column_parser_test_");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false); 
    bool input_ok = true;
    if(argc != 3) {
    	input_ok = false;
    }else if(strcasecmp(argv[1],"KEYWORD") != 0 && strcasecmp(argv[1],"DOWNLOAD") != 0){
    	input_ok = false;    	
    }
	if(!input_ok){	
        printf("ÊäÈë²ÎÊý´íÎó: column_parser_test [KEYWORD/DOWNLOAD] http\n");   
        printf("example: column_parser_test KEYWORD https://www.google.com.hk/search?q=unicode+to+utf8+c%2B%2B&....\n");   
        printf("example: column_parser_test DOWNLOAD http://dldir1.qq.com/qqfile/qq/QQ5.2/10446/QQ5.2.exe\n");   
        WOCIQuit(); 
    	return 0;        
    }
    
    if(strcasecmp(argv[1],"KEYWORD") == 0 ){
        test_get_keyword_from_http(argv[2]);
    }else if(strcasecmp(argv[1],"DOWNLOAD") ==0){
        test_get_download_from_http(argv[2]);
    }else{
        assert(0);
    }

    WOCIQuit(); 
    return 1;
}

int test_get_keyword_from_http(const char* http){
    Column_parser cp;
    std::string keyword;
    int ret = cp.get_keyword_from_http(http,keyword);
    if(ret >0){
        printf("the http[%s] --->\nkeyword[%s]\n",http,keyword.c_str());
    }else{
        printf("the http[%s] --->\nkeyword[null]\n",http);
    }
    return 0;
}

int test_get_download_from_http(const char* http){
    Column_parser cp;
    std::string download;
    char* p_download_extern_name = "*.apk;*.exe;*.jpj";
    cp.set_download_extern_name(p_download_extern_name);
    int ret = cp.get_download_from_http(http,download);
    if(ret >0){
        printf("the http[%s] --->\ndownload[%s]\n",http,download.c_str());
    }else{
        printf("the http[%s] --->\ndownload[null]\n",http);
    }
    return 0;
}