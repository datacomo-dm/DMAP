#include "column_parser.h"

#ifndef DEFINE_DOWNLOAD_EXTERN_NAME_LIST
#define DEFINE_DOWNLOAD_EXTERN_NAME_LIST
// 下载扩展名称定义
const char *DOWNLOAD_EXTERN_NAME_LIST[]={     
	".exe", // windows down load 
	".apk", // application/vnd.android 
	".ipa", // application/vnd.iphone
	".mp3", // audio/x-mpeg 
	".mp4", // video/mp4 
	".mmf", // audio/mmf 
	".mfm", // audio/mfm 
	".pmd", // audio/pmd 
	".adp", // audio/adp 
	".amr", // audio/amr 
	".3gp", // video/3gpp 
	".cab", // application/vnd.smartpohone 
	".gif", // image/gif  
	".pxl", // application/vnd.iphone 
	".ded", // application/vnd.iphone 
	".app", // application/vnd.iphone 
	".hme", // application/vnd.smartphone.thm 
	".jad", // text/vnd.sun.j2me.app-descriptor 
	".jar", // application/java-archive 
	".jpg", // image/jpeg 
	".mid", // audio/midi 
	".ogg", // application/ogg 
	".pdb", // application/ebook 
	".rm", // video/rm 
	".rng", // application/vnd.nokia.ringing-tone 
	".sdt", // application/vnd.sie.thm 
	".sis", // application/vnd.symbian.install 
	".thm", // application/vnd.eri.thm 
	".tsk", // application/vnd.ppc.thm 
	".umd", // application/umd 
	".utz", // application/vnd.uiq.thm 
	".wav", // audio/x-wav 
	".wbmp", // image/vnd.wap.wbmp 
	".wml", // text/vnd.wap.wml 
	".wmlc", // application/vnd.wap.wmlc 
	".wmls", // text/vnd.wap.wmlscript 
	".wmlsc", // application/vnd.wap.wmlscriptc 
	".wmv", // video/x-ms-wmv 
	".wsc", // application/vnd.wap/wmlscriptc 
	//TODO add ..
	"\0" // the last end
};

#define DOWNLOAD_SEPARATOR  '/' // 下载分隔符

#endif

#ifndef min
 #define min(a,b) ((a)>(b)?(b):(a))
#endif
#ifndef max
 #define max(a,b) ((a)>(b)?(a):(b))
#endif

Column_parser::Column_parser(){
    init_adms_and_key_info();
    st_iconv_utf8.is_open = false;
    st_iconv_gbk.is_open = false;
}

Column_parser::~Column_parser(){
    if(st_iconv_gbk.is_open){
        iconv_close(st_iconv_gbk.cd);
        st_iconv_gbk.is_open = false;
    }
    if(st_iconv_utf8.is_open){
        iconv_close(st_iconv_utf8.cd);
        st_iconv_utf8.is_open = false;
    }
}

// pc xp/win7 浏览器(ie,chrome),关键字符和数字的都是原样的,但是中文的位unicode编码,需要转换成gbk编码或者utf8编码
// unicode 与 utf8互转工具:  http://www.falconhan.com/tools/utf8-conv.aspx
// unicode 与 gbk互转工具:  http://www.falconhan.com/tools/gb2312-conv.aspx
enum SEARCH_ENGINES_TYPE{
    INVALID_KEYWORD=-1, // 无效关键字 
    www_bai_com=0,      // 百度       域名[www.baidu.com]       搜索范围[wd=,&]            关键字[unicode编码--->UTF8编码]
    www_google_cn,      // google     域名[www.google.com]      搜索范围[q=,&]             关键字[unicode编码--->UTF8编码]
    www_google_cn_hk,   // google.hk  域名[www.google.com.hk]   搜索范围[q=,&]             关键字[unicode编码--->UTF8编码] 
    www_so_com,         // 360        域名[www.so.com]          搜索范围[q=,&]             关键字[unicode编码--->UTF8编码]
    www_sogou_com,      // 搜狐       域名[www.sogou.com]       搜索范围[query=,&]         关键字[unicode编码--->UTF8编码]  
    cn_bing_com,        // 微软搜索   域名[cn.bing.com]         搜索范围[search?q=,&]      关键字[unicode编码--->UTF8编码] 
    s_taobao_com,       // 淘宝搜索   域名[s.taobao.com]        搜索范围[search?q=,&]      关键字[unicode编码-->GBK/GB2312编码]
    www_youdao_com,     // 网易搜索   域名[www.youdao.com]      搜索范围[search?q=,&]      关键字[unicode编码--->UTF8编码] 
    music_baidu_com,    // 百度音乐   域名[music.baidu.com]     搜索范围[search?key=,]     关键字[unicode编码--->UTF8编码
};

void Column_parser::init_adms_and_key_info(){
	for(int i=0;i<MAX_SEARCH_ENGINES;i++){
		adms_search_engines_info[i].lSearchId = i;

        switch(i){
            case www_bai_com:{
                // 百度网页是对的
                // 关键字编码类型: unicode编码--->UTF8编码
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_UTF8;// 这里应该是UTF8
    			adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"wd=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"www.baidu.com");
    			adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "oe=UTF-8");
    		}
            break;
            
    		case music_baidu_com:{
                // 关键字编码类型: unicode编码--->UTF8编码
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 3;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"search?key=");					       
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"");
        		strcpy(adms_search_engines_info[i].cHostName,"music.baidu.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
    		}
            break;

            case s_taobao_com:{
                // 关键字类型为: unicode编码-->GBK/GB2312
                // 参考工具   http://www.falconhan.com/tools/gb2312-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_GBK;
                adms_search_engines_info[i].cKeyType = 2;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"s.taobao.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
    		}
            break;
            
            case www_so_com:{
                // 关键字编码类型: unicode编码--->UTF8编码
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.so.com");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
            case www_sogou_com:{
                // 关键字编码类型: unicode编码--->UTF8编码
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"query=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.sogou.com");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
    		case www_google_cn:{
                // goole
                // 关键字编码类型: unicode编码--->UTF8编码
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.google.cn");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
    		case www_google_cn_hk:{
                // 关键字编码类型: unicode编码--->UTF8编码
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
                // 网页/新闻 搜索是对的          
                adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 1;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
                strcpy(adms_search_engines_info[i].cHostName,"www.google.com.hk");
                strcpy(adms_search_engines_info[i].cMatchStrFilter , "complete");
    		}
            break;
            
            case cn_bing_com:{
                // 关键字类型为: unicode编码-->utf8
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 2;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"search?q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"cn.bing.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
    		}
            break;

            case www_youdao_com:{
                // 关键字类型为: unicode编码-->utf8
                // 参考工具   http://www.falconhan.com/tools/utf8-conv.aspx
    			adms_search_engines_info[i].cEncodingType = SET_UTF8;
                adms_search_engines_info[i].cKeyType = 2;
    			strcpy(adms_search_engines_info[i].cMatchStrStart,"search?q=");
    			strcpy(adms_search_engines_info[i].cMatchStrEnd,"&");
    			strcpy(adms_search_engines_info[i].cHostName,"www.youdao.com");
                adms_search_engines_info[i].cMatchStrFilter[0] = '\0';
            }
            break;

            default:
                adms_search_engines_info[i].lSearchId = INVALID_KEYWORD;
                return ;
        } // end switch
        
	} // end for

    download_extern_name_list.clear();
}

int Column_parser::is_text_utf8(const char* in_stream,const int stream_len){ 
    // 函数实现，参考:http://blog.csdn.net/shanshu12/article/details/6716118
   short nBytes=0;              // UFT8 可用1-6个字节编码,ASCII用一个字节  
   
   bool bAllAscii=true;         // 如果全部都是ASCII, 说明不是UTF-8  
   
   for(int i=0;i<stream_len;i++){  
       if( ((*in_stream)&0x80) != 0 ){ // 判断是否ASCII编码,如果不是,说明有可能是UTF-8,ASCII用7位编码,但用一个字节存,最高位标记为0,o0xxxxxxx  
           bAllAscii= false;  
       }
       
       if(nBytes==0){ //如果不是ASCII码,应该是多字节符,计算字节数  
           if( (*in_stream) >= 0x80 ){  
               if( (*in_stream) >= 0xFC && (*in_stream) <= 0xFD ){  
                   nBytes=6;  
               }else if( (*in_stream) >= 0xF8 ){  
                   nBytes=5;  
               }else if( (*in_stream) >= 0xF0 ){ 
                   nBytes=4;  
               }else if( (*in_stream) >= 0xE0 ){  
                   nBytes=3;  
               }else if( (*in_stream) >= 0xC0 ){
                   nBytes=2;  
               }else{  
                   return 0;  
               }  
               nBytes--;  
           }  
       }else{ //多字节符的非首字节,应为 10xxxxxx  
           if( ( (*in_stream)&0xC0 ) != 0x80 ){  
               return 0;  
           }  
           nBytes--;  
       }  

       in_stream++;
   }  
   
   if( nBytes > 0 ){ //违返规则  
       return 0;  
   }  
   if( bAllAscii ){ //如果全部都是ASCII, 说明不是UTF-8  
       return 0;  
   }  
   
   return 1;  
}

// get keyword(just get keyword without decoding from unicode) from http
// success: return the adms_search_engines_info.lsearchid
// error: return -1;
// get process:
// 1. get hostname pos
// 2. get keyword start pos
// 3. make sure hostname pos < start pos
// 4. get keyword end pos
// 5. make sure start pos < end pos
// 6. get the keyword <----- http[startpos,endpos]
int Column_parser::_get_keyword_from_http(const stru_adms_search_engines_info* st,const std::string& http,std::string& keyword){
    std::string _cHostName = "";
    int _cHostName_pos = -1;

    std::string _cMatchStrStart = "";
    int _cMatchStrStart_pos = -1;
    
    std::string _cMatchStrEnd = "";
    int _cMatchStrEnd_pos = -1;

    int _lSearchId = -1;
    for(int i=0;i<MAX_SEARCH_ENGINES;i++){
        if(st[i].lSearchId == INVALID_KEYWORD){
            keyword="";
            break;
        }

        _cHostName = (std::string)st[i].cHostName;
        _cMatchStrStart = (std::string)st[i].cMatchStrStart;
        _cMatchStrEnd = (std::string)st[i].cMatchStrEnd;

        // 1. get hostname pos
        _cHostName_pos = http.find(_cHostName);
        if(std::string::npos == _cHostName_pos){ // do not find the hostname
            continue;
        }

        if(_cHostName_pos >= _cHostName.size()){
            continue;
        }

        // 2. get keyword start pos
        // 3. make sure hostname pos < start pos
        _cMatchStrStart_pos = http.find(_cMatchStrStart,_cHostName_pos+1);
        if(std::string::npos == _cMatchStrStart_pos || _cHostName_pos >= _cMatchStrStart_pos ){ // do not find the MatchStrStart
            continue;
        }

        // 4. get keyword end pos
        // 5. make sure start pos < end pos
        if(music_baidu_com == st[i].lSearchId){ // skip the MatchStrEnd

            // 6. get the keyword <----- http[startpos,endpos]
            keyword = http.substr((_cMatchStrStart_pos+_cMatchStrStart.size()),(http.size()-_cMatchStrStart.size()-_cMatchStrStart_pos));     
            _lSearchId = st[i].lSearchId;
            
            break;
        }else{//music_baidu_com != st[i].lSearchId
            _cMatchStrEnd_pos = http.find(_cMatchStrEnd,_cMatchStrStart_pos+1);

            if(std::string::npos == _cMatchStrEnd_pos || _cMatchStrStart_pos >= _cMatchStrEnd_pos){
                continue;
            }

            // 6. get the keyword <----- http[startpos,endpos]
            keyword = http.substr((_cMatchStrStart_pos+_cMatchStrStart.size()),(_cMatchStrEnd_pos-_cMatchStrStart_pos-_cMatchStrStart.size()));      
            _lSearchId = st[i].lSearchId;

            break;
        }              
    }

    return _lSearchId;
}

int Column_parser::code_convert(char *from_charset,char *to_charset,const char *inbuf, size_t inlen,char *outbuf, size_t& outlen){
     char **pin = (char**)&inbuf;
     char **pout = &outbuf;

#if 0 
     // 必须每次都进行重新打开
     if(strcmp(from_charset,"UTF-8") == 0){ // utf8 ---> gbk
        if(!st_iconv_utf8.is_open){
            st_iconv_utf8.cd = iconv_open(to_charset,from_charset);
            if(st_iconv_utf8.cd == 0) return -1;
            st_iconv_utf8.is_open = true;
        }
        if (iconv(st_iconv_utf8.cd, pin,(size_t*)&inlen,pout, (size_t*)&outlen)==-1){
            return -1;      
        }         
     }else if(strcmp(from_charset,"GBK") == 0){ // gbk ---> gbk
        if(!st_iconv_gbk.is_open){
            st_iconv_gbk.cd = iconv_open(to_charset,from_charset);
            if(st_iconv_gbk.cd == 0) return -1;
            st_iconv_gbk.is_open = true;
        }
        if (iconv(st_iconv_gbk.cd, pin,(size_t*)&inlen,pout, (size_t*)&outlen)==-1){
            return -1;      
        }    
     }else{
        assert(0);
     }
#else     
     // 下面代码是正确的，只是考虑到效率将其屏蔽处理
     iconv_t cd; 
     size_t origin_len=outlen;
     cd = iconv_open(to_charset,from_charset);
     if (cd==0) return -1;
     memset(outbuf,0,outlen);
     if (iconv(cd, pin,(size_t*)&inlen,pout, (size_t*)&outlen)==-1) return -1;
     iconv_close(cd);
     outlen=origin_len-outlen;
#endif   

     return 0;

}

// 搜索引擎 unicode ---> utf8 ---> gbk
// 淘宝搜索 unicode ---> gbk
int Column_parser::change_buff_info(char* oribuf)	
{
	int re1,re2,i,inlen=0;

    char inbuf[1024];
	char *in  = inbuf;

    size_t orilen = strlen(oribuf);
	
	for(i=0 ; i < orilen ; )
	{
		if( oribuf[i] == '%')
		{
			if(oribuf[i+1] >= 'A' && oribuf[i+1] <= 'F'){
				re1 = oribuf[i+1] - 'A' + 10;
			}else if(oribuf[i+1] >= 'a' && oribuf[i+1] <= 'f'){
				re1 = oribuf[i+1] - 'a' + 10;
			}else if(oribuf[i+1]>='0' && oribuf[i+1]<='9'){ 
				re1 = oribuf[i+1] - '0';
			}
            
            if(oribuf[i+2]>='A' && oribuf[i+2]<='F'){ 
				re2 = oribuf[i+2] - 'A' + 10;
            }else if(oribuf[i+2]>='a' && oribuf[i+2]<='f'){ 
				re2 = oribuf[i+2] - 'a' + 10;
			}else if(oribuf[i+2]>='0' && oribuf[i+2]<='9'){ 
				re2 = oribuf[i+2] - '0';
			}

			inbuf[inlen++] = re1 * 16 + re2;
			i += 3;
		}else{ //if(oribuf[i] >= 0 && oribuf[i] <= 127)
			inbuf[inlen++] = oribuf[i];
			i++;
		}
	}

	inbuf[inlen] = '\0';
    inlen ++;
	memcpy(oribuf,inbuf,inlen);
	
	return inlen;
}

// get keyword from http
int   Column_parser::get_keyword_from_http(const char* phttp,std::string& keyword){
    std::string _http(phttp);
    std::string _unicode_keyword = "";
    char out_buf[2048];
    char in_buf[2048];
    size_t out_len = 2048;
    memset(out_buf,0,2048);
    
    int valid_len = 0;

    int search_id = this->_get_keyword_from_http(this->adms_search_engines_info,_http,_unicode_keyword);
    if(search_id == INVALID_KEYWORD){
        // get invalid keyword
        keyword = "";
        return keyword.size();
    }else{
        // get valid keyword

        // get cEncodingType type
        int _cEncodingType = this->adms_search_engines_info[search_id].cEncodingType;
		
		strcpy(in_buf,_unicode_keyword.c_str());
        size_t in_len = change_buff_info(in_buf);
        
        // gbk 编码的直接返回就可以了
        if(_cEncodingType == (int)SET_GBK){
        	keyword = in_buf;
        	return keyword.size();
        }
        
        valid_len = min(in_len,strlen(in_buf));
        in_len = valid_len;
             
        // 搜索引擎的utf8编码
       	if(is_text_utf8(in_buf,in_len)){
			_cEncodingType=SET_UTF8;
		}else{
			_cEncodingType=SET_GBK;
		}
       	
       	int ret = 0;	
        if(_cEncodingType == (int)SET_UTF8){ // unicode to UTF-8
            ret = code_convert((char*)"UTF-8",(char*)"GBK",in_buf,in_len,out_buf,out_len);
        }else if(_cEncodingType == (int)SET_GBK){ // unicode to gbk
            ret = code_convert((char*)"GBK",(char*)"GBK",in_buf,in_len,out_buf,out_len);
        }else{
            ThrowWith("Get the error encoding type .");
        }
        
        if(-1 == ret){
            lgprintf("code_convert error, return -1.");            
            keyword="";
        }else{
            int valid_len = min(strlen(out_buf),out_len);
            out_buf[valid_len] = 0;
            char* _p = out_buf;
            while(*_p){ // 
                if(*_p == '\r' || *_p == '\n'){
                    *_p = '\t';
                }
                _p++;
            }
            keyword = out_buf;
        }        
    }
    
    return keyword.size();
}

// p_download_extern_name : *.apk;*.exe
#ifndef SEPERATE_FLAG
#define SEPERATE_FLAG   ';'
#endif
void  Column_parser::set_download_extern_name(const char* p_download_extern_name){
    if(!download_extern_name_list.empty()){
        return;
    }
    char _tmp_lst[300];
    strcpy(_tmp_lst,p_download_extern_name);
    const char* p = _tmp_lst;
    char *q = _tmp_lst + (strlen(_tmp_lst)-1);

    while((*p) == SEPERATE_FLAG){p++;};
    while((*q) == SEPERATE_FLAG){*q = 0; q--;};

    char _path[300];
    int _path_len = 0;
    while(*p){
        if(*p == SEPERATE_FLAG){ 
            strncpy(_path,(p-_path_len),_path_len);
            _path[_path_len] = 0;
            std::string _tmp_item(_path+1);// skip *
            download_extern_name_list.push_back(_tmp_item);
            _path_len = 0;
        }else{
            _path_len++;            
        }    
        p++;
    }
    // 最后一个扩展名
    {
        strncpy(_path,(p-_path_len),_path_len);
        _path[_path_len] = 0;    
        std::string _tmp_item(_path+1); //skip *
        download_extern_name_list.push_back(_tmp_item);
    }      

}

int   Column_parser::get_download_from_http(const char* phttp,std::string& download){
    for(int i=0;i<download_extern_name_list.size();i++){
        const char* pext_name = download_extern_name_list[i].c_str();
        int ext_len = strlen(pext_name);
        if(ext_len == 0){
            download = "";
            break;
        }else{
            int http_len = strlen(phttp);
            if(http_len<=ext_len){
                continue;
            }

            // find the download
            if(strcmp(phttp+(http_len - ext_len),pext_name) == 0){
                const char* q = phttp+(http_len-1);
                int _tmp_len = http_len;

                // 常用:"/"代表"%2F","#"代表"%23","&"代表"%26","="代表"%3D","?"代表"%3F"
                // %252F 也是代表 /
                int sperator_len = 0;
                while( (_tmp_len-->1) && (*q)){
                    if(*q == DOWNLOAD_SEPARATOR){
                        sperator_len = 1;
                        break;
                    }else if((http_len-_tmp_len > 3) && strncasecmp(q,"%2F",3)==0){
                        sperator_len = 3;
                        break;
                    }else if((http_len-_tmp_len > 5) && strncasecmp(q,"%252F",5)==0){
                        sperator_len = 5;
                        break;
                    }else if((http_len-_tmp_len > 4) && strncasecmp(q,"%02F",4)==0){
                        sperator_len = 4;
                        break;
                    }

                    q--;
                }

                if(_tmp_len>1){ // find download 
                    download = q + sperator_len;
                    break;
                }                
            }
        }
    }
   
    return download.size();
}
