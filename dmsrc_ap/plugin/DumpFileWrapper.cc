#include "DumpFileWrapper.h"

LibAdmin::LibAdmin(const char *libname) {
	dlerror();
	handle = dlopen(libname, RTLD_NOW);
	pFileParser=NULL;
	if(handle == NULL){
		ThrowWith(dlerror());
	}
	dlerror();

	MODULE_HANDLE fn = (void *(*)())dlsym(handle, BUILDPARSER_SYM);

	if(fn == NULL){
		ThrowWith(dlerror());
	}
	pFileParser =static_cast<IFileParser *>(fn());
}
IFileParser *LibAdmin::getParser() {
	return pFileParser;
}

LibAdmin::~LibAdmin()   
  {
                if(pFileParser) delete pFileParser;
                dlclose(handle);
  }


int DumpFileWrapper::split(char *text,char **tokens,char delimter) {
	int wordct=0;
	char *psep=strchr(text,delimter);
	if(psep==NULL) return -1;
	*psep++=0;
	tokens[0]=text;tokens[1]=psep;
	return 1;
}

DumpFileWrapper::~DumpFileWrapper() {
	if(pla)
		delete pla;
}
IFileParser *DumpFileWrapper::getParser() 
{
	return pla->getParser();
}

// 装入控制文件参数，以及文件分解模块
#define PARAM_MAXLEN 8000
DumpFileWrapper::DumpFileWrapper(const char *ctlfile) 
{
	char cfilename[300];
	char line[PARAM_MAXLEN];
	int len=0;
	pla=NULL;
	char *words[3];
	char *ptext;
	char section[300],key[300],value[3000];
	strcpy(cfilename,getenv("DATAMERGER_HOME"));
	strcat(cfilename,"/ctl/");
	strcat(cfilename,ctlfile);
	FILE *fp=fopen(cfilename,"rt");
	if(fp==NULL) ThrowWith("Open control file '%s' error!",cfilename);
	section[0]=0;
	int linect=0;
	while(fgets(line,PARAM_MAXLEN,fp)!=NULL) {
		linect++;
		//remove all blank
		len=strlen(line);
		bool inner=false;
		for(int i=0;i<len;i++) {
			if(line[i]=='\n' || line[i]=='\r' ) {line[i]=0;len=i;break;}
			if((line[i]=='\'' || line[i]=='\"') && line[i-1]=='\\') {
				memmove(line+i-1,line+i,len-i+1);len--;
			}
			if((line[i]=='\'' || line[i]=='\"') && line[i-1]!='\\') {
				memmove(line+i,line+i+1,len-i+1);len--;i--;
				inner=!inner;
			}
			if((line[i]==' '|| line[i]=='\t')&& !inner) {
				memmove(line+i,line+i+1,len-i+1);len--;i--;
			}

			// Begin: add 20121114 DM-182 移动位置后，在判断末尾是否存在\n,\r
			if(line[i]=='\n' || line[i]=='\r' ) {line[i]=0;len=i;break;}
			// End: add 20121114 DM-182 移动位置后，在判断末尾是否存在\n,\r

		}
		//empty line
		len=strlen(line);
		if(len<1) continue;
		if(*line=='#') continue;
		// get a section line
		if(*line=='[') {
			char *ptrim=line+len-1;
			ptext=line+1;
			while(ptrim>ptext && *ptrim!=']') ptrim--;
			if(ptrim==ptext) ThrowWith("Get a invalid section on line %d of control file '%s'",
				linect,cfilename); 
			*ptrim=0;
			ptrim=ptext;
			while(*ptrim) {*ptrim++=tolower(*ptrim);}
			strcpy(section,ptext);
			continue;
		}
		if(split(line,words,'=')==-1)
			ThrowWith("Get a invalid item on line %d of control file '%s'",
			linect,cfilename);
		char *trans=words[0];
		while(*trans) {*trans++=tolower(*trans);}
		std::string key(section),value(words[1]);
		key+=":";
		key+=words[0];
		options.insert(std::map<std::string,std::string>::value_type(key,value));
	}
	//load library now:
	pla=new LibAdmin(getOption("parser:parsermodule",""));
}
// 取控制参数
// keyval:对于按照类别分开的参数，统一按照section:item的格式，比如
//    ftp:username
//  
const char *DumpFileWrapper::getOption(const char *keyval,const char *defaults) {
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= options.find(key); 
	if(it == options.end()) {
		return defaults;
	}
	else {
		return it->second.c_str();
	} 
}
const char DumpFileWrapper::getOption(const char *keyval,const char defaults) {
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= options.find(key); 
	if(it == options.end()) {
		return defaults;
	}
	else {
		return it->second.c_str()[0];
	} 
}
int DumpFileWrapper::getOption(const char *keyval,int defaults){
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= options.find(key); 
	if(it == options.end()) {
		return defaults;
	}
	else {
		return atoi(it->second.c_str());
	} 
}
double DumpFileWrapper::getOption(const char *keyval,double defaults){
	std::string key(keyval);
	std::map<std::string,std::string>::iterator it= options.find(key); 
	if(it == options.end()) {
		return defaults;
	}
	else {
		return atof(it->second.c_str());
	} 
}

