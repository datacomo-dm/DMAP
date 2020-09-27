#ifndef IBFILEWRAPPER
#define IBFILEWRAPPER
#include "AutoHandle.h"
#include "dt_svrlib.h"
#include "zlib.h"
#include "ucl.h"
#include <lzo1x.h>
#include <bzlib.h>
#define IBDATAFILE	0x01230123
#define IBDATAVER	0x0100
struct ib_datafile_header
{
	int fileflag,maxblockbytes;
	LONG64 rownum;
	int blocknum;
	int fileversion;
	// default compress lzo
};

struct ib_block_header
{
	int originlen,blocklen;
	int rownum;
};

class IBDataFile {
		FILE *fp;
		char filename[300];
		ib_datafile_header idh;
		ib_block_header ibh;
		int buflen;
		char *blockbuf;
		char *pwrkmem;
	public:
		IBDataFile(){
			blockbuf=NULL;fp=NULL;buflen=0;
			ResetHeader();
			pwrkmem = 
					new char[LZO1X_MEM_COMPRESS+2048];
      memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
	  }
		void ResetHeader() {
			memset(&idh,0,sizeof(idh));
			idh.fileflag=IBDATAFILE;
			idh.fileversion=IBDATAVER;
		}			
		void SetFileName(char *_filename) {
			strcpy(filename,_filename);
			if(fp) fclose(fp);
			fp=NULL;
		}
		~IBDataFile(){
			if(fp) fclose(fp);
			if(blockbuf) delete[]blockbuf;
			fp=NULL;blockbuf=NULL;
			delete []pwrkmem;
		}
		//return piped record number;
		LONG64 Pipe(FILE *fpipe,FILE *keep_file) {
			if(fp) fclose(fp);
			LONG64 recnum=0;
			LONG64 writed=0;
			fp=fopen(filename,"rb");
			if(!fp) ThrowWith("File '%s' open failed.",filename);
			if(fread(&idh,sizeof(idh),1,fp)!=1) ThrowWith("File '%s' read hdr failed.",filename);
			for(int i=0;i<idh.blocknum;i++) {
				fread(&ibh,sizeof(ibh),1,fp);
				if((ibh.originlen+ibh.blocklen)>buflen || !blockbuf) {
					if(blockbuf) delete [] blockbuf;
					buflen=(int)((ibh.originlen+ibh.blocklen)*1.2);
					blockbuf=new char[buflen*2];
				}
				int rt=0;
				if(fread(blockbuf,1,ibh.blocklen,fp)!=ibh.blocklen) ThrowWith("File '%s' read block %d failed.",filename,i+1);
				lzo_uint dstlen=ibh.originlen;
				#ifdef USE_ASM_5
				rt=lzo1x_decompress_asm_fast((unsigned char*)blockbuf,ibh.blocklen,(unsigned char *)blockbuf+ibh.blocklen,&dstlen,NULL);
				#else
				rt=lzo1x_decompress((unsigned char*)blockbuf,ibh.blocklen,(unsigned char *)blockbuf+ibh.blocklen,&dstlen,NULL);
				#endif
				if(rt!=Z_OK) 
					ThrowWith("Compress failed,datalen:%d,compress flag%d,errcode:%d",
					ibh.blocklen,5,rt);
				if(dstlen!=ibh.originlen) ThrowWith("File '%s' decompress block %d failed,len %d should be %d.",filename,i+1,dstlen,ibh.originlen);
				fwrite(blockbuf+ibh.blocklen,1,dstlen,fpipe);
				if(keep_file)
					fwrite(blockbuf+ibh.blocklen,1,dstlen,keep_file);
				if(recnum<=13716396 && recnum+ibh.rownum>=13716396) {
					 printf("offset for 13716396:%d,%d,%ld",recnum,ibh.rownum,writed);
					 char *seekbf=blockbuf+ibh.blocklen;
					 int start=recnum;
					 while(start!=13716396) {
					 	seekbf+=*(short *)seekbf;
					 	start++;
					}
					printf("offset for 13716396:%d,%ld",start,writed+(seekbf-blockbuf)+ibh.blocklen);
				}
				recnum+=ibh.rownum;
				writed+=dstlen;
			}
			fclose(fp);
			fp=NULL;
			return recnum;
		}
		
		void CreateInit(int _maxblockbytes) {
			if(buflen<_maxblockbytes) {
				if(blockbuf) delete []blockbuf;
				buflen=_maxblockbytes;
				//prepare another buffer  for store compressed data
				blockbuf=new char[buflen*2];
			}
			if(fp) fclose(fp);
			fp=fopen(filename,"w+b");
			if(!fp) ThrowWith("File '%s' open for write failed.",filename);
			ResetHeader();
			idh.maxblockbytes=_maxblockbytes;
			fwrite(&idh,sizeof(idh),1,fp);
		}
		int Write(int mt) {
			if(!fp) ThrowWith("Invalid file handle for '%s'.",filename);
			int rownum=wociGetMemtableRows(mt);
			unsigned int startrow=0;
			int actual_len;
			int writesize=0;
			while(startrow<rownum) {
				int writedrows=wociCopyToIBBuffer(mt,startrow,
					blockbuf,buflen,&actual_len);
				lzo_uint dstlen=buflen;
				int rt=0;
				rt=lzo1x_1_compress((const unsigned char*)blockbuf,actual_len,(unsigned char *)blockbuf+buflen,&dstlen,pwrkmem);
				ibh.originlen=actual_len;
				ibh.rownum=writedrows;
				ibh.blocklen=dstlen;
				fwrite(&ibh,sizeof(ibh),1,fp);
				fwrite(blockbuf+buflen,dstlen,1,fp);
				idh.rownum+=writedrows;
				idh.blocknum++;
				startrow+=writedrows;
				writesize=sizeof(ibh)+dstlen;
			}
			return writesize;
		}
		void CloseReader() {
			if(fp) fclose(fp);
				fp=NULL;
		}
		// end of file write,flush file header
		void CloseWriter() {
			if(!fp) ThrowWith("Invalid file handle for '%s' on close.",filename);
			fseek(fp,0,SEEK_SET);
			fwrite(&idh,sizeof(idh),1,fp);
			fclose(fp);
			fp=NULL;
		}
};

#endif

