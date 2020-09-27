#ifndef DBS_TURBO_LIB_HEADER
#define DBS_TURBO_LIB_HEADER
#ifdef __unix
#include <pthread.h>
#include <malloc.h>
#include <stdlib.h>
#ifndef BOOL
#define BOOL int
#endif
#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif
#else
#include <windows.h>
#include <process.h>
#include <conio.h>
#endif
#include <stdio.h>
#include <vector>
#include <string.h>

#include "AutoHandle.h"
#include "dt_svrlib.h"
#include "zlib.h"
#include "ucl.h"
#include "mt_worker.h"
#include <lzo1x.h>
#include <bzlib.h>


#ifdef PLATFORM_64
#define MAX_LOADIDXMEM  200000
#else
#define MAX_LOADIDXMEM  200000
#endif

class DataDump
{
    AutoMt fnmt;
    int taskid,indexid,datapartid;
    char tmpfn[300];
    char tmpidxfn[300];
    char dumpsql[MAX_STMT_LEN];
    dumpparam dp;
    long memlimit;
    int dtdbc;
    int fnorder;
    int maxblockrn;
    int blocksize;
    mytimer fiotm,sorttm,adjtm;
public:
    DataDump(int dtdbc,int maxmem,int _blocksize);
    int BuildMiddleFilePath(int _fid) ;
    int CheckAddFiles(SysAdmin &sp);
    void ProcBlock(SysAdmin &sp,int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid,bool filesmode);
    int DoDump(SysAdmin &sp,SysAdmin &plugin_sp,const char *systype=NULL) ;
    int heteroRebuild(SysAdmin &sp);
    ~DataDump() {};
};

/*IBData file:
    目标数据文件是已经按binary格式处理好的导入二进制文件，文件长度没有做2gb限制，每次整理操作一个文件
  按最大数据库长度的限制，把数据整理生成的结果顺序写入数据块，对数据库做压缩
  在装入阶段，将数据库解压缩，按PIPE方式写入loader过程
  */
#define IBDATAFILE  0x01230123
#define IBDATAVER   0x0100


struct ib_datafile_header {
    int fileflag,maxblockbytes;
    LONG64 rownum;
    int blocknum;
    int fileversion;
    // default compress lzo
};

struct ib_block_header {
    int originlen,blocklen;
    int rownum;
};

class IBDataFile
{
    FILE *fp;
    char filename[300];
    ib_datafile_header idh;
    ib_block_header ibh;
    int buflen;
    char *blockbuf;
    char *pwrkmem;
public:
    IBDataFile()
    {
        blockbuf=NULL;
        fp=NULL;
        buflen=0;
        ResetHeader();
        pwrkmem =
            new char[LZO1X_MEM_COMPRESS+2048];
        memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
    }
    void ResetHeader()
    {
        memset(&idh,0,sizeof(idh));
        idh.fileflag=IBDATAFILE;
        idh.fileversion=IBDATAVER;
    }
    void SetFileName(char *_filename)
    {
        strcpy(filename,_filename);
        if(fp) fclose(fp);
        fp=NULL;
    }
    ~IBDataFile()
    {
        if(fp) fclose(fp);
        if(blockbuf) delete[]blockbuf;
        fp=NULL;
        blockbuf=NULL;
        delete []pwrkmem;
    }
    //return piped record number;
    LONG64 Pipe(FILE *fpipe,FILE *keep_file)
    {
        if(fp) fclose(fp);
        LONG64 recnum=0;
        LONG64 writed=0;
        fp=fopen(filename,"rb");
        if(!fp) ThrowWith("File '%s' open failed.",filename);
        if(fread(&idh,sizeof(idh),1,fp)!=1) ThrowWith("File '%s' read hdr failed.",filename);
        for(int i=0; i<idh.blocknum; i++) {
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
                ThrowWith("decompress failed,datalen:%d,compress flag%d,errcode:%d",ibh.blocklen,5,rt);
            if(dstlen!=ibh.originlen) ThrowWith("File '%s' decompress block %d failed,len %d should be %d.",filename,i+1,dstlen,ibh.originlen);
            if(fwrite(blockbuf+ibh.blocklen,1,dstlen,fpipe)==0) {
                fclose(fp);
                return -1;
            }
            if(keep_file)
                fwrite(blockbuf+ibh.blocklen,1,dstlen,keep_file);
            recnum+=ibh.rownum;
            writed+=dstlen;
        }
        fclose(fp);
        fp=NULL;
        return recnum;
    }

    void CreateInit(int _maxblockbytes)
    {
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
    int Write(int mt)
    {
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
            if(rt!=Z_OK) {
                ThrowWith("compress failed,datalen:%d,compress flag%d,errcode:%d",ibh.blocklen,5,rt);
            }

            ibh.originlen=actual_len;
            ibh.rownum=writedrows;
            ibh.blocklen=dstlen;
            rt = fwrite(&ibh,sizeof(ibh),1,fp);
            if(rt != 1) {
                ThrowWith("write ibh failed ,return %d ",rt);
            }

            rt = fwrite(blockbuf+buflen,dstlen,1,fp);
            if(rt != 1) {
                ThrowWith("write blockbuf failed ,return %d ",rt);
            }

            idh.rownum+=writedrows;
            idh.blocknum++;
            startrow+=writedrows;
            writesize=sizeof(ibh)+dstlen;
        }
        return writesize;
    }
    void CloseReader()
    {
        if(fp) fclose(fp);
        fp=NULL;
    }
    // end of file write,flush file header
    void CloseWriter()
    {
        if(!fp) ThrowWith("Invalid file handle for '%s' on close.",filename);
        fseek(fp,0,SEEK_SET);
        fwrite(&idh,sizeof(idh),1,fp);
        fclose(fp);
        fp=NULL;
    }
};

class MiddleDataLoader
{
    //int blockmaxrows;
    //int maxindexrows;
    SysAdmin *sp;
    AutoMt mdindexmt;
    AutoMt indexmt;
    AutoMt blockmt;
    ///AutoMt mdblockmt;
    AutoMt mdf;
    dumpparam dp;
    int tmpfilenum;
    file_mt *pdtf;
    unsigned short *pdtfid;
    //int dtfidlen;
public:
    void CheckEmptyDump();
    int Load(int MAXINDEXRN,long LOADTIDXSIZE,bool useOldBlock=true) ;
    int homo_reindex(const char *dbname,const char *tname);
    int dtfile_chk(const char *dbname,const char *tname) ;
    int AutoAddPartition(SysAdmin &sp);
    int CreateLike(const char *dbn,const char *tbn,const char *nsrcowner,const char *nsrctbn,const char * ndstdbn,const char *ndsttbn,const char *taskdate,bool presv_fmt);
    MiddleDataLoader(SysAdmin *_sp);

    //>> Begin:dm-228,创建数据任务表，通过源表命令参数设置
    int CreateSolidIndexTable(const char* orasvcname,const char * orausrname,const char* orapswd,
                              const char* srcdbname,const char* srctabname,const char* dstdbname,const char* dsttabname,
                              const char* indexlist,const char* tmppath,const char* backpath,const char *taskdate,
                              const char* solid_index_list_file,char* ext_sql);

    // 忽略大字段列，并更新采集数据sql语句
    int IgnoreBigSizeColumn(int dts,const char* dbname,const char* tabname,char* dp_datapart_extsql);
    //<< End:dm-228

    ~MiddleDataLoader()
    {
        if(pdtf) delete [] pdtf;
        if(pdtfid) delete [] pdtfid;
        pdtf=NULL;
        pdtfid=NULL;
        //dtfidlen=0;
    }
};

class DestLoader
{
    int indexid;
    int datapartid;
    int tabid;
    SysAdmin *psa;
    dumpparam dp;
public :
    int ReCompress(int threadnum);
    DestLoader(SysAdmin *_psa)
    {
        psa=_psa;
    }
    int MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname);
    int Load (bool directIO=false) ;
    LONG64 GetTableSize(const char* dbn,const char* tbn,const bool echo = true);
    int ToMySQLBlock(const char *dbn, const char *tabname);
    int ReLoad ();
    ~DestLoader() {};
    int RecreateIndex(SysAdmin *_Psa) ;
    int UpdateTableSizeRecordnum(int tabid);
    int MoveFilelogInfo(const int tabid,const int datapartid);

public:
    int RemoveTable_exclude_struct(const char *dbn,const char *tabname,const char *partname);    
    int RemoveTable(const char *dbn,const char *tabname,const char *partname,bool prompt=true,bool checkdel=true);
    int RemoveTable(const char *dbn,const char *tabname,const int partid,bool prompt=true);
    int RemoveTable(const char *dbn,const char *tabname,bool prompt=true/*删除表前是否需要进行确认动作*/);
    int RemoveTable(const int tabid,const int partid,bool exclude_struct = false);
};

//>> begin: fix dma-737,add by liujs
// 表备份还原操作
class BackupTables
{
protected:
    SysAdmin *psa;

public:
    BackupTables(SysAdmin *_psa)
    {
        psa=_psa;
    }
    virtual ~BackupTables() {};
    int BackupTable(const char *dbn,const char *tabname,const char  *bkpath,const bool  bktaskinfo = false);
    int RestoreTable(const char* dbn,const char* tabname,const char* bkfiledir,const bool bktaskinfo = false);

    int RenameTable(const char *src_dbn,const char* src_tbn,const char* dst_dbn,const char* dst_tbn);

protected:
    int CheckTaskInfo(const char* back_path);
    int BackupTaskInfo(const int tabid,const char* back_path);
    int GenTaskInfo2file(const int tabid,const char* back_path,const int struct_type);
    int GenTaskInfofromfile(const int tabid,const char* back_path,const int struct_type);
    int RestoreTaskInfo(const int tabid,const char* back_path,const char* dbname,const char* tabname);


protected:
    int GetTableId(const char* pbasepth,const char *dbn,const char *tabname,int & tabid);
    int getTableId(const char* pth,int & tabid);

    int UpdateTableId(const char* pbasepth,const char *dbn,const char *tabname,const int tableid);
    int updateTableId(const char* pth,const int tabid);

    bool DoesFileExist(const char* pfile);
    bool DoesRsiExist(const char* filepattern);

    typedef std::vector<std::string>  string_vector;
    int GetPatternFile(const char* filepattern,string_vector& rsi_file_vec,std::string &old_dbname);

    bool DoesTableExist(const char* dbn,const char* tabname);

    bool ClearRestorePath(const char* pth,bool success_flag = false);
    bool RollbackRestore(const char* pth,const char* dbn,const char* tabname,const int tabid = 0);

    void GetTarfileLst(const char* dbn,const char* tabname,char* tarfilelst);
    int  CheckRestorePackageOK(const char* tmppth,char* tarfilelst);

    int  GetSeqId(const char* pfile,int & tabid);
    int  UpdateSeqId(const char* pfile,const int tabid);

    bool UpdateRsiID(const char* pth,const int tabid,const char* pattern = NULL);
    bool GenerateNew_table_tcb(const char* pth,const char* pnewtabname,const int tabid);
};
//<< end: fix dma-737,add by liujs

class blockcompress: public worker
{
    int compress;
    char *pwrkmem;
public :
    blockcompress(int cmpflag)
    {
        compress=cmpflag;
        pwrkmem=NULL;
    };
    virtual ~blockcompress()
    {
        if(pwrkmem) delete[] pwrkmem;
    };
    int work()
    {
        //memcpy(outbuf,inbuf,sizeof(block_hdr));
        //char *cmprsbf=outbuf+sizeof(block_hdr);
        //char *bf=inbuf+sizeof(block_hdr);
        memcpy(outbuf,inbuf,blockoffset);
        char *cmprsbf=outbuf+blockoffset;
        char *bf=inbuf+blockoffset;
        block_hdr * pbh=(block_hdr * )(inbuf);
        unsigned int len=pbh->origlen;

        int rt=0;
        //dstlen=outbuflen-sizeof(block_hdr);
        dstlen=outbuflen-blockoffset;
        /******bz2 compress *********/
        if(compress==10) {
            unsigned int dst2=dstlen;
            rt=BZ2_bzBuffToBuffCompress(cmprsbf,&dst2,bf,len,1,0,0);
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
            dstlen=dst2;
            //  printf("com %d->%d.\n",len,dst2);
        }
        /****   UCL compress **********/
        else if(compress==8) {
            unsigned int dst2=dstlen;
            rt = ucl_nrv2d_99_compress((Bytef *)bf,len,(Bytef *)cmprsbf, &dst2,NULL,5,NULL,NULL);
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
            dstlen=dst2;
        }
        /******* lzo compress ****/
        else if(compress==5) {
            if(!pwrkmem) {
                pwrkmem = //new char[LZO1X_999_MEM_COMPRESS];
                    new char[LZO1X_MEM_COMPRESS+2048];
                memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
            }
            lzo_uint dst2=dstlen;
            rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,&dst2,pwrkmem);
            dstlen=dst2;
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
        }
        /*** zlib compress ***/
        else if(compress==1) {
            rt=compress2((Bytef *)cmprsbf,&dstlen,(Bytef *)bf,len,1);
            if(dstlen>outbuflen-blockoffset)
                ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
        } else
            ThrowWith("Invalid compress flag %d",compress);
        if(rt!=Z_OK) {
            ThrowWith("Compress failed on compressworker,datalen:%d,compress flag%d,errcode:%d",
                      len,compress,rt);
        }
        //lgprintf("workid %d,data len 1:%d===> cmp as %d",workid,len,dstlen);
        pbh=(block_hdr *)outbuf;
        pbh->storelen=dstlen+blockoffset-sizeof(block_hdr);
        pbh->compressflag=compress;
        dstlen+=blockoffset;
        //printf("storelen:%d,dstlen:%d\n",pbh->storelen,dstlen);
        LockStatus();
        //isidle=true;
        isdone=true;
        Unlock();
        return 1;
    }
};

#endif
