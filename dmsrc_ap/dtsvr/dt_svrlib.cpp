//#include <cstring>
//#include <queue>
#include "dt_common.h"
#include "dt_svrlib.h"
#include <stdlib.h>
#include <ctype.h>
#include "zlib.h"
#include <lzo1x.h>
#include "lzo_asm.h"
#include <ucl.h>
#include <ucl_asm.h>
//#include <lzo1z.h>
//#include <lzo1c.h>
//#include <lzo1.h>
//#include <zzlib.h>
#include <bzlib.h>
#include "dtio.h"
#include <math.h>
#include <my_global.h>
#include <m_ctype.h>
extern "C" {
#include <decimal.h>
}
#ifdef __unix
#define thread_rt void *
#define thread_end return NULL
#else
#include <process.h>
#define thread_rt void
#define thread_end return
#endif



//#define dbgprintf printf
#define dbgprintf
//using namespace std;
SvrAdmin *SvrAdmin::psa=NULL;
AutoHandle *SvrAdmin::pdts=NULL;
int SvrAdmin::svrstarted=0;
int SvrAdmin::shutdown=0;
#ifdef __unix
pthread_t SvrAdmin::hd_pthread_t;
#endif
//int getMyRowLen(int *coltp,int *collen,int colct) ;
DTIOExport const char *GetDPLibVersion()
{
#ifdef MYSQL_VER_51
    return "1.7.0";
#else
    return "3.2.9";
#endif
}

#ifndef DPIOSH_VERSION
#define DPIOSH_VERSION "dpiosh 1.6.0"
#endif
void  DpioshPrintfVersion()
{
    lgprintf("DPIOSH-[%s]",DPIOSH_VERSION);
}

void  Trim(char * Text,const char pseperator)
{
    //--------- trim right
    for (int i=strlen(Text)-1; i>0; i--) {
        if (Text[i] == ' ' || Text[i] == '\r' || Text[i] == '\n' || Text[i] == '\t'  ||  Text[i] == pseperator)  Text[i] = 0;
        else  break;
    }

    //--------- trim left
    int trimLeft = 0;
    int len = strlen(Text);
    while (*Text == ' ' || *Text == '\r' || *Text == '\n' || *Text == '\t' ||  *Text == pseperator) {
        Text++;
        trimLeft++;
    }
    strcpy(Text-trimLeft,Text);
}


thread_rt Handle_Read(void *ptr)
{
    dt_file *p=(dt_file *)ptr;
    while(1) {
        //数据可用并且已用过
        p->WaitBufEmpty(); //Lock buf empty,unlock buf empty by dt_file's main thread(ReadMT)
        //p->WaitBufReady(); //Lock buf ready
        if(p->Terminated()) {
            p->SetBufReady();
            break;
        }
        try {
            p->ReadMtThread();
        } catch(char *errstr) {
            p->SetReadError(errstr);
        }
        p->SetBufReady();
    }
    //p->ResetTerminated();
    thread_end;
}

dt_file::dt_file(bool _paral)
{
    fp=NULL;
    openmode=0;
    fnid=0;
    file_version=FILE_DEF_VER;
    filename[0]=0;
    bflen=0;
    filesize=0;
    pwrkmem=NULL;
    cmprslen=0;
    terminate=false;
    isreading=false;
    readedrn=bufrn=-1;
    readedoffset=curoffset=bufoffset=-1;
    readedblocklen=bufblocklen=(unsigned int)-1;
    readedfnid=buffnid=-1;
    offlinelen=-1;
    blockbf=NULL;
    cmprsbf=NULL;
    offlineblock=NULL;
    if (lzo_init() != LZO_E_OK) {
        ThrowWith("lzo_init() failed !!!\n");
    }
    paral=false;
    contct=0;
    SetParalMode(_paral);
    memset(errstr,0,sizeof(errstr));
    pdtio=NULL;
    pdtfile=NULL;
    delmaskbf=new char[(MAX_BLOCKRN+7)/8];
    memset(delmaskbf,0,(MAX_BLOCKRN+7)/8);
}


long dt_file::dtfseek(long offset)
{
    if(pdtfile) pdtfile->SeekAt(offset);
    else return fseek(fp,offset,SEEK_SET);
    return (long)offset;
}
size_t dt_file::dtfread(void *ptr,size_t size)
{
    if(pdtfile) return pdtfile->ReadBuf((char *)ptr,(unsigned long)size);
    return fread(ptr,1,size,fp);
}

dt_file::~dt_file()
{
    SetParalMode(false);
    if(blockbf) delete[]blockbf;
    if(cmprsbf) delete[]cmprsbf;
    if(pwrkmem) delete[]pwrkmem;
    if(offlineblock) delete[]offlineblock;
    Close();
    if(pdtio) delete pdtio;
    if(pdtfile) delete pdtfile;
    if(delmaskbf) delete []delmaskbf;
}

void dt_file::SetStreamName(const char *sfn)
{
    if(pdtio!=NULL) delete pdtio;
    if(pdtfile!=NULL) delete pdtfile;
    pdtio=new dtioStreamFile("./");
    pdtio->SetStreamName(sfn);
    pdtio->SetWrite(false);
    pdtio->StreamReadInit();
    if(pdtio->GetStreamType()!=FULL_BACKUP)
        ThrowWith("指定的文件不是原始备份文件!");
    pdtfile=new dtiofile(pdtio,true);
}

void dt_file::SetParalMode(bool val)
{
    if(val) {
        if(paral) return;
        paral=val;
#ifdef __unix
        if(pthread_mutex_init(&buf_ready,NULL)!=0)
            ThrowWith("create mutex faid");
        if(pthread_mutex_init(&buf_empty,NULL)!=0)
            ThrowWith("create mutex faid");
        WaitBufEmpty();// Lock buf empty,in case thread reading.
        WaitBufReady();

        pthread_create(&hd_pthread_t,NULL,Handle_Read,(void *)this);
        pthread_detach(hd_pthread_t);
#else
        static int evtct=0;
        char evtname[100];
        sprintf(evtname,"dt_file_%d",evtct++);
        buf_ready=CreateEvent(NULL,false,false,evtname);
        sprintf(evtname,"dt_file_%d",evtct++);
        buf_empty=CreateEvent(NULL,false,false,evtname);
        WaitBufEmpty();// Lock buf empty,in case thread reading.
        WaitBufReady();
        _beginthread(Handle_Read,81920,(void *)this);
#endif
    } else {
        if(paral) {
            if(isreading)
                WaitBufReady();
            terminate=true;
            SetBufEmpty();
            WaitBufReady();
            paral=false;
#ifdef __unix
            pthread_mutex_unlock(&buf_ready);
            pthread_mutex_destroy(&buf_empty);
#else
            CloseHandle(buf_ready);
            CloseHandle(buf_empty);
#endif
        }
        paral=false;
        isreading=false;
    }
}


void dt_file::SetFileHeader(int rn,const char *nextfile)
{
    if(!fp) ThrowWith("Write on open failed file at SetNextFile,filename:%s",filename);
    //file_hdr fh;
    //fread(&fh,sizeof(file_hdr),1,fp);
    if(nextfile==NULL) {
        fh.nextfile[0]=0;
        fh.islastfile=1;
    } else {
        strcpy(fh.nextfile,nextfile);
        fh.islastfile=0;
    }
    if(rn>0)
        fh.rownum=rn;

    fseek(fp,0,SEEK_SET);
#ifdef MYSQL_VER_51
    if(file_version==FILE_VER40) fh.fileflag-=0x0100;
#endif
    fh.ReverseByteOrder();
    dp_fwrite(&fh,sizeof(file_hdr),1,fp);
    fh.ReverseByteOrder();
    curoffset=sizeof(file_hdr);
}
//openmode 0:rb read 1:w+b 2:wb
void dt_file::Open(const char *filename,int openmode,int fnid)
{
    //索引数据文件和数据文件具有相同的fnid,openmode也有可能相同
    //if(fnid==this->fnid && fp && this->openmode==openmode) return;
    if(fp) fclose(fp);
    fp=NULL;
    //bk_lastrn=0;
    if(pdtio!=NULL && openmode!=0)
        ThrowWith("Open file error!a stream packed file can only open in read mode.\n filename:%s,openmode:%d",
                  filename,openmode);
    if(pdtio) pdtfile->OpenDtDataFile(filename);
    else {
        if(openmode==0)
            fp=fopen(filename,"rb");
        else if(openmode==1)
            fp=fopen(filename,"w+b");
        else if(openmode==2)
            fp=fopen(filename,"r+b");
        if(!fp) ThrowWith("Open file error! filename:%s,openmode:%d",
                              filename,openmode);
    }
    if(fnid==-1) this->fnid=0;
    else this->fnid=fnid;
    strcpy(this->filename,filename);
    this->openmode=openmode;

    curoffset=0;
    contct=0;
    bufoffset=0;

    if(openmode!=1) {
        if(pdtio) {
            filesize=pdtfile->GetFileLen();
        } else {
            fseek(fp,0,SEEK_END);
            filesize=ftell(fp);
        }
        dtfseek(0);
        ReadHeader();
        SetMySQLRowLen();
    }

}

int dt_file::WriteHeader(int mt,int rn,int fid,const char *nextfilename)
{
    if(!fp) ThrowWith("Write on open failed file,filename:%s",filename);
    fseek(fp,0,SEEK_SET);
    fh.fileflag=FILEFLAGEDIT;
    fh.fnid=fnid=fid;
    fh.islastfile=1;
    fh.rownum=rn;
    void *pcd=NULL;
    if(!mt) {
        fh.cdlen=fh.cdnum=fh.rowlen=0;
    } else {
        wociReverseCD(mt);
        wociGetColumnDesc(mt,&pcd,fh.cdlen,fh.cdnum);
        char *pcd1=new char [fh.cdlen];
        memcpy(pcd1,pcd,fh.cdlen);
        pcd=pcd1;
        wociReverseCD(mt);
        fh.rowlen=wociGetRowLen(mt);
    }
    if(nextfilename) {
        fh.islastfile=false;
        strcpy(fh.nextfile,nextfilename);
    } else {
        fh.islastfile=true;
        fh.nextfile[0]=0;
    }
    //先保存，再做字节序的变换。
    int cdlen=fh.cdlen;
    int fileflag=fh.fileflag;
    if(fileflag==FILEFLAGEDIT) {
        memset(&fhe,0,sizeof(fhe));
        fhe.dtp_sync=1;
        fhe.ReverseByteOrder();
    }
    fh.ReverseByteOrder();
    if(dp_fwrite(&fh,sizeof(file_hdr),1,fp)!=1 ||
       (fileflag==FILEFLAGEDIT && dp_fwrite(&fhe,sizeof(fhe),1,fp)!=1) ||
       dp_fwrite(pcd,1,cdlen,fp)!=cdlen)
        ThrowWith("Write file header failed! filename:%s,fnid:%d",
                  filename,fnid);
    fh.ReverseByteOrder();
    if(fileflag==FILEFLAGEDIT)
        fhe.ReverseByteOrder();
    if(pcd!=NULL) delete [] (char *)pcd;
    curoffset=sizeof(file_hdr)+fh.cdlen+sizeof(fhe);
    return curoffset;
}

int dt_file::AppendRecord(const char *rec,bool deleteable)
{
    if(fh.fileflag!=FILEFLAGEDIT) {
        ThrowWith("AppendRecord have been called on a readonly file :%s%s .",
                  filename);
    }
    long off=readedoffset;
    int rn=1;
    long noff=GetFileSize();
    int startrow=0;
    char *outbf=offlineblock;
    memcpy(outbf,rec,mysqlrowlen);
    if(fhe.lst_blk_off>0) {
        startrow=rn=ReadMySQLBlock(fhe.lst_blk_off,0,&outbf);
        if(rn<MAX_BLOCKRN) {
            memcpy(outbf+rn*mysqlrowlen,rec,mysqlrowlen);
            rn++;
            noff=fhe.lst_blk_off;
        } else rn=1;
    }
    if(noff>MAX_DATAFILELEN) return -1;
    Open(filename,2,fnid);
    fseek(fp,0,SEEK_SET);
    fh.rownum++;
    fh.ReverseByteOrder();
    dp_fwrite(&fh,sizeof(file_hdr),1,fp);
    fh.ReverseByteOrder();
    // modify file header extend area
    fhe.insertrn++;
    fhe.dtp_sync=0;
    fhe.ReverseByteOrder();
    dp_fwrite(&fhe,sizeof(fhe),1,fp);
    fhe.ReverseByteOrder();

    //删除矩阵保持原有值
    delmaskbf[rn/8]^=(1<<(rn%8));
    fseek(fp,noff,SEEK_SET);
    WriteBlock(outbf,rn*mysqlrowlen,1,false,deleteable?BLOCKFLAGEDIT:BLOCKFLAG);
    //恢复WriteBlock中对fh.rownum的增加
    if(fh.rowlen>0) fh.rownum-=rn;
    return startrow;
}

long dt_file::WriteBlock(char *bf,unsigned int len,int compress,bool packed,char bflag)
{
    if(!fp) ThrowWith("Write on open failed file,filename:%s",filename);
    //可以允许可编辑文件中出现不可删除数据块,例如索引数据文件可以追加记录,但不需要记录删除标记
    //if(fh.fileflag==FILEFLAGEDIT && !EditEnabled(bflag))
    //  ThrowWith("对文件'%s'的修改使用了不正确的块识别代码.",filename);
    if(packed) {
        block_hdr *pbh=(block_hdr *)bf;
        pbh->ReverseByteOrder();
        if(dp_fwrite(bf,1,len,fp)!=len)
            ThrowWith("Write file failed! filename:%s,blocklen:%d,offset:%ld",
                      filename,len,curoffset);
        curoffset+=len;
        pbh->ReverseByteOrder();
        if(fh.rowlen>0) fh.rownum+=(len-sizeof(block_hdr))/fh.rowlen;
        return curoffset;
    }

    block_hdr bh;
    bh.blockflag=bflag;
    bh.compressflag=compress;
    bh.origlen=len;
    bh.storelen=len;
    char *dstbf=bf;
    if(compress>0) {
        unsigned int len2=max((int)(len*1.2),1024);
        if(cmprslen<len2) {
            if(cmprsbf) delete [] cmprsbf;
            cmprsbf= new char[len2];
            if(!cmprsbf) ThrowWith("MemAllocFailed on WriteBlock len:%d,len/3:%d",
                                       len,len2);
            cmprslen=len2;
        }
        int rt=0;
        uLong dstlen=len2;
        /**********bzip2 compress**************/
        if(compress==10) {
            unsigned int dstlen2=len2;
            rt=BZ2_bzBuffToBuffCompress(cmprsbf,&len2,bf,len,1,0,0);
            dstlen=len2;
            dstbf=cmprsbf;
        }
        /****   UCL compress **********/
        else if(compress==8) {
            unsigned int dstlen2=len2;
            rt = ucl_nrv2d_99_compress((Bytef *)bf,len,(Bytef *)cmprsbf, &len2,NULL,5,NULL,NULL);
            dstlen=len2;
            dstbf=cmprsbf;
        }
        /******* lzo compress ****/
        else if(compress==5) {
            if(!pwrkmem)  {
                pwrkmem = //new char[LZO1X_999_MEM_COMPRESS];
                    new char[LZO1X_MEM_COMPRESS+2048];
                memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
            }
            lzo_uint dstlen2=len2;
            rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,&dstlen2,pwrkmem);
            dstbf=cmprsbf;
            dstlen=dstlen2;
        }
        /*** zlib compress ***/
        else if(compress==1) {
            rt=compress2((Bytef *)cmprsbf,&dstlen,(Bytef *)bf,len,1);
            dstbf=cmprsbf;
        } else
            ThrowWith("Invalid compress flag %d",compress);
        if(rt!=Z_OK) {
            ThrowWith("Compress failed,datalen:%d,compress flag%d,errcode:%d",
                      len,compress,rt);
        } else {
            bh.storelen=dstlen;
        }
    }
    int storelen=bh.storelen;
    unsigned int dml=0;
    if(EditEnabled(bflag)) {
        if(len/GetBlockRowLen(bflag)>MAX_BLOCKRN)
            ThrowWith("Build a editable block exceed maximum row limit,blockrn:%d,max :%d,filename:%s",
                      len/GetBlockRowLen(bflag),MAX_BLOCKRN,filename);
        dml=(len/GetBlockRowLen(bflag) +7)/8;
        bh.storelen+=dml+sizeof(dmh);
        dmh.rownum=len/GetBlockRowLen(bflag);
        dmh.deletedrn=0;
        dmh.ReverseByteOrder();
        memset(delmaskbf,0,dml);
    }
    bh.ReverseByteOrder();
    if(dp_fwrite(&bh,sizeof(block_hdr),1,fp)!=1 ||
       (EditEnabled(bflag) && dp_fwrite(&dmh,sizeof(dmh),1,fp)!=1) ||
       (EditEnabled(bflag) && dp_fwrite(delmaskbf,1,dml,fp)!=dml) ||
       dp_fwrite(dstbf,1,storelen,fp)!=storelen)
        ThrowWith("Write file failed! filename:%s,blocklen:%d,offset:%ld",
                  filename,len,curoffset);
    curoffset+=sizeof(block_hdr)+storelen+(EditEnabled(bflag)?(dml+sizeof(dmh)):0);
    if(fh.rowlen>0) fh.rownum+=len/GetBlockRowLen(bflag);
    if(EditEnabled(bflag))
        dmh.ReverseByteOrder();
    return curoffset;
}

// rn>0,用于设置按列存储时的各列起始地址(如果baseaddr!=NULL)和列宽，对于计算字段总长度没有影响。
int getMyRowLen(int *coltp,int *collen,int *colscale,int colct,int rn=0,char **mycolptr=NULL,int *mycollen=NULL,char *baseaddr=NULL,int file_version=FILE_DEF_VER)
{
    int off=(colct+7)/8;
    int colpos=0;
    if(rn>0) {
        mycolptr[colpos]=baseaddr;
        mycollen[colpos++]=off;
        baseaddr+=off*rn;
    }
    for(int i=0; i<colct; i++) {
        int clen=collen[i];
//          int slen;
        switch(coltp[i]) {
            case COLUMN_TYPE_CHAR   :
#ifdef MYSQL_VER_51
                if(file_version==FILE_VER40)
                    clen--;
                else {
                    // for mysql5.1 ,this decrease is not needed.
                    if(clen>256) clen++; //如果长度大于255，需要两字节的长度指示(5.1)
                    else if (clen<=4) clen--;
                }
#else
                clen--;
#endif
                off+=clen;
                break;
            case COLUMN_TYPE_FLOAT  :
                clen=sizeof(double);
                off+=clen;
                break;
            case COLUMN_TYPE_NUM    : {
#ifdef MYSQL_VER_51
                if(file_version==FILE_VER40) {
                    clen=(collen[i]<=colscale[i])?(colscale[i]+1):collen[i];
                    clen+=2;
                    off+=clen;
                } else {
                    clen=decimal_bin_size(collen[i],colscale[i]);
                    off+=clen;
                }
#else
                clen=(collen[i]<=colscale[i])?(colscale[i]+1):collen[i];
                clen+=2;
                off+=clen;
#endif
                break;
            }
            case COLUMN_TYPE_INT    :
                clen=sizeof(int);
                off+=clen;
                break;
            case COLUMN_TYPE_BIGINT :
                clen=sizeof(LONG64);
                off+=clen;
                break;
            case COLUMN_TYPE_DATE   :
                clen=sizeof(LONG64);
                off+=clen;
                break;
            default :
                ThrowWith("Invalid column type :%d,id:%d",coltp[i],i);
                break;
        }
        if(rn>0) {
            mycolptr[colpos]=baseaddr;
            baseaddr+=clen*rn;
            mycollen[colpos++]=clen;
        }
    }
    return off;
}

void setNullBit(char *buf, int colid)
{
    static unsigned char marks[8]= {1,2,4,8,16,32,64,128};
    buf[colid/8]|=(char )marks[colid%8];
}

long dt_file::WriteMySQLMt(int mt,int compress,bool corder)
{
    if(!mt)
        ThrowWith("check a empty memory table in dt_file::WriteMySQLMt,mt=NULL.");
    colct=wociGetColumnNumber(mt);
    if(colct>MAX_COLS_IN_DT)
        ThrowWith("exceed maximun columns number, filename:%s,colnum:%d",
                  filename,colct);
    if(colct<1)
        ThrowWith("check a empty memory table in dt_file::WriteMySQLMt,colct:%d.",colct);
    char *colptr[MAX_COLS_IN_DT];
    int collen[MAX_COLS_IN_DT];
    int colscale[MAX_COLS_IN_DT];
    int coltp[MAX_COLS_IN_DT];
    wociAddrFresh(mt,colptr,collen,coltp);
    for(int lp=0; lp<colct; lp++) {
        collen[lp]=wociGetColumnDataLenByPos(mt,lp);
        colscale[lp]=wociGetColumnScale(mt,lp);
        coltp[lp]=wociGetColumnType(mt,lp);
    }
    char *mycolptr[MAX_COLS_IN_DT+1];//加上mask区，数量为实际字段数加1
    int mycollen[MAX_COLS_IN_DT+1];
    int rl=getMyRowLen(coltp,collen,colscale,colct);
    int rn=wociGetMemtableRows(mt);
    mysqlrowlen=rl;
    unsigned int bfl1=rl*rn;
    if(bfl1>bflen || !blockbf) {
        bflen=(unsigned int)(bfl1*1.3);
        if(blockbf) delete[]blockbf;
        blockbf=new char[bflen];
        if(!blockbf) ThrowWith("Memory allocation faild in WriteMt of dt_file,size:%d",bfl1);
        memset(blockbf,0x0,bflen);
    }
    if(corder)
        getMyRowLen(coltp,collen,colscale,colct,rn,mycolptr,mycollen,blockbf,file_version);
    char *buf=blockbf;
#ifdef MYSQL_VER_51
    memset(buf,0,bfl1);
#else
    memset(buf,' ',bfl1);
#endif
    for(int pos=0; pos<rn; pos++) {
        int i,j;
        bool filled=false;
        int off=(colct+7)/8;
        if(corder) buf=mycolptr[0]+off*pos;
        memset(buf,0,off);
        int extralen=0;
        for(i=0; i<colct; i++) {
            int clen=collen[i];
            int slen;
            char *pdst=(char*)buf+off;
            int chd=1;
            if(corder) pdst=mycolptr[i+1]+pos*mycollen[i+1];
            switch(coltp[i]) {
                case COLUMN_TYPE_CHAR   : {
                    if(clen>256) chd++;
                    clen--;
                    char *src=(char *)(colptr[i]+(clen+1)*pos);
                    if(*src!=0) {
#ifdef MYSQL_VER_51
                        if(clen<4) {
                            //FIX CHAR column type,JIRA DM-57
                            memset(pdst,' ',clen);
                            char *lst=(char *)memccpy(pdst,src,0,clen);
                            if(lst) *--lst=' ';
                        } else {
                            char *lst=(char *)memccpy(pdst+chd,src,0,clen);
                            // in mysql 5.1 ,using 0 terminated string
                            short int cl=clen;
                            if(lst) cl=lst-(pdst+chd)-1;
                            if(clen>255) {
                                revShort(&cl);
                                *((short *)pdst)=cl;
                            } else *pdst=(char)cl;
                            clen+=chd;
                        }
#else
                        char *lst=(char *)memccpy(pdst,src,0,clen);
                        if(lst) *--lst=' ';
#endif
                    } else {
#ifdef MYSQL_VER_51
                        if(clen>3)
                            clen+=chd;
#endif
                        setNullBit(buf,i);
                    }
                }
                off+=clen;
                break;
                case COLUMN_TYPE_FLOAT  :
                    if(wociIsNull(mt,i,pos)) {
                        setNullBit(buf,i);
                        LONG64 mdt=0;
                        memcpy(pdst,&mdt,sizeof(double));
                    } else {
                        double v=((double *)colptr[i])[pos];
                        revDouble(&v);
                        memcpy(pdst,&v,sizeof(double));
                    }
                    clen=sizeof(double);
                    off+=clen;
                    break;
                case COLUMN_TYPE_NUM    :
#ifdef MYSQL_VER_51
                {
                    bool isnull=wociIsNull(mt,i,pos);
                    int binlen=decimal_bin_size(clen,colscale[i]);
                    decimal_digit_t dec_buf[9];
                    decimal_t dec;
                    decimal_digit_t dec_buf2[9];
                    decimal_t dec2;
                    dec.buf=dec_buf;
                    dec.len= 9;
                    dec2.buf=dec_buf2;
                    dec2.len=9;
                    double2decimal(isnull?(double)0:((double *)colptr[i])[pos],&dec2);
                    /* extract data from oracle to dm, cause lost significant part fraction...
                          we missed roud process..
                       example :
                         create table testnum (telnum varchar2(10),localfee number(11,2))

                         insert into testnum (telnum,localfee) values (131,8.20);
                       field localfee became 8.19 at dm
                       */
                    decimal_round(&dec2,&dec,colscale[i],HALF_EVEN);
                    decimal2bin(&dec,(uchar *)pdst,clen,colscale[i]);
                    clen=binlen;
                    off+=binlen;
                    if(isnull) setNullBit(buf,i);
                }
#else
                clen=collen[i]<=colscale[i]?(colscale[i]+1):collen[i];
                clen+=2;
                if(wociIsNull(mt,i,pos)) {
                    setNullBit(buf,i);
                    memset(pdst,' ',clen);
                } else {
                    if(((double *)colptr[i])[pos]>=pow(10.0,double(clen-2)) ||
                       ((double *)colptr[i])[pos]<=-pow(10.0,(double)(clen-2)) )
                        ThrowWith("第%d行，第%d字段的值超过允许精度.",pos+1,i+1);
                    sprintf(pdst,"%*.*f",clen,colscale[i],((double *)colptr[i])[pos]);
                }   //slen=(unsigned int) strlen(pdst);
                    //if (slen > clen)
                    //  return 1;
                    //else
                    //{
                    //        //char *to=psrc(char *)buf+off;
                    //  memmove(pdst+(clen-slen),pdst,slen);
                    //  for (j=clen-slen ; j-- > 0 ;)
                    //      *pdst++ = ' ' ;
                    //}
                off+=clen;
#endif
                break;
                case COLUMN_TYPE_INT    :
                    //以下语句在pa-risc平台会产生BUS-ERROR
                    //*(int *)pdst=((int *)colptr[i])[pos];//*sizeof(int));
                    //revInt(pdst);
                    if(wociIsNull(mt,i,pos)) {
                        setNullBit(buf,i);
                        LONG64 mdt=0;
                        memcpy(pdst,&mdt,sizeof(int));
                    } else {
                        int v=((int *)colptr[i])[pos];
                        revInt(&v);
                        memcpy(pdst,&v,sizeof(int));
                    }
                    slen=sizeof(int);
                    off+=slen;
                    break;
                case COLUMN_TYPE_BIGINT :
                    //以下语句在pa-risc平台会产生BUS-ERROR
                    //*(int *)pdst=((int *)colptr[i])[pos];//*sizeof(int));
                    //revInt(pdst);
                    if(wociIsNull(mt,i,pos)) {
                        setNullBit(buf,i);
                        LONG64 mdt=0;
                        memcpy(pdst,&mdt,sizeof(LONG64));
                    } else {
                        LONG64 v=((LONG64 *)colptr[i])[pos];
                        revDouble(&v);
                        memcpy(pdst,&v,sizeof(LONG64));
                    }
                    slen=sizeof(LONG64);
                    off+=slen;
                    break;
                default:
                    ThrowWith("Invalid column type :%d,id:%d",coltp[i],i);
                    break;
                case COLUMN_TYPE_DATE   : {
                    char *src=colptr[i]+7*pos;
                    if(*src==0 || (unsigned char )*src<101 || (unsigned char )*src>199) {//公元200-10000年为有效，否则当作空值
                        setNullBit(buf,i);
                        LONG64 mdt=0;
                        memcpy(pdst,&mdt,sizeof(LONG64));
                        off+=sizeof(LONG64);
                        //extralen+=sizeof(double);
                    } else {
                        LONG64 mdt=0;
                        mdt=LL(LLNY)*(((unsigned char)src[0]-LL(100))*100+(unsigned char)src[1]-100);
                        mdt+=LL(LLHMS)*src[2];
                        mdt+=LL(1000000)*src[3];
                        mdt+=LL(10000)*(src[4]-1);
                        mdt+=100*(src[5]-1);
                        mdt+=src[6]-1;
                        memcpy(pdst,&mdt,sizeof(LONG64));
                        rev8B(pdst);
                        off+=sizeof(LONG64);
                    }
                }
                break;
            }
        }
        off+=extralen;
        buf+=off;
    }
    return WriteBlock(blockbf,bfl1,compress,false,fh.fileflag==FILEFLAGEDIT?(corder?MYCOLBLOCKFLAGEDIT:MYSQLBLOCKFLAGEDIT):MYSQLBLOCKFLAG);
}

long dt_file::WriteMt(int mt,int compress,int rn,bool deleteable)
{
    rn=rn==0?wociGetMemtableRows(mt):rn;
    unsigned int bfl1=(wociGetRowLen(mt)+wociGetColumnNumber(mt)*sizeof(int))*rn;
    if(bfl1>bflen || !blockbf) {
        bflen=(unsigned int)(bfl1*1.3);
        if(blockbf) {
            delete[]blockbf;
            blockbf = NULL;
        }

        blockbf=new char[bflen];

        if(!blockbf) ThrowWith("Memory allocation faild in WriteMt of dt_file,size:%d",bfl1);

        memset(blockbf,0x0,bflen); // try to fix dma-1318
    }
    wociExportSomeRowsWithNF(mt,blockbf,0,rn);
    return WriteBlock(blockbf,bfl1,compress,false,BLOCKNULLEDIT);
    // backup before add NULL indicator
    //fh.fileflag==FILEFLAGEDIT?(deleteable?BLOCKFLAGEDIT:BLOCKFLAG):BLOCKFLAG);
}

int dt_file::CreateMt(int maxrows)
{
    if(curoffset!=sizeof(file_hdr)+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0) )
        ReadHeader();
    if(fh.cdlen<1)
        ThrowWith("CreateMt on file '%s' which does not include column desc info",filename);
    char *pbf=new char[fh.cdlen];
    if(dtfread(pbf,fh.cdlen)!=fh.cdlen) {
        delete [] pbf;
        ThrowWith("CreateMt read column desc info failed on file '%s'",
                  filename);
    }
    int mt=wociCreateMemTable();
    curoffset=sizeof(file_hdr)+fh.cdlen+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0);
    wociImport(mt,NULL,0,pbf,fh.cdnum,fh.cdlen,maxrows,0);
    delete []pbf;
    return mt;
}

int dt_file::GetFirstBlockOffset()
{
    return sizeof(file_hdr)+fh.cdlen+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0);
}

int dt_file::SetMySQLRowLen()
{
    int mt=CreateMt(1);
    if(!mt)
        ThrowWith("check a empty memory table in dt_file::ReadHeader");
    colct=wociGetColumnNumber(mt);
    if(colct>MAX_COLS_IN_DT)
        ThrowWith("exceed maximun columns number, filename:%s,colnum:%d",
                  filename,colct);
    if(colct<1)
        ThrowWith("check a empty memory table in dt_file::ReadHeader");
    char *colptr[MAX_COLS_IN_DT+1]; //为后面计算编译量共用
    int collen[MAX_COLS_IN_DT];
    int colscale[MAX_COLS_IN_DT];
    int coltp[MAX_COLS_IN_DT];
    wociAddrFresh(mt,colptr,collen,coltp);
    for(int lp=0; lp<colct; lp++) {
        collen[lp]=wociGetColumnDataLenByPos(mt,lp);
        colscale[lp]=wociGetColumnScale(mt,lp);
        coltp[lp]=wociGetColumnType(mt,lp);
    }
    //取出偏移量，便于按列组织的mysql数据块提取行数据
    char **tempptr=colptr; //临时变量，占位
    mysqlrowlen=getMyRowLen(coltp,collen,colscale,colct,1,tempptr,mycollen,NULL,file_version);
    wocidestroy(mt);
    return 0;
}

int dt_file::ReadHeader()
{
    dtfseek(0);
    if(dtfread(&fh,sizeof(file_hdr))!=sizeof(file_hdr))
        ThrowWith("Read file header error on '%s'",filename);
    fh.ReverseByteOrder();
#ifdef MYSQL_VER_51
    if(fh.fileflag<FILE_VER51) {
        file_version=FILE_VER40;
        fh.fileflag+=0x0100;
    } else file_version=FILE_VER51;
#endif

    if(fh.fileflag==FILEFLAGEDIT && dtfread(&fhe,sizeof(fhe))!=sizeof(fhe))
        ThrowWith("Read file header error on '%s'",filename);
    curoffset=sizeof(file_hdr)+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0);

    fnid=fh.fnid;
    return (int)curoffset;
}

bool dt_file::OpenNext()
{
    if(fh.islastfile!=0) return false;
    int oldfilesize=filesize;
    int oldcdlen=fh.cdlen;
    int oldfnid=fnid;
    Open(fh.nextfile,0);
    if(oldcdlen!=fh.cdlen)
        ThrowWith("同一索引数据集的两个数据文件字段格式不一致,fileid1:%d,cdlen1:%d,fileid2:%d,cdlen2:%d,filename2 '%s'",
                  oldfnid,oldcdlen,fnid,fh.cdlen,filename);
    curoffset=GetFirstBlockOffset();
    dtfseek(curoffset);
    return true;
}

int dt_file::ReadMySQLBlock(long offset, int storesize,char **poutbf,int _singlefile)
{
    bool contread=false;
    int rn=ReadBlock(offset,storesize,contread,_singlefile);
    if(rn<0) return rn;
    block_hdr *pbh=(block_hdr *)cmprsbf;
    if(offlinelen<pbh->origlen) {
        if(offlineblock) delete []offlineblock;
        offlineblock=new char[int(pbh->origlen*1.2)+mysqlrowlen];
        offlinelen=int(pbh->origlen*1.2+mysqlrowlen);
    }
    memcpy(offlineblock,pblock,pbh->origlen);
    if(pbh->blockflag == MYSQLBLOCKFLAGEDIT || pbh->blockflag==MYCOLBLOCKFLAGEDIT) {
        char *pbf=cmprsbf+sizeof(block_hdr);
        memcpy(&dmh,pbf,sizeof(dmh));
        dmh.ReverseByteOrder();
        pbf+=sizeof(dmh);
        int dml=(pbh->origlen/mysqlrowlen+7)/8;
        memcpy(delmaskbf,pbf,dml);
    }

    if(contread && paral) {
        if(curoffset<filesize ||(curoffset>=filesize && OpenNext())) {
            curblocklen=0;
            SetBufEmpty();// Start thread reading...
        }
    }
    *poutbf=offlineblock;
    return rn;
}


int dt_file::GeneralRead(long offset,int storesize,AutoMt &mt,char **ptr,int clearfirst,int _singlefile,char *cacheBuf,int cachelen)
{
    bool contread=false;
    int rn=ReadBlock(offset,storesize,contread,_singlefile,cacheBuf,cachelen);
    if(rn<0) return rn;

    block_hdr *pbh=(block_hdr *)cmprsbf;
    if(pbh->blockflag==MYSQLBLOCKFLAG || pbh->blockflag==MYSQLBLOCKFLAGEDIT ||pbh->blockflag==MYCOLBLOCKFLAGEDIT) {
        if(offlinelen<pbh->origlen) {
            if(offlineblock) delete []offlineblock;
            offlineblock=new char[int(pbh->origlen*1.2)];
            offlinelen=int(pbh->origlen*1.2);
        }
        if(paral) {
            memcpy(offlineblock,pblock,pbh->origlen);
            *ptr=offlineblock;
        } else {
            if(cacheBuf==NULL)
                *ptr=pblock;
            else *ptr=cacheBuf;
        }
    } else {
        if(cacheBuf!=NULL)
            ThrowWith("内存表不能读到缓冲块,fileid:%d,offset:%ld,filesize:%ld,filename '%s'",
                      fnid,offset,filesize,filename);
        if(clearfirst) {
            if(mt.GetMaxRows()<readedrn) {
                mt.SetMaxRows((int)(readedrn*1.3));
                mt.Build();
            }
            wociReset(mt);
        }
        wociAppendRows(mt,pblock,readedrn);
    }
    if(contread && paral) {
        if(curoffset<filesize ||(curoffset>=filesize && OpenNext())) {
            curblocklen=0;
            SetBufEmpty();// Start thread reading...
        }
    }
    return rn;
}


int dt_file::ReadBlock(long offset, int storesize,bool &contread,int _singlefile,char *cacheBuf,int cachelen)
{
    bool bufok=false;
    contread=false,
    dbgprintf("DBG(ReadMt): isreading:%d,bufoffset:%ld,offset:%ld.\n",isreading,bufoffset,offset);
    if(isreading) {
        WaitBufReady();
    }
    if(offset==0 || offset<-1)
        offset=GetFirstBlockOffset();
    else if(offset==-1) {
        if(readedoffset<GetFirstBlockOffset()) offset=GetFirstBlockOffset();//第一个文件的开始
        else offset=readedoffset+readedblocklen;
        if(offset>curoffset) offset=GetFirstBlockOffset();//已经跳到下一文件或者文件被重新复位(重新打开)
    }
    if(offset>filesize)
        ThrowWith("读数据失败,指定位置超过文件最大长度,fileid:%d,offset:%ld,filesize:%ld,filename '%s'",
                  fnid,offset,filesize,filename);

    dbgprintf("DBG(ReadMt):adj offset to%ld,buffnid %d,fnid %d.\n",offset,buffnid,fnid);
    if(storesize<0) storesize=0;
    if(bufoffset!=offset || buffnid!=fnid || cacheBuf) {
        //连续文件检查
        curblocklen=storesize;
        curoffset=offset;
        if(curoffset>=filesize && (_singlefile==1 || !OpenNext())) {
            dbgprintf("DBG(ReadMt) come to eof,curoffset:%ld,filesize:%ld,bufoffset:%ld,offset:%ld.\n",
                      curoffset,filesize,bufoffset,offset);
            return -1;
        }
        offset=curoffset;
        SetBufEmpty(cacheBuf,cachelen);// Start thread reading...
        WaitBufReady();// Wait Reading complete.
    } else {
        if(curoffset<bufoffset+bufblocklen)
            curoffset=bufoffset+bufblocklen;
    }
    if(bufoffset==-2)
        ThrowWith(errstr);
    if(bufoffset!=offset) {
        ThrowWith("读数据失败,fileid:%d,offset:%ld,bufoffset:%ld,filename '%s'",
                  fnid,offset,bufoffset,filename);
    }

    if(readedoffset+readedblocklen==bufoffset) {
        if(contct>3) contread=true;
        else contct++;
    } else  {
        contct=0;
        dbgprintf("DBG(ReadMt) readedoff:%ld,readedblocklen:%d,off+len %ld,bufoffset:%ld,break cont read.\n",
                  readedoffset,readedblocklen,readedoffset+readedblocklen,bufoffset);
    }
    readedrn=bufrn;
    readedoffset=bufoffset;

    readedblocklen=bufblocklen;
    readedfnid=buffnid;
    dbgprintf("DBG(ReadMt) Got %d rows from off %ld,next should be %ld.\n",readedrn,readedoffset,readedoffset+readedblocklen);
    return readedrn;
}

int dt_file::ReadMt(long offset, int storesize,AutoMt &mt,int clearfirst,int _singlefile,char *poutbf,BOOL forceparal,BOOL dm)
{
    bool contread=false;
    int rn=ReadBlock(offset,storesize,contread,_singlefile);
    if(rn<0) return rn;
    block_hdr *pbh=(block_hdr *)cmprsbf;
    if(poutbf) {
        if(dm) {
            memcpy(poutbf,cmprsbf,GetDataOffset(pbh));
            memcpy(poutbf+GetDataOffset(pbh),pblock,pbh->origlen);
        } else {
            memcpy(poutbf,pbh,sizeof(block_hdr));
            memcpy(poutbf+sizeof(block_hdr),pblock,pbh->origlen);
        }
    } else {
        if(pbh->blockflag!=BLOCKFLAG && pbh->blockflag!=BLOCKFLAGEDIT && pbh->blockflag!=BLOCKNULLEDIT)
            ThrowWith("文件'%s'格式为MySQL,不允许dt_file::ReadMt调用.",filename);
        if(clearfirst) {
            if(mt.GetMaxRows()<readedrn) {
                mt.SetMaxRows((int)(readedrn*1.3));
                mt.Build();
            }
            wociReset(mt);
        }
        if(pbh->blockflag==BLOCKNULLEDIT)
            wociAppendRowsWithNF(mt,pblock,readedrn);
        else wociAppendRows(mt,pblock,readedrn);
    }
    if(contread && paral) {

        if(curoffset<filesize ||(curoffset>=filesize && OpenNext())) {
            curblocklen=0;
            SetBufEmpty();// Start thread reading...
        }
    }
    return readedrn;
}
int SysParam::GetMySQLLen(int mt)
{
    if(!mt)
        ThrowWith("check a empty memory table in SysParam::GetMySQLLen,mt=NULL.");
    int colct=wociGetColumnNumber(mt);
    if(colct<1)
        ThrowWith("check a empty memory table in SysParam::GetMySQLLen,colct:%d.",colct);
    int collen[MAX_COLS_IN_DT];
    int colscale[MAX_COLS_IN_DT];
    int coltp[MAX_COLS_IN_DT];
    for(int lp=0; lp<colct; lp++) {
        collen[lp]=wociGetColumnDataLenByPos(mt,lp);
        colscale[lp]=wociGetColumnScale(mt,lp);
        coltp[lp]=wociGetColumnType(mt,lp);
    }
    return getMyRowLen(coltp,collen,colscale,colct);
}
// System parameter container
int SysParam::BuildSrcDBC(int tabid,int datapartid)
{
    AutoMt mt(dts,10);
    if(datapartid<1)
        mt.FetchAll("select sysid sid from dp.dp_table where tabid=%d",tabid);
    else
        mt.FetchAll("select srcsysid sid from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
    if(mt.Wait()<1)
        ThrowWith("在%s中找不到相应的记录: tabid:%d,datapartid:%d.",datapartid<1?"dp_table":"dp_datapart",tabid,datapartid);
    int srcid=mt.GetInt("sid",0);
    int srcidp=wociSearchIK(dt_srcsys,srcid);
    if(srcidp<0) ThrowWith("BuildSrcDBC has a invalid srcsysid in dp.dp_datasrc table:sysid=%d",srcid);
    //int tabid=dt_srctable.GetInt("tabid",srctabp);
    return BuildDBC(srcidp);
};

int SysParam::BuildSrcDBCByID(int dbsrcid)
{
    int srcidp=wociSearchIK(dt_srcsys,dbsrcid);
    return BuildDBC(srcidp);
}
//datapartid用于提取数据导出临时路径
int SysParam::GetSoledIndexParam(int datapartid,dumpparam *dp,int tabid)
{
    int tabp=wociSearchIK(dt_table,tabid);
    if(tabp<0) ThrowWith("(GetSoledIndexParam),源表(id:%d)在dt_table中找不到,检查是否任务设置中有错误",
                             tabid);
    AutoMt idxmt(dts,MAX_DST_INDEX_NUM);
    idxmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 order by indexgid limit %d",
                   tabid,MAX_DST_INDEX_NUM);
    idxmt.Wait();
    int rn=idxmt.Wait();
    if(rn<1) ThrowWith("编号为%d的表未定义主索引",tabid);
    dp->rowlen=dt_table.GetInt("recordlen",tabp);
    if(dp->rowlen<1)
        ThrowWith( "目标表的记录长度异常,请重新构造目标表结构.异常记录长度为%d,tabid:%d.",dp->rowlen,tabid) ;
    //AutoMt cdf(dts);
    dp->psoledindex=0;
    for(int i=0; i<rn; i++) {
        dp->idxp[i].colnum=idxmt.GetInt("idxfieldnum",i);
        strcpy(dp->idxp[i].idxcolsname,idxmt.PtrStr("columnsname",i));
        strcpy(dp->idxp[i].idxreusecols,idxmt.PtrStr("reusecols",i));
        dp->idxp[i].idxid=idxmt.GetInt("indexgid",i);
        strcpy(dp->idxp[i].idxtbname,idxmt.PtrStr("indextabname",i));
        dp->idxp[i].idinidx=idxmt.GetInt("seqinidxtab",i);
        dp->idxp[i].idindat=idxmt.GetInt("seqindattab",i);
        if(idxmt.GetInt("issoledindex",i)==2)
            dp->psoledindex=i;
    }

    dp->dstpathid[0]=dt_table.GetInt("dstpathid",tabp);
    strcpy(dp->dstpath[0],GetMySQLPathName(dp->dstpathid[0]));

    if(datapartid>0)
        idxmt.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
    else idxmt.FetchAll("select * from dp.dp_datapart where tabid=%d limit 3",tabid);
    if(idxmt.Wait()<1)
        ThrowWith("找不到源数据组成(dp_datapart),tabid:%d; datapartid:%d.",tabid,datapartid);
    dp->tmppathid[0]=idxmt.GetInt("tmppathid",0);
    strcpy(dp->tmppath[0],GetMySQLPathName(dp->tmppathid[0]));

    dp->soledindexnum=rn;
    dp->maxrecnum=0;
    dp->tabid=tabid;
    return rn;
}


int SysParam::GetFirstTaskID(TASKSTATUS ts,int &tabid,int &datapartid)
{
    AutoMt tasksch(dts);
    tasksch.FetchAll("select * from dp.dp_datapart where begintime<sysdate() and status=%d order by begintime limit 10",ts);
    int rn=tasksch.Wait();
    if(rn<1) return 0;
    datapartid=tasksch.GetInt("datapartid",0);
    tabid=tasksch.GetInt("tabid",0);
    return tabid;
};

int SysParam::UpdateTaskStatus(TASKSTATUS ts,int tabid,int datapartid)
{
    AutoStmt tasksch(dts);
    //CMNET:增加对新增字段 touchtime,pid,hostname的设置
    char hostname[300];
    gethostname(hostname,300);

        if( DUMPING == ts){
            tasksch.Prepare("update dp.dp_datapart set status=%d,touchtime=now(),pid=%d,hostname='%s'  where tabid=%d and datapartid=%d and status<>%d and status not in(2,3,5)",
                        ts,getpid(),hostname,tabid,datapartid,ts);
        }else{
            tasksch.Prepare("update dp.dp_datapart set status=%d,touchtime=now(),pid=%d,hostname='%s'  where tabid=%d and datapartid=%d and status<>%d",
                        ts,getpid(),hostname,tabid,datapartid,ts);
        }   

    //char dt[10];
    //wociGetCurDateTime(dt);
    //tasksch.BindDate(1,dt);
    tasksch.Execute(1);
    tasksch.Wait();
    int rn=wociGetFetchedRows(tasksch);
    if(rn<1)
        ThrowWith("任务状态修改为%d时失败，可能是与其它进程冲突。\n"
                  " 表%d(%d).",ts,tabid,datapartid);
    wociCommit(dts);
    return rn;
};

int SysParam::GetDumpSQL(int tabid,int datapartid,char *sqlresult)
{
    AutoMt tasksch(dts);
    tasksch.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d ",tabid,datapartid);
    int rn=tasksch.Wait();
    if(rn<1) ThrowWith("数据组查找失败(GetDumpSQL) ,tabid:%d,datapartid%d",tabid,datapartid);
    //wociMTCompactPrint(tasksch,0,NULL);
    strcpy(sqlresult,tasksch.PtrStr("extsql",0));
    if(strlen(sqlresult)<6)
        ThrowWith("SQL语句错误 :%s, tabid %d,datapartid %d.",sqlresult,tabid,datapartid);
    return datapartid;
}

const char *SysParam::GetMySQLPathName(int pathid,const char *pathtype)
{
    const char *rt=internalGetPathName(pathid,pathtype);
    if(!rt) ThrowWith("在路径配置表中找不到需要的路径,类型: '%s'/ID :%d",pathtype,pathid);
    if(xmkdir(rt)) ThrowWith("路径 '%s' 不存在或者没有权限，并且不能建立。%s/%d.",rt,pathtype,pathid);
    return rt;
}

const char * SysParam::internalGetPathName(int pathid, const char *pathtype)
{
    //下面的代码由潜在的危险:直接修改MT中的字符字段,有个能越界
    if(pathtype==NULL) {
        int p=wociSearchIK(dt_path,pathid);
        if(p<0) return NULL;
        const char *cstptr=dt_path.PtrStr("pathval",p);
        if(cstptr[strlen(cstptr)-1]!='/' && cstptr[strlen(cstptr)-1]!='\\') {
            int nstrLen = dt_path.GetColLen("pathval");
            char *pS = new char[nstrLen];
            memset(pS,0,nstrLen);
            strcpy(pS,cstptr);
            strcat(pS,PATH_SEP);
            dt_path.SetStr("pathval",p,pS);
            cstptr = dt_path.PtrStr("pathval",p);
            delete[] pS;
        }
        return cstptr;
        //return dt_path.PtrStr("pathval",p);
    }
    int rn=wociGetMemtableRows(dt_path);
    int colid=dt_path.GetPos("pathtype",COLUMN_TYPE_CHAR);
    for(int i=0; i<rn; i++) {
        if(STRICMP(dt_path.PtrStr(colid,i),pathtype)==0) {
            const    char *cstptr=dt_path.PtrStr("pathval",i);
            if(cstptr[strlen(cstptr)-1]!='/' && cstptr[strlen(cstptr)-1]!='\\') {
                int nstrLen = dt_path.GetColLen("pathval");
                char *pS = new char[nstrLen];
                memset(pS,0,nstrLen);
                strcpy(pS,cstptr);
                strcat(pS,PATH_SEP);
                dt_path.SetStr("pathval",i,pS);
                cstptr = dt_path.PtrStr("pathval",i);
                delete[] pS;
            }
            return cstptr;
            //return dt_path.PtrStr("pathval",i);
        }
    }
    return NULL;
}

//中间临时导出文件的id号
int SysParam::NextTmpFileID()
{
    return GetSeqID("fileid");//"dt_fileid.nextval"
}

int SysParam::GetSeqID(const char *seqfield)
{
    bool ec=wociIsEcho();
    wociSetEcho(FALSE);
    {
        AutoStmt st(dts);
        st.DirectExecute("lock tables dp.dp_seq write");
        st.DirectExecute("update dp.dp_seq set id=id+1");
    }
    AutoMt seq(dts,10);
    seq.FetchAll("select id as fid from dp.dp_seq");
    seq.Wait();
    {
        AutoStmt st(dts);
        st.DirectExecute("unlock tables");
    }
    wociSetEcho(ec);
    if(seq.GetInt("fid",0)<1) {
        ThrowWith("dp.dp_seq表中的值必须大于0 .");
    }
    return seq.GetInt("fid",0);
}

int SysParam::NextTableID()
{
    return GetSeqID("tabid");//"dt_tableid.nextval"
}

int SysParam::NextDstFileID(int tabid)
{
    bool ec=wociIsEcho();
    wociSetEcho(FALSE);
    {
        AutoStmt st(dts);
        st.DirectExecute("lock tables dp.dp_table write");
        st.DirectExecute("update dp.dp_table set lstfid=lstfid+1 where tabid=%d",tabid);
    }
    AutoMt seq(dts,10);
    //seq.FetchAll("select dt_tableid.nextval as fid from dual");
    seq.FetchAll("select lstfid as fid from dp.dp_table where tabid=%d",tabid);
    seq.Wait();
    {
        AutoStmt st(dts);
        st.DirectExecute("unlock tables");
    }
    wociSetEcho(ec);
    return seq.GetInt("fid",0);
}

#define DICT_CACHE_ROWS 200000
void SysParam::Reload()
{
    dt_path.Clear();
    dt_path.SetMaxRows(500);
    dt_path.FetchAll("select * from dp.dp_path");
    dt_path.Wait();
    wociSetIKByName(dt_path,"pathid");
    wociOrderByIK(dt_path);
    /*****************************/

    dt_srcsys.Clear();
    dt_srcsys.SetMaxRows(200);
    dt_srcsys.FetchAll("select * from dp.dp_datasrc");
    dt_srcsys.Wait();
    /*****************************/
    wociSetIKByName(dt_srcsys,"sysid");
    wociOrderByIK(dt_srcsys);

    dt_table.Clear();
    dt_table.SetMaxRows(DICT_CACHE_ROWS);
    dt_table.FetchAll("select * from dp.dp_table order by tabid");
    dt_table.Wait();
    wociSetIKByName(dt_table,"tabid");
    wociOrderByIK(dt_table);
    wociSetSortColumn(dt_table,"tabname");
    wociSort(dt_table);

    dt_index.Clear();
    dt_index.SetMaxRows(DICT_CACHE_ROWS);
    dt_index.FetchAll("select * from dp.dp_index order by tabid,indexgid,issoledindex desc");
    dt_index.Wait();
    wociSetSortColumn(dt_index,"tabid");
    wociSort(dt_index);
    /*****************************/
}




void SysParam::GetSrcSys(int sysid,char *sysname,char *svcname,char *username,char *pwd)
{
    int res=wociSearchIK(dt_srcsys,sysid);
    if(res<0) ThrowWith("srcsys get error,sysid:%d",sysid);
    dt_srcsys.GetStr("sysname",res,sysname);
    dt_srcsys.GetStr("svcname",res,svcname);
    dt_srcsys.GetStr("username",res,username);
    dt_srcsys.GetStr("pswd",res,pwd);
}


int SysParam::BuildDBC(int srcidp)
{
    const char *pwd=dt_srcsys.PtrStr("pswd",srcidp);
    char pswd[200];
    if(!*pwd) {
        printf("SYSID:%d\nSVCNAME:%s\nUSERNAME:%s\n Input password:",
               dt_srcsys.GetInt("sysid",srcidp),dt_srcsys.PtrStr("svcname",srcidp),
               dt_srcsys.PtrStr("username",srcidp));
        scanf("%s",pswd);
        pwd=pswd;
    } else {
        strcpy(pswd,pwd);
        //decode(pswd);
        pwd=pswd;
    }
    int dbc=0;
    int systype=dt_srcsys.GetInt("systype",srcidp);
    if(systype!=1 && systype!=2 && systype!=3 && systype!=4 && systype!=5)  ThrowWith("源数据库系统的类型错误");
    if(systype==1)
        dbc=wociCreateSession(dt_srcsys.PtrStr("username",srcidp),
                              pwd,dt_srcsys.PtrStr("jdbcdbn",srcidp),DTDBTYPE_ORACLE);//如果jdbcdbn字段为空，需要执行update dp_datasrc修改svcname,jdbcdbn的值
    else dbc=wociCreateSession(dt_srcsys.PtrStr("username",srcidp),
                                   pwd,dt_srcsys.PtrStr("svcname",srcidp),DTDBTYPE_ODBC);
    /* check db init process */
    int sirn=0;
    AutoMt dinit(dts);
    try {
        dinit.FetchAll("select * from dp.dp_datasrcinit where sysid=%d",dt_srcsys.GetInt("sysid",srcidp));
        sirn=dinit.Wait();
    } catch(...) {
        lgprintf("查找数据源初始化语句参数失败，参数表需要升级！");
    }
    if(sirn>0) {
        AutoStmt st(dbc);
        st.DirectExecute(dinit.PtrStr("sqltext",0));
        lgprintf("数据源初始化执行：%s.",dinit.PtrStr("sqltext",0));
    }

    return dbc;
}

int SysParam::GetMiddleFileSet(int procstate)
{
    AutoMt mdfile(dts);
    //mdfile.FetchAll("select * from dt_middledatafile where procstate=%d and rownum<2 ",procstate);
    // Not use limit 1 ,which means desc columns only for DT mysqld.
    mdfile.FetchAll("select * from dt_middledatafile where procstate=%d limit 2 ",procstate);
    int rn=mdfile.Wait();
    if(rn<1) return 0;
    int mt;
    AutoStmt st(dts);
    st.Prepare("select * from dt_middledatefile where tabid=%d and datapartid=%d and indexgid=%d order by mdfid ",
               mdfile.GetInt("tabid",0),mdfile.GetInt("datapartid",0),mdfile.GetInt("indexgid",0));
    mt=wociCreateMemTable();
    wociBuildStmt(mt,st,MAX_MIDDLE_FILE_NUM);
    wociFetchAll(mt);
    wociWaitLastReturn(mt);
    return mt;
}


int SysParam::GetMaxBlockRn(int tabid)
{
    int p=wociSearchIK(dt_table,tabid);
    if(p<0) ThrowWith("get maxrecinblock from table with tabid error,tabid:%d",tabid);
    return dt_table.GetInt("maxrecinblock",p);
}


thread_rt DT_CreateInstance(void *p)
{
    SvrAdmin::CreateInstance();
    thread_end;
}
// tabpath 相对路径,(.../var/)<tabpath>
void SysAdmin::UpdateLoadPartition(int connid,const char *tabpath,bool islastfile,const char *partname)
{
    char loadfile[300];
    sprintf(loadfile,"%s/%s/loadinfo.ctl",GetMySQLPathName(0,"msys"),tabpath);
    FILE *fp=fopen(loadfile,"w+b");
    if(fp==NULL)
        ThrowWith("load ctl file '%s' open failed.",loadfile);
    if(fwrite("LOADCTRL",1,8,fp)!=8) {
        fclose(fp);
        ThrowWith("load ctl file write failed.");
    }
    fwrite(&connid,sizeof(int),1,fp);
    short lastfile = islastfile?1:0;
    fwrite(&lastfile,sizeof(short),1,fp);
    short len=strlen(partname);
    fwrite(&len,sizeof(short),1,fp);
    fwrite(partname,1,len,fp);
    fclose(fp);
}

void SvrAdmin::CreateInstance()
{
    if(!pdts) return ;
    bool ec=wociIsEcho();
    wociSetEcho(FALSE);
    try {
        SvrAdmin *_psa=new SvrAdmin(*pdts);
        printf("reload dt parameters start.\n");
        _psa->Reload();
        psa=_psa;
        SetSvrStarted();
        ReleaseDTS();
    } catch(WDBIError &er) {
        int erc;
        char *buf;
        er.GetLastError(erc,&buf);
        fprintf(stderr,"Error code-%d: %s .\n",erc,buf);
        //throw buf;
    } catch(char *err) {
        fprintf(stderr," %s .\n",err);
        //throw err;
    }
    printf("reload end.\n");
    wociSetEcho(ec);
}

///* Database server extendted interface
SvrAdmin *SvrAdmin::GetInstance()
{
    /*      if(psa==NULL ) {
                            void *pval=NULL;
                psa=(SvrAdmin *)0x0001;
    #ifdef __unix
                pthread_create(&hd_pthread_t,NULL,DT_CreateInstance,(void *)pval);
                pthread_detach(hd_pthread_t);
    #else
                _beginthread(DT_CreateInstance,81920,(void *)pval);
    #endif
                //psa=new SvrAdmin(*pdts);
                //psa->Reload();
            }
    */
    return svrstarted?(psa==(SvrAdmin *)0x1?NULL:psa):NULL;
}

SvrAdmin *SvrAdmin::RecreateInstance(const char *svcname,const char *usrnm,const char *pswd)
{
    if(psa==(void *)0x1) return NULL;
    ReleaseInstance();
    void *pval=NULL;
    CreateDTS(svcname,usrnm,pswd);
#ifdef __unix
    pthread_create(&hd_pthread_t,NULL,DT_CreateInstance,(void *)pval);
    pthread_detach(hd_pthread_t);
#else
    _beginthread(DT_CreateInstance,81920,(void *)pval);
#endif
    return NULL;              //psa=new SvrAdmin(*pdts);
    //psa->Reload();
}

void SvrAdmin::ReleaseInstance()
{
    if(psa!=NULL && psa!=(void *)0x1) {
        SvrAdmin *_psa=psa;
        psa=(SvrAdmin *)0x1;
        svrstarted=0;
        delete _psa;
    }
}

int SvrAdmin::CreateDTS(const char *svcname,const char *usrnm,const char *pswd)
{
    if(pdts==NULL) {
        pdts=new AutoHandle;
        try {
            pdts->SetHandle(wociCreateSession(usrnm,pswd,svcname,DTDBTYPE_ORACLE));
        } catch(WDBIError &er) {
            int erc;
            char *buf;
            er.GetLastError(erc,&buf);
            fprintf(stderr,"Error code-%d: %s .\n",erc,buf);
            throw buf;
        }
    }
    return *pdts;
}

void SvrAdmin::ReleaseDTS()
{
    if(pdts)
        delete pdts;
    pdts=NULL;
}

int SvrAdmin::GetIndexStruct(int p)
{
    int mt=wociCreateMemTable();
    wociAddColumn(mt,"idxidintab",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(mt,"idxidinidx",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(mt,"soledflag",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(mt,"tableoff",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(mt,"indexgid",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(mt,"tabname",NULL,COLUMN_TYPE_CHAR,30,0);
//      wociAddColumn(mt,"reuseindexid",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(mt,"idxfieldnum",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(mt,"databasename",NULL,COLUMN_TYPE_CHAR,20,0);
    wociBuild(mt,100);
    int tabid=dsttable.GetInt("tabid",p);
    const char *pdbn=dsttable.PtrStr("databasename",p);
    int *ptab=dt_index.PtrInt("tabid",0);
    int *pidxtb=dt_index.PtrInt("seqindattab",0);
    int *pidxidx=dt_index.PtrInt("seqinidxtab",0);
    int *pissoled=dt_index.PtrInt("issoledindex",0);
    int *pindexid=dt_index.PtrInt("indexgid",0);
    int idxtabnamep=dt_index.GetPos("indextabname",COLUMN_TYPE_CHAR);
    int rn=wociGetMemtableRows(dt_index);
    int solect=0;
    int i;
    for( i=0; i<rn; i++ ) {
        if(ptab[i]!=tabid) continue;
        else {
            void *ptr[20];
            ptr[0]=pidxtb+i;
            ptr[1]=pidxidx+i;
            ptr[2]=pissoled+i;
            int soledflag=-1;
            if(pissoled[i]==1) soledflag=solect++;
            ptr[3]=&soledflag;
            ptr[4]=pindexid+i;
            ptr[5]=(void *)dt_index.PtrStr(idxtabnamep,i);
            //ptr[6]=dt_index.PtrInt("reuse ",i);
            ptr[6]=dt_index.PtrInt("idxfieldnum",i);
            ptr[7]=(void *)pdbn;//dt_index.PtrInt("idxfieldnum",i);
            ptr[8]=NULL;
            wociInsertRows(mt,ptr,NULL,1);
        }
    }
    /*
    int totidxnum=GetTotIndexNum(p);
    //int *pissole=NULL;
    int *ptableoff=NULL;
    int *pidxid=NULL;
    int *preuseid;
    //wociGetIntAddrByName(mt,"soledflag",0,&pissole);
    wociGetIntAddrByName(mt,"tableoff",0,&ptableoff);
    wociGetIntAddrByName(mt,"reuseindexid",0,&preuseid);
    wociGetIntAddrByName(mt,"indexgid",0,&pidxid);
    for(i=0;i<totidxnum;i++) {
        if(preuseid[i]>0) {
            for(int j=0;j<totidxnum;j++) {
                if(pidxid[j]==preuseid[i]) {
                    ptableoff[i]=ptableoff[j];
                    break;
                }
            }
        }
    }*/

    return mt;
}

int SvrAdmin::Search(const char *pathval)
{
    void *ptr[2];
    ptr[0]=(void *)pathval;
    ptr[1]=NULL;
    return wociSearch(dsttable,ptr);
}

const char *SvrAdmin::GetFileName(int tabid,int fileid)
{
    void *ptr[3];
    ptr[0]=&tabid;
    ptr[1]=&fileid;
    ptr[2]=NULL;
    int i=wociSearch(filemap,ptr);
    if(i<0) {
        lgprintf("Invalid file id :%d",fileid);
        return NULL;
    }
    return filemap.PtrStr(filenamep,i);
}

void SvrAdmin::Reload()
{
    SysParam::Reload();
    filemap.FetchAll("select * from dt_datafilemap where fileflag is null or fileflag=0");
    filemap.Wait();
    wociSetSortColumn(filemap,"tabid,fileid");
    wociSort(filemap);
    filenamep=filemap.GetPos("filename",COLUMN_TYPE_CHAR);

    wociClear(dsttable);
    wociAddColumn(dsttable,"tabid",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(dsttable,"soledindexnum",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(dsttable,"totindexnum",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(dsttable,"recordnum",NULL,COLUMN_TYPE_INT,0,0);
    wociAddColumn(dsttable,"pathval",NULL,COLUMN_TYPE_CHAR,300,0);
    wociAddColumn(dsttable,"firstfile",NULL,COLUMN_TYPE_CHAR,300,0);
    wociAddColumn(dsttable,"databasename",NULL,COLUMN_TYPE_CHAR,300,0);
    //wociAddColumn(dsttable,"colnum",NULL,COLUMN_TYPE_INT,0,0);
    int rn=wociGetMemtableRows(dt_table);
    dsttable.SetMaxRows(rn);
    dsttable.Build();
    for(int i=0; i<rn; i++) {
        int recnum=dt_table.GetInt("recordnum",i);
        if(recnum>0) {
            void *ptr[20];
            char pathval[300];
            ptr[0]=dt_table.PtrInt("tabid",i);
            ptr[1]=dt_table.PtrInt("soledindexnum",i);
            ptr[2]=dt_table.PtrInt("totindexnum",i);
            sprintf(pathval,"./%s/%s",dt_table.PtrStr("databasename",i),
                    dt_table.PtrStr("tabname",i));
            ptr[3]=&recnum;
            ptr[4]=pathval;
            ptr[5]=(void *)GetFileName(dt_table.GetInt("tabid",i),dt_table.GetInt("firstdatafileid",i));
            ptr[6]=(void *)dt_table.PtrStr("databasename",i);
            if(ptr[5]==NULL) ptr[5]=(void *)"";//continue;
            ptr[7]=NULL;
            wociInsertRows(dsttable,ptr,NULL,1);
        }
    }
    wociSetSortColumn(dsttable,"pathval");
    wociSort(dsttable);
}

const char * SvrAdmin::GetDbName(int p)
{
    return dsttable.PtrStr(6,p);
}

int dt_file::ReadMtThread(char *cacheBuf,int cachelen)
{

    dtfseek(curoffset);

    if(curblocklen==0) {
        block_hdr bh;
        if(dtfread(&bh,sizeof(block_hdr))!=sizeof(block_hdr))
            ThrowWith("File read failed on '%s',offset:%ld,size:%d",
                      filename,curoffset,curblocklen);
        bh.ReverseByteOrder();
        if(!CheckBlockFlag(bh.blockflag))
            ThrowWith("Invalid block flag on '%s' ,offset :%ld",
                      filename,curoffset);
        curblocklen=bh.storelen+sizeof(block_hdr);
        if(cmprslen<curblocklen) {
            if(cmprsbf) delete []cmprsbf;
            cmprsbf=new char[int(curblocklen*1.3)];
            cmprslen=int(curblocklen*1.3);
        }
        memcpy(cmprsbf,&bh,sizeof(block_hdr));
        if(dtfread(cmprsbf+sizeof(block_hdr),bh.storelen)!=bh.storelen)
            ThrowWith("File read failed on '%s',offset:%ld,size:%d",
                      filename,curoffset,curblocklen);
    } else {
        if(cmprslen<curblocklen) {
            if(cmprsbf) delete []cmprsbf;
            cmprsbf=new char[int(curblocklen*1.3)];
            cmprslen=int(curblocklen*1.3);
        }
        if(dtfread(cmprsbf,curblocklen)!=curblocklen) {
            ThrowWith("File read failed on '%s',offset:%ld,size:%d",
                      filename,curoffset,curblocklen);
        }
        //直接在cmprsbf地址上作位序变换
        block_hdr *pbh1=(block_hdr *)cmprsbf;
        pbh1->ReverseByteOrder();
    }
    block_hdr *pbh=(block_hdr *)cmprsbf;
    int dml=0;
    int brn=0;
    if(!CheckBlockFlag(pbh->blockflag))
        ThrowWith("Invalid block flag on '%s' ,offset :%ld",
                  filename,curoffset);
    if(pbh->storelen+sizeof(block_hdr)!=curblocklen)
        ThrowWith("Invalid block length on '%s' ,offset :%ld,len:%d,should be:%d.",
                  filename,curoffset,curblocklen,pbh->storelen+sizeof(block_hdr));
    bufoffset=curoffset;
    dbgprintf("DBG: ReadThread curoffset%ld,bufoffset:%ld.\n",curoffset,bufoffset);
    bufblocklen=curblocklen;
    buffnid=fnid;
    curoffset+=curblocklen;
    // very pool code,to avoid mixed del mask for datafile and indexfile
#ifdef  BAD_DEEPCOMPRESS_FORMAT
    if(EditEnabled(pbh->blockflag) && (pbh->compressflag!=10 || pbh->blockflag==MYCOLBLOCKFLAGEDIT)) {
#else
    if(EditEnabled(pbh->blockflag)) {
#endif
        pblock=cmprsbf+sizeof(block_hdr);
        memcpy(&dmh,pblock,sizeof(dmh));
        dmh.ReverseByteOrder();
        brn=dmh.rownum;
        pblock+=sizeof(dmh);
        dml=(brn+7)/8;//pbh->origlen/GetBlockRowLen(pbh->blockflag)
        if(brn>MAX_BLOCKRN) {
            //ThrowWith("Read from block exceed maximum delete mask matrix limit,blockrn:%d,max :%d,filename:%s",
            //        pbh->origlen/GetBlockRowLen(pbh->blockflag),MAX_BLOCKRN,filename);
            errprintf("Read from block exceed maximum delete mask matrix limit,blockrn:%d,max :%d,filename:%s",
                      brn,MAX_BLOCKRN,filename);
        } else {
            memcpy(delmaskbf,pblock,dml);
        }
        pblock+=dml;
        dml+=sizeof(dmh);
    } else pblock=cmprsbf+sizeof(block_hdr);
    if(pbh->compressflag!=0) {
        int rt=0;
        uLong dstlen;
        char *destbuf;
        if(cacheBuf!=NULL) {
            dstlen=cachelen;
            destbuf=cacheBuf;
        } else {
            if(bflen<pbh->origlen) {
                bflen=max(int(pbh->origlen*1.2),1024);
                if(blockbf) delete []blockbf;
                blockbf=new char[bflen];
                memset(blockbf,0x0,bflen);
            }
            destbuf=blockbf;
            dstlen=bflen;
        }
        if(dstlen<pbh->origlen) {
            ThrowWith("数据缓冲块太小，需要%d字节，但只有%d字节.文件'%s':%ld.",
                      pbh->origlen,dstlen,filename,bufoffset);
        }
        int rcl=pbh->storelen-dml;//dml already include delmask_hdr length.  -sizeof(delmask_hdr) ;
        //zzlib
        /***************************
        if(pbh->compressflag==11) {
        memcpy(blockbf,bk_psrc,pbh->storelen);
        dstlen = ZzUncompressBlock((unsigned char *)bk_psrc);
        if(dstlen<0) rt=dstlen;
        }
        else
        ****************************/
        //bzlib2
        if(pbh->compressflag==10) {
            unsigned int dstlen2=dstlen;
            rt=BZ2_bzBuffToBuffDecompress(destbuf,&dstlen2,pblock,rcl,0,0);
            dstlen=dstlen2;
            pblock=blockbf;
        }
        /***********UCL decompress ***********/
        else if(pbh->compressflag==8) {
            unsigned int dstlen2=dstlen;
#ifdef USE_ASM_8
            rt = ucl_nrv2d_decompress_asm_fast_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
#else
            rt = ucl_nrv2d_decompress_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
#endif
            dstlen=dstlen2;
            pblock=blockbf;
        }
        /******* lzo compress ****/
        else if(pbh->compressflag==5) {
            lzo_uint dstlen2=dstlen;
#ifdef USE_ASM_5
            rt=lzo1x_decompress_asm_fast((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
#else
            rt=lzo1x_decompress((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
#endif
            dstlen=dstlen2;
            pblock=blockbf;
        }
        /*** zlib compress ***/
        else if(pbh->compressflag==1) {
            rt=uncompress((Bytef *)destbuf,&dstlen,(Bytef *)pblock,rcl);
            pblock=blockbf;
        } else
            ThrowWith("Invalid uncompress flag %d",pbh->compressflag);
        if(rt!=Z_OK) {
            ThrowWith("Decompress failed,fileid:%d,off:%ld,datalen:%d,compress flag%d,errcode:%d,dml:%d,origlen:%d,rowlen:%d,blockflag:%d,rlen:%d,mrlen:%d,dmh.rownum%d,rcl:%d.",
                      fnid,bufoffset,pbh->storelen,pbh->compressflag,rt,dml,pbh->origlen,GetBlockRowLen(pbh->blockflag),pbh->blockflag,fh.rowlen,mysqlrowlen,dmh.rownum,rcl);
        } else if(dstlen!=pbh->origlen) {
#ifdef  BAD_DEEPCOMPRESS_FORMAT
            if(EditEnabled(pbh->blockflag) && pbh->compressflag==10) {
                pblock=destbuf;
                memcpy(&dmh,pblock,sizeof(dmh));
                dmh.ReverseByteOrder();
                pblock+=sizeof(dmh);
                dml=(pbh->origlen/GetBlockRowLen(pbh->blockflag)+7)/8;
                if(pbh->origlen/GetBlockRowLen(pbh->blockflag)>MAX_BLOCKRN) {
                    errprintf("Read from block exceed maximum delete mask matrix limit,blockrn:%d,max :%d,filename:%s",
                              pbh->origlen/GetBlockRowLen(pbh->blockflag),MAX_BLOCKRN,filename);
                } else {
                    memcpy(delmaskbf,pblock,dml);
                }
                pblock+=dml;
                dml+=sizeof(dmh);
                dstlen-=dml;
                if(dstlen!=pbh->origlen)
                    ThrowWith("Decompress failed,fileid:%d,off:%ld,datalen:%d,compress flag%d,decompress to len%d,should be %d.",
                              fnid,bufoffset,pbh->storelen,pbh->compressflag,dstlen,pbh->origlen);
                memmove(destbuf,destbuf+dml,dstlen);
            }
#else
            ThrowWith("Decompress failed,datalen %d should be %d.fileid:%d,off:%ld,datalen:%d,compress flag%d,errcode:%d,rcl %d",
                      dstlen,pbh->origlen,fnid,bufoffset,pbh->storelen,pbh->compressflag,rt,rcl);
#endif
        }
    } else if(cacheBuf!=NULL)
        ThrowWith("非压缩数据块不能使用Cache模式");
    bufrn=pbh->origlen/GetBlockRowLen(pbh->blockflag);
    buforiglen=pbh->origlen;
    //刷新偏移量数组，便于按行组织的mysql数据块重组数据
    /*
    int offpos;
    int s_off=0;
    for(offpos=0;offpos<=colct;offpos++)
    {
        mycoloff[offpos]=s_off*bufrn;
        s_off+=mycoloff_s[0];
    }
    */
    if(bufrn*GetBlockRowLen(pbh->blockflag)!=pbh->origlen) {
        FILE *fp=fopen("/tmp/dm_error.dat","wb");
        fwrite(pblock,pbh->origlen,1,fp);
        fclose(fp);
        ThrowWith("data block error on file '%s',offset :%ld,original length %d,should be %d,block type:%d,rlen:%d,mrlen:%d.",
                  filename,bufoffset,pbh->origlen,bufrn*GetBlockRowLen(pbh->blockflag),pbh->blockflag,fh.rowlen,mysqlrowlen);
    }
    blockflag=pbh->blockflag;
    dbgprintf("DBG(ReadThread): off %ld,coff %ld,olen%d,len%d.\n",bufoffset,curoffset,buforiglen,bufblocklen);
    return bufrn;
}
/*
int dt_file::WaitEnd(int tm)
{
    #ifdef __unix
    int rt=pthread_mutex_lock(&hd_end);
    #else
    DWORD rt=WAIT_OBJECT_0;
    rt=WaitForSingleObject(hd_end,tm);
    #endif
    if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
        #ifndef __unix
        rt=GetLastError();
        #endif
        ThrowWith("Waitend failed ,code:%d",rt);
    }
    return rt;
}

void dt_file::Start()
{
    if(paral) {
#ifdef __unix
     pthread_mutex_unlock(&hd_start);
#else
    SetEvent(hd_start);
#endif
    }
}
*/
int dt_file::WaitBufReady(int tm)
{
    dbgprintf("DBG:WaitBufReady.\n");
    if(paral) {
#ifdef __unix
        int rt=pthread_mutex_lock(&buf_ready);
#else
        DWORD rt=WAIT_OBJECT_0;
        rt=WaitForSingleObject(buf_ready,tm);
#endif
        if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
#ifndef __unix
            rt=GetLastError();
#endif
            ThrowWith("Wait Buffer ready failed ,code:%d",rt);
        }
        dbgprintf("DBG:WaitBufReady(locked).\n");
        isreading=false;
        return rt;
    }
    return 0;
}

int dt_file::WaitBufEmpty(int tm)
{
    dbgprintf("DBG:WaitBufEmpty.\n");
    if(paral) {
#ifdef __unix
        int rt=pthread_mutex_lock(&buf_empty);
#else
        DWORD rt=WAIT_OBJECT_0;
        rt=WaitForSingleObject(buf_empty,tm);
#endif
        if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
#ifndef __unix
            rt=GetLastError();
#endif
            ThrowWith("Wait Buffer empty failed ,code:%d",rt);
        }
        dbgprintf("DBG:WaitBufEmpty(locked).\n");
        return rt;
    }
    return 0;
}

void dt_file::SetBufReady()
{
    dbgprintf("DBG:SetBufReady.\n");
    if(paral) {
#ifdef __unix
        pthread_mutex_unlock(&buf_ready);
#else
        SetEvent(buf_ready);
#endif
    }
}

void dt_file::SetBufEmpty(char *cacheBuf,int cachelen)
{
    dbgprintf("DBG:SetBufEmpty.\n");
    if(paral) {
        isreading=true;
#ifdef __unix
        pthread_mutex_unlock(&buf_empty);
#else
        SetEvent(buf_empty);
#endif
    } else ReadMtThread(cacheBuf,cachelen);
}

#define CHARENC(a)  ((a)>25?('0'+(a)-26):('A'+(a)))
#define CHARDEC(a)  ((a)<'A'?((a)-'0'+26):((a)-'A'))

DTIOExport void encode(char *str)
{
    char str1[17];
    char cd[35];
    int i;
    int len=strlen(str);
    if(len<2 || len>15) {
        printf("输入2-15个字符.\n");
        return;
    }
    static bool inited=false;
    if(!inited) {
        srand( (unsigned)time( NULL ) );
        inited=true;
    }
    for(i = len+1;   i < 17; i++ )
        str[i]=rand()%127;
    int pos[]= {13,8,9,14,7,0,10,1,4,12,5,15,6,2,11,3};
    for( i=0; i<16; i++) {
        str1[i]=str[pos[i]];
    }
    for(i=0; i<16; i++) {
        str1[i]^=str[16];//0x57;
        int off=(str[16]+i)%20;
        cd[2*i]=CHARENC((str1[i]>>4)+off);
        cd[2*i+1]=CHARENC((str1[i]&0x0f)+off);
    }
    cd[32]=CHARENC(str[16]>>4);
    cd[33]=CHARENC((str[16]&0x0f));
    cd[34]=0;
    printf("加密为:%s\n",cd);
    //return ;
    //m_strEnc.SetWindowText(cd);

    /***Decode
    str1[16]=(CHARDEC(cd[32])<<4)+CHARDEC(cd[33]);
    for(i=0;i<16;i++) {
        int off=(str1[16]+i)%20;
        str1[i]=((CHARDEC(cd[2*i])-off)<<4)+CHARDEC(cd[2*i+1])-off;
        str1[i]^=str1[16];
    }
    for(i=0;i<16;i++) {
        str[pos[i]]=str1[i];
    }
    str[16]=0;
    printf("解密:%s\n",str);
    **************/
}

DTIOExport void decode(char *str)
{
    char cd[40];
    char str1[20];
    int i;
    strcpy(cd,str);
    //Decode
    str1[16]=(CHARDEC(cd[32])<<4)+CHARDEC(cd[33]);
    for(i=0; i<16; i++) {
        int off=(str1[16]+i)%20;
        str1[i]=((CHARDEC(cd[2*i])-off)<<4)+CHARDEC(cd[2*i+1])-off;
        str1[i]^=str1[16];
    }
    int pos[]= {13,8,9,14,7,0,10,1,4,12,5,15,6,2,11,3};
    for(i=0; i<16; i++) {
        str[pos[i]]=str1[i];
    }
    str[16]=0;
}

int dt_file::SetLastWritePos(unsigned int off)
{
    if(!fp) ThrowWith("Write on open failed file at SetLastWritePos,filename:%s",filename);
    if(fh.fileflag!=FILEFLAGEDIT) ThrowWith("Call SetLastWritePos on readonly dt_file:%s",filename);
    fhe.lst_blk_off=off;
    fhe.ReverseByteOrder();
    fseek(fp,sizeof(fh)+sizeof(fhe),SEEK_SET);
    dp_fwrite(&fhe,sizeof(fhe),1,fp);
    fhe.ReverseByteOrder();
    curoffset=sizeof(file_hdr)+sizeof(fhe);
    return curoffset;
}

int file_mt::AppendMt(int amt, int compress, int rn)
{
    int startrow=0;
    if(fh.fileflag!=FILEFLAGEDIT) ThrowWith("AppendMt have been call on a readonly dt_file:%s",GetFileName());
    if(fhe.lst_blk_off>0) {
        int fmt=ReadMtOrBlock(fhe.lst_blk_off,0,1,NULL);
        if(fmt==0) return -1;
        startrow=GetRowNum();
        if(startrow+rn<MAX_APPEND_BLOCKRN) {
            wociCopyRowsTo(amt,fmt,-1,0,rn);
            dtfseek(GetOldOffset());
            WriteMt(fmt,compress);
            return startrow;
        }
    }
    unsigned int off=GetFileSize();
    dtfseek(GetFileSize());
    WriteMt(amt,compress,rn);
    SetLastWritePos(off);
    return startrow;
}
#ifndef MYSQL_SERVER
bool SysAdmin::GetBlankTable(int &tabid)
{
    AutoMt mt(dts,100);
    mt.FetchFirst("select distinct dt.tabid tabid from dp.dp_table dt,dp.dp_datapart dp where dt.tabid=dp.tabid and dt.cdfileid=0 and ifnull(dp.blevel,0)%s100 ",
                  GetNormalTask()?"<":">=");
    if(mt.Wait()>0)
        tabid=mt.GetInt("tabid",0);
    else return false;
    return true;
}

bool SysAdmin::CreateDT(int tabid)
{
    //AutoMt mt(dts,100);
    //mt.FetchAll("select * from dp.dp_datapart where tabid=%d %s",tabid,GetNormalTaskDesc());
    //if(mt.Wait()<1) return false;
    char sqlbf[MAX_STMT_LEN];
    // try to lock table for init structure.
    SetTrace("parsesource",tabid);
    Reload();
    lgprintf("准备解析源表%d...",tabid);
    sprintf(sqlbf,"update dp.dp_table set recordlen=-1 where tabid=%d and recordlen!=-1",tabid);
    int ern=DoQuery(sqlbf);
    if(ern!=1) {
        lgprintf("源表%d已在其它进程中解析。",tabid);
        return false;
    }
    // release lock while catch a error.
    try {
        lgprintf("解析源表,重构表结构,目标表编号%d.",tabid);
        int tabp=wociSearchIK(dt_table,tabid);
        int srcid=dt_table.GetInt("sysid",tabp);
        int srcidp=wociSearchIK(dt_srcsys,srcid);
        if(srcidp<0) {
            log(tabid,0,DUMP_SOURCE_CONNECT_CFG_ERROR, "找不到源系统连接配置,表%d,源系统号%d",tabid,srcid);
            ThrowWith( "找不到源系统连接配置,表%d,源系统号%d",tabid,srcid) ;
        }
        //建立到数据源的连接
        AutoHandle srcsys;
        srcsys.SetHandle(BuildDBC(srcidp));
        //构造源数据的内存表结构
        AutoMt srcmt(srcsys,0);
        srcmt.SetHandle(GetSrcTableStructMt(tabp,srcsys));
        if(wociGetRowLen(srcmt)<1) {
            log(tabid,0,DUMP_SOURCE_TABLE_PARSE_ERROR, "源表%d解析错误,记录长度为%d",tabid,wociGetRowLen(srcmt));
            ThrowWith( "源表解析错误,记录长度为%d",wociGetRowLen(srcmt)) ;
        }
        char tbname[150],idxname[150];
        GetTableName(tabid,-1,tbname,idxname,TBNAME_DEST);
        int colct=srcmt.GetColumnNum();
#define WDBI_DEFAULT_NUMLEN 16
#define WDBI_DEFAULT_SCALE  3
        for(int ci=0; ci<colct; ci++) {
            if(wociGetColumnType(srcmt,ci)==COLUMN_TYPE_NUM) {
                if(wociGetColumnDataLenByPos(srcmt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(srcmt,ci)==WDBI_DEFAULT_SCALE) {

#ifdef MYSQL_VER_51
                    AutoMt mt(dts,100);
                    char coln[200];
                    wociGetColumnName(srcmt,ci,coln);
                    mt.FetchAll("select prec,scale from dp.dp_columndef where tabid=%d and upper(columnname)=upper('%s')",tabid,coln);
                    if(mt.Wait()>0) {
                        wociSetColumnDisplay(srcmt,NULL,ci,coln,mt.GetInt("scale",0),mt.GetInt("prec",0));
                        continue;
                    }
#endif
                    char sql_st[MAX_STMT_LEN];
                    wociGetCreateTableSQL(srcmt,sql_st,tbname,false);
                    char *psub;
                    while(psub=strstr(sql_st,"16,3")) memcpy(psub,"????",4);
                    log(tabid,0,DUMP_SOURCE_TABLE_PARSE_ERROR,"源表解析错误,一些数值字段缺乏明确的长度定义,请使用格式表重新配置.参考建表语句(修改????的部分): \n %s .",sql_st) ;
                    sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100  where tabid=%d",
                            tabid);
                    DoQuery(sqlbf);
                    ThrowWith( "源表解析错误,一些数值字段缺乏明确的长度定义,请使用格式表重新配置.\n参考建表语句(修改????的部分): \n %s .",sql_st) ;
                }
            }
        }

        lgprintf("重建目标表结构（CreateDP)以前需要禁止对表的访问.由于数据或索引结构可能有变化,因此在结构重建后不做数据恢复.");
        lgprintf("记录数清零...");

        AutoMt indexmt(dts,10);
        indexmt.FetchAll("select indexgid ct from dp.dp_index where tabid=%d and issoledindex>0",tabid);
        int rn=indexmt.Wait();
        if(rn==1) {
            CloseTable(tabid,tbname,true);
            //>> begin: Jira dma-460,20130116
            CreateTableOnMysql(srcmt,tabid,tbname,indexmt.GetInt("ct",0),true);
            //<< end
        } else for(int i=0; i<rn; i++) {
                char tabname1[300];
                //max 10 soled index
                sprintf(tabname1,"%s_dma%d",tbname,indexmt.GetInt("ct",i));
                CloseTable(tabid,tabname1,true);
                //>> begin: Jira dma-460,20130116
                CreateTableOnMysql(srcmt,tabid,tabname1,indexmt.GetInt("ct",i),true);
                //<< end
            }

        /* DMA NOT use index table ,but split soled index to target tables.*/

        //CreateAllIndexTable(tabid,srcmt,TBNAME_DEST,true);
        sprintf(sqlbf,"update dp.dp_table set cdfileid=1 , recordlen=%d where tabid=%d",
                GetMySQLLen(srcmt)/*wociGetRowLen(srcmt)*/, tabid);
        DoQuery(sqlbf);
        log(tabid,0,DUMP_SOURCE_TABLE_PARSER_NOTIFY,"结构重建:字段数%d,记录长度%d字节.",colct,wociGetRowLen(srcmt));
    } catch(...) {
        log(tabid,0,DUMP_SOURCE_TABLE_PARSE_ERROR,"源表%d解析错误.",tabid) ;
        sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100  where tabid=%d",
                tabid);
        DoQuery(sqlbf);
        sprintf(sqlbf,"update dp.dp_table set recordlen=0,cdfileid=0 where tabid=%d",
                tabid);
        DoQuery(sqlbf);
        throw;
    }
    return true;
}

int SysParam::logext(int tabid,int datapartid,int evt_type,const char *format,...)
{
    if(!wociTestTable(dts,"dp.dp_notify_ext")) return 0;
    char msg[LOG_MSG_LEN+MAX_STMT_LEN];
#ifdef __unix
    va_list vlist;
#else
    va_list vlist;
#endif
    va_start(vlist,format);
    int rt=vsprintf(msg,format,vlist);
    va_end(vlist);
    if(strlen(msg)>LOG_MSG_LEN) // truncate log to maximum length
        msg[LOG_MSG_LEN]=0;
    try {
        AutoMt mt(dts,10);
        mt.FetchAll("select * from dp.dp_notify_ext limit 100");
        int extrn=mt.Wait();
        if(extrn<1) return 0;
        for(int ei=0; ei<extrn; ei++) {
            AutoMt tabinfo(dts,10);
            tabinfo.FetchAll("select lower(databasename) dbname,lower(tabname) tabname from dp.dp_table where tabid=%d",tabid);
            tabinfo.Wait();
            AutoMt partinfo(dts,10);
            partinfo.FetchAll("select lower(partfullname) partfullname  from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
            partinfo.Wait();

            AutoHandle extdb;
            int srcidp=wociSearchIK(dt_srcsys,mt.GetInt("dbsysid",ei));
            if(srcidp<0)
                ThrowWith("找不到外部数据库，系统号%d",mt.GetInt("dbsysid",0)) ;
            extdb.SetHandle(BuildDBC(srcidp));
            AutoStmt st(extdb);
            int rn=st.DirectExecute("update %s set IMP_STATUS=%d where lower(NEW_DBNAME_INDM)='%s' and lower(NEW_TABNAME_INDM)='%s' and lower(NEW_PARTFULLNAME)='%s'",
                                    mt.PtrStr("tabname",ei),evt_type,tabinfo.PtrStr("dbname",0),tabinfo.PtrStr("tabname",0),partinfo.PtrStr("partfullname",0));
            if(rn>0) {
                lgprintf("已更新外部数据库状态：表%d,状态%d.",tabid,evt_type);
                break;
            }
        }
    } catch(...) {
        lgprintf("更新外部数据异常，忽略.");
    }
    return 1;
}

int SysParam::log(int tabid,int datapartid,int evt_type,const char *format,...)
{
    char msg[LOG_MSG_LEN+MAX_STMT_LEN];
#ifdef __unix
    va_list vlist;
#else
    va_list vlist;
#endif
    va_start(vlist,format);
    int rt=vsprintf(msg,format,vlist);
    va_end(vlist);
    if(strlen(msg)>LOG_MSG_LEN) // truncate log to maximum length
        msg[LOG_MSG_LEN]=0;

    // 获取通知类型
    int notifystatus = 0;
    AutoMt st_et(dts,10);
    st_et.FetchFirst("select eventlevel from dp.dp_eventtype where eventtypeid = %d",evt_type);
    if(st_et.Wait()>0) {
        switch(st_et.GetInt("eventlevel",0)) {
            case DUMP_LOG:             // 导出正常处理日志
            case MLOAD_LOG:            // 整理正常处理日志
            case LOAD_LOG:             // 装入正常处理日志
                notifystatus=DO_NOT_NEED_TO_NOTIFY;   // 不需要通知
                break;

            case DUMP_WARNING:         // 导出警告
            case MLOAD_WARNING:        // 整理警告
            case LOAD_WARNING:         // 装入警告
            case DUMP_ERROR:           // 导出错误
            case MLOAD_ERROR:          // 整理错误
            case LOAD_ERROR:           // 装入错误
                notifystatus=WAIT_TO_NOTIFY;         // 等待通知
                break;

            default:
                lgprintf("告警事件级别获取失败，错误的事件类型[%d].",evt_type);
                break;
        }
    }
    //st_et.Clear();

    AutoStmt st_log(dts);
    // add to bypass mysql5.1 report error string in sql
    st_log.Prepare("insert into dp.dp_log(`tabid`,`datapartid`,`evt_tm`,`evt_tp`,`event`,`notifystatus`) "
                   " values (%d,%d,now(),%d,?,%d)",tabid,datapartid,evt_type,notifystatus);
    st_log.BindStr(1,msg,strlen(msg));

    st_log.Execute(1);
    st_log.Wait();
    return rt;
}

//索引数据表清空,但保留表结构和索引结构
bool SysAdmin::EmptyIndex(int tabid)
{
    AutoMt indexmt(dts,MAX_DST_INDEX_NUM);
    indexmt.FetchAll("select databasename from dp.dp_table where tabid=%d",tabid);
    int rn=indexmt.Wait();
    char dbn[150];
    char tbname[150],idxtbn[150];
    strcpy(dbn,indexmt.PtrStr("databasename",0));
    if(rn<1) ThrowWith("清空索引表时找不到%d目标表.",tabid);
    indexmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    rn=indexmt.Wait();
    if(rn<1) ThrowWith("清空索引表时找不到独立索引记录(%d目标表).",tabid);
    char sqlbf[MAX_STMT_LEN];
    for(int i=0; i<rn; i++) {
        int ilp=0;
        while(GetTableName(tabid,indexmt.GetInt("indexgid",i),tbname,idxtbn,TBNAME_DEST,ilp++)) {
            lgprintf("清空索引表%s...",idxtbn);
            sprintf(sqlbf,"truncate table %s",idxtbn);
            DoQuery(sqlbf);
        }
    }
    return true;
}

//>> Begin: DMA-451,after prepare
#define COLUMN_CRYPTO_FLAG   1               // 数据库中加密字段判断标志: dp.dp_field_expand.expand_id
bool SysAdmin::AdjustCryptoColumn(int stmt,int tabid)
{
    if(stCryptoAssist.valid == CRYPTO_NOT_SUPPORT) {
        return true;
    }
    if(stCryptoAssist.tabid == tabid && stCryptoAssist.valid == CRYPTO_NO_COLUMN) {
        return true;
    }

    bool colTypeError = false;  // 错误的字段类型
    try {
        //>> 第一步: 获取对应表所有的加密列信息,从dp.dp_column_info表中
        int rn = 0,colnum = 0;
        colnum = wociGetStmtColumnNum(stmt);
        if( stCryptoAssist.tabid != tabid ) {
            stCryptoAssist.tabid = tabid;

            dt_crypto.FetchFirst("select column_name from dp.dp_column_info a, dp.dp_field_expand b "
                                 " where a.table_id = %d and a.expand_id = b.expand_id and b.expand_id = %d",
                                 tabid,COLUMN_CRYPTO_FLAG);
            rn = dt_crypto.Wait();
            if(rn == 0 || colnum == 0) {
                stCryptoAssist.valid = CRYPTO_NO_COLUMN ;
                return false;
            }
            stCryptoAssist.valid = CRYPTO_HAS_COLUMN;
        } else {
            rn = dt_crypto.GetRows();  // 记录函数
        }


        //>> 第二步: 获取stmt中所有列与加密列比较，如果存在相同的列(即:需要加密)，则修改stmt中指定加密列的宽度
        IOpensslUtility::DesCryptoRandomKeyHelper objCrypto;
        char rn_col_name[250];
        char stmt_col_name[250];
        for(int i=0; i<rn; i++) {
            strcpy(rn_col_name,dt_crypto.PtrStr("column_name",i));
            for(int j=0; j<colnum; j++) {
                wociGetStmtColumnName(stmt,j,stmt_col_name);
                if(strcasecmp(rn_col_name,stmt_col_name) == 0) {
                    // 判断数据类型是否正确，目前只支持字符串类型数据
                    if( wociGetStmtColumnType(stmt,j) != SQLT_CHR && wociGetStmtColumnType(stmt,j) != SQLT_STR &&
                        wociGetStmtColumnType(stmt,j) != SQLT_AFC && wociGetStmtColumnType(stmt,j) != SQLT_AVC ) {
                        colTypeError = true;
                        ThrowWith("自动调整加密列宽度,表(%d),字段(name = %s,col = %d,type = %d) 不支持加密!",
                                  tabid,rn_col_name,j,wociGetStmtColumnType(stmt,j));
                    }

                    int cipherLen = objCrypto.GetEncryedLenFromSourceLen(wociGetStmtColumnSize(stmt,j));
                    wociSetStmtColumnSize(stmt,j,cipherLen);
                    break;
                }
            }
        }
        return true;
    } catch(...) {
        lgprintf("自动调整设置加密列宽度AdjustCryptoColumn(tabid:%d)错误.",tabid);
        stCryptoAssist.valid = CRYPTO_NOT_SUPPORT;
        if(colTypeError) {
            throw;
        }
    }

    return false;
}

// 密钥存储结构，服务端加密的密钥，数据来源:dp.dp_keysrouce.keyInfo 进行base64解密即可
typedef struct StruCryptoKeySrc {
    int key_type;
    int key_src_len;
    unsigned char *key_src;
    StruCryptoKeySrc():key_type(-1),key_src_len(-1),key_src(NULL) {};
    ~StruCryptoKeySrc()
    {
        if(key_src != NULL) {
            delete [] key_src;
            key_src = NULL;
        }
    }
} StruCryptoKeySrc,*StruCryptoKeySrcPtr;
StruCryptoKeySrc g_stCryptoKeySrc;                 // 密钥信息

// 对已经准备数据的mt进行判断加密,如果存在加密字段就对其加密，否则就不加密
bool SysAdmin::CryptoMtColumn(AutoMt* mem_mt,int tabid)
{
    if(stCryptoAssist.valid == CRYPTO_NOT_SUPPORT) {
        return true;  // 不支持加密
    }
    if(stCryptoAssist.tabid == tabid && stCryptoAssist.valid == CRYPTO_NO_COLUMN) {
        return true;   // 该表没有加密列
    }
    int rn = 0,colnum = 0;
    try {
        //>> 第一步: 获取对应表所有的加密列信息,从dp.dp_column_info表中
        colnum = mem_mt->GetColumnNum();
        if( stCryptoAssist.tabid != tabid ) {
            stCryptoAssist.tabid = tabid;
            dt_crypto.FetchFirst("select column_name from dp.dp_column_info a, dp.dp_field_expand b "
                                 " where a.table_id = %d and a.expand_id = b.expand_id and b.expand_id = %d",
                                 tabid,COLUMN_CRYPTO_FLAG);
            rn = dt_crypto.Wait();
            if(rn == 0 || colnum == 0) {
                stCryptoAssist.valid = CRYPTO_NO_COLUMN ;
                return false;
            }
            stCryptoAssist.valid = CRYPTO_HAS_COLUMN;
        } else {
            rn = dt_crypto.GetRows();  // 记录函数
        }
    } catch(...) {
        stCryptoAssist.valid = CRYPTO_NOT_SUPPORT;
        lgprintf("加密MT失败CryptoMtColumn(tabid:%d),获取加密列错误.",tabid);
    }

    try {
        //>> 第二步: 从数据库中获取服务端加密的密钥，如果已经获取过将不再进行获取
        {
            if(g_stCryptoKeySrc.key_src_len < 0 && g_stCryptoKeySrc.key_type < 0) {
                if(NULL != g_stCryptoKeySrc.key_src) {
                    delete [] g_stCryptoKeySrc.key_src;
                    g_stCryptoKeySrc.key_src = NULL;
                }

                // 加载对象
                AutoMt mt(GetDTS(),10);
                mt.FetchFirst("select keytype,keylen,keysrclen,keyInfo from dp.dp_keysource limit 5");
                if(mt.Wait()>0) {
                    IOpensslUtility::Base64 objBase64;

                    int keysrclen = mt.GetInt("keysrclen",0);
                    int keybuffLen = objBase64.GetBytesNumFromB64(mt.GetInt("keylen",0));
                    g_stCryptoKeySrc.key_src = new unsigned char[keybuffLen+1];
                    g_stCryptoKeySrc.key_src_len = keysrclen;
                    g_stCryptoKeySrc.key_type =  mt.GetInt("keytype",0);

                    // 解析密钥，将密钥从base64转换成字节
                    unsigned int RetLen = 0;
                    int ret = 0;
                    ret = objBase64.B64ToBytes(mt.PtrStr("keyInfo",0),mt.GetInt("keylen",0),g_stCryptoKeySrc.key_src,keybuffLen,&RetLen);
                    if(0 != ret || RetLen != keysrclen) {
                        ThrowWith("数据库中存在的密钥长度不对，无法进行加密操作.");
                    }
                } else {
                    ThrowWith("数据库中不存在密钥，无法进行加密操作.");
                }
            }
        }

        //>> 第三步: 设置加密对象的密钥信息
        IOpensslUtility::DesCryptoRandomKeyHelper objCrypto;
        objCrypto.DumpKeysSourceFile(g_stCryptoKeySrc.key_src,g_stCryptoKeySrc.key_src_len);

        //>> 第四步: 对指定的列进行加密
        char rn_col_name[250],mt_col_name[250];
        for(int i=0; i<rn; i++) {
            strcpy(rn_col_name,dt_crypto.PtrStr("column_name",i));
            for(int colIdx=0; colIdx<colnum; colIdx++) {
                wociGetColumnName(*mem_mt,colIdx,mt_col_name);
                if(strcasecmp(rn_col_name,mt_col_name) == 0) {
                    // 判断数据类型是否正确，目前只支持字符串类型数据
                    int _type = mem_mt->getColType(colIdx);
                    if( _type != SQLT_CHR && _type != SQLT_STR && _type != SQLT_AFC && _type != SQLT_AVC ) {
                        ThrowWith("自动调整加密列宽度,表(%d),字段(name = %s,col = %d,type = %d) 不支持加密!",
                                  tabid,rn_col_name,colIdx,_type);
                    }

                    // 该字段存在加密，对所有的行的该列进行加密操作
                    int rn_mt = mem_mt->GetRows();

                    // 目前只能够加密CHAR,VARCHAR,STR类型的数据
                    char * cipherCode = NULL;
                    int cipherBufLen = mem_mt->getColLen(colIdx);
                    cipherCode = new char[cipherBufLen+1];
                    unsigned int cipherRetLen = 0;

                    // 对所有行的该列加密
                    for(int crypto_rn_idx = 0; crypto_rn_idx<rn_mt; crypto_rn_idx++) {
                        if(strlen(mem_mt->PtrStr(mt_col_name,crypto_rn_idx)) < 1)
                            continue;

                        int ret = objCrypto.Encrypt(mem_mt->PtrStr(mt_col_name,crypto_rn_idx),
                                                    strlen(mem_mt->PtrStr(mt_col_name,crypto_rn_idx)),
                                                    cipherCode,cipherBufLen,&cipherRetLen);

                        if(0 == ret) {
                            if(cipherRetLen < cipherBufLen) {
                                //  strcpy(mem_mt->PtrStr(mt_col_name,crypto_rn_idx),cipherCode);
                                mem_mt->SetStr(mt_col_name,crypto_rn_idx,cipherCode);
                            } else {
                                ThrowWith("表(%d),字段:%s,加密后长度(%d)超过数据库存储长度(%d).",
                                          tabid,mt_col_name,cipherRetLen,mem_mt->getColLen(colIdx));
                            }
                        } else {
                            ThrowWith("表(%d),字段:%s,加密失败，返回值:%d.",tabid,mt_col_name,ret);
                        }
                    }
                    if(NULL != cipherCode) {
                        delete [] cipherCode;
                        cipherCode = NULL;
                    }
                }
            }
        }
        return true;
    } catch(...) {
        lgprintf("加密MT失败CryptoMtColumn(tabid:%d)错误.",tabid);
        throw;
    }

    return false;
}


//<< End: DMA-451

//返回的mt只能操作内存表,不能操作数据库(fetch),因为语句对象在函数退出时已经释放
int SysAdmin::BuildMtFromSrcTable(int srcsys,int tabid,AutoMt *mt)
{
    int tabp=wociSearchIK(dt_table,tabid);
    const char *srctbn=dt_table.PtrStr("srctabname",tabp);
    AutoStmt srcst(srcsys);
    srcst.Prepare("select * from %s.%s",dt_table.PtrStr("srcowner",tabp),
                  srctbn);

    //>>Begin: fix DMA-451
    AdjustCryptoColumn(srcst,tabid);
    //<<End:fix DMA-451

    wociBuildStmt(*mt,srcst,mt->GetMaxRows());
#ifdef MYSQL_VER_51
    int colct=mt->GetColumnNum();
#define WDBI_DEFAULT_NUMLEN 16
#define WDBI_DEFAULT_SCALE  3
    for(int ci=0; ci<colct; ci++) {
        if(wociGetColumnType(*mt,ci)==COLUMN_TYPE_NUM) {
            if(wociGetColumnDataLenByPos(*mt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(*mt,ci)==WDBI_DEFAULT_SCALE) {
                AutoMt mtc(dts,100);
                char coln[200];
                wociGetColumnName(*mt,ci,coln);
                mtc.FetchAll("select prec,scale from dp.dp_columndef where tabid=%d and upper(columnname)=upper('%s')",tabid,coln);
                if(mtc.Wait()>0) {
                    wociSetColumnDisplay(*mt,NULL,ci,coln,mtc.GetInt("scale",0),mtc.GetInt("prec",0));
                    continue;
                }
            }
        }
    }
#endif
    return 0;
}

int SysAdmin::GetSrcTableStructMt(int tabp, int srcsys)
{
    AutoStmt srcst(srcsys);
    srcst.Prepare("select * from %s.%s",dt_table.PtrStr("srcowner",tabp),dt_table.PtrStr("srctabname",tabp));

    //>>Begin: fix DMA-451
    AdjustCryptoColumn(srcst,dt_table.GetInt("tabid",tabp));
    //<<End:fix DMA-451

    //>> Begin: DM-230 ignore big column
    char fetch_sql[4000];
    sprintf(fetch_sql,"select * from %s.%s",dt_table.PtrStr("srcowner",tabp),dt_table.PtrStr("srctabname",tabp));
    IgnoreBigSizeColumn(srcsys,dt_table.PtrStr("srcowner",tabp),dt_table.PtrStr("srctabname",tabp),fetch_sql);
    srcst.Prepare(fetch_sql);
    //<< End:DM-230 ignore big column

    int mt=wociCreateMemTable();
    wociBuildStmt(mt,srcst,10);

    //>> Begin: DM-230 change mysql key word column name
    char cfilename[256];
    strcpy(cfilename,getenv("DATAMERGER_HOME"));
    strcat(cfilename,"/ctl/");
    strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
    ChangeMtSqlKeyWord(mt,cfilename);
    //<< End: DM-230 change mysql key word column name


#ifdef MYSQL_VER_51
    int colct=wociGetColumnNumber(mt);
#define WDBI_DEFAULT_NUMLEN 16
#define WDBI_DEFAULT_SCALE  3
    for(int ci=0; ci<colct; ci++) {
        if(wociGetColumnType(mt,ci)==COLUMN_TYPE_NUM) {
            if(wociGetColumnDataLenByPos(mt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(mt,ci)==WDBI_DEFAULT_SCALE) {
                AutoMt mtc(dts,100);
                char coln[200];
                wociGetColumnName(mt,ci,coln);
                mtc.FetchAll("select prec,scale from dp.dp_columndef where tabid=%d and upper(columnname)=upper('%s')",dt_table.GetInt("tabid",tabp),coln);
                if(mtc.Wait()>0) {
                    wociSetColumnDisplay(mt,NULL,ci,coln,mtc.GetInt("scale",0),mtc.GetInt("prec",0));
                    continue;
                }
            }
        }
    }
#endif
    return mt;
}

void Str2Lower(char *str)
{
    while(*str!=0) {
        *str=tolower(*str);
        str++;
    }
}


//>> Begin: DM-230 转换sql关键字，将其转换成下划线字段,例如:sql--->_sql
// return 1: 需要调用wdbi接口设置列名
// return 0: 不需要调用wdbi接口设置列名
int SysAdmin::ChangeColumns(char *columnName,char* MysqlKeyWordReplaceFile)
{
    FILE *fp = NULL;
    fp = fopen(MysqlKeyWordReplaceFile,"rt");
    if(fp==NULL) {
        lgprintf("Mysql替换关键字列表文件[%s]不存在，无法替换关键字列表.",MysqlKeyWordReplaceFile);
        return 0;
    }
    char lines[300];
    while(fgets(lines,300,fp)!=NULL) {
        int sl = strlen(lines);
        if(lines[sl-1]=='\n') lines[sl-1]=0;
        if(strcasecmp(lines,columnName) == 0) {
            // 替换关键字
            char _columnName[250];
            sprintf(_columnName,"`%s`",columnName);
            strcpy(columnName,_columnName);


            fclose(fp);
            fp = NULL;
            return 1;
        }
    }
    fclose(fp);
    fp=NULL;
    return 0;
}

// 修改Mt的sql关键字
bool SysAdmin::ChangeMtSqlKeyWord(int mt,char* MysqlKeyWordReplaceFile)
{
    char col_name[255];
    for(int i=0; i<wociGetColumnNumber(mt); i++) {
        wociGetColumnName(mt,i,col_name);
        if(ChangeColumns(col_name,MysqlKeyWordReplaceFile)) {
            // 存在需要替换的列
            wociSetColumnName(mt,i,col_name);
        }
    }
    return true;
}
//>> End: DM-230 转换sql关键字，将其转换成下划线字段,例如:sql--->_sql


//>> Begin: DM-230 忽略大字段列，合成新的采集sql语句
void SysAdmin::IgnoreBigSizeColumn(int dts,char* dp_datapart_extsql)
{
    // 获取源表列属性信息
    AutoStmt table_stmt(dts);
    Str2Lower(dp_datapart_extsql);
    table_stmt.Prepare(dp_datapart_extsql);
    int colct = wociGetStmtColumnNum(table_stmt);
    bool ExistBigCol = false;
    int colType = 0;
    for(int i=0; i<colct; i++) {
        colType = wociGetStmtColumnType(table_stmt,i);
        if( (colType == SQLT_CLOB) || (colType == SQLT_BLOB) ||
            (colType == SQLT_BFILEE) || (colType == SQLT_CFILEE) ||
            (colType == SQLT_BIN) || (colType == SQLT_LVB) ||
            (colType == SQLT_LBI)) {
            ExistBigCol = true;
            break;
        }
    }

    // 获取sql中第一个from后面的字符串
    char sqlAfterFirstFrom[3000];
    char *p = strstr(dp_datapart_extsql,"from");
    strcpy(sqlAfterFirstFrom,p);

    if(ExistBigCol) {
        char colName[COLNAME_LEN] = {0};
        char extsql[4000] = {0};
        strcpy(extsql,"select ");
        for(int i=0; i<colct; i++) {
            wociGetStmtColumnName(table_stmt,i,colName);
            bool has_valid_column = false;
            switch(wociGetStmtColumnType(table_stmt,i)) {
                case SQLT_CLOB:
                case SQLT_BLOB:
                case SQLT_BFILEE:
                case SQLT_CFILEE:
                case SQLT_BIN:
                case SQLT_LVB:
                case SQLT_LBI:       // LONG RAW ,FIX DM-252
                    break;
                default:
                    if(has_valid_column) {
                        strcat(extsql,",");
                    }
                    strcat(extsql,colName);
                    has_valid_column = true;
                    break;
            }
        }

        sprintf(dp_datapart_extsql,"%s %s",extsql,sqlAfterFirstFrom);
    }
}
int SysAdmin::IgnoreBigSizeColumn(int dts,const char* dbname,const char* tbname,char* dp_datapart_extsql)
{
    // 获取源表列属性信息
    AutoStmt table_stmt(dts);
    table_stmt.Prepare("select * from %s.%s",dbname,tbname);
    int colct = wociGetStmtColumnNum(table_stmt);
    bool ExistBigCol = false;
    int colType = 0;
    for(int i=0; i<colct; i++) {
        colType = wociGetStmtColumnType(table_stmt,i);
        if( (colType == SQLT_CLOB) || (colType == SQLT_BLOB) ||
            (colType == SQLT_BFILEE) || (colType == SQLT_CFILEE) ||
            (colType == SQLT_BIN) || (colType == SQLT_LVB)||
            (colType == SQLT_LBI)) {
            ExistBigCol = true;
            break;
        }
    }

    if(ExistBigCol) {
        char colName[COLNAME_LEN] = {0};
        char extsql[4000] = {0};
        strcpy(extsql,"select ");
        for(int i=0; i<colct; i++) {
            wociGetStmtColumnName(table_stmt,i,colName);
            bool has_valid_column = false;
            switch(wociGetStmtColumnType(table_stmt,i)) {
                case SQLT_CLOB:
                case SQLT_BLOB:
                case SQLT_BFILEE:
                case SQLT_CFILEE:
                case SQLT_BIN:
                case SQLT_LVB:
                case SQLT_LBI:       // LONG RAW ,FIX DM-252
                    break;
                default:
                    if(has_valid_column) {
                        strcat(extsql,",");
                    }
                    strcat(extsql,colName);
                    has_valid_column = true;
                    break;
            }
        }

        sprintf(extsql+strlen(extsql)," from %s.%s ",dbname,tbname);
        strcpy(dp_datapart_extsql,extsql);
    }

    return 0;
}

//>> End: DM-230 忽略大字段列，合成新的采集sql语句


//>> Create column comment , Jira DMA-460 20130116
bool SysAdmin::CreateTableOnMysql(int srcmt,int tabid,char* tabname,int indexgid,bool forcecreate)
{
    //如果目标表已存在，先删除
    char sqlbf[MAX_STMT_LEN] = {0};
    bool exist=conn.TouchTable(tabname,true);
    if(exist && !forcecreate)
        ThrowWith("Create MySQL Table '%s' failed,table already exists.",tabname);
    if(exist) {
        printf("table %s has exist,dropped.\n",tabname);
        sprintf(sqlbf,"drop table %s",tabname);
        conn.DoQuery(sqlbf);
    }

    /*
    select a.table_id as tabid,a.column_name as columnname,a.expand_id as expandid,b.expand_name as expandname from
       dp.dp_column_info a,dp.dp_field_expand b where a.expand_id = b.expand_id and b.expand_save_type = 1 and a.table_id = 12222
       and a.indexgid = 1
       order by tabid,columnname,expandid
    */
    memset(sqlbf,0,MAX_STMT_LEN);
    sprintf(sqlbf,"select a.table_id as tabid,a.column_name as columnname,a.expand_id as expandid,b.expand_name as expandname from"
            " dp.dp_column_info a,dp.dp_field_expand b where a.expand_id = b.expand_id and b.expand_save_type = 1 and a.table_id = %d "
            " and a.indexgid = %d order by tabid,columnname,expandid ",tabid,indexgid);

    AutoMt mt(dts,1000);
    mt.FetchAll(sqlbf);
    int rn = mt.Wait();
    if(rn <= 0) {
        // 建立无comment备注的表
        memset(sqlbf,0,MAX_STMT_LEN);
        wociGetCreateTableSQL(srcmt,sqlbf,tabname,true);
        strcat(sqlbf," ENGINE=BRIGHTHOUSE DEFAULT CHARSET=gbk");
        //printf("%s.\n",sqlbf);
        conn.DoQuery(sqlbf);
        mSleep(300);
        return true;
    }

    // 合成表名称，列名称，列comment参数信息
    // 列属性组织格式:
    struct StruColumnCommentInfo {
        // 修改长度与wociGetCreateColumnCommentSQL函数内部同步
        char column_name[64];           // 列名称
        char column_comment[256];       // 列comment信息,以";"进行分割
        StruColumnCommentInfo()
        {
            memset(column_name,0x0,64);
            memset(column_comment,0x0,256);
        }
    };
    StruColumnCommentInfo* ptrcolumninfo = NULL;
    ptrcolumninfo = (StruColumnCommentInfo*)malloc(rn*sizeof(StruColumnCommentInfo));
    memset(ptrcolumninfo,0x0,rn*sizeof(StruColumnCommentInfo));

    int validColumnNum = 0; // 对象的有效个数
    for(int i = 0; i< rn; i ++) {
        if(strcmp(ptrcolumninfo[validColumnNum>0?validColumnNum-1:validColumnNum].column_name,mt.PtrStr("columnname",i)) != 0) {
            strcpy(ptrcolumninfo[validColumnNum].column_name,mt.PtrStr("columnname",i));
            validColumnNum++;
        }
        strcat(ptrcolumninfo[validColumnNum-1].column_comment,mt.PtrStr("expandname",i));
        strcat(ptrcolumninfo[validColumnNum-1].column_comment,",");
    }
    // comment配置末尾的逗号
    for(int i=0; i<validColumnNum; i++) {
        char *p = ptrcolumninfo[i].column_comment;
        p[strlen(p) - 1] = 0;
    }

    // 获取更新列的sql语句
    memset(sqlbf,0,MAX_STMT_LEN);
    wociGetCreateTableColumnCommentSQL(srcmt,sqlbf,tabname,ptrcolumninfo,validColumnNum,true);
    strcat(sqlbf," ENGINE=BRIGHTHOUSE DEFAULT CHARSET=gbk");
    conn.DoQuery(sqlbf);
    if(NULL != ptrcolumninfo) {
        free(ptrcolumninfo);
        ptrcolumninfo = NULL;
    }
    mSleep(300);

    return true;
}

//如果去掉字段描述文件的支持,则下面的函数会大大简化
bool SysAdmin::CreateTableOnMysql(int srcmt,const char *tabname,bool forcecreate)
{
    //如果目标表已存在，先删除
    char sqlbf[MAX_STMT_LEN];
    bool exist=conn.TouchTable(tabname,true);
    if(exist && !forcecreate)
        ThrowWith("Create MySQL Table '%s' failed,table already exists.",tabname);
    if(exist) {
        printf("table %s has exist,dropped.\n",tabname);
        sprintf(sqlbf,"drop table %s",tabname);
        conn.DoQuery(sqlbf);
    }
    //建立目标标及其表结构的描述文件
    wociGetCreateTableSQL(srcmt,sqlbf,tabname,true);
    strcat(sqlbf," ENGINE=BRIGHTHOUSE DEFAULT CHARSET=gbk");
    //printf("%s.\n",sqlbf);
    conn.DoQuery(sqlbf);
    mSleep(300);
    return true;
}
void SysAdmin::ReleaseTable()
{
    if(lastlocktable[0]!=0) {
        lgprintf("释放对表'%s'的锁定并刷新表",lastlocktable);
        connlock.DoQuery("unlock tables");
        connlock.FlushTables(lastlocktable);
        memset(lastlocktable,0,sizeof(lastlocktable));
    }
}

void SysAdmin::CloseTable(int tabid,char *tbname,bool cleandt,bool withlock)
{
    char tabname[300];
    char indextabname[300];
    indextabname[0]=0;
    //AutoStmt st(dts);
    //if(cleandt)
    //      st.Prepare("update dp.dp_table set recordnum=0,firstdatafileid=0,totalbytes=0 where tabid=%d",tabid);
    //else
    //      st.Prepare("update dp.dp_table set recordnum=0 where tabid=%d",tabid);
    //st.Execute(1);
    //st.Wait();
    //wociCommit(dts);
    if(tbname==NULL) {
        // 2010-07-18 在MySQL 5.1中，增量数据迁移或者数据二次压缩时，
        // 上线过程中lock 目标表 write,返回merge index table中的子表为只读，
        //不能加write lock的错误，造成不能上线。结合测试验证和MySQL文档中对锁机制的描述
        // http://dev.mysql.com/doc/refman/5.1/en/lock-tables.html
        // 使用merge的主索引表，可以成功做lock write,而且,加上LOW_PRIORITY选项，效果等同于
        // 目标表加锁：
        //   1.如果有读操作，lock write等待
        //   2. 如果lock write（主索引merge表)等待过程中有目标表的读操作，不会阻塞
        //   3. 一旦lock write 成功，后续对目标表的读操作被阻塞。
        AutoMt indexmt(dts,10);
        indexmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 limit 10",tabid);
        int rn=indexmt.Wait();
        if(rn>0) //ThrowWith("关闭目标表时找不到独立索引记录(目标表 %d).",tabid);
            GetTableName(tabid,indexmt.GetInt("indexgid",0),tabname,indextabname,TBNAME_DEST);
        else  GetTableName(tabid,-1,tabname,indextabname,TBNAME_DEST);
    } else strcpy(tabname,tbname);
    lgprintf("关闭'%s'表...",tabname);
    if(withlock) {
        char sql[MAX_STMT_LEN];
        // In case of a dead lock for write.
        //  A table has been locked and is not a same table ,then unlock it first

        if(lastlocktable[0]!=0 && strcmp(lastlocktable,indextabname)!=0)
            ReleaseTable();
        //The second lock of same table,will be skipped!
        if(strcmp(lastlocktable,indextabname)!=0) {
            sprintf(sql,"lock tables %s LOW_PRIORITY write ",indextabname);
            lgprintf("对'%s'表做低级锁--只有该表没有读操作，才继续上线过程...",tabname);
            connlock.DoQuery(sql);
            lgprintf("对'%s'表做低级锁--成功.",tabname);
            strcpy(lastlocktable,indextabname);
            // TODO:
            // 20100718 对目标表加锁改为对merge索引表加锁后（见前述),
            //上线过程中不能对索引表做flush table操作，即使是同一个会话也死锁，因此
            // 在这里解锁---确保后续操作开始前，没有在执行中的读操作
            ReleaseTable();
        }
    }
    {
        lgprintf("删除DP参数文件.");
        char basedir[300];
        char streamPath[300];
        char tbntmp[200];
        strcpy(basedir,GetMySQLPathName(0,"msys"));
        strcpy(tbntmp,tabname);
        char *psep=strstr(tbntmp,".");
        if(psep==NULL)
            ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
        *psep='/';
        sprintf(streamPath,"%s%s.DTP",basedir,tbntmp);
#ifdef WIN32
        _chmod(streamPath,_S_IREAD | _S_IWRITE );
        _unlink(streamPath);
#else
        unlink(streamPath);
#endif
    }
    // table has been locked,so dont flush again!
    if(strcmp(lastlocktable,indextabname)!=0 && indextabname[0]!=0)
        connlock.FlushTables(indextabname);

    lgprintf("表'%s'已清空...",tabname);
    // if withlock is true, not forgot to unlock tables!!!!!!
}

double StatMySQLTable(const char *path,const char *fulltbn)
{
    char srcf[300],fn[300];
    strcpy(fn,fulltbn);
    char *psep=strstr(fn,".");
    if(psep==NULL)
        ThrowWith("Invalid table name format'%s',should be dbname.tbname.",fn);
    *psep='/';
    struct stat st;
    double rt=0;
    // check destination directory
    sprintf(srcf,"%s%s%s",path,fn,".frm");
    stat(srcf,&st);
    rt+=st.st_size;
    sprintf(srcf,"%s%s%s",path,fn,".MYD");
    stat(srcf,&st);
    rt+=st.st_size;
    sprintf(srcf,"%s%s%s",path,fn,".MYI");
    stat(srcf,&st);
    rt+=st.st_size;
    return rt;
}

//建立表并更新复用字段值(dt_index.reusecols)
//数据上线有以下几种情况
//1. 有完整的两组数据文件,需要备份原有的表和数据记录.
//2. 上线前表中无数据,不需要备份原有的表和数据记录.
//3. 上线前后使用同一组数据文件,但要使用新的表和索引结构替换原数据,这时也不需要备份原有的表和数据记录.
//
//   有可能只是部分数据上线(一个或几个分区)
//
//  调用数据上线以前需要确保数据的完整性:
//    1.部分数据上线,则其它的数据已就绪.
//    2.全部数据上线,则所有的数据都无缺失.
void SysAdmin::DataOnLine(int tabid)
{
    char tbname[150],idxname[150];
    char tbname_p[150],idxname_p[150];
    char sql[MAX_STMT_LEN];
    bool ec=wociIsEcho();
    wociSetEcho(FALSE);
    lgprintf("%d表上线...",tabid);

    AutoMt mtp(dts,200);
//  mtp.FetchAll("select distinct datapartid,oldstatus from dp.dp_datapart where tabid=%d and status=21 and begintime<now() %s order by datapartid",tabid,GetNormalTaskDesc());
    mtp.FetchAll("select distinct datapartid,oldstatus from dp.dp_datapart where tabid=%d and status=21 and begintime<now() order by datapartid",tabid);
    int rn=mtp.Wait();
    if(rn<1)
        return ;
    AutoMt mti(dts,200);
    mti.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    AutoStmt st(dts);
    int rni=mti.Wait();
    if(rni<1)
        ThrowWith("找不到对应于%d表的任何索引。",tabid);

    double idxtbsize=0;
    int datapartid=-1;
    AutoMt mt(dts,200);
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=21 and begintime<now() limit 10",tabid);
    bool onlypart=mt.Wait()>0;
    int pi;
    try {
        CloseTable(tabid,NULL,false,true);
        // close all index tables;
        for(int idi=0; idi<rni; idi++) {
            int indexgid=mti.GetInt("indexgid",idi);
            GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST) ;
            sprintf(sql,"flush tables  %s",idxname);
            conn.DoQuery(sql);
            for(pi=0; pi<rn; pi++) {
                datapartid=mtp.GetInt("datapartid",pi);
                GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
                sprintf(sql,"flush tables  %s",idxname);
                conn.DoQuery(sql);
            }
        }
        /* lock tables ... write ,then only these locked table allowed to query,and rename table command not support on
              locked tables or none locked tables.*/
        //GetTableName(tabid,-1,tbname,idxname,TBNAME_DEST) ;
        //sprintf(sql,"lock tables %s write",tbname);
        //lgprintf("准备上线: %s.",sql);
        //conn.DoQuery(sql);
        for(pi=0; pi<rn; pi++) {
            //检查是否需要备份原有的数据,如果是重新装入,重新压缩等情况,则在同一组数据上操作,不需要备份.
            bool replace=(mtp.GetInt("oldstatus",pi)==4);
            datapartid=mtp.GetInt("datapartid",pi);
            if(replace) {
                st.DirectExecute("update dp.dp_datafilemap set fileflag=2 where tabid=%d and fileflag=0 and datapartid=%d",tabid,datapartid);
                st.DirectExecute("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=1 and datapartid=%d",tabid,datapartid);
            }
            mt.FetchAll("select sum(recordnum) rn  from dp.dp_datafilemap "
                        " where tabid=%d and fileflag=0 and isfirstindex=1 ",tabid);
            mt.Wait();
            double sumrn=mt.GetDouble("rn",0);
            for(int idi=0; idi<rni; idi++) {
                int indexgid=mti.GetInt("indexgid",idi);
                GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
                if(replace && conn.TouchTable(idxname,true)) {
                    GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_FORDELETE,-1,datapartid);
                    conn.RenameTable(idxname,idxname_p,true);
                }
                GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_PREPONL,-1,datapartid);
                //如果表不存在,则表示该分区数据为空
                if(conn.TouchTable(idxname_p,true)) {
                    conn.RenameTable(idxname_p,idxname,true);
                    idxtbsize+=StatMySQLTable(GetMySQLPathName(0,"msys"),idxname);
                } else if(sumrn>0)
                    ThrowWith("表%d的分区%d,指示记录数为%.0f,但找不到索引表'%s'",tabid,datapartid,sumrn,idxname_p);
            }
        }


        //如果只是部分分区装入,则不修改目标表.

        //下面的代码与锁表操作冲突，不能执行--修改依赖索引设置后重新装入，不能调整索引，因此，还是需要
        //  假定目标表总是已经设置好了格式，而且不需要再更改。

        if(!onlypart) {
            connlock.DoQuery("unlock tables");
            strcpy(lastlocktable,"");
            GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_FORDELETE);
            if(conn.TouchTable(tbname))
                conn.RenameTable(tbname,tbname_p,true);
            GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
            conn.RenameTable(tbname_p,tbname,true);
        }

        // 由于上面的代码不执行，需要清空预上线目标表
        //GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
        //sprintf(sql,"drop table %s",tbname_p);
        //conn.DoQuery(sql);
        //建立文件链接
        mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid ,fileid",tabid);
        rn=mt.Wait();
        int k;
        for(k=0; k<rn; k++) {
            //Build table data file link information.
            if(k+1==rn) {
                dt_file df;
                df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
                df.SetFileHeader(0,NULL);
                df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
                df.SetFileHeader(0,NULL);
            } else {
                dt_file df;
                df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
                df.SetFileHeader(0,mt.PtrStr("filename",k+1));
                df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
                df.SetFileHeader(0,mt.PtrStr("idxfname",k+1));
            }
        }

        mt.FetchAll("select sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1",tabid);
        mt.Wait();
        double sumrn=mt.GetDouble("rn",0);
        mt.FetchAll("select sum(recordnum) rn,sum(filesize) tsize,sum(idxfsize) itsize ,count(*) fnum from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) ",tabid);
        mt.Wait();
        double tsize=mt.GetDouble("tsize",0);
        double itsize=mt.GetDouble("itsize",0);
        int fnum=mt.GetDouble("fnum",0);
        mt.FetchAll("select fileid from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid ,fileid ",tabid);
        int firstfid=0;
        if(mt.Wait()<1) {
            lgprintf("装入一个空表 ，请检查数据抽取任务的配置是否有误.");
            firstfid=0;
            itsize=tsize=0;
            fnum=0;
            sumrn=0;
        } else firstfid=mt.GetInt("fileid",0);
        st.DirectExecute("update dp.dp_table set recordnum=%.0f,firstdatafileid=%d,totalbytes=%15.0f,"
                         "datafilenum=%d,cdfileid=2 where tabid=%d",
                         sumrn,firstfid,tsize,fnum,tabid);
        CreateMergeIndexTable(tabid);

        st.DirectExecute("update dp.dp_datapart set status=5 where tabid=%d and status=21",
                         tabid);
        st.DirectExecute("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0 ",
                         tabid);
        lgprintf("状态4(DestLoaded)-->5(Complete),tabid:%d.",tabid);

        BuildDTP(tbname);
        ReleaseTable();
        lgprintf("表'%s'成功上线,记录数%.0f,数据%.0f,索引%.0f. MySQL刷新...",tbname,sumrn,tsize+itsize,idxtbsize);
        wociSetEcho(ec);
        log(tabid,0,115,"表已成功上线,记录数%.0f,数据%.0f,索引%.0f. ",sumrn,tsize+itsize,idxtbsize);
    } catch(...) {
        //恢复数据文件和索引表
        //只在数据新装入时检查需要恢复的项目,重装入等情况不再需要恢复.

        //释放对表的锁，这将使前台看到空表。如果不释放，可能形成死锁。
        ReleaseTable();
        mt.FetchAll("select distinct datapartid from dp.dp_datafilemap "
                    " where fileflag=2 and tabid=%d",tabid);
        rn=mt.Wait();
        st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100,status=%d where tabid=%d and status=21 ",rn<1?30:3,tabid);
        lgprintf("任务状态已恢复为重新装入.");
        char deltbname[150];
        for(pi=0; pi<rn; pi++) {
            //恢复原有的数据.
            datapartid=mt.GetInt(0,pi);
            st.DirectExecute("update dp.dp_datafilemap set fileflag=1 where tabid=%d and fileflag=0 and datapartid=%d",tabid,datapartid);
            st.DirectExecute("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=2 and datapartid=%d",tabid,datapartid);
            for(int idi=0; idi<rni; idi++) {
                //恢复索引表
                int indexgid=mti.GetInt("indexgid",idi);
                GetTableName(tabid,indexgid,tbname_p,deltbname,TBNAME_FORDELETE,-1,datapartid);
                if(conn.TouchTable(deltbname)) {
                    GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_PREPONL,-1,datapartid);
                    GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
                    if(conn.TouchTable(idxname))
                        conn.RenameTable(idxname,idxname_p,true);
                    conn.RenameTable(deltbname,idxname,true);
                }
            }
        }

        if(!onlypart) {
            GetTableName(tabid,-1,deltbname,idxname_p,TBNAME_FORDELETE);
            if(conn.TouchTable(deltbname)) {
                GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
                if(conn.TouchTable(tbname))
                    conn.RenameTable(tbname,tbname_p,true);
                conn.RenameTable(deltbname,tbname,true);
            }
        }
        throw;
    }
}

void SysAdmin::BuildDTP(const char *tbname)
{
    lgprintf("建立DP参数文件.");
    char basedir[300];
    char streamPath[300];
    char tbntmp[200];

    strcpy(basedir,GetMySQLPathName(0,"msys"));

    dtioStream *pdtio=new dtioStreamFile(basedir);
    strcpy(tbntmp,tbname);
    char *psep=strstr(tbntmp,".");
    if(psep==NULL)
        ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
    *psep=0;
    psep++;
    sprintf(streamPath,"%s%s" PATH_SEP "%s.DTP",basedir,tbntmp,psep);
    try {
        pdtio->SetStreamName(streamPath);
        pdtio->SetWrite(false);
        pdtio->StreamWriteInit(DTP_BIND);

        pdtio->SetOutDir(basedir);
        {
            dtioDTTable dtt(tbntmp,psep,pdtio,false);
            dtt.FetchParam(dts);
            dtt.SerializeParam();
        }
        pdtio->SetWrite(true);
        pdtio->StreamWriteInit(DTP_BIND);
        pdtio->SetOutDir(basedir);
        {
            dtioDTTable dtt(tbntmp,psep,pdtio,false);
            dtt.FetchParam(dts);
            dtt.SerializeParam();
        }
        delete pdtio;
    } catch(...) {
        unlink(streamPath);
        throw;
    }
    if(lastlocktable[0]!=0)
        ReleaseTable();
    else conn.FlushTables(tbname);
}

//表名必须是全名(含数据库名)
void SysAdmin::GetPathName(char *path,const char *tbname,const char *surf)
{
    char dbname[150];
    strcpy(dbname,tbname);
    char *dot=strstr(dbname,".");
    if(dot==NULL)
        ThrowWith("表名必须是全名('%s')",tbname);
    char *mtbname=dot+1;
    *dot=0;
    const char *pathval=GetMySQLPathName(0,"msys");
    sprintf(path,"%s" PATH_SEP "%s" PATH_SEP "%s.%s",pathval,dbname,mtbname,surf);
}

//type TBNAME_DEST: destination name
//type TBNAME_PREPONL: prepare for online
//type TBNAME_FORDELETE: fordelete

//使用序号datapartoff或datapartid指定分区号,序号优先
// 如果都不指定(都为-1),则返回不带分区的索引表名称(老的格式).
bool SysAdmin::GetTableName(int tabid,int indexid,char *tbname,char *idxname,int type,int datapartoff,int datapartid)
{
    char tbname1[150],idxname1[150],dbname[130];
    idxname1[0]=0;
    if(tabid==-1)
        ThrowWith("Invalid tabid parameter on call SysAdmin::GetTableName.");
    AutoMt mt(dts,300);
    mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
    int rn=mt.Wait();
    if(rn<1)
        ThrowWith("Tabid is invalid  :%d",tabid);
    strcpy(tbname1,mt.PtrStr("tabname",0));
    strcpy(dbname,mt.PtrStr("databasename",0));
    if(indexid!=-1) {
        if(datapartoff>=0 || datapartid>0) {
            if(datapartoff>=0)
                mt.FetchAll("select datapartid from dp.dp_datapart where tabid=%d order by datapartid",tabid);
            else
                mt.FetchAll("select datapartid from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
            rn=mt.Wait();
            if(rn<1)
                ThrowWith("tabid:%d,indexid:%d,datapartoff:%d,datapartid:%d 无效.",tabid,indexid,datapartoff,datapartid);
            if(datapartoff>=0) {
                if(datapartoff>=rn) return false;
                datapartid=mt.GetInt(0,datapartoff);
            }
        }
        mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
        rn=mt.Wait();
        if(rn<1)
            ThrowWith("tabid:%d,indexid:%d 无效.",tabid,indexid);

        //返回带分区或不带分区(老格式)的索引表名称
        if(datapartid>=0)
            sprintf(idxname1,"%sidx%d_p_%d",tbname1,indexid,datapartid);
        else if(strlen(mt.PtrStr("indextabname",0))>0)
            strcpy(idxname1,mt.PtrStr("indextabname",0));
        else sprintf(idxname1,"%sidx%d",tbname1,indexid);
    }
//    if(STRICMP(idxname1,tbname1)==0)
//     ThrowWith("索引表名'%s'和目标名重复:indexid:%d,tabid:%d!",indexid,tabid);
    if(type!=TBNAME_DEST) {
        if(indexid!=-1) {
            strcat(idxname1,"_");
            strcat(idxname1,dbname);
        }
        strcat(tbname1,"_");
        strcat(tbname1,dbname);
        if(type==TBNAME_PREPONL)
            strcpy(dbname,PREPPARE_ONLINE_DBNAME);
        else if(type==TBNAME_FORDELETE)
            strcpy(dbname,FORDELETE_DBNAME);
        else ThrowWith("Invalid table name type :%d.",type);
    }
    if(tbname) sprintf(tbname,"%s.%s",dbname,tbname1);
    if(indexid!=-1 && idxname)
        sprintf(idxname,"%s.%s",dbname,idxname1);
    return true;
}

void SysAdmin::CreateAllIndex(int tabid,int nametype,bool forcecreate,int ci_type,int datapartid)
{
    AutoMt mt(dts,MAX_DST_INDEX_NUM);
    mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
    int rn=mt.Wait();
    if(rn<1)
        ThrowWith("找不到%d表的独立索引。",tabid);
    for(int i=0; i<rn; i++)
        CreateIndex(tabid,mt.GetInt("indexgid",i),nametype,forcecreate,ci_type,datapartid);
}

void SysAdmin::RepairAllIndex(int tabid,int nametype,int datapartid)
{
    AutoMt mt(dts,MAX_DST_INDEX_NUM);
    mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
    int rn=mt.Wait();
    if(rn<1)
        ThrowWith("找不到%d表的独立索引。",tabid);
    for(int i=0; i<rn; i++) {
        int indexid=mt.GetInt("indexgid",i);
        char tbname[100],idxname[100];
        //int ilp=0;
        if(GetTableName(tabid,indexid,tbname,idxname,nametype,-1,datapartid)) {
            if(conn.TouchTable(idxname)) {
                lgprintf("索引表刷新...");
                FlushTables(idxname);
                lgprintf("索引表重建:%s...",idxname);

                char fn[500],tpath[500];
                GetPathName(fn,idxname,"MYI");
                char cmdline[500];
                // -n 选项用于强制使用排序方式修复
                //mysql5.1 : windows 平台tmpdir后面的参数不能用单引号！
                strcpy(tpath,GetMySQLPathName(0,"msys"));
#ifdef WIN32
                if(tpath[strlen(tpath)-1]=='\\')
                    tpath[strlen(tpath)-1]=0;
#endif
                sprintf(cmdline,"myisamchk -rqvn --tmpdir=\"%s\" %s ",tpath,fn);
                printf(cmdline);
                int rt=system(cmdline);
                //wait(&rt);
                if(rt)
                    ThrowWith("索引重建失败!");
                FlushTables(idxname);
            }
        }
        //char sqlbf[MAX_STMT_LEN];
        //sprintf(sqlbf,"repair table %s quick",idxname);
        //conn.DoQuery(sqlbf);
    }
}

void SysAdmin::CreateIndex(int tabid,int indexid,int nametype,bool forcecreate,int ci_type,int datapartoff,int datapartid)
{
    AutoMt mt(dts,10);
    mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
    int rn=mt.Wait();
    if(rn<1)
        ThrowWith("Indexgid is invalid or not a soled :%d",indexid);
    char colsname[300];
    strcpy(colsname,mt.PtrStr("columnsname",0));
    char tbname[100],idxname[100];
    int ilp=0;
    if(GetTableName(tabid,indexid,tbname,idxname,nametype,datapartoff,datapartid)) {
        //建立独立索引：
        if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
            CreateIndex(idxname,mt.GetInt("seqinidxtab",0),colsname,forcecreate);
        if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
            CreateIndex(tbname,mt.GetInt("seqindattab",0),colsname,forcecreate);

        //建立依赖索引：
        //查找该独立索引附带的非独立索引，并以此为依据建立索引表
        mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
                    tabid,indexid);
        int srn=mt.Wait();
        for(int j=0; j<srn; j++) {
            strcpy(colsname,mt.PtrStr("columnsname",j));
            if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
                CreateIndex(idxname,mt.GetInt("seqinidxtab",j),colsname,forcecreate);
            if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
                CreateIndex(tbname,mt.GetInt("seqindattab",j),colsname,forcecreate);
        }
    }
}

void SysAdmin::CreateIndex(const char *dtname, int id, const char *colsname, bool forcecreate)
{
    //create index语句中的索引名称不允许带数据库名。
    const char *tbname=strstr(dtname,".");
    if(tbname==NULL) tbname=dtname;
    else tbname++;
    char sqlbf[MAX_STMT_LEN];
    if(forcecreate) {
        try {
            sprintf(sqlbf,"drop index %s_%d on %s",tbname,
                    id,dtname);
            conn.DoQuery(sqlbf);
        } catch(...) {};
    }
    sprintf(sqlbf,"create index %s_%d on %s(%s)",
            tbname,id,
            dtname,colsname);
    lgprintf("建立索引:%s.",sqlbf);
    conn.DoQuery(sqlbf);
}

// 数据文件状态为2：等待删除的数据全部清除，在新数据成功上线后执行。
int SysAdmin::CleanData(bool prompt,int tabid)
{
    AutoMt mt(dts,100);
    AutoStmt st(dts);
    mt.FetchAll("select tabid,indexgid,sum(recordnum) recordnum from dp.dp_datafilemap where fileflag=2 group by tabid,indexgid");
    int rn=mt.Wait();
    if(rn<1) {
        printf("没有数据需要清空!\n");
    } else {
        lgprintf("永久删除数据...");
        //for(int i=0;i<rn;i++) {
        tabid=mt.GetInt("tabid",0);
        //  int recordnum=mt.GetInt("recordnum",0);
        char tbname[150],idxname[150];
        GetTableName(tabid,-1,tbname,idxname,TBNAME_FORDELETE);
        AutoMt dtmt(dts,MAX_DST_DATAFILENUM);
        dtmt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
                      tabid);
        int frn=dtmt.Wait();
        AutoMt idxmt(dts,200);
        idxmt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
                       tabid);
        idxmt.Wait();
        //int firstdatafileid=mt.GetInt("firstdatafileid",0);
        //int srctabid=mt.GetInt("srctabid",0);
        if(prompt) {
            while(true) {
                printf("\n表 '%s': 将被删除，编号:%d,继续?(Y/N)?",
                       tbname,tabid);
                char ans[100];
                fgets(ans,100,stdin);
                if(tolower(ans[0])=='n') {
                    lgprintf("取消删除。 ");
                    return 0;
                }
                if(tolower(ans[0])=='y') break;
            }
        }

        for(int j=0; j<frn; j++) {
            lgprintf("删除'%s'和附加的depcp,dep5文件",dtmt.PtrStr("filename",j));
            unlink(dtmt.PtrStr("filename",j));
            char tmp[300];
            sprintf(tmp,"%s.depcp",dtmt.PtrStr("filename",j));
            unlink(tmp);
            sprintf(tmp,"%s.dep5",dtmt.PtrStr("filename",j));
            unlink(tmp);
            lgprintf("删除'%s'和附加的depcp,dep5文件",idxmt.PtrStr("filename",j));
            unlink(idxmt.PtrStr("filename",j));
            sprintf(tmp,"%s.depcp",idxmt.PtrStr("filename",j));
            unlink(tmp);
            sprintf(tmp,"%s.dep5",idxmt.PtrStr("filename",j));
            unlink(tmp);
        }
        st.Prepare(" delete from dp.dp_datafilemap where tabid=%d  and fileflag=2",tabid);
        st.Execute(1);
        st.Wait();
    }
    //千万不能删!!!!!!!!!!!
    //st.Prepare(" delete from dt_table where tabid=%d",tabid);
    //st.Execute(1);
    //st.Wait();
    DropDTTable(tabid,TBNAME_FORDELETE);
    lgprintf("表%d及其数据、索引冗余文件已删除.",tabid);
    return 1;
}

//带分区和不分区的索引表都删除(只要存在)
void SysAdmin::DropDTTable(int tabid,int nametype)
{
    char sqlbf[MAX_STMT_LEN];
    AutoMt mt(dts,MAX_DST_INDEX_NUM);
    char tbname[150],idxname[150];
    mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("找不到%d表的独立索引.",tabid);
    for(int i=0; i<rn; i++) {
        GetTableName(tabid,mt.GetInt("indexgid",i),tbname,idxname,nametype);
        sprintf(sqlbf,"drop table %s",idxname);
        if(conn.TouchTable(idxname))
            DoQuery(sqlbf);
        int ilp=0;
        while(GetTableName(tabid,mt.GetInt("indexgid",i),tbname,idxname,nametype,ilp++)) {
            sprintf(sqlbf,"drop table %s",idxname);
            if(conn.TouchTable(idxname))
                DoQuery(sqlbf);
        }
    }
    sprintf(sqlbf,"drop table %s",tbname);
    if(conn.TouchTable(tbname))
        DoQuery(sqlbf);
}

//
//返回不含公共字段的索引字段数
//destmt:目标表的内存表,含字段格式信息
//indexid:索引编号
int SysAdmin::CreateIndexMT(AutoMt &idxtarget,int destmt,int tabid,int indexid,int *colidx,char *colsname,bool update_idxtb)
{
    bool ec=wociIsEcho();
    wociSetEcho(FALSE);
    AutoMt idxsubmt(dts,10);
    idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
    int rn=idxsubmt.Wait();
    if(rn<1) {
        ThrowWith("建立索引结构:在dp.dp_index中无%d独立索引的记录。",indexid);
    }
    wociClear(idxtarget);
    strcpy(colsname,idxsubmt.PtrStr("columnsname",0));
    wociCopyColumnDefine(idxtarget,destmt,colsname);
    //查找该独立索引附带的非独立索引，并以此为依据建立索引表
    idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
                      tabid,indexid);
    int srn=idxsubmt.Wait();
    for(int j=0; j<srn; j++) {
        //重复的字段自动剔除
        wociCopyColumnDefine(idxtarget,destmt,idxsubmt.PtrStr("columnsname",j));
    }
    //重构索引复用段
    char reusedcols[300];
    int cn1;
    cn1=wociConvertColStrToInt(destmt,colsname,colidx);
    reusedcols[0]=0;
    int tcn=wociGetMtColumnsNum(idxtarget);
    if(tcn>cn1) {
        for(int i=cn1; i<tcn; i++) {
            if(i!=cn1) strcat(reusedcols,",");
            wociGetColumnName(idxtarget,i,reusedcols+strlen(reusedcols));
        }
        strcat(colsname,",");
        strcat(colsname,reusedcols);
    }
    if(update_idxtb) {
        lgprintf("修改索引%d的复用字段为'%s'.",indexid,reusedcols);
        AutoStmt st(dts);
        if(strlen(reusedcols)>0)
            st.Prepare("update dp.dp_index set reusecols='%s' where tabid=%d and indexgid=%d and issoledindex>0",
                       reusedcols,tabid,indexid);
        else
            st.Prepare("update dp.dp_index set reusecols=null where tabid=%d and indexgid=%d and issoledindex>0",
                       tabid,indexid);
        st.Execute(1);
        st.Wait();
    }

    //索引表公共字段
    wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
    wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
    wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
    wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
    wociAddColumn(idxtarget,"startrow",NULL,COLUMN_TYPE_INT,10,0);
    wociAddColumn(idxtarget,"idx_rownum",NULL,COLUMN_TYPE_INT,10,0);
    idxtarget.Build();
    //取独立索引和复用索引在blockmt(目标数据块内存表)结构中的位置，
    // 检查结构描述文件建立的索引是否和系统参数表中指定的字段数相同。
    int bcn=cn1;//wociConvertColStrToInt(destmt,colsname,colidx);(colsname与reusedcols的内容相同）
    bcn+=wociConvertColStrToInt(destmt,reusedcols,colidx+bcn);
    if(wociGetColumnNumber(idxtarget)!=bcn+6) {
        ThrowWith("Column number error,colnum:%d,deserved:%d",
                  wociGetColumnNumber(idxtarget),bcn+6);
    }
    //设置dt_index中的idxfieldnum
    if(update_idxtb) {
        lgprintf("修改%d索引及复用索引的字段总数为%d.",indexid,bcn);
        AutoStmt st(dts);
        st.Prepare("update dp.dp_index set idxfieldnum=%d where tabid=%d and indexgid=%d ",
                   bcn,tabid,indexid);
        st.Execute(1);
        st.Wait();
    }
    wociSetEcho(ec);
    return bcn;
}

//datapartid==-1:缺省值,表示在所有分区上建立索引表
//  !=-1: 只在指定分区上建立索引表
void SysAdmin::CreateAllIndexTable(int tabid,int destmt,int nametype,bool createidx,int ci_type,int datapartid)
{
    AutoMt mt(dts,100);
    mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
    int rn=mt.Wait();
    for(int i=0; i<rn; i++)
        CreateIndexTable(tabid,mt.GetInt("indexgid",i),-1,destmt,nametype,createidx,ci_type,datapartid);
}


void SysAdmin::CreateMergeIndexTable(int tabid)
{
    AutoMt mtidx(dts,MAX_DST_DATAFILENUM);
    mtidx.FetchAll("select indexgid from dp.dp_index where tabid=%d and issoledindex>0 order by indexgid",tabid);
    int irn=mtidx.Wait();
    AutoMt mt(dts,MAX_DST_DATAFILENUM);
    mt.FetchAll("select * from dp.dp_datapart where tabid=%d and begintime<now() order by datapartid",tabid);
    int dpn=mt.Wait();
    char tbname[300],idxname[300],sqlbf[MAX_STMT_LEN];
    for(int idx=0; idx<irn; idx++) {
        int indexid=mtidx.GetInt("indexgid",idx);
        GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST,-1,mt.GetInt("datapartid",0));
        if(!conn.TouchTable(idxname,true))
            ThrowWith("分区索引表%s不存在,无法建立目标索引表. tabid:%d,indexid:%d,datapartid:%d",
                      idxname,tabid,indexid,mt.GetInt("datapartid",0));
        AutoMt idxmt(dts,100);
        idxmt.FetchAll("select * from %s limit 10",idxname);
        idxmt.Wait();
        GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST);
        if(conn.TouchTable(idxname,true)) {
            sprintf(sqlbf,"drop table %s",idxname);
            conn.DoQuery(sqlbf);
        }
        wociGetCreateTableSQL(idxmt,sqlbf,idxname,true);
        strcat(sqlbf,"  CHARSET=gbk TYPE=MERGE UNION=( ");
        for(int part=0; part<dpn; part++) {
            GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST,-1,mt.GetInt("datapartid",part));
            //char *tbn=strstr(idxname,".");
            //tbn++;
            strcat(sqlbf,idxname);
            strcat(sqlbf,(part+1)==dpn?") INSERT_METHOD=LAST":",");
        }
        printf("\nmerge syntax :\n%s.\n",sqlbf);
        conn.DoQuery(sqlbf);
        CreateIndex(tabid,indexid,TBNAME_DEST,true,CI_IDX_ONLY,-1,-1);
    }
}

//如果indexmt为-1，则destmt必须有效。
// datapartid==-1,建立所有分区的索引
//  datapartid!=-1,只建立制定分区的索引
void SysAdmin::CreateIndexTable(int tabid,int indexid,int indexmt,int destmt,int nametype,bool createidx,int ci_type,int datapartid)
{
    AutoMt targetidxmt(dts,10);
    if(indexmt==-1) {
        int colidx[50];
        char colsname[600];
        int cn=CreateIndexMT(targetidxmt,destmt,tabid,indexid,colidx,colsname,false);
        indexmt=targetidxmt;
    }
    char tbname[300],idxname[300];
    int ilp=0;
    while(true) {
        if(datapartid==-1 && ! GetTableName(tabid,indexid,tbname,idxname,nametype,ilp++)) break;
        if(datapartid>=0)
            GetTableName(tabid,indexid,tbname,idxname,nametype,-1,datapartid);

        CreateTableOnMysql(indexmt,idxname,true);
        if(createidx) {
            //2005/12/01修改,增加创建索引表/目标表控制
            CreateIndex(tabid,indexid,nametype,true,ci_type,datapartid==-1?ilp-1:-1,datapartid);//tabname,idxtabname,0,conn,tabid,
            //indexid,true);
        }
        if(datapartid!=-1) break;
    }
}


//2006/07/03: 修改数据文件的查找方式,忽略fileflag调用参数
int SysAdmin::CreateDataMtFromFile(AutoMt &destmt,int rownum,int tabid,int fileflag)
{
    AutoMt mt(dts,10);
    mt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d and fileflag in(0,1) limit 2",tabid);
    int rn=mt.Wait();
    if(rn<1) return 0;
    dt_file idf;
    idf.Open(mt.PtrStr("filename",0),0);
    destmt.SetHandle(idf.CreateMt(rownum));
    return wociGetColumnNumber(destmt);
}

int SysAdmin::CreateIndexMtFromFile(AutoMt &indexmt,int rownum,int tabid,int indexid)
{
    AutoMt mt(dts,10);
    mt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d and indexgid=%d  and (fileflag=0 or fileflag is null) limit 2",tabid,indexid);
    int rn=mt.Wait();
    if(rn<1) ThrowWith("创建索引表结构时找不到数据文件。");
    dt_file idf;
    idf.Open(mt.PtrStr("filename",0),0);
    indexmt.SetHandle(idf.CreateMt(rownum));
    return wociGetColumnNumber(indexmt);
}

void SysAdmin::OutTaskDesc(const char *prompt,int tabid,int datapartid,int indexid)
{
    AutoMt mt(dts,100);
    char tinfo[300];
    tinfo[0]=0;
    lgprintf(prompt);
    if(datapartid>0 && tabid>0) {
        mt.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
        if(mt.Wait()<1) ThrowWith("错误的数据分组:%d->%d.",tabid,datapartid);
        lgprintf("任务描述 : %s.",mt.PtrStr("partdesc",0));
        lgprintf("数据分组 : %d.",datapartid);
        lgprintf("源系统 : %d.",mt.GetInt("srcsysid",0));
        lgprintf("数据抽取 : \n%s.",mt.PtrStr("extsql",0));
    }
    if(indexid>0 && tabid>0) {
        mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
        if(mt.Wait()<1) ThrowWith("错误的索引组号:%d->%d.",tabid,indexid);
        lgprintf("索引组号%d,索引字段: %s.",indexid,mt.PtrStr("columnsname",0));
    }
    if(tabid>0) {
        mt.FetchAll("select concat(databasename,'.',tabname) as dstdesc,srcowner,srctabname from dp.dp_table where tabid=%d",tabid);
        if(mt.Wait()>0) {
            printf("目标表 :%s,源表:%s.%s\n.",mt.PtrStr("dstdesc",0),mt.PtrStr("srcowner",0),mt.PtrStr("srctabname",0));
            lgprintf("目标表 :%s,源表:%s.%s.",mt.PtrStr("dstdesc",0),mt.PtrStr("srcowner",0),mt.PtrStr("srctabname",0));

        }
    }
}

void SysAdmin::SetTrace(const char *type,int tabid)
{
    AutoMt mt(dts,100);
    char fn[300];
    mt.FetchAll("select concat(databasename,'.',tabname) as dstdesc,srcowner,srctabname from dp.dp_table where tabid=%d",tabid);
    if(mt.Wait()>0) {
        sprintf(fn,"%s/%s_%d.",type,mt.PtrStr("dstdesc",0),getpid());
        wociSetTraceFile(fn);
    }
}
#endif
