#include "odf.h"
   
   void datablockstore::checksvrparam()
   {
   	if(ds_sn<1 || ds_id<1) 
   		ThrowWith("数据集或批次无效!");
   }
   
   void datablockstore::checkcltparam()
   {
   	if(client_id<1) 
   		ThrowWith("客户号无效!");
   }
   
   char *datablockstore::GetExtFileName(char *fn,int block_type,int &fid) {
   	char path[MAX_PATH];
   	checksvrparam();
   	AutoMt mt(*pdb,10);
   	mt.FetchAll("select seq_gen.nextval v from dual");
   	mt.Wait();
   	sprintf(fn,"%s/%d_%d_%d_%d.blk",pdb->GetDSPath(ds_id,path),ds_id,ds_sn,block_type,mt.GetInt("v",0));
   	fid=mt.GetInt("v",0);
   	return fn;
   }
   
   int datablockstore::TranslateToFileID(int type,int id)
   {
   	checksvrparam();
   	AutoMt mt(*pdb,2000);
   	mt.FetchAll("select fileid from tabt_extrec where ds_id=%d and ext_sn=%d and blocktype=%d order by fileid",ds_id,ds_sn,type);
   	int rn=mt.Wait();
   	if(id>=rn)
   	 ThrowWith("无效的文件序号%d,ds_id=%d,ds_sn=%d!",id,ds_id,ds_sn);
   	return mt.GetInt("fileid",id);
   }
   
   datablockstore::datablockstore(param_db *_pdb,filetrans *_pfs)
   {
   	pdb=_pdb;pfs=_pfs;client_id=ds_id=ds_sn=0;
   }
   
   //如果不关心客户端属性，可以置_client_id=0
   void datablockstore::setdataset(int _ds_sn,int _ds_id,int _client_id)
   {
   	ds_sn=_ds_sn;ds_id=_ds_id;client_id=_client_id;
   	pfs->SetContext(ds_id,client_id);
   }
   
   //GetExtracted block file numbers in table TABT_EXTREC
   int datablockstore::GetBlockNum(int ds_sn,int type)
   {
   	checksvrparam();
   	AutoMt mt(*pdb,10);
   	mt.FetchAll("select count(*) rn from tabt_extrec where ds_id=%d and ext_sn=%d and blocktype=%d order by fileid",ds_id,ds_sn,type);
   	int rn=mt.Wait();
   	return mt.GetInt("rn",0);
   }
   
   // id spec a sequence of current extract task(dataset).
   void datablockstore::WriteBlock(int mt,int type)
   {
   	char path[MAX_PATH];
   	int fid;
   	GetExtFileName(path,type,fid);
   	dt_file di;
	di.Open(path,1);
	di.WriteHeader(mt,wociGetMemtableRows(mt));
	int flen=di.WriteMt(mt,pdb->GetCompressFlag(ds_id));
	AutoStmt stmt(*pdb);
	stmt.DirectExecute("insert into tabt_extrec values (%d,'%s',%d,%d,%d,%d,%d,2,sysdate,NULL)",
	         fid,path,ds_id,ds_sn,type,flen,wociGetMemtableRows(mt));
   }
   
   //Check block file's error,list block file information
   bool datablockstore::CheckBlockFile(int type,int id)
   {
   	return CheckBlockFile(TranslateToFileID(type,id));
   }
   
   bool datablockstore::CheckBlockFile(int fileid)
   {
   	ThrowWith("CheckBlockFile 暂未实现!");
	return true;
   }
   
   void datablockstore::EraseBlock(int fileid) {
	AutoMt mt(*pdb,10);
	mt.FetchAll("select filename from tabt_extrec where fileid=%d and status=2",fileid);
   	if(mt.Wait()<1)
   	 ThrowWith("无效的文件号%d!",fileid);
	unlink(mt.PtrStr("filename",0));
	AutoStmt stmt(*pdb);
	stmt.DirectExecute("update tabt_extrec set status=3 where fileid=%d",fileid);
   }
   
   void datablockstore::EraseBlock(int type,int id) {
   	EraseBlock(TranslateToFileID(type,id));
   }
   
   //下面四个方法需要客户端标识
   void datablockstore::EraseLocalBlock(int type,int id) {
   	EraseLocalBlock(TranslateToFileID(type,id));
   }
   
   void datablockstore::EraseLocalBlock(int fileid) {
	AutoMt mt(*pdb,10);
	mt.FetchAll("select * from tabt_transrec where fileid=%d ",fileid);
   	if(mt.Wait()<1)
   	 ThrowWith("无效的文件号%d!",fileid);
	//原位文件不在这里删除
	if(mt.GetInt("local_status",0)!=2)
	 unlink(mt.PtrStr("local_filename",0));
	AutoStmt stmt(*pdb);
	stmt.DirectExecute("update tabt_transrec set local_status=1 where fileid=%d",fileid);
   }
   
   void datablockstore::TransferBlock(int type,int id)
   {
   	TransferBlock(TranslateToFileID(type,id));
   }
   
   void datablockstore::TransferBlock(int fileid) {
   	pfs->Transfer(fileid);	
   }
   
   void datablockstore::GetExtBlockProp(int type,int id,char *filename,int &fileid,int &flen,int &rownum) {
   	fileid=TranslateToFileID(type,id);
   	AutoMt mt(*pdb,10);
   	mt.FetchAll("select * from tabt_dataset where fileid=%d",fileid);
   	if(mt.Wait()<1)
   	 ThrowWith("无效的文件号%d!",fileid);
   	mt.GetStr("filename",0,filename);
   	flen=mt.GetInt("filesize",0);
   	rownum=mt.GetInt("blockrownum",0);
   }
   
   int datablockstore::BuildBlockMT(int type,int id)
   {
     return BuildBlockMT(TranslateToFileID(type,id));
   }
   
   int datablockstore::BuildBlockMT(int fileid)
   {
	AutoMt mt(*pdb,10);
	mt.FetchAll("select local_filename from tabt_transrec where fileid=%d and local_status!=1",fileid);
   	if(mt.Wait()<1)
   	 ThrowWith("无效的文件号%d!",fileid);
	dt_file idxf;
	idxf.Open(mt.PtrStr("local_filename",0),0);
	AutoMt idxmt(0);
	idxmt.SetHandle(idxf.CreateMt(idxf.GetRowNum()),true);
	idxf.ReadMt(0,0,idxmt,false);
	return idxmt;
   }
   
