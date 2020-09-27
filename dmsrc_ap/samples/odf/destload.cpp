#include "odf.h"   

   bool destload::LockForLoad() {
	AutoStmt stmt(*pdb);
   	if(stmt.DirectExecute("update tabt_dssn_client set load_status=2,lstactive_tm=sysdate where load_status=1 and client_id=%d and ds_sn=%d",client_id,ds_sn)<1) return false;
   	return true;
   }   	
   
   void destload::changeLoadStatus(int status) {
	AutoStmt stmt(*pdb);
   	stmt.DirectExecute("update tabt_dssn_client set load_status=%d,lstactive_tm=sysdate where client_id=%d and ds_sn=%d",status,client_id,ds_sn);
   }   	
   
   destload::destload(param_db *_pdb,datablockstore *_pdbs,int _client_id,int _ds_id, int _ds_sn)
   {
     pdb=_pdb;pdbs=_pdbs;client_id=_client_id;	ds_id=_ds_id;ds_sn=_ds_sn;
   }
   
   void destload::Load() {
   	try {
   		internalLoad();
   	}
   	catch(...) {
   		//异常恢复
   		lgprintf("恢复装入状态.");
   		changeLoadStatus(1);
   	}
   }
   
   void destload::internalLoad()
   {
	lgprintf("开始数据装入,批次号%d,数据集号%d。",ds_sn,ds_id);
 	char sql[SQL_LEN];
 	pdbs->setdataset(ds_sn,ds_id,client_id);
 	int pbn=pdbs->GetBlockNum(ds_sn,2);
	int bn=pdbs->GetBlockNum(ds_sn,1);
	if(bn<1)
	  ThrowWith("找不到数据文件记录,批次号%d,数据集号%d!",ds_sn,ds_id);
	if(!LockForLoad())
	  return ;
	lgprintf("数据装入:参数文件%d个,数据文件%d个.",pbn,bn);

	lgprintf("数据装入:参数文件和第一个数据文件...",pbn,bn);
    if(pbn>0) pdbs->TransferBlock(2,0);
	pdbs->TransferBlock(1,0);
	int transfered=1;
	AutoMt blkmt(0,1);
	blkmt.SetHandle(pdbs->BuildBlockMT(1,0));

   	AutoMt mt(*pdb,10);
   	mt.FetchAll("select * from tabt_dsbind where ds_id=%d and client_id=%d",ds_id,client_id);
   	mt.Wait();
   	AutoHandle dstdbc;
   	dstdbc.SetHandle(pdb->BuildDstDBC(ds_id,client_id));
   	
   	//检查是否需要建立目标表
   	char *dest_tab=mt.PtrStr("dest_tab_name",0);
   	lgprintf("数据装入:目标表'%s'.",dest_tab);
    if(!wociTestTable(dstdbc,dest_tab)) {
   	    lgprintf("数据装入:目标表不存在，需要创建...");
   		bool deny_create=false;
   		bool isdefined=pdb->GetCreateTabSQL(ds_id,client_id,deny_create,sql);
   		if(deny_create) 
   			ThrowWith("在目标系统中表'%s'不存在，但不允许自动建表!",dest_tab);
   		if(isdefined) {
   			AutoStmt stmt(dstdbc);
   			stmt.DirectExecute(sql);
   		}
   		else
   		  wociGeneTable(blkmt,dest_tab,dstdbc);
   		//目前仅支持单次索引建立
   		if(pdb->GetIndexSQL(ds_id,client_id,sql)) {
   	        lgprintf("数据装入:创建索引...");
   			AutoStmt stmt(dstdbc);
   			stmt.DirectExecute(sql);
   		}
   	}
   	
   	//数据装入前的清理准备工作
   	bool useparamblock=false;
   	bool repeat;
   	bool exists=pdb->GetClearSQL(ds_id,client_id,useparamblock,repeat,sql);
   	if(exists) {
   	    lgprintf("数据装入:开始装入前的数据清理工作...");
   		if(useparamblock && pbn<1)
   		  ThrowWith("清除数据需要使用参数文件，但服务器未生成。数据集%d,客户%d.",ds_id,client_id);
   		if(useparamblock) {
   			AutoMt pmt(0,1);
   			pmt.SetHandle(pdbs->BuildBlockMT(2,0));
   			AutoStmt stmt(dstdbc);
   			stmt.Prepare(sql);
   			int prn=wociGetMemtableRows(pmt);
   			while(true) {
   			    wociBindToStatment(pmt,stmt,NULL,0);
   				stmt.Execute(prn);
				int rn=stmt.Wait();
				wociCommit(dstdbc);
   				if(rn<1 || !repeat) break;
   			}
	 		pdbs->EraseLocalBlock(2,0);
   	        lgprintf("数据装入:清理结束，参数文件已删除.");
   		}
   		else {
   	        lgprintf("数据装入:开始装入前的数据清理工作(无参数).");
   			AutoStmt stmt(dstdbc);
   			stmt.Prepare(sql);
   			while(true) {
   				stmt.Execute(1);
   				if(stmt.Wait()<1 || !repeat) break;
   			}
   	        lgprintf("数据装入:清理结束(无参数).");
		}   			
	}
	
   	lgprintf("数据装入:开始数据装入...");
	changeLoadStatus(3);
	//开始数据装入
	int loadedrn=0;
	int toloadid=0;
	while(true) {
	 int est=pdb->GetExtStatus(ds_id);
	 if(est!=2 && est!=3)
   		ThrowWith("服务器数据抽取异常终止,已装入%d行数据。数据集%d,客户%d.",loadedrn,ds_id,client_id);
   	 bn=pdbs->GetBlockNum(ds_sn,1);
	 if(toloadid<transfered) {
		lgprintf("装入...");
	 	wociAppendToDbTable(blkmt,dest_tab,dstdbc,true);//自动提交
		loadedrn+=wociGetMemtableRows(blkmt);
		lgprintf("已装入%d个文件(%d行),已抽取文件数%d.",transfered,loadedrn,bn);
	 	pdbs->EraseLocalBlock(1,toloadid);
	 	toloadid++;
	 }
	 if(est==3 && toloadid==bn) {
	 	break;
	 }
	 if(toloadid<bn) {
		pdbs->TransferBlock(1,toloadid);
		transfered++;
		blkmt.SetHandle(pdbs->BuildBlockMT(1,toloadid));
	 }
	 else {
	 	printf("等待服务端生成数据...");
	 	mSleep(120000);
	 }
	}
	changeLoadStatus(5); //4: 装入完成，临时数据已删除
 	lgprintf("数据装入正常结束.");
   }
