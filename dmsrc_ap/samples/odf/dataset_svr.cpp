#include "odf.h"

   void dataset_svr::beginExt() {
	AutoStmt st(*pdb);
  	//在多个客户端公用的情况下，不只一条记录更新
  	st.DirectExecute("update tabt_dssn_client set status=2,load_status=1 where ds_sn=%d",ds_sn);//状态2 参数块和至少一个数据块已可用
  	st.DirectExecute("update tabt_dataset set ext_status=2,ext_lastrownum=0,ext_lstbegin=sysdate,ext_lstsn=%d where ds_id=%d",
  	        ds_sn,ds_id);//状态2 参数块和至少一个数据块已可用
   }

   void dataset_svr::endExt(int status,int rn) {
	AutoStmt st(*pdb);
  	//在多个客户端公用的情况下，不只一条记录更新
  	st.DirectExecute("update tabt_dssn_client set status=%d,ext_rownum=%d where ds_sn=%d",status,rn,ds_sn);//状态2 参数块和至少一个数据块已可用
  	st.DirectExecute("update tabt_dataset set ext_status=%d,ext_lastrownum=%d,ext_lstend=sysdate,ext_lstsn=%d where ds_id=%d",
  	        status,rn,ds_sn,ds_id);//状态2 参数块和至少一个数据块已可用
   }
   
   void dataset_svr::abortExt() {
	AutoStmt st(*pdb);
  	//在多个客户端公用的情况下，不只一条记录更新
  	st.DirectExecute("update tabt_dssn_client set status=4,load_status=0 where ds_sn=%d",ds_sn);//状态4 异常终止
  	st.DirectExecute("update tabt_dataset set ext_status=0,ext_lastrownum=0,ext_lstbegin=NULL where ds_id=%d",
  	        ds_id);//状态2 参数块和至少一个数据块已可用
   }
   
   //Lock and change status,if lock failed ,return false,other error throw exception;
   bool dataset_svr::lockForExt()
   {
   	AutoStmt stmt(*pdb);
   	int rn=stmt.DirectExecute("update tabt_dataset set ext_status=%d where ext_status=%d and ds_id=%d",1,0,ds_id);
   	if(rn!=1) return false;
   	return true;
   }
  
   //Lock and change status,if lock failed ,return false,other error throw exception;
   bool dataset_svr::lockForClear()
   {
   	AutoStmt stmt(*pdb);
   	int rn=stmt.DirectExecute("update tabt_dataset set ext_status=4 where ext_status=3 and ds_id=%d",ds_id);
   	//if(rn!=1) return false;
	AutoMt mt(*pdb,100);
	mt.FetchAll("select count(*) rn from tabt_dssn_client where ds_id=%d and ds_sn=%d",ds_id,ds_sn);
   	rn=mt.Wait();
	if(rn!=stmt.DirectExecute("update tabt_dssn_client set status=0 where status=3 and ds_id=%d and load_status>=4 and ds_sn=%d",ds_id,ds_sn))
   	if(rn<1) {
		stmt.DirectExecute("update tabt_dataset set ext_status=3 where ext_status=4 and ds_id=%d",ds_id);
		lgprintf("并非所有客户端都下载，数据集%d清除失败",ds_id);
		return false;
	}
	return true;
   }
   
   dataset_svr::dataset_svr(param_db *_pdb,datablockstore *_pdbs,int _ds_id)
   {
  	pdb=_pdb;pdbs=_pdbs;ds_id=_ds_id;ds_sn=0;
   }
   // return false for lock failed ,throw exception for other errors.
  bool dataset_svr::DoExtract()
  {
  	try {
  		AutoMt mt(*pdb,10);
  		if(!lockForExt()) {
  			errprintf("数据抽取时锁定失败，数据集编号%d.",ds_id);
  			return false;
  		}
  		mt.FetchAll("select seq_dssn.nextval v from dual");
  		mt.Wait();
  		ds_sn=mt.GetInt("v",0);
  		pdbs->setdataset(ds_sn,ds_id,0);
  		mt.FetchAll("select * from tabt_dataset where ds_id=%d",ds_id);
  		mt.Wait();
  		lgprintf("数据集%d -'%s' 开始提取数据，批次号%d.",ds_id,mt.PtrStr("ds_name",0),ds_sn);
  		AutoStmt st(*pdb);
  		//如果存在相同批次号记录，置为已取消状态
  		//st.DirectExecute("update tabt_dssn_client set status=4 where ds_sn=%d",ds_sn);
  		lgprintf("生成数据抽取批次%d,修改抽取状态...",ds_sn);
  		st.DirectExecute("delete from tabt_dssn_client where ds_sn=%d",ds_sn);
  		st.DirectExecute("insert into tabt_dssn_client "
  		     " select ds_id,%d,client_id,1,sysdate,sysdate,0,0 from tabt_dsbind "
  		     " where ds_id=%d order by client_id",
  		     ds_sn,ds_id);
  		AutoHandle extdbc;
  		lgprintf("建立到源系统的连接...");
   		extdbc.SetHandle(pdb->BuildSrcDBC(ds_id));
  		char extsql[SQL_LEN],paramsql[SQL_LEN];
  		int paramrl=pdb->GetParamSQL(ds_id,paramsql);
		pdb->GetExtSQL(ds_id,extsql);
  		int prn=0;
  		AutoMt parammt(*pdb,paramrl);
		lgprintf("数据抽取参数块最大行数%d.",paramrl);
  		if(paramrl>0) {
  		  lgprintf("查询数据抽取参数...");
		  parammt.FetchAll(paramsql);
  		  prn=parammt.Wait();
  		  if(prn<1)
  		   ThrowWith("参数数据无效,数据集%d !",ds_id);
  		  lgprintf("抽取参数存储...");
  		  pdbs->WriteBlock(parammt,2); //1:数据块，2：参数块
		} 
  		int brn=pdb->GetBlockRows(ds_id);
		lgprintf("数据抽取每数据块%d行,控制参数%d行.",brn,prn);
  		AutoMt extmt(0,brn);
  		AutoStmt extstmt(extdbc);
  		extstmt.Prepare(extsql);
  		extmt.Build(extstmt);
  		int trn=0;
  		int rn=0;
  		int off=0;
  		bool firstblock=true;
		bool executed=false;
		if(paramrl>0) {
  		   for(int i=0;i<prn;i++){
  		   	wociBindToStatment(parammt,extstmt,NULL,i);
			if(!executed) {
				executed=true;
				wociExecute(extstmt,0);
				extstmt.Wait();
			}

  		   	wociFetchAt(extmt,brn-off,off);
  		   	rn=extmt.Wait();
  		   	while(rn==brn-off) {
			  trn+=off+rn;
			  lgprintf("数据块存储。已取得数据%d行。",trn);
			  pdbs->WriteBlock(extmt,1); //1:数据块，2：参数块
			  if(firstblock) {
			  	firstblock=false;
			  	//设置状态
			  	beginExt();
			  }
			  off=0;
  		   	  wociFetchAt(extmt,brn-off,off);
  		   	  rn=extmt.Wait();
  		   	}
  		   	off+=rn;
		   }
		   if(off>0) {
		    pdbs->WriteBlock(extmt,1); //1:数据块，2：参数块
		    if(firstblock) {
			  	firstblock=false;
			  	//设置状态
			  	beginExt();
		    }
		    trn+=off;
		    lgprintf("数据块存储。已取得数据%d行。",trn);
		   }
		}
		else {
		  extmt.FetchFirst();
		  rn=extmt.Wait();
		  while(rn>0) {
			  if(firstblock) {
			  	firstblock=false;
			  	//设置状态
			  	beginExt();
			  }
			  trn+=rn;
			  lgprintf("数据块存储。已取得数据%d行。",trn);
			  pdbs->WriteBlock(extmt,1); //1:数据块，2：参数块
			  off=0;
  		   	  extmt.FetchNext();
  		   	  rn=extmt.Wait();
		  }
		}
		if(firstblock) //空记录；
		{
		  beginExt();
		  endExt(3,0);
		  return false;
		}
		endExt(3,trn);
  	}
  	catch(...) {
  		//恢复任务状态
  		abortExt();
  		throw;
  	}
	lgprintf("数据抽取正常结束。");
	return  true;
  }

    bool dataset_svr::DoClear(int _ds_sn)
	{
		ds_sn=_ds_sn;
		lgprintf("开始数据集清除,编号%d,批次%d.",ds_id,ds_sn);
		AutoStmt st(*pdb);
  		if(!lockForClear()) {
  			errprintf("数据清除时锁定失败，数据集编号%d,批次%d.",ds_id,ds_id);
  			return false;
  		}
		try {
			AutoMt mt(*pdb,10000);
			mt.FetchAll("select * from tabt_extrec "
				" where ds_id=%d and ext_sn=%d",ds_id,ds_sn);
			int rn=mt.Wait();
			lgprintf("删除临时抽取文件...");
			for(int i=0;i<rn;i++) 
				unlink(mt.PtrStr("filename",i));
			lgprintf("共删除%d个文件.",rn);
			st.DirectExecute("update tabt_extrec set status=3 where ds_id=%d and ext_sn=%d",
				ds_id,ds_sn);
			lgprintf("文件状态修改为已删除.");
			mt.FetchAll("select * from tabt_dataset "
				" where ds_id=%d ",ds_id);
			mt.Wait();
			if(mt.GetInt("ext_repeatsec",0)>1000) {
			 lgprintf("数据集为重复模式,修改数据抽取状态为'待抽取'.");
			 st.DirectExecute("update tabt_dataset set ext_status=0 where ds_id=%d ",ds_id);
			}
		}
		catch(...) {
			errprintf("数据集清除时出现异常(id:%d,sn:%d)，需要人工干预！",
				ds_id,ds_sn);
			return false;
		}
		lgprintf("数据集清除正常结束.");
		return true;
	}