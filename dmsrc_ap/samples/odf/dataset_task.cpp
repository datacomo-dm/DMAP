#include "odf.h"

   dataset_task::dataset_task(param_db *_pdb)
   {
   	pdb=_pdb;
   }
   // return 0 for no legal task
   // others for dataset id
   //     only check ds_id but no lock status.
   bool dataset_task::GetExtTask(int &ds_id,int &ext_status)
   {
   	AutoMt mt(*pdb,500);
//   	mt.FetchAll("select ds_id,ext_status from tabt_dataset where (ext_status=0 or ext_status=3) and nvl(ext_lstbegin,to_date('20000501','yyyymmdd'))+ext_repeatsec/3600/24<sysdate");
   	mt.FetchAll("select ds_id,ext_status,ext_repeatsec from tabt_dataset where ext_status=0  and nvl(ext_lstbegin,to_date('20000501','yyyymmdd'))+ext_repeatsec/3600/24<sysdate");
   	int rn=mt.Wait();
   	if(rn<1) return 0;
   	for(int i=0;i<rn;i++) {
   	        //if(mt.GetInt("ext_status",i)==3 && mt.GetInt("ext_repeatsec",i)==0) continue;
   		if(pdb->CheckTimeLimit(mt.GetInt("ds_id",i))) {
   			ds_id=mt.GetInt("ds_id",i);
   			ext_status=mt.GetInt("ext_status",i);
   			return true;
   		}
   	}
   	return false;
   }
   
   bool dataset_task::GetLoadTask(int client_id,int &ds_id,int &ds_sn)
   {
   	AutoMt mt(*pdb,500);
   	//检查需要装入的数据集
   	mt.FetchAll("select ds_id,ds_sn from tabt_dssn_client "
   	" where client_id=%d and load_status=1",client_id);
   	int rn=mt.Wait();
   	if(rn<1) return false;
	ds_id=mt.GetInt("ds_id",0);
   	ds_sn=mt.GetInt("ds_sn",0);
   	return true;
   }
   
   // Extract and load complete,but no 'clean' performed.
   bool dataset_task::GetCompleteTask(int &ds_id,int &ds_sn)
   {
   	AutoMt mt(*pdb,500);
   	//对应批次号的所有客户都已装入完毕。
   	mt.FetchAll("select distinct ds_id,ds_sn from tabt_dssn_client d1 where status=3 and load_status=5 and not exists ( "
   	" select * from tabt_dssn_client where ds_sn=d1.ds_sn and load_status!=5"
   	" ) ");
   	int rn=mt.Wait();
   	if(rn<1) return false;
	ds_id=mt.GetInt("ds_id",0);
   	ds_sn=mt.GetInt("ds_sn",0);
   	return true;
   }
   
   int dataset_task::GetTimeoutTask()
   {
	   return 0;
   }
   

/*
   void dataset_task::TouchLoadStatus(int client_id,int ds_sn) {
   	AutoStmt stmt(*pdb);
   	stmt.Prepare("update tabt_dssn_client set lstactive_tm=sysdate where client_id=%d and ds_sn=%d",
   	  client_id,ds_sn);
   	stmt.Execute(1);
   	stmt.Wait();
   	wociCommit(*pdb);
   }
*/
   
   bool dataset_task::GetFirstLoadStatus(int client_id,int &ds_id,int &ds_sn,int &status)
   {
   	AutoMt mt(*pdb,500);
   	mt.FetchAll("select * from tabt_dssn_client where client_id=%d and status<3",client_id);
   	int rn=mt.Wait();
   	if(rn<1) return false;
   	ds_id=mt.GetInt("ds_id",0);
   	ds_sn=mt.GetInt("ds_sn",0);
   	status=mt.GetInt("status",0);
   	return true;
   }
