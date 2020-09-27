/* Copyright (C) 2000 MySQL AB, portions Copyright (C) 2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License version 2.0 for more details.

You should have received a copy of the GNU General Public License
version 2.0 along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA */

#include "handler/BrighthouseHandler.h"
#include "core/RCTableImpl.h"
#include "loader/NewValuesSetRows.h"
#include "loader/RCTable_load.h"

using namespace std;

//char infobright_version[] = "Infobright Community Edition";
char infobright_version[] = "DataMerger(A)";

handler *rcbase_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);
int rcbase_panic_func(handlerton *hton, enum ha_panic_function flag);
int rcbase_close_connection(handlerton *hton, THD* thd);
int rcbase_commit(handlerton *hton, THD *thd, mysql_bool all);
int rcbase_rollback(handlerton *hton, THD *thd, mysql_bool all);
uchar* rcbase_get_key(RCBASE_SHARE *share, size_t *length, my_bool not_used __attribute__((unused)));
bool rcbase_show_status(handlerton *hton, THD *thd, stat_print_fn *print, enum ha_stat_type stat);

extern my_bool infobright_bootstrap;
extern HASH rcbase_open_tables; // Hash used to track open tables

ulonglong
BrighthouseHandler::table_flags() const
{
        return HA_REC_NOT_IN_SEQ | HA_PARTIAL_COLUMN_READ; 
}

int rcbase_init_func(void *p)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	rcbase_hton= (handlerton *)p;
	//VOID(pthread_mutex_init(&rcbase_mutex, MY_MUTEX_INIT_FAST));
	(void) hash_init(&rcbase_open_tables, system_charset_info, 32, 0, 0, (hash_get_key) rcbase_get_key, 0, 0);

	rcbase_hton->state = SHOW_OPTION_YES;
	rcbase_hton->db_type = DB_TYPE_BRIGHTHOUSE_DB;
	rcbase_hton->create = rcbase_create_handler;
	rcbase_hton->flags = HTON_NO_FLAGS;
	rcbase_hton->flags |= HTON_ALTER_NOT_SUPPORTED;
	rcbase_hton->panic = rcbase_panic_func;
	rcbase_hton->close_connection = rcbase_close_connection;
	rcbase_hton->commit = rcbase_commit;
	rcbase_hton->rollback = rcbase_rollback;
	rcbase_hton->show_status = rcbase_show_status;
	
	// When mysqld runs as bootstrap mode, we do not need to initialize memmanager.
	if (infobright_bootstrap)
		DBUG_RETURN(0);

	int ret = 1;
	rceng = NULL;

	try {
		rceng = new RCEngine();
		ret = rceng->Init(ha_rcbase_exts[0], total_ha);
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	//FIX: Can not be deleted right now. Some internal variables need to
	// deleted properly before deleting rceng.
	//if (ret ==1 && rceng != NULL)
	//	delete rceng;

	DBUG_RETURN(ret);
}

/*
 The idea with handler::store_lock() is the following:

 The statement decided which locks we should need for the table
 for updates/deletes/inserts we get WRITE locks, for SELECT... we get
 read locks.

 Before adding the lock into the table lock handler (see thr_lock.c)
 mysqld calls store lock with the requested locks.  Store lock can now
 modify a write lock to a read lock (or some other lock), ignore the
 lock (if we don't want to use MySQL table locks at all) or add locks
 for many tables (like we do when we are using a MERGE handler).

 Berkeley DB for example  changes all WRITE locks to TL_WRITE_ALLOW_WRITE
 (which signals that we are doing WRITES, but we are still allowing other
 reader's and writer's.

 When releasing locks, store_lock() are also called. In this case one
 usually doesn't have to do anything.

 In some exceptional cases MySQL may send a request for a TL_IGNORE;
 This means that we are requesting the same lock as last time and this
 should also be ignored. (This may happen when someone does a flush
 table when we have opened a part of the tables, in which case mysqld
 closes and reopens the tables and tries to get the same locks at last
 time).  In the future we will probably try to remove this.

 Called from lock.cc by get_lock_data().
 */
THR_LOCK_DATA **BrighthouseHandler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
	if(lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK)
		m_lock.type = lock_type;

	*to++= &m_lock;
	return to;
}

/*
 First you should go read the section "locking functions for mysql" in
 lock.cc to understand this.
 This create a lock on the table. If you are implementing a storage engine
 that can handle transacations look at ha_berkely.cc to see how you will
 want to goo about doing this. Otherwise you should consider calling flock()
 here.

 Called from lock.cc by lock_external() and unlock_external(). Also called
 from sql_table.cc by copy_data_between_tables().
 */
int BrighthouseHandler::external_lock(THD* thd, int lock_type)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	if (thd->lex->sql_command == SQLCOM_LOCK_TABLES) {
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
	try {
		SetConnectInfo(rceng->GetThreadParams(*thd));
		if(lock_type == F_UNLCK) { // RELEASE LOCK
			m_rctable = NULL;
			if (thd->lex->sql_command == SQLCOM_UNLOCK_TABLES)
				m_conn->GetTransaction()->ExplicitUnlockTables();

			// Rollback the transaction if operation finished with error
			if(thd->killed)
				rceng->Rollback(thd, true);
			table_lock_manager.ReleaseLockIfNeeded(table_lock, !(thd->options & OPTION_NOT_AUTOCOMMIT), thd->lex->sql_command == SQLCOM_ALTER_TABLE);

		} else {
			if (thd->lex->sql_command == SQLCOM_LOCK_TABLES)
				m_conn->GetTransaction()->ExplicitLockTables();
			if(lock_type == F_RDLCK) { // READ LOCK
				if((table_lock = table_lock_manager.AcquireReadLock(*m_conn->GetTransaction(), m_table_path, thd->killed, thd->some_tables_deleted)).expired())
					DBUG_RETURN(1);
			} else {
				if((table_lock = table_lock_manager.AcquireWriteLock( *m_conn->GetTransaction(), string(m_table_path), thd->killed, thd->some_tables_deleted)).expired())
					DBUG_RETURN(1);
				trans_register_ha(thd, true, rcbase_hton);
				trans_register_ha(thd, false, rcbase_hton);
				if (thd->lex->sql_command != SQLCOM_ALTER_TABLE) {
					m_conn->GetTransaction()->AddTableWR(m_table_path, thd, table);
				}
			}
		}

	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(0);
}

class WriteRowsStore {
	struct WriteSession {
		NewValuesSetBase ** nvs;
		_int64 rows_writed;
		int num_fields;
		RCTableLoad *loadtable;
		WriteSession(int fields) {
			num_fields=fields;
			loadtable=NULL; rows_writed=0;
			nvs =new NewValuesSetBase *[num_fields];
			memset(nvs,0,sizeof(NewValuesSetBase *)*num_fields);
		}
		void ClearAttrs() {
			for(int i=0;i<num_fields;i++)
				if(nvs[i])
				 delete nvs[i];
			memset(nvs,0,sizeof(NewValuesSetBase *)*num_fields);
			rows_writed=0;
		}
		~WriteSession() {
			if(loadtable)
				delete loadtable;
			for(int i=0;i<num_fields;i++)
				if(nvs[i])
				 delete nvs[i];
			delete []nvs;
		}
	};
			
	static WriteRowsStore *obj;
	// sessionid ,load object:
	std::map<int,WriteSession *> m_nvs;

	WriteRowsStore() {
	}
	
 	~WriteRowsStore() {
		m_nvs.clear();
	}
			
public :
	
	static WriteRowsStore *GetInstance() {
		if(obj==NULL) obj=new WriteRowsStore();
		return obj;
	}

	//void SetNVS(int connid,NewValuesSetBase ** nvs,int fields) {
	//	DeleteNVS(connid,fields);
	//	m_nvs[connid]=nvs;
	//}

	void AddWritedRows(int connid,_int64 rows) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter!=m_nvs.end()) {
			iter->second->rows_writed+=rows;
		}
	}

	void ClearData(int connid) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter!=m_nvs.end()) {
			iter->second->ClearAttrs();
		}
	}
	
	_int64 GetWritedRows(int connid) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter!=m_nvs.end()) {
			return iter->second->rows_writed;
		}
		return 0;
	}
	
	RCTableLoad * GetLoadTable(int connid) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter!=m_nvs.end()) {
			return iter->second->loadtable;
		}
		return NULL;
	}

	int GetFields(int connid) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter!=m_nvs.end()) {
			return iter->second->num_fields;
		}
		return 0;
	}
	
	NewValuesSetBase **SetFields(int connid,int fields) {
		Delete(connid);
		m_nvs[connid]=new WriteSession(fields);
        if(m_nvs[connid] == NULL){// try to fix : dma-1076 
            return NULL;
        }
		return m_nvs[connid]->nvs;
	}

	// store load table ,and release as delete session by Delete
	void SetLoadTable(int connid,RCTableLoad *_loadtable) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter!=m_nvs.end()) {
			iter->second->loadtable=_loadtable;
		}
	}

	void Delete(int connid) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter!=m_nvs.end()) {
			WriteSession *pnvs=iter->second;
			delete pnvs;
			m_nvs.erase(iter);
		}
	}
	
	NewValuesSetBase **GetNVS(int connid) {
		std::map<int,WriteSession *>::iterator iter=m_nvs.find(connid);
		if(iter==m_nvs.end()) return NULL;
		return iter->second->nvs;
	}
};

WriteRowsStore *WriteRowsStore::obj=NULL;
/*
 write_row() inserts a row. No extra() hint is given currently if a bulk load
 is happeneding. buf() is a byte array of data. You can use the field
 information to extract the data from the native byte array type.
 Example of this would be:
 for (Field **field=table->field ; *field ; field++)
 {
 ...
 }

 See ha_tina.cc for an example of extracting all of the data as strings.
 ha_berekly.cc has an example of how to store it intact by "packing" it
 for ha_berkeley's own native storage type.

 See the note for update_row() on auto_increments and timestamps. This
 case also applied to write_row().

 Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
 sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.
 */
 #define FillIntNVS(nvs,v,field,newNVS) \
 	{ \
 	 if(newNVS) \
	 	nvs=(*field)->is_null()?new NewValuesSetRows(NewValuesSetRows::STYPE_INT):new NewValuesSetRows(v);\
	 else (*field)->is_null()?((NewValuesSetRows *)nvs)->AddIntNull():((NewValuesSetRows *)nvs)->PushInt(v);\
 	}

#define FillRealNVS(nvs,v,field,newNVS) \
 	{ \
 	 if(newNVS) \
	 	nvs=(*field)->is_null()?new NewValuesSetRows(NewValuesSetRows::STYPE_REAL):new NewValuesSetRows(v);\
	 else (*field)->is_null()?((NewValuesSetRows *)nvs)->AddRealNull():((NewValuesSetRows *)nvs)->PushReal(v);\
 	}

#define FillStrNVS(nvs,v,len,field,newNVS) \
 	{ \
 	 if(newNVS) \
	 	nvs=(*field)->is_null()?new NewValuesSetRows(NewValuesSetRows::STYPE_STR):new NewValuesSetRows(v,len);\
	 else (*field)->is_null()?((NewValuesSetRows *)nvs)->AddStrNull():((NewValuesSetRows *)nvs)->PushStr(v,len);\
 	}


int BrighthouseHandler::end_bulk_insert() {
	uint connid=0;
    if(last_action!=TRANS_ACT_INSERT){
        return BHERROR_BHLOADER_UNAVAILABLE;
    }
	if(ConnectionInfoOnTLS.IsValid()){
  		connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;
	}
	WriteRowsStore *pws=WriteRowsStore::GetInstance();
	RCTableLoad *rct=pws->GetLoadTable( connid);
	// insert 0 row,no rct
	if(rct==NULL) 
		return 0;
	NewValuesSetBase **nvses=pws->GetNVS(connid);
	uint last_packrow = JustATable::PackIndex(rct->NoObj());
	// has inserted already?
	if(rct==NULL || nvses==NULL) return 0;
	if(nvses[0]!=NULL && !do_bulk_work(connid)){
		return BHERROR_BHLOADER_UNAVAILABLE;
	}
    try{
    	rct->LoadDataEnd();
    }catch(...){
        last_action = BHERROR_BHLOADER_UNAVAILABLE;
        rclog << lock << "Info: BrighthouseHandler::end_bulk_insert has error , finish ..." << unlock;
        return BHERROR_BHLOADER_UNAVAILABLE;
    }
	//update last pack 
	last_packrow = JustATable::PackIndex(rct->NoObj());
	if((rct->NoObj()%65536)==0) --last_packrow;
    //release pack data in cache(Ref:DMA-1573)
    m_conn->GetTransaction()->ReleasePackRow(rct->GetID(), last_packrow);

	// Build to target db from map file directly
	if(0 != rct->MergePackHash(m_conn->GetTransaction()->GetID())){	
		return -1;
	}
	// before commit /refer to :RCEngine::ExternalLoad
	int tid = rct->GetID();
	if(rsi_manager)
		rsi_manager->UpdateDefForTable(tid); 
	m_conn->GetTransaction()->RefreshTable(string(m_table_path));
	query_cache_invalidate3(&m_conn->Thd(), table, 0);
	pws->Delete(connid);
    
    if(m_conn->Thd().lock) {
		mysql_unlock_tables(&m_conn->Thd(), m_conn->Thd().lock);        
        m_conn->Thd().lock=0;
		if(table_lock_manager.AcquireWriteLock(*m_conn->GetTransaction(),string(m_table_path),m_conn->Thd().killed, m_conn->Thd().some_tables_deleted).expired()) {
            return BHERROR_BHLOADER_UNAVAILABLE;
		}
	}
	
    rclog << lock << "Info: BrighthouseHandler::end_bulk_insert finish ..." << unlock;
 	return 0;
}
// WRONG_COMMAND on default base handler class ,why??
int BrighthouseHandler::end_bulk_delete() {
	DBUG_ASSERT(FALSE);
	return HA_ERR_WRONG_COMMAND; 
}

void BrighthouseHandler::end_bulk_update() {
	return;
}

 bool BrighthouseHandler::do_bulk_work(int connid) {
 	// update/delete has not implemented yet!
		// force write to a pack
	WriteRowsStore *pws=WriteRowsStore::GetInstance();
 	try{
		assert(last_action==TRANS_ACT_INSERT);
		RCTableLoad *rct=pws->GetLoadTable( connid);
		NewValuesSetBase **nvses=pws->GetNVS(connid);
		assert(rct!=NULL);
		assert(nvses!=NULL);
		uint last_packrow = JustATable::PackIndex(rct->NoObj());
		
		rct->LoadData(connid,nvses);
       
        if(last_packrow>0) {
            //release pack object the pack before lastpack.
            // the last pack maybe need be used again
            int tid = rct->GetID();
            last_packrow--;
    		//release pack data in cache
    		m_conn->GetTransaction()->ReleasePackRow(tid, last_packrow);
        }

		pws->ClearData(connid);
 	}
	catch(...) {
		pws->Delete(connid);
		return false;
	}
	return true;
 }


void BrighthouseHandler::start_bulk_insert(ha_rows rows) {
	uint connid=0;
	assert(ConnectionInfoOnTLS.IsValid());
  	connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;    

	// Create TableLoad object
	WriteRowsStore *pws=WriteRowsStore::GetInstance();
	NewValuesSetBase **nvses=pws->GetNVS(connid);
	if(nvses==NULL){
		nvses=pws->SetFields(connid, table_share->fields);
        if(nvses == NULL){ // try to fix : dma-1076   
            rclog << lock << "Error: BrighthouseHandler::start_bulk_insert pws->SetFields return NULL !" << unlock;
            last_action= BHERROR_OUT_OF_MEMORY;
            return;
        }
	}

    // ---------------------------------------------------------
    // 多线程并发insert处理流程
    // 1. 释放锁(让其他线程进行insert提交或者bhloader提交装入数据)
    // 2. 判断loading文件是否存在?
    //    2.1> loading 文件存在,继续等待
    //    2.2> loading 文件不存在,执行流程3
    // 3. 获取表操作的锁
    // 4. 判断loading 文件是否存在?
    //    4.1> loading 文件存在,释放锁继续等待,执行流程2
    //    4.2> loading 文件不存在,执行流程5
    // 5. 创建loading文件
    // 6. 释放锁操作
    // 7. 判断进程是否正常
    // ---------------------------------------------------------
    
    if(m_conn->Thd().lock) {
        mysql_unlock_some_tables(&m_conn->Thd(),&this->table,1);              
    	table_lock_manager.ReleaseWriteLocks(*m_conn->GetTransaction());
    }
    
    std::string table_lock_file = std::string(GetTablePath()) +".loading";
   
    long wait_start_insert_cnt = 0;
    do{
        if (DoesFileExist(table_lock_file)) {				
            if(wait_start_insert_cnt++ % 10 == 0){
                rclog << lock << "Warning: BrighthouseHandler::start_bulk_insert Table ["<< table_lock_file <<"] is loading . sleep 1 second , continue..."<< unlock;
            }
            sleep(wait_start_insert_cnt & 5);
            continue;
        } else {         
        	if(table_lock_manager.AcquireWriteLock(*m_conn->GetTransaction(),string(m_table_path),m_conn->Thd().killed, m_conn->Thd().some_tables_deleted).expired()) {
                continue;
        	}

            if (DoesFileExist(table_lock_file)) {	
                table_lock_manager.ReleaseWriteLocks(*m_conn->GetTransaction());
                continue;
            }
            
            struct stat	sb;
            int fd = ::open(table_lock_file.c_str(), O_WRONLY | O_CREAT, DEFFILEMODE);
            if (fd == -1 || fstat(fd, &sb) || ::close(fd)) {
                rclog << lock << "Error: BrighthouseHandler::start_bulk_insert Cannot open lock file ["<< table_lock_file <<"] ."<< unlock;
                wait_start_insert_cnt = -1;
                table_lock_manager.ReleaseWriteLocks(*m_conn->GetTransaction());
                break;
            }
            
            { // 重新获取开始insert数据包的位置,以便insert能够正确进行
            	RCTableLoad *rct=pws->GetLoadTable( connid);
				if(rct==NULL) {
					try {
						vector<DTCollation> charsets;
						if(table_share) {
							for(uint i = 0; i < table_share->fields; i++) {
								const Field_str* fstr = dynamic_cast<const Field_str*>(table_share->field[i]);
								if(fstr)
									charsets.push_back(DTCollation(fstr->charset(), fstr->derivation()));
								else
									charsets.push_back(DTCollation());
							}
						}
						string file_path = string(m_table_path) + ".bht";
			           	//rct = new RCTableLoad(file_path.c_str(), 0, charsets);
			           	rct = new RCTableLoad(file_path.c_str(), 1, charsets);// fix dma-1329
						pws->SetLoadTable(connid, rct);
						rct->SetWriteMode(m_conn->GetTransaction()->GetID());
						rct->PrepareLoadData(connid);
						
						// try to fix dma-1116
						m_conn->GetTransaction()->RefreshTable(GetTablePath());					    
						
					} catch(DatabaseRCException&) {
						table_lock_manager.ReleaseWriteLocks(*m_conn->GetTransaction());
						last_action= BHERROR_BHLOADER_UNAVAILABLE;
                        return;
					}
				}            	
            }
                           
            table_lock_manager.ReleaseWriteLocks(*m_conn->GetTransaction());
            
            wait_start_insert_cnt = -1;
        }
    	last_action=TRANS_ACT_INSERT;

        if(m_conn->killed()){ // dpshut , exit
            m_conn->Thd().send_kill_message();
            break;
        }
    }while(wait_start_insert_cnt>0);
    
    rclog << lock << "Info: BrighthouseHandler::start_bulk_insert starting ..." << unlock;
}
 
int BrighthouseHandler::write_row(uchar* buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	
    if(last_action!=TRANS_ACT_INSERT)
        DBUG_RETURN(BHERROR_BHLOADER_UNAVAILABLE);
	uint connid=0;
	if(ConnectionInfoOnTLS.IsValid())
  		connid=(uint) ConnectionInfoOnTLS.Get().Thd().variables.pseudo_thread_id;

	// Create TableLoad object
	WriteRowsStore *pws=WriteRowsStore::GetInstance();
	NewValuesSetBase **nvses=pws->GetNVS(connid);
	assert(nvses!=NULL);
	//nvses[0] is null while new allocate above or cleared by last pack load.
	bool newNVS=nvses[0]==NULL;
	RCTableLoad *rct=pws->GetLoadTable( connid);
	assert(rct!=NULL);
	//parse values
	int col_id=0;
	for(Field **field = table->field; *field; field++, col_id++) {
		// set read bitmap,otherwise crash on assert
			bitmap_set_bit((*field)->table->read_set, (*field)->field_index);
			switch((*field)->type()) {
				case MYSQL_TYPE_VARCHAR: 
					if((*field)->is_null()) {
						FillStrNVS(nvses[col_id],NULL,0,field,newNVS);
					}
					else {
					 if((*field)->field_length <= 255)
					 	{
						FillStrNVS(nvses[col_id],(char *)(*field)->ptr+1,(*field)->data_length(),field,newNVS);
					  }
					 else
						FillStrNVS(nvses[col_id],(char *)(*field)->ptr+2,(*field)->data_length(),field,newNVS);
					}
					break;
				case MYSQL_TYPE_STRING:
					FillStrNVS(nvses[col_id],(char *)(*field)->ptr,(*field)->data_length(),field,newNVS);
					break;
				case MYSQL_TYPE_TINY:
					FillIntNVS(nvses[col_id],_int64(*(char*)(*field)->ptr),field,newNVS);
					break;						
				case MYSQL_TYPE_SHORT:
					FillIntNVS(nvses[col_id],_int64(*(short*)(*field)->ptr),field,newNVS);
					break;
				case MYSQL_TYPE_INT24:
					FillIntNVS(nvses[col_id],_int64(sint3korr((*field)->ptr)),field,newNVS);
					break;
				case MYSQL_TYPE_LONG:
					FillIntNVS(nvses[col_id],_int64(*(int*)(*field)->ptr),field,newNVS);
					break;
				case MYSQL_TYPE_LONGLONG:
					FillIntNVS(nvses[col_id],*(_int64*)(*field)->ptr,field,newNVS);
					break;
				case MYSQL_TYPE_FLOAT:
					FillRealNVS(nvses[col_id],double(*( float *)(*field)->ptr),field,newNVS);
					break;
				case MYSQL_TYPE_DOUBLE:
					FillRealNVS(nvses[col_id],double(*( double *)(*field)->ptr),field,newNVS);
					break;
				case MYSQL_TYPE_DECIMAL:
					FillRealNVS(nvses[col_id],((Field_decimal*)field)->val_real(),field,newNVS);
					break;
				case MYSQL_TYPE_NEWDECIMAL: 
					FillRealNVS(nvses[col_id],((Field_new_decimal*)field)->val_real(),field,newNVS);
					break;
				case MYSQL_TYPE_BLOB: 
					rclog << lock << "Error: BrighthouseHandler::write_row MYSQL_TYPE_BLOB error."<< unlock;
					assert(0);// not impl.
					break;
				case MYSQL_TYPE_TIME :
					{
						int v=((Field_date *)*field)->val_int();
						RCDateTime rcv = RCDateTime((short)(v/10000), (short) ((v%10000)/100), (short) (v%100), RC_TIME);
						FillIntNVS(nvses[col_id],rcv.GetInt64(),field,newNVS);
						break;
					}
				case MYSQL_TYPE_DATE:
					{
						int v=((Field_date *)*field)->val_int();
						RCDateTime rcv = RCDateTime((short)(v/10000), (short) ((v%10000)/100), (short) (v%100), RC_DATE);
						FillIntNVS(nvses[col_id],rcv.GetInt64(),field,newNVS);
						break;
					}
				case MYSQL_TYPE_NEWDATE:
					{
						int v=((Field_newdate *)*field)->val_int();
						RCDateTime rcv = RCDateTime((short)(v/10000), (short) ((v%10000)/100), (short) (v%100), RC_DATE);
						FillIntNVS(nvses[col_id],rcv.GetInt64(),field,newNVS);
						break;
					}
				case MYSQL_TYPE_TIMESTAMP:
					{
					MYSQL_TIME mtime;
					((Field_timestamp*)*field)->get_date(&mtime, 0);
					RCDateTime rcv = RCDateTime((short)mtime.year, (short) mtime.month, (short) mtime.day, 
						  (short) mtime.hour, (short) mtime.minute,(short) mtime.second, RC_TIMESTAMP);
					FillIntNVS(nvses[col_id],rcv.GetInt64(),field,newNVS);
					break;
						break;
					}
				case MYSQL_TYPE_DATETIME: {
					MYSQL_TIME mtime;
					((Field_datetime*)*field)->get_date(&mtime, 0);
					RCDateTime rcv = RCDateTime((short)mtime.year, (short) mtime.month, (short) mtime.day, 
						  (short) mtime.hour, (short) mtime.minute,(short) mtime.second, RC_DATETIME);
					FillIntNVS(nvses[col_id],rcv.GetInt64(),field,newNVS);
					break;
					}
				default:
					rclog << lock << "Error: BrighthouseHandler::write_row default error."<< unlock;
					assert(0);// not support types
					break;
		}
		bitmap_clear_bit((*field)->table->read_set, (*field)->field_index);
	}
	
	pws->AddWritedRows(connid,1);
	
	// inserted into last pack of last partition by default
	if((rct->NoObj()+pws->GetWritedRows(connid))%65536==0) 
		if(!do_bulk_work(connid))
			DBUG_RETURN(BHERROR_BHLOADER_UNAVAILABLE); 
	DBUG_RETURN(BHERROR_SUCCESS);
}

/*
 Yes, update_row() does what you expect, it updates a row. old_data will have
 the previous row record in it, while new_data will have the newest data in
 it.
 Keep in mind that the server can do updates based on ordering if an ORDER BY
 clause was used. Consecutive ordering is not guarenteed.
 Currently new_data will not have an updated auto_increament record, or
 and updated timestamp field. You can do these for example by doing these:
 if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
 table->timestamp_field->set_time();
 if (table->next_number_field && record == table->record[0])
 update_auto_increment();

 Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.
 */
int BrighthouseHandler::update_row(const uchar* old_data, uchar* new_data)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 This will delete a row. buf will contain a copy of the row to be deleted.
 The server will call this right after the current row has been called (from
 either a previous rnd_nexT() or index call).
 If you keep a pointer to the last row or can access a primary key it will
 make doing the deletion quite a bit easier.
 Keep in mind that the server does no guarentee consecutive deletions. ORDER BY
 clauses can be used.

 Called in sql_acl.cc and sql_udf.cc to manage internal table information.
 Called in sql_delete.cc, sql_insert.cc, and sql_select.cc. In sql_select it is
 used for removing duplicates while in insert it is used for REPLACE calls.
 */
int BrighthouseHandler::delete_row(const uchar * buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

void BrighthouseHandler::SimpleDelete()
{

}

/*
 Used to delete all rows in a table. Both for cases of truncate and
 for cases where the optimizer realizes that all rows will be
 removed as a result of a SQL statement.

 Called from item_sum.cc by Item_func_group_concat::clear(),
 Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
 Called from sql_delete.cc by mysql_delete().
 Called from sql_select.cc by JOIN::rein*it.
 Called from sql_union.cc by st_select_lex_unit::exec().
 */
 
 /* 删除分区功能在这里入口
     目前还不支持直接命令删除,而是需要预先生成一个控制文件(xxx.bht/truncinfo.ctl),指定删除分区和会话信息
 */
int BrighthouseHandler::delete_all_rows()
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	THD &thd=m_conn->Thd();
	//if(start_stmt(&thd, TL_WRITE))
	//	DBUG_RETURN(HA_ERR_LOCK_WAIT_TIMEOUT);
	
	RCTablePtr tablep = rceng->GetTableShared(m_table_path, table_share);
	Transaction* trs = m_conn->GetTransaction();	
	// has been called in mysql_delete->open_and_lock_tables->BrighthouseHandler::external_lock
	//if(table_lock_manager.AcquireWriteLock(*trs, string(m_table_path), thd.killed, thd.some_tables_deleted).expired())
	//			DBUG_RETURN(HA_ERR_LOCK_WAIT_TIMEOUT);
	// has been called in mysql_delete->open_and_lock_tables->BrighthouseHandler::external_lock
	//trs->AddTableWR(string(m_table_path),&thd,table);
	bool ret=tablep->TruncatePartition();
	

	
	if(!ret) {
	 // return HA_ERR_WRONG_COMMAND will cause delete rows one by one!
	 //DBUG_RETURN(HA_ERR_WRONG_COMMAND);//HA_ERR_INTERNAL_ERROR
	  //table_lock_manager.ReleaseWriteLocks(*trs);
	  rceng->Rollback(&thd,true);
	  DBUG_RETURN(HA_ERR_NO_PARTITION_FOUND);
    }	
	// refer to : Transaction::Commit
   // do commit manually:
	int id = tablep->GetID();
	TransactionBase::FlushTableBuffers(tablep.get());
	
	GlobalDataCache::GetGlobalDataCache().ReleasePackRow(id, 0);

	uint no_attrs = tablep->NoAttrs();
	for(uint a = 0; a < no_attrs; a++)
	{
		GlobalDataCache::GetGlobalDataCache().DropObject(FTreeCoordinate(id,a));
		GlobalDataCache::GetGlobalDataCache().DropObject(SpliceCoordinate(id, a, 0 / DEFAULT_SPLICE_SIZE ));
	}

	if(rsi_manager)
			rsi_manager->UpdateDefForTable(id);
	GlobalDataCache::GetGlobalDataCache().ReleasePacks(id);
	GlobalDataCache::GetGlobalDataCache().ReleaseDPNs(id);
	GlobalDataCache::GetGlobalDataCache().DropObject(FilterCoordinate(id));
    
  // 先从transaction中删除这个表,再LoadDirtyData,否则会从local_cache中读到脏数据
	// dropped in rollback
	//trs->DropTable(string(m_table_path));
	// load dirty data cause attr got SESSION_WRITE on current_state? move to call in RefreshTable
	//table->LoadDirtyData(trs->GetID());
	
	rceng->ReplaceTable(m_table_path, trs->RefreshTable(string(m_table_path)));
	// has been called by rollback!
	//table_lock_manager.ReleaseWriteLocks(*trs);
	// Clean transaction: remove modified table list
	rceng->Rollback(&thd,true);
	//switch ab state
	//Commit中包含 A/B切换,leveldb合并,因此不能在这里调用.可以考虑在RCAttr::Commit/Rollback中增加事务类型的分类处理
	//rceng->Commit(&thd,true);
	DBUG_RETURN(0);
}

int BrighthouseHandler::rename_table(const char * from, const char * to)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
  Sets brighthouse alter table flag.

  Called from mysql_alter_table (sql_table.cc)
*/
void BrighthouseHandler::set_brighthouse_alter_table(int& brighthouse_alter_table)
{
}

/*
  Alters table except for rename table.

  Called from mysql_alter_table (sql_table.cc)
*/
int BrighthouseHandler::brighthouse_alter_table(int drop_last_column)
{
	return FALSE;
}
