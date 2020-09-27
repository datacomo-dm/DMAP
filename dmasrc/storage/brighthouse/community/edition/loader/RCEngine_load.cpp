/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include "core/RCEngine.h"
#include "edition/core/Transaction.h"
#include "core/RCTableImpl.h"
#include "loader/RCTable_load.h"
#include <cstdlib>
#include <exception>
#include <exception_defines.h>
#include <cxxabi.h>
#include <stdexcept>
using namespace std;

BHEngineReturnValues RCEngine::RunLoader(THD* thd, sql_exchange* ex, TABLE_LIST* table_list, BHError& bherror)
{
    return ExternalLoad(thd, ex, table_list, bherror);
}

BHEngineReturnValues RCEngine::ExternalLoad(THD* thd, sql_exchange* ex, TABLE_LIST* table_list, BHError& bherror)
{
    BHEngineReturnValues ret = LD_Successed;

    char name[FN_REFLEN];
    TABLE* table;
    int error=0;
    String* field_term = ex->field_term;
    int transactional_table = 0;
    boost::shared_array<char> BHLoader_path(new char[4096]);
    ConnectionInfo* tp = NULL;
    string table_lock_file;

    COPY_INFO info;
    string table_path;
    try {
        // GA
        size_t len = 0;
        dirname_part(BHLoader_path.get(), my_progname, &len);
        BHASSERT_WITH_NO_PERFORMANCE_IMPACT(BHLoader_path[len] == 0);

        strcat(BHLoader_path.get(), BHLoaderAppName);

        if(field_term->length() > 1) {
            my_message(ER_WRONG_FIELD_TERMINATORS,ER(ER_WRONG_FIELD_TERMINATORS), MYF(0));
            bherror = BHError(BHERROR_SYNTAX_ERROR, "Wrong field terminator.");
            return LD_Failed;
        }
        //   unlock while lock query on transaction until next commit
        if(open_and_lock_tables(thd, table_list)) {
            bherror = BHError(BHERROR_UNKNOWN, "Could not open or lock required tables.");
            return LD_Failed;
        }

        if(!RCEngine::IsBHTable(table_list->table)) {
            mysql_unlock_tables(thd, thd->lock);
            thd->lock=0;
            //close_thread_tables(thd);
            return LD_Continue;
        }
        auto_ptr<Loader> bhl = LoaderFactory::CreateLoader(LOADER_BH);


        table = table_list->table;
        transactional_table = table->file->has_transactions();

        IOParameters iop;
        BHError bherr;
        if((bherror = RCEngine::GetIOParameters(iop, *thd, *ex, table)) != BHERROR_SUCCESS) {
            mysql_unlock_tables(thd, thd->lock);
            throw LD_Failed;
        }

        // ---------------------------------------------------------
        // 多线程并发insert/bhloader 处理流程
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
        if(thd->lock) {
            mysql_unlock_tables(thd, thd->lock);
            thd->lock=0;
            //do not commit or rollback transaction,just unlock write table only
            Transaction* trs = GetThreadParams(*thd)->GetTransaction();
            table_lock_manager.ReleaseWriteLocks(*trs);
        }

        table_path = GetTablePath(thd, table);
        table_lock_file = table_path +".loading";
        bool lastload=false;
        uint last_packrow =0;
        ulong warnings = 0;

        long wait_start_insert_cnt = 0;

        bool st_mergetable = false;  // 短会话数据合并,add by liujs
        bool lt_mergetable = false;  // 长会话数据合并,add by liujs
        RCBString _file_name(ex->file_name);
        RCBString _st_mergetable("st_mergetable");
        RCBString _lt_mergetable("lt_mergetable");
        if(strcmp(_file_name,_st_mergetable) == 0) {
            st_mergetable = true;
        } else if(strcmp(_file_name,_lt_mergetable) == 0) {
            lt_mergetable = true;
        }

        do {
            if(DoesFileExist(table_lock_file)) {

                // 长短会话上提交数据,如果有进程在insert或者bhloader工作,则不能进行数据合并,add by liujs
                if(st_mergetable) {
                    rclog << lock << "Warning: RCEngine::ExternalLoad Table ["<< table_lock_file << "] is loading . exit short time merge table. "<< unlock;
                } else if(lt_mergetable) {
                    rclog << lock << "Warning: RCEngine::ExternalLoad Table ["<< table_lock_file << "] is loading . exit long time merge table. "<< unlock;
                }

                if(wait_start_insert_cnt++ % 10 == 0) {
                    rclog << lock << "Warning: RCEngine::ExternalLoad Table ["<< table_lock_file << "] is loading . sleep 5 second , continue..."<< unlock;
                }
                sleep(wait_start_insert_cnt & 5);

                continue;
            } else {
                Transaction* trs = GetThreadParams(*thd)->GetTransaction();
                if(table_lock_manager.AcquireWriteLock(*trs,table_path,thd->killed, thd->some_tables_deleted).expired()) {
                    continue;
                }

                if (DoesFileExist(table_lock_file)) {
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    continue;
                }
                struct stat sb;
                int fd = open(table_lock_file.c_str(), O_WRONLY | O_CREAT, DEFFILEMODE);
                if (fd == -1 || fstat(fd, &sb) || close(fd)) {
                    bherror = BHError(BHERROR_UNKNOWN, "Cannot open lock file \"" + table_lock_file + "\" .");
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    throw LD_Failed;
                }

                // already checked that this is a BH table
                tp = GetThreadParams(*thd);
                int tab_id = tp->GetTransaction()->GetTable(table_path, table->s)->GetID();
                if(rsi_manager) {
                    rsi_manager->UpdateDefForTable(tab_id);
                }

                table->copy_blobs=0;
                thd->cuted_fields=0L;

                bhl->Init(BHLoader_path.get(), &iop);
                bhl->MainHeapSize() = this->m_loader_main_heapsize;
                bhl->CompressedHeapSize() = this->m_loader_compressed_heapsize;

                uint transaction_id = tp->GetTransaction()->GetID();
                bhl->TransactionID() = transaction_id;
                bhl->ConnectID()=tp->GetConnectID();
                bhl->SetTableId(tab_id);

                last_packrow = JustATable::PackIndex(tp->GetTransaction()->GetTable(table_path, table->s)->NoObj());

                // check if last load of a partition
                lastload=false;
                {
                    char ctl_filename[300];
                    sprintf(ctl_filename,"%s.bht/loadinfo.ctl",table_path.c_str());
                    IBFile loadinfo;
                    loadinfo.OpenReadOnly(ctl_filename);
                    char ctl_buf[200];
                    loadinfo.Read(ctl_buf,14);
                    lastload=*(short *)(ctl_buf+12)==1;
                }

                if(thd->killed != 0 ) { // dpshut , exit
                    thd->send_kill_message();
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    break;
                }

                table_lock_manager.ReleaseWriteLocks(*trs);

                wait_start_insert_cnt = -1;
            }
        } while(wait_start_insert_cnt>0);

        if(lt_mergetable || st_mergetable) { //  分布式排序过后的数据入库流程,add by liujs
            /*
            \ 实现流程:
            \ 1. 根据数据表合并类型,进行数据合并过程
            \ 2. 合并完成后,锁住表
            \ 3. 提交数据表合并
            \ 4. 释放表
            */

            RCTableLoadAutoPtr rct;  // try to fix dma-1554
            uint connid=(uint)thd->variables.pseudo_thread_id;
            try {
                std::vector<DTCollation> charsets;
                if(table) {
                    for(uint i = 0; i < table->s->fields; i++) {
                        const Field_str* fstr = dynamic_cast<const Field_str*>(table->s->field[i]);
                        if(fstr)
                            charsets.push_back(DTCollation(fstr->charset(), fstr->derivation()));
                        else
                            charsets.push_back(DTCollation());
                    }
                }
                std::string file_path = table_path + ".bht";
                rct = RCTableLoadAutoPtr(new RCTableLoad(file_path.c_str(), 1, charsets)); // fix dma-1329
                rct->SetWriteMode( GetThreadParams(*thd)->GetTransaction()->GetID());
            } catch(DatabaseRCException &e) {
                //bherror = BHError(BHERROR_UNKNOWN, "Merge table process create RCTableLoad error.");
            	rclog << lock << "Error in ExternalLoad: [DatabaseRCException]--"<<e.what()<< unlock;
                bherror = BHError(BHERROR_UNKNOWN, "Error in ExternalLoad: [DatabaseRCException].");
                throw ;//keep exception type and instance, rethrow
            }

            //  1. 根据数据表合并类型,进行数据合并过程
            int _mergetabe_ret = 0;
			try{
			 try { 
                if(st_mergetable) {
                    _mergetabe_ret = rct->merge_table_from_sorted_data(st_mergetable);
                    rclog << lock << "Info: mysqld merge st_session data to table begin..."<< unlock;
                } else {
                    _mergetabe_ret = rct->merge_table_from_sorted_data(st_mergetable);
                    rclog << lock << "Info: mysqld merge lt_session data to table begin"<< unlock;
                }
                if(_mergetabe_ret !=0){  // mergertable 过程报错
                	throw InternalRCException("_mergetabe_ret error");
                }
             } // release rct caused crash,so we pre-check exception here and output info before throw again
             //TODO: exception type should based on std::exception(std::runtime_error) anywhere.
             catch(InternalRCException& e){
            	rclog << lock << "Error in ExternalLoad: [InternalRCException *]"<< unlock;
                bherror = BHError(BHERROR_UNKNOWN, "Error in ExternalLoad: [InternalRCException *].");
                throw e;
             }
             catch(DatabaseRCException& e){
                 rclog << lock << "Error in ExternalLoad: [DatabaseRCException *]"<< unlock;
                 bherror = BHError(BHERROR_UNKNOWN, "Error in ExternalLoad: [DatabaseRCException *].");
                 throw e;
             }
			 catch (const char * e)	{
            	rclog << lock << "Error in ExternalLoad: [char *]--"<<e<< unlock;
                bherror = BHError(BHERROR_UNKNOWN, "Error in ExternalLoad: [char *]--.");
				throw InternalRCException(e);
			 }catch (std::string &e)	{
            	rclog << lock << "Error in ExternalLoad: [std::string]--"<<e<<unlock;
                bherror = BHError(BHERROR_UNKNOWN, "Error in ExternalLoad: [std::string]--.");
				throw InternalRCException(e);
			 }catch (long & e)	{
            	rclog << lock << "Error in ExternalLoad: [long]--"<<e<< unlock;
				char errinfo[100];
				sprintf(errinfo,"error (long) :%ld.",e);
                bherror = BHError(BHERROR_UNKNOWN,errinfo);
				throw InternalRCException(errinfo);
			 }catch (int & e)	{
            	rclog << lock << "Error in ExternalLoad: [int]--"<<e<< unlock;
				char errinfo[100];
				sprintf(errinfo,"error (int) :%d.",e);
                bherror = BHError(BHERROR_UNKNOWN,errinfo);
				throw InternalRCException(errinfo);
			 }catch (std::exception& e)	{
            	rclog << lock << "Error in ExternalLoad: [std::exception(and derived))]--"<<e.what()<< unlock;
                bherror = BHError(BHERROR_UNKNOWN,"Error in ExternalLoad: [std::exception(and derived))]--");
				throw LD_Failed;
			 } catch (...) {
			 	//never reach here or throw a very strange exception
			    //TODO: move codes below to caller's catch block while release rct fixed
			    // Make sure there was an exception; 
    			std::type_info *t = abi::__cxa_current_exception_type();
    			if (t)
    			{
          			// Note that "name" is the mangled name.
          			char const *name = t->name();
          			int status = -1;
          			char *dem = 0;
          			dem = abi::__cxa_demangle(name, 0, 0, &status);
					rclog << lock << "Error in ExternalLoad:  Exception of type "<<(status==0?dem:name)<< unlock;
          			if (status == 0)
            			free(dem);
     			}
				else{
					rclog << lock << "Error in ExternalLoad:  Exception of type Unknow(without exception?)."<< unlock;
				}
                bherror = BHError(BHERROR_UNKNOWN,"Error in ExternalLoad:  Exception of catch(...)");
				throw LD_Failed;
			 }
			}
			catch(...) {
                bherror = BHError(BHERROR_UNKNOWN,"Error in ExternalLoad:  Exception of catch2(...)");
                // mysqld insert can't execute
                // rct->ClearLoadingFile(); // connection id is not matched , can't not rollback
				throw LD_Failed;
			}
            rclog << lock << "Info: mysqld mergetable finish."<< unlock;

            int _tid = rct->GetID();
            if(rsi_manager){
                rsi_manager->UpdateDefForTable(_tid);
            }
            
            //  2. mysqld 进行mergetable 已经完成,开始锁表,准备进行数据提交，提交数据过程在Transaction::Commit(THD* thd)中进行
            Transaction* trs = GetThreadParams(*thd)->GetTransaction();
            if(table_lock_manager.AcquireWriteLock(*trs,table_path,thd->killed, thd->some_tables_deleted).expired()) {
                bherror = BHError(BHERROR_UNKNOWN, "AcquireWriteLock error.");
                throw LD_Failed;
            }

            // 更新需要提交的会话ID
            rclog << lock << "Info: update prepare merge session . "<< unlock;

            if(true) { // 刷新缓存
                int tid = tp->GetTransaction()->GetTable(table_path, table->s)->GetID();
                tp->GetTransaction()->ReleasePackRow(tid, last_packrow);
                for(int a = 0; a < tp->GetTransaction()->GetTable(table_path, table->s)->NoAttrs(); a++) {
                    tp->GetTransaction()->DropLocalObject(SpliceCoordinate(tid, a, last_packrow / DEFAULT_SPLICE_SIZE ));
                }

                tp->GetTransaction()->RefreshTable(table_path.c_str()); // thread id used temp to store transaction id ...

                if(lt_mergetable) { // 是否所有的包
                    bool release_lt_session_load_data_pack = true;
                    tp->GetTransaction()->SetTableReleaseFlag(table_path.c_str(),release_lt_session_load_data_pack);
                }
            }

            // 4. 释放表
            table_lock_manager.ReleaseWriteLocks(*trs);

        } else { // 原bhloader装入数据流程
            rclog << lock << "Info: Begin to start bhloader ..."<< unlock;
#ifdef __WIN__
            if(bhl->Proceed(tp->pi.hProcess) && !tp->killed())
#else
            if(bhl->Proceed(tp->pi) && !tp->killed())
#endif
            {
                rclog << lock << "Info: bhloader finish."<< unlock;
                Transaction* trs = GetThreadParams(*thd)->GetTransaction();
                if(table_lock_manager.AcquireWriteLock(*trs,table_path,thd->killed, thd->some_tables_deleted).expired()) {
                    bherror = BHError(BHERROR_UNKNOWN, "AcquireWriteLock error.");
                    throw LD_Failed;
                }

                //deprecated: merge to interal temporary hash_map only,keep memory not been released!!
                // Build to target db from map file directly
                //
                if(!lastload && 0 != tp->GetTransaction()->GetTable(table_path, table->s)->MergePackHash(tp->GetTransaction()->GetID())) {
                    bherror = BHError(BHERROR_UNKNOWN, "Merge table pack index error.");
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    throw LD_Failed;
                }

                // deprecated : packindex 分区合并的时候，不同的进程(bhloader和mysqld)无法打开同一个库(即分开运行的时候不能放入bhloader中)
                //              A+B --> A 和 A+B ---> C 的时候，只能在mysqld中进行
                // new db produced in bhloader
                /*
                if(lastload && 0 != tp->GetTransaction()->GetTable(table_path, table->s)->MergePackIndex(tp->GetTransaction()->GetID()))
                {
                    bherror = BHError(BHERROR_UNKNOWN, "Merge pack index of last file in a partition error.");
                    throw LD_Failed;
                }
                */

                if((_int64) bhl->NoRecords() > 0) {
                    int tid = tp->GetTransaction()->GetTable(table_path, table->s)->GetID();
                    tp->GetTransaction()->ReleasePackRow(tid, last_packrow);
                    for(int a = 0; a < tp->GetTransaction()->GetTable(table_path, table->s)->NoAttrs(); a++) {
                        tp->GetTransaction()->DropLocalObject(SpliceCoordinate(tid, a, last_packrow / DEFAULT_SPLICE_SIZE ));
                    }
                    tp->GetTransaction()->RefreshTable(table_path.c_str()); // thread id used temp to store transaction id ...
                }
                std::ifstream warnings_in( bhl->GetPathToWarnings().c_str() );
                if ( warnings_in.good() ) {
                    std::string line;
                    while(std::getline(warnings_in, line).good()) {
                        push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, line.c_str());
                        warnings++;
                    }
                    warnings_in.close();
                }
                RemoveFile(bhl->GetPathToWarnings().c_str(), false);

                table_lock_manager.ReleaseWriteLocks(*trs);
            } else {

                RCTableLoadAutoPtr rct;                  
                std::string file_path = table_path + ".bht";
                std::vector<DTCollation> charsets;
                rct = RCTableLoadAutoPtr(new RCTableLoad(file_path.c_str(), 1, charsets)); 
                rct->ClearLoadingFile(); // connection id is not matched , can't not rollback
                
                Transaction* trs = GetThreadParams(*thd)->GetTransaction();
                if(table_lock_manager.AcquireWriteLock(*trs,table_path,thd->killed, thd->some_tables_deleted).expired()) {
                    bherror = BHError(BHERROR_UNKNOWN, "AcquireWriteLock error.");
                    throw LD_Failed;
                }
                RemoveFile(bhl->GetPathToWarnings().c_str(), false);
                if(tp->killed()) {
                    table_lock_manager.ReleaseWriteLocks(*trs);
                    bherror = BHError(BHERROR_KILLED, error_messages[BHERROR_KILLED]);
                    thd->send_kill_message();
                }

                else if(*bhl->ErrorMessage() != 0)
                    bherror = BHError(bhl->ReturnCode(), bhl->ErrorMessage());
                else
                    bherror = BHError(bhl->ReturnCode());
                error = 1; //rollback required
                table_lock_manager.ReleaseWriteLocks(*trs);
                
                throw LD_Failed;
            }
        }

        // We must invalidate the table in query cache before binlog writing and
        // ha_autocommit_...
        query_cache_invalidate3(thd, table_list, 0);

        info.records = ha_rows((_int64)bhl->NoRecords());
        info.copied  = ha_rows((_int64)bhl->NoCopied());
        info.deleted = ha_rows((_int64)bhl->NoDeleted());

        sprintf(name, ER(ER_LOAD_INFO), (ulong) info.records, (ulong) info.deleted,
                (ulong) (info.records - info.copied), (ulong) warnings);
        my_ok(thd, info.copied+info.deleted, 0L, name);
        //send_ok(thd,info.copied+info.deleted,0L,name);
    } catch(BHEngineReturnValues& bherv) {
        bherror = BHError(BHERROR_UNKNOWN, error_messages[BHERROR_UNKNOWN]);
        ret = bherv;
    } catch(...) {
        bherror = BHError(BHERROR_UNKNOWN, error_messages[BHERROR_UNKNOWN]);
        ret = LD_Failed;
    }

    if(thd->transaction.stmt.modified_non_trans_table){
        thd->transaction.all.modified_non_trans_table= TRUE;
    }
    
    if(transactional_table){
        error = ha_autocommit_or_rollback(thd, error);
    }

    // Load in other process or terminated unnormal(mannual remove file)
    if(ret==LD_Locked) {        
        ret=LD_Failed;
    } else {
        Transaction* trs = GetThreadParams(*thd)->GetTransaction();
        if(table_lock_manager.AcquireWriteLock(*trs,table_path,thd->killed, thd->some_tables_deleted).expired()) {
            bherror = BHError(BHERROR_UNKNOWN, "Cannot lock table for write before commit .");
            ret= LD_Failed;
        }
    }
    thd->abort_on_warning = 0;
    return ret;
}
