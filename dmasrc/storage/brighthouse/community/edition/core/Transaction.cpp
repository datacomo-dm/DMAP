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

#include "edition/core/Transaction.h"
#include "core/RCTableImpl.h"
#include "core/TransactionManager.h"
#include "edition/core/Transaction.h"
RCTablePtr Transaction::GetTableShared(const std::string& table_path, struct st_table_share* table_share)
{
    stdext::hash_map<std::string, TableView>::iterator iter = m_modified_tables.find(table_path);
    if (iter != m_modified_tables.end())
        return iter->second.table;
    return m_rcengine.GetTableShared(table_path, table_share);
}

void Transaction::AddTableWR(const std::string& table_path, THD* thd, st_table* table)
{
    if (m_modified_tables.find(table_path) == m_modified_tables.end()) {
        RCTablePtr tab = rceng->GetTableForWrite(rceng->GetTablePath(thd, table).c_str(), table->s);
        m_modified_tables[table_path] = tab;
    }
}

RCTablePtr Transaction::RefreshTable(string const& table_path, uint packrow_size, bool fill_up_last_packrow)
{
    stdext::hash_map<std::string, TableView>::iterator iter = m_modified_tables.find(table_path);
//  BHASSERT_WITH_NO_PERFORMANCE_IMPACT(iter != m_modified_tables.end());
    std::vector<DTCollation> charsets;
    std::string tab_path = table_path;
    tab_path += rceng->m_tab_ext;
    RCTablePtr tab;
    if(iter != m_modified_tables.end()) {
        tab= iter->second.table;
        if(tab)
            charsets = tab->GetCharsets();
    }
    tab = RCTablePtr(new RCTable(tab_path.c_str(), charsets, 0, RCTableImpl::OpenMode::FORCE));

    // m_modified_tables[table_path].table = tab;
    m_modified_tables[table_path] = tab;
    tab->LoadDirtyData(id);
    return tab;
}

void Transaction::SetTableReleaseFlag(std::string const& table_path,bool release_cache_pack)
{
    stdext::hash_map<std::string, TableView>::iterator iter = m_modified_tables.find(table_path);

    std::string tab_path = table_path;
    tab_path += rceng->m_tab_ext;
    RCTablePtr tab;
    if(iter != m_modified_tables.end()) {
        tab= iter->second.table;
        if(tab){
            tab->Set_LoadDataFromTruncatePartition(release_cache_pack);
        }
    }
}


void Transaction::ApplyPendingChanges(THD* thd, const std::string& table_path, struct st_table_share* table_share)
{
}

void Transaction::ApplyPendingChanges(THD* thd, const std::string& table_path, struct st_table_share* table_share, RCTable* tab)
{
}

int Transaction::Commit(THD* thd)
{
    //TODO: <Michal> exceptions!!!
    IBGuard drop_rename_mutex_guard(drop_rename_mutex);
    stdext::hash_map<std::string, TableView>::iterator iter;

    try {
        VerifyTablesIntegrity();
    } catch (...) {
        rceng->Rollback(thd, true);
        return 1;
    }

    for (iter=m_modified_tables.begin(); iter!=m_modified_tables.end(); iter++) {
        iter->second.save_result = iter->second.table->CommitSaveSession(id);

        // try to fix dma-1118
        long conn_id =id; //(long)thd->variables.pseudo_thread_id;
        iter->second.table->ClearLoadCtlFile(conn_id);
    }

    if(!thd->transaction_rollback_request && !thd->killed) {
        {
            IBGuard global_mutex_guard(global_mutex);
            for (iter=m_modified_tables.begin(); iter!=m_modified_tables.end(); iter++) {
                RCTable* tab = iter->second.table.get();

                if (sync_buffers)
                    TransactionBase::FlushTableBuffers(tab);

                tab->CommitSwitch(iter->second.save_result);

                if(tab->Get_LoadDataFromTruncatePartition()){  // 长会话合并完成后,刷新缓存

                     ReleaseLocalObject(tab->GetID());
                     ReleaseGlobalObject(tab->GetID());
                     GlobalDataCache::GetGlobalDataCache().ReleaseDPNs(tab->GetID());
                     GlobalDataCache::GetGlobalDataCache().ReleasePacks(tab->GetID());
                }
                else{
                    int initial_packrow_id = iter->second.initial_no_packs ? iter->second.initial_no_packs - 1 : 0;
                    GlobalDataCache::GetGlobalDataCache().ReleasePackRow(tab->GetID(), initial_packrow_id);
                    for(int a = 0; a < tab->NoAttrs(); a++) {
                        GlobalDataCache::GetGlobalDataCache().DropObject(FTreeCoordinate(tab->GetID(),a));
                        GlobalDataCache::GetGlobalDataCache().DropObject(SpliceCoordinate( tab->GetID(), a, initial_packrow_id / DEFAULT_SPLICE_SIZE ));
                    }
                }
                tab->Set_LoadDataFromTruncatePartition(false);
            }
            GlobalDataCache::GetGlobalDataCache().MoveCaches(m_localCache);
            for (iter=m_modified_tables.begin(); iter!=m_modified_tables.end(); iter++) {
                
                // try to fix dma-1361:合并数据后查询不到数据的问题, delete by liujs
                // rceng->ReplaceTable(iter->first, iter->second.table);

                { // 采用下面的代码进行实现
                    std::vector<DTCollation> charsets;
                    RCTablePtr tab_new;
                    RCTable* tab = iter->second.table.get();
                    if(tab){
                        charsets = tab->GetCharsets();
                    }                
                    tab_new = RCTablePtr(new RCTable(tab->GetPath().c_str(), charsets, 0, RCTableImpl::OpenMode::FORCE));
                    rceng->ReplaceTable(iter->first, tab_new);
                }
                if(rsi_manager){
                    rsi_manager->UpdateDefForTable(iter->second.table->GetID());
                }
            }
        }
        for (iter=m_modified_tables.begin(); iter!=m_modified_tables.end(); iter++) {
            RCTablePtr t = iter->second.table;
            for ( int i( 0 ), cols( t->NoAttrs() ); i < cols; ++ i ) {
                if ( iter->second._outliers[i] > 0 ) {
                    rccontrol << lock << "Decomposition of " << iter->first << "." << t->GetAttr(i)->Name()
                              << " left " << iter->second._outliers[i] << " outliers." << unlock;
                } else if ( iter->second._outliers[i] == 0 ) {
                    rccontrol << lock << "Decomposition of " << iter->first << "." << t->GetAttr(i)->Name()
                              << " left no outliers." << unlock;
                }
            }
            table_lock_manager.ReleaseLocks(*this, iter->first);
        }
        m_modified_tables.clear();
        table_lock_manager.ReleaseWriteLocks(*this);

    } else {
        rceng->Rollback(thd, true);
        return 1;
    }
    return 0;
}

void Transaction::Rollback(THD* thd, bool force_error_message)
{
    IBGuard drop_rename_mutex_guard(drop_rename_mutex);
    if (m_modified_tables.size()) {
        //if(force_error_message || thd->query_error || (!all && (thd->options & OPTION_NOT_AUTOCOMMIT))) {
        // GA make sure query_error is not required for ga migration
        if(force_error_message) {
            std::string message("The Infobright storage engine has encountered an unexpected error. The current transaction has been rolled back.");
            rclog << lock << message.c_str() << unlock;
            message += " For details on the error please see the brighthouse.log file in the " + infobright_data_dir + " directory.";
            my_message(ER_XA_RBROLLBACK, message.c_str(), MYF(0));
        }
    }
    stdext::hash_map<std::string, TableView>::iterator iter;
    for (iter=m_modified_tables.begin(); iter!=m_modified_tables.end(); iter++) {
        RCTable* tab = iter->second.table.get();
        tab->Rollback(id);

        // try to fix dma-1118
        long conn_id = id;//(long)thd->variables.pseudo_thread_id;
        tab->ClearLoadCtlFile(conn_id);

        table_lock_manager.ReleaseLocks(*this, iter->first);
        if(rsi_manager)
            rsi_manager->UpdateDefForTable(tab->GetID());
    }
    m_modified_tables.clear();
    table_lock_manager.ReleaseWriteLocks(*this);
}
