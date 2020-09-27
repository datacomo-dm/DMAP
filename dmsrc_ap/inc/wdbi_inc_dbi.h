#ifndef _WDBI_INC_H
#define _WDBI_INC_H
#define INT_INVALID_HANDLE	-1
#define INT_ALLOCATE_FAIL   -2
#define COLUMN_TYPE_CHAR	1 //SQLT_CHR
#define COLUMN_TYPE_FLOAT	4 //SQLT_FLT
#define COLUMN_TYPE_NUM		2 //SQLT_NUM
#define COLUMN_TYPE_INT		3 //SQLT_INT
#define COLUMN_TYPE_LONG	8 //SQLT_LNG
#define COLUMN_TYPE_BIGINT	8 //SQLT_LNG
#define COLUMN_TYPE_DATE	12 //SQLT_DAT
#define VALUESET_SET		0 
#define VALUESET_ADD		1
#define VALUESET_SUB		2
#define DTDBTYPE_ORACLE	1002
#define DTDBTYPE_ODBC	1004
#ifdef __unix
#define getch getchar
#endif
#include <stdio.h>
#include "wdbierr.h"
#ifndef BOOL
#define FALSE 0
#define TRUE 1
#define BOOL int
#endif

//////////////////////////////////////////////////////////////////
// Common function (5). 
//
//
//////////////////////////////////////////////////////////////////

#ifndef WDBI_DEBUG
#define wdbiMainEntrance(func,asyncmode,ptr,catchLevel)				   _wdbiMainEntrance(func,asyncmode,ptr,catchLevel)				  
#define wdbidestroy(handle)                                                        _wdbidestroy(handle)                                                      
#define wdbiDestroyAll()                                                           _wdbiDestroyAll()                                                         
#define wdbiBreakAll()                                                             _wdbiBreakAll()                                                           
#define wdbiCreateSession(username,password,svcname,dbtype)                        _wdbiCreateSession(username,password,svcname,dbtype)                             
#define wdbiCommit(sess)                                                           _wdbiCommit(sess)                                                         
#define wdbiRollback(sess)                                                         _wdbiRollback(sess)                                                       
#define wdbiSetTerminate(sess,val)                                                 _wdbiSetTerminate(sess,val)                                               
#define wdbiIsTerminate(sess)                                                      _wdbiIsTerminate(sess)                                                    
#define wdbiSetNonBlockMode(sess)                                                  _wdbiSetNonBlockMode(sess)                                                
#define wdbiCreateStatment(sess)                                                   _wdbiCreateStatment(sess)                                                 
#define wdbiBindStrByPos(stmt,pos,ptr,len)                                         _wdbiBindStrByPos(stmt,pos,ptr,len)                                       
#define wdbiBindDoubleByPos(stmt,pos,ptr)                                          _wdbiBindDoubleByPos(stmt,pos,ptr)                                        
#define wdbiBindIntByPos(stmt,pos,ptr)                                             _wdbiBindIntByPos(stmt,pos,ptr)                                           
#define wdbiBindDateByPos(stmt,pos,ptr)                                            _wdbiBindDateByPos(stmt,pos,ptr)                                          
#define wdbiDefineStrByPos(stmt,pos,ptr,len)                                       _wdbiDefineStrByPos(stmt,pos,ptr,len)                                     
#define wdbiDefineDoubleByPos(stmt,pos,ptr)                                        _wdbiDefineDoubleByPos(stmt,pos,ptr)                                      
#define wdbiDefineIntByPos(stmt,pos,ptr)                                           _wdbiDefineIntByPos(stmt,pos,ptr)                                         
#define wdbiDefineDateByPos(stmt,pos,ptr)                                          _wdbiDefineDateByPos(stmt,pos,ptr)                                        
#define wdbiBreakAndReset(stmt)                                                    _wdbiBreakAndReset(stmt)                                                  
#define wdbiExecute(stmt,times)                                                    _wdbiExecute(stmt,times)                                                  
#define wdbiExecuteAt(stmt,times,offset)                                           _wdbiExecuteAt(stmt,times,offset)                                         
#define wdbiFetch(stmt,rows)                                                       _wdbiFetch(stmt,rows)                                                     
#define wdbiGetStmtColumnsNum(stmt)                                                _wdbiGetStmtColumnsNum(stmt)                                              
#define wdbiGetMtColumnsNum(mt)                                                    _wdbiGetMtColumnsNum(mt)                                                  
#define wdbiGetFetchedRows(stmt)                                                   _wdbiGetFetchedRows(stmt)                                                 
#define wdbiGetSession(stmt)                                                       _wdbiGetSession(stmt)                                                     
#define wdbiGetStmtType(stmt)                                                      _wdbiGetStmtType(stmt)                                                    
#define wdbiPrepareStmt(stmt,sqlstmt)                                              _wdbiPrepareStmt(stmt,sqlstmt)                                            
#define wdbiGetCreateTableSQL(memtab,buf,tabname,ismysql)                          _wdbiGetCreateTableSQL(memtab,buf,tabname,ismysql)                        
#define wdbiCreateMemTable()                                                       _wdbiCreateMemTable()                                                     
#define wdbiAddColumn(memtab,name,dspname,ctype,length,scale)                      _wdbiAddColumn(memtab,name,dspname,ctype,length,scale)                    
#define wdbiBuildStmt(memtab,stmt,rows)                                            _wdbiBuildStmt(memtab,stmt,rows)                                          
#define wdbiBuild(memtab,rows)                                                     _wdbiBuild(memtab,rows)                                                   
#define wdbiClear(memtab)                                                          _wdbiClear(memtab)                                                        
#define wdbiAppendToDbTable(memtab,tablename,sess,autocommit)                      _wdbiAppendToDbTable(memtab,tablename,sess,autocommit)                    
#define wdbiFetchAll(memtab)                                                       _wdbiFetchAll(memtab)                                                     
#define wdbiFetchFirst(memtab,rows)                                                _wdbiFetchFirst(memtab,rows)                                              
#define wdbiFetchAt(memtab,rows,st)                                                _wdbiFetchAt(memtab,rows,st)                                              
#define wdbiFetchNext(memtab,rows)                                                 _wdbiFetchNext(memtab,rows)                                               
#define wdbiGeneTable(memtab,tablename,sess)                                       _wdbiGeneTable(memtab,tablename,sess)                                     
#define wdbiSetMTName(memtab,mtname)                                               _wdbiSetMTName(memtab,mtname)                                             
#define wdbiGetStrAddrByName(memtab,col,rowst,pstr,celllen)                        _wdbiGetStrAddrByName(memtab,col,rowst,pstr,celllen)                      
#define wdbiGetDateAddrByName(memtab,col,rowst,pstr)                               _wdbiGetDateAddrByName(memtab,col,rowst,pstr)                             
#define wdbiGetDoubleAddrByName(memtab,col,rowst,pstr)                             _wdbiGetDoubleAddrByName(memtab,col,rowst,pstr)                           
#define wdbiGetIntAddrByName(memtab,col,rowst,pstr)                                _wdbiGetIntAddrByName(memtab,col,rowst,pstr)                              
#define wdbiGetStrValByName(memtab,col,rowst,pstr)                                 _wdbiGetStrValByName(memtab,col,rowst,pstr)                               
#define wdbiGetDateValByName(memtab,col,rowst,pstr)                                _wdbiGetDateValByName(memtab,col,rowst,pstr)                              
#define wdbiGetDoubleValByName(memtab,col,rowst)                                   _wdbiGetDoubleValByName(memtab,col,rowst)                                 
#define wdbiGetIntValByName(memtab,col,rowst)                                      _wdbiGetIntValByName(memtab,col,rowst)                                    
#define wdbiGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                         _wdbiGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                       
#define wdbiGetDateAddrByPos(memtab,col,rowst,pstr)                                _wdbiGetDateAddrByPos(memtab,col,rowst,pstr)                              
#define wdbiGetDoubleAddrByPos(memtab,col,rowst,pstr)                              _wdbiGetDoubleAddrByPos(memtab,col,rowst,pstr)                            
#define wdbiGetIntAddrByPos(memtab,col,rowst,pstr)                                 _wdbiGetIntAddrByPos(memtab,col,rowst,pstr)                               
#define wdbiGetBufferLen(memtab)                                                   _wdbiGetBufferLen(memtab)                                                 
#define wdbiGetCell(memtab,row,col,str,rawpos)                                     _wdbiGetCell(memtab,row,col,str,rawpos)                                   
#define wdbiGetColumnDisplayWidth(memtab,col)                                      _wdbiGetColumnDisplayWidth(memtab,col)                                    
#define wdbiGetColumnPosByName(memtab,colname)                                     _wdbiGetColumnPosByName(memtab,colname)                                   
#define wdbiGetColumnName(memtab,id,colname)                                       _wdbiGetColumnName(memtab,id,colname)                                     
#define wdbiGetColumnDataLenByPos(memtab,colid)                                    _wdbiGetColumnDataLenByPos(memtab,colid)                                  
#define wdbiGetColumnNumber(memtab)                                                _wdbiGetColumnNumber(memtab)                                              
#define wdbiGetColumnScale(memtab,colid)                                           _wdbiGetColumnScale(memtab,colid)                                         
#define wdbiGetColumnTitle(memtab,colid,str,len)                                   _wdbiGetColumnTitle(memtab,colid,str,len)                                 
#define wdbiGetColumnType(memtab,colid)                                            _wdbiGetColumnType(memtab,colid)                                          
#define wdbiGetLine(memtab,row,str,rawpos,colsnm)                                  _wdbiGetLine(memtab,row,str,rawpos,colsnm)                                
#define wdbiGetCompactLen(memtab,rowstart,rownum,colsnm,clen)			           _wdbiGetCompactLen(memtab,rowstart,rownum,colsnm,clen)
#define wdbiGetCompactLine(memtab,row,str,rawpos,colsnm,clen)                      _wdbiGetCompactLine(memtab,row,str,rawpos,colsnm,clen)                               
#define wdbiGetCompactTitle(memtab,str,len,colsnm,clen)                            _wdbiGetTitle(memtab,str,len,colsnm,clen)                                      
#define wdbiGetMemtableRows(memtab)                                                _wdbiGetMemtableRows(memtab)                                              
#define wdbiGetTitle(memtab,str,len,colsnm)                                        _wdbiGetTitle(memtab,str,len,colsnm)                                      
#define wdbiIsIKSet(memtab)                                                        _wdbiIsIKSet(memtab)                                                      
#define wdbiOrderByIK(memtab)                                                      _wdbiOrderByIK(memtab)                                                    
#define wdbiSearchIK(memtab,key)                                                   _wdbiSearchIK(memtab,key)                                                 
#define wdbiSearchIKLE(memtab,key)                                                 _wdbiSearchIKLE(memtab,key)                                               
#define wdbiSetColumnDisplayName(memtab,colnm,str)                                 _wdbiSetColumnDisplayName(memtab,colnm,str)                               
#define wdbiSetIKByName(memtab,str)                                                _wdbiSetIKByName(memtab,str)                                              
#define wdbiSetSortColumn(memtab,colsnm)                                           _wdbiSetSortColumn(memtab,colsnm)                                         
#define wdbiReInOrder(memtab)                                                      _wdbiReInOrder(memtab)                                                    
#define wdbiSetStrValues(memtab,colid,rowstart,rownum,bf)                          _wdbiSetStrValues(memtab,colid,rowstart,rownum,bf)                        
#define wdbiSetDateValues(memtab,colid,rowstart,rownum,bf)                         _wdbiSetDateValues(memtab,colid,rowstart,rownum,bf)                       
#define wdbiSetDoubleValues(memtab,colid,rowstart,rownum,bf)                       _wdbiSetDoubleValues(memtab,colid,rowstart,rownum,bf)                     
#define wdbiSetIntValues(memtab,colid,rowstart,rownum,bf)                         _wdbiSetIntValues(memtab,colid,rowstart,rownum,bf)                       
#define wdbiSort(memtab)                                                           _wdbiSort(memtab)                                                         
#define wdbiSortHeap(memtab)                                                       _wdbiSortHeap(memtab)                                                     
#define wdbiSetGroupSrc(memtab,src)                                                _wdbiSetGroupSrc(memtab,src)                                              
#define wdbiSetIKGroupRef(memtab,ref,colnm)                                        _wdbiSetIKGroupRef(memtab,ref,colnm)                                      
#define wdbiSetGroupSrcCol(memtab,colsnm)                                          _wdbiSetGroupSrcCol(memtab,colsnm)                                        
#define wdbiSetGroupRefCol(memtab,colsnm)                                          _wdbiSetGroupRefCol(memtab,colsnm)                                        
#define wdbiSetSrcSumCol(memtab,colsnm)                                            _wdbiSetSrcSumCol(memtab,colsnm)                                          
#define wdbiGroup(memtab,rowstart,rownum)                                          _wdbiGroup(memtab,rowstart,rownum)                                        
#define wdbiCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                      _wdbiCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                    
#define wdbiGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                            _wdbiGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                          
#define wdbiCopyColumnDefine(memtab,memtabfrom,colsname)                           _wdbiCopyColumnDefine(memtab,memtabfrom,colsname)                         
#define wdbiExportSomeRows(memtab,pData,startrn,rnnum)                             _wdbiExportSomeRows(memtab,pData,startrn,rnnum)                           
#define wdbiExport(memtab,pData,_len,_maxrows,_rowct)                              _wdbiExport(memtab,pData,_len,_maxrows,_rowct)                            
#define wdbiImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)       _wdbiImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)     
#define wdbiAppendRows(memtab,pData,rnnum)                                         _wdbiAppendRows(memtab,pData,rnnum)                                       
#define wdbiReset(memtab)                                                          _wdbiReset(memtab)                                                        
#define wdbiClearIK(memtab)                                                        _wdbiClearIK(memtab)                                                      
#define wdbiClearSort(memtab)                                                      _wdbiClearSort(memtab)                                                    
#define wdbiGetMaxRows(memtab)                                                     _wdbiGetMaxRows(memtab)                                                   
#define wdbiCompact(memtab)                                                        _wdbiCompact(memtab)                                                      
#define wdbiGetRowLen(memta)                                                       _wdbiGetRowLen(memta)                                                     
#define wdbiCreateExcelEnv()                                                       _wdbiCreateExcelEnv()                                                     
#define wdbiSetDir(excel,strTemplate,strReport)                                    _wdbiSetDir(excel,strTemplate,strReport)                                  
#define wdbiSetMemTable(excel,memtab)                                              _wdbiSetMemTable(excel,memtab)                                            
#define wdbiLoadTemplate(excel,tempname)                                           _wdbiLoadTemplate(excel,tempname)                                         
#define wdbiSelectSheet(excel,sheetname)                                           _wdbiSelectSheet(excel,sheetname)                                         
#define wdbiFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)  _wdbiFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)
#define wdbiFillTitle(excel,tocol,torow,fromcol,colnum)                            _wdbiFillTitle(excel,tocol,torow,fromcol,colnum)                          
#define wdbiSaveAs(excel,filename)                                                 _wdbiSaveAs(excel,filename)                                               
#define wdbiGetMemUsed(memtab)                                                     _wdbiGetMemUsed(memtab)                                                   
#define wdbiSetSortedGroupRef(memtab,mtref, colssrc)                               _wdbiSetSortedGroupRef(memtab,mtref, colssrc)                             
#define wdbiSearch(memtab,ptr)                                                     _wdbiSearch(memtab,ptr)                                                   
#define wdbiSearchLE(memtab,ptr)                                                   _wdbiSearchLE(memtab,ptr)                                                 
#define wdbiValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                    _wdbiValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                  
#define wdbiDeleteRow(memtab,rown)                                                 _wdbiDeleteRow(memtab,rown)                                                  
#define wdbiInsertRows(memtab,ptr,colsname,num)                                    _wdbiInsertRows(memtab,ptr,colsname,num)                                     
#define wdbiBindToStatment(memtab,stmt,colsname,rowst)                             _wdbiBindToStatment(memtab,stmt,colsname,rowst)                              
#define wdbiTestTable(sess,tablename)                                              _wdbiTestTable(sess,tablename)                                               
#define wdbiGetRawrnBySort(memtab,ind)                                             _wdbiGetRawrnBySort(memtab,ind)                                              
#define wdbiGetRawrnByIK(memtab,ind)                                               _wdbiGetRawrnByIK(memtab,ind)                                                
#define wdbiCompressBf(memtab)                                                     _wdbiCompressBf(memtab)                                                      
#define wdbiSearchQDel(memtab,key,schopt)                                          _wdbiSearchQDel(memtab,key,schopt)                                           
#define wdbiIsQDelete(memtab,rownm)                                                _wdbiIsQDelete(memtab,rownm)                                                 
#define wdbiQDeletedRows(memtab)                                                   _wdbiQDeletedRows(memtab)                                                    
#define wdbiQDeleteRow(memtab,rownm)                                               _wdbiQDeleteRow(memtab,rownm)                                                
#define wdbiWaitStmt(stmt)                                                         _wdbiWaitStmt(stmt)                                                          
#define wdbiWaitMemtable(memtab)                                                   _wdbiWaitMemtable(memtab)                                                    
#define wdbiWaitLastReturn(handle)                                                 _wdbiWaitLastReturn(handle)                                                  
#define wdbiWaitTime(handle,time)                                                  _wdbiWaitTime(handle,time)                                                   
#define wdbiFreshRowNum(memtab)                                                    _wdbiFreshRowNum(memtab)                                                     
#define wdbiBatchSelect(result,param,colsnm)                                       _wdbiBatchSelect(result,param,colsnm)                                        
#define wdbiMTPrint(memtab,rownm,colsnm)                                           _wdbiMTPrint(memtab,rownm,colsnm,false)                                            
#define wdbiMTCompactPrint(memtab,rownm,colsnm)                                    _wdbiMTPrint(memtab,rownm,colsnm,true)                                            
#define wdbiGetCurDateTime(date)                                                   _wdbiGetCurDateTime(date)                                                    
#define wdbiSetDateTime(date,year,mon,day,hour,min,sec)                            _wdbiSetDateTime(date,year,mon,day,hour,min,sec)                             
#define wdbiSetDate(date,year,mon,day)                                             _wdbiSetDate(date,year,mon,day)                                              
#define wdbiDateTimeToStr(date,str)                                                _wdbiDateTimeToStr(date,str)                                                 
#define wdbiGetYear(date)                                                          _wdbiGetYear(date)                                                           
#define wdbiGetMonth(date)                                                         _wdbiGetMonth(date)                                                          
#define wdbiGetDay(date)                                                           _wdbiGetDay(date)                                                            
#define wdbiGetHour(date)                                                          _wdbiGetHour(date)                                                           
#define wdbiGetMin(date)                                                           _wdbiGetMin(date)                                                            
#define wdbiGetSec(date)                                                           _wdbiGetSec(date)                                                            
#define wdbiSetEcho(val)                                                           _wdbiSetEcho(val)                                                            
#define wdbiIsEcho()                                                           	   _wdbiIsEcho()                                                            
#define wdbiMTToTextFile(memtab,fn,rownm,colsnm)                                   _wdbiMTToTextFile(memtab,fn,rownm,colsnm)                                    
#define wdbiGetMTName(memtab,bf)                                                   _wdbiGetMTName(memtab,bf)                                                    
#define wdbiReadFromTextFile(memtab,fn,rowst,rownm)                                _wdbiReadFromTextFile(memtab,fn,rowst,rownm)                                 
#define wdbiFetchAllAt(memtab,st)                                                  _wdbiFetchAllAt(memtab,st)                                                   
#define wdbiDateDiff(d1,d2)                                                        _wdbiDateDiff(d1,d2)                                                         
#define wdbiSetOutputToConsole(val)                                                _wdbiSetOutputToConsole(val)                                                 
#define wdbiSetTraceFile(fn)                                             	   _wdbiSetTraceFile(fn)                                              
#define wdbiSetErrorFile(fn)                                                       _wdbiSetErrorFile(fn)                                                        
#define wdbiSetLogFile(fn)                                                         _wdbiSetLogFile(fn)                                                          
#define WDBIInit(appname)                                                          _WDBIInit(appname)                                                           
#define WDBIQuit()                                                                 _WDBIQuit()                                                                  
#define wdbiReplaceStmt(memtab,stmt)                                               _wdbiReplaceStmt(memtab,stmt)                                                
#define wdbiConvertColStrToInt(memtab,colsn,pcol)                                  _wdbiConvertColStrToInt(memtab,colsn,pcol)                                   
#define wdbiSaveSort(memtab,fp)                                                    _wdbiSaveSort(memtab,fp)                                                     
#define wdbiLoadSort(memtab,fp)                                                    _wdbiLoadSort(memtab,fp)                                                     
#define wdbiCopyToMySQL(memtab,startrow,rownum,fp)                                 _wdbiCopyToMySQL(memtab,startrow,rownum,fp)                                  
#define wdbiCompareSortRow(memtab,row1, row2)                                      _wdbiCompareSortRow(memtab,row1, row2)                                       
#define wdbiList(pout)                                                             _wdbiList(pout)                                                              
#define wdbiGetLastError(handle)                                                   _wdbiGetLastError(handle)                                                    
#define wdbiAddrFresh(memtab,colval,collen,tp)                                     _wdbiAddrFresh(memtab,colval,collen,tp)                                      
#define wdbiGetColumnInfo(mt,crttab,ismysql)                               	   _wdbiGetColumnInfo(mt,crttab,ismysql)                                      
#define wdbiGetFetchSize(stmt)							   _wdbiGetFetchSize(stmt)
#define wdbiSetFetchSize(stmt,fsize)						   _wdbiSetFetchSize(stmt,fsize)
#else
#define LOGWHERE _wdbilogwhere(__FILE__,__LINE__)
#define wdbiMainEntrance(func,asyncmode,ptr,catchLevel)				   (LOGWHERE,_wdbiMainEntrance(func,asyncmode,ptr,catchLevel)				  )
#define wdbidestroy(handle)                                                        (LOGWHERE,_wdbidestroy(handle)                                                         )                            
#define wdbiDestroyAll()                                                           (LOGWHERE,_wdbiDestroyAll()                                                            )
#define wdbiBreakAll()                                                             (LOGWHERE,_wdbiBreakAll()                                                              )
#define wdbiCreateSession(username,password,svcname,dbtype)                        (LOGWHERE,_wdbiCreateSession(username,password,svcname,dbtype)                         )
#define wdbiCommit(sess)                                                           (LOGWHERE,_wdbiCommit(sess)                                                            )
#define wdbiRollback(sess)                                                         (LOGWHERE,_wdbiRollback(sess)                                                          )
#define wdbiSetTerminate(sess,val)                                                 (LOGWHERE,_wdbiSetTerminate(sess,val)                                                  )
#define wdbiIsTerminate(sess)                                                      (LOGWHERE,_wdbiIsTerminate(sess)                                                       )
#define wdbiSetNonBlockMode(sess)                                                  (LOGWHERE,_wdbiSetNonBlockMode(sess)                                                   )
#define wdbiCreateStatment(sess)                                                   (LOGWHERE,_wdbiCreateStatment(sess)                                                    )
#define wdbiBindStrByPos(stmt,pos,ptr,len)                                         (LOGWHERE,_wdbiBindStrByPos(stmt,pos,ptr,len)                                          )
#define wdbiBindDoubleByPos(stmt,pos,ptr)                                          (LOGWHERE,_wdbiBindDoubleByPos(stmt,pos,ptr)                                           )
#define wdbiBindIntByPos(stmt,pos,ptr)                                             (LOGWHERE,_wdbiBindIntByPos(stmt,pos,ptr)                                              )
#define wdbiBindDateByPos(stmt,pos,ptr)                                            (LOGWHERE,_wdbiBindDateByPos(stmt,pos,ptr)                                             )
#define wdbiDefineStrByPos(stmt,pos,ptr,len)                                       (LOGWHERE,_wdbiDefineStrByPos(stmt,pos,ptr,len)                                        )
#define wdbiDefineDoubleByPos(stmt,pos,ptr)                                        (LOGWHERE,_wdbiDefineDoubleByPos(stmt,pos,ptr)                                         )
#define wdbiDefineIntByPos(stmt,pos,ptr)                                           (LOGWHERE,_wdbiDefineIntByPos(stmt,pos,ptr)                                            )
#define wdbiDefineDateByPos(stmt,pos,ptr)                                          (LOGWHERE,_wdbiDefineDateByPos(stmt,pos,ptr)                                           )
#define wdbiBreakAndReset(stmt)                                                    (LOGWHERE,_wdbiBreakAndReset(stmt)                                                     )
#define wdbiExecute(stmt,times)                                                    (LOGWHERE,_wdbiExecute(stmt,times)                                                     )
#define wdbiExecuteAt(stmt,times,offset)                                           (LOGWHERE,_wdbiExecuteAt(stmt,times,offset)                                            )
#define wdbiFetch(stmt,rows)                                                       (LOGWHERE,_wdbiFetch(stmt,rows)                                                        )
#define wdbiGetStmtColumnsNum(stmt)                                                (LOGWHERE,_wdbiGetStmtColumnsNum(stmt)                                                 )
#define wdbiGetMtColumnsNum(mt)                                                    (LOGWHERE,_wdbiGetMtColumnsNum(mt)                                                     )
#define wdbiGetFetchedRows(stmt)                                                   (LOGWHERE,_wdbiGetFetchedRows(stmt)                                                    )
#define wdbiGetSession(stmt)                                                       (LOGWHERE,_wdbiGetSession(stmt)                                                        )
#define wdbiGetStmtType(stmt)                                                      (LOGWHERE,_wdbiGetStmtType(stmt)                                                       )
#define wdbiPrepareStmt(stmt,sqlstmt)                                              (LOGWHERE,_wdbiPrepareStmt(stmt,sqlstmt)                                               )
#define wdbiGetCreateTableSQL(memtab,buf,tabname,ismysql)                          (LOGWHERE,_wdbiGetCreateTableSQL(memtab,buf,tabname,ismysql)                           )
#define wdbiCreateMemTable()                                                       (LOGWHERE,_wdbiCreateMemTable()                                                        )
#define wdbiAddColumn(memtab,name,dspname,ctype,length,scale)                      (LOGWHERE,_wdbiAddColumn(memtab,name,dspname,ctype,length,scale)                       )
#define wdbiBuildStmt(memtab,stmt,rows)                                            (LOGWHERE,_wdbiBuildStmt(memtab,stmt,rows)                                             )
#define wdbiBuild(memtab,rows)                                                     (LOGWHERE,_wdbiBuild(memtab,rows)                                                      )
#define wdbiClear(memtab)                                                          (LOGWHERE,_wdbiClear(memtab)                                                           )
#define wdbiAppendToDbTable(memtab,tablename,sess,autocommit)                      (LOGWHERE,_wdbiAppendToDbTable(memtab,tablename,sess,autocommit)                       )
#define wdbiFetchAll(memtab)                                                       (LOGWHERE,_wdbiFetchAll(memtab)                                                        )
#define wdbiFetchFirst(memtab,rows)                                                (LOGWHERE,_wdbiFetchFirst(memtab,rows)                                                 )
#define wdbiFetchAt(memtab,rows,st)                                                (LOGWHERE,_wdbiFetchAt(memtab,rows,st)                                                 )
#define wdbiFetchNext(memtab,rows)                                                 (LOGWHERE,_wdbiFetchNext(memtab,rows)                                                  )
#define wdbiGeneTable(memtab,tablename,sess)                                       (LOGWHERE,_wdbiGeneTable(memtab,tablename,sess)                                        )
#define wdbiSetMTName(memtab,mtname)                                               (LOGWHERE,_wdbiSetMTName(memtab,mtname)                                                )
#define wdbiGetStrAddrByName(memtab,col,rowst,pstr,celllen)                        (LOGWHERE,_wdbiGetStrAddrByName(memtab,col,rowst,pstr,celllen)                         )
#define wdbiGetDateAddrByName(memtab,col,rowst,pstr)                               (LOGWHERE,_wdbiGetDateAddrByName(memtab,col,rowst,pstr)                                )
#define wdbiGetDoubleAddrByName(memtab,col,rowst,pstr)                             (LOGWHERE,_wdbiGetDoubleAddrByName(memtab,col,rowst,pstr)                              )
#define wdbiGetIntAddrByName(memtab,col,rowst,pstr)                                (LOGWHERE,_wdbiGetIntAddrByName(memtab,col,rowst,pstr)                                 )
#define wdbiGetStrValByName(memtab,col,rowst,pstr)                                 (LOGWHERE,_wdbiGetStrValByName(memtab,col,rowst,pstr)                                  )
#define wdbiGetDateValByName(memtab,col,rowst,pstr)                                (LOGWHERE,_wdbiGetDateValByName(memtab,col,rowst,pstr)                                 )
#define wdbiGetDoubleValByName(memtab,col,rowst)                                   (LOGWHERE,_wdbiGetDoubleValByName(memtab,col,rowst)                                    )
#define wdbiGetIntValByName(memtab,col,rowst)                                      (LOGWHERE,_wdbiGetIntValByName(memtab,col,rowst)                                       )
#define wdbiGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                         (LOGWHERE,_wdbiGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                          )
#define wdbiGetDateAddrByPos(memtab,col,rowst,pstr)                                (LOGWHERE,_wdbiGetDateAddrByPos(memtab,col,rowst,pstr)                                 )
#define wdbiGetDoubleAddrByPos(memtab,col,rowst,pstr)                              (LOGWHERE,_wdbiGetDoubleAddrByPos(memtab,col,rowst,pstr)                               )
#define wdbiGetIntAddrByPos(memtab,col,rowst,pstr)                                 (LOGWHERE,_wdbiGetIntAddrByPos(memtab,col,rowst,pstr)                                  )
#define wdbiGetBufferLen(memtab)                                                   (LOGWHERE,_wdbiGetBufferLen(memtab)                                                    )
#define wdbiGetCell(memtab,row,col,str,rawpos)                                     (LOGWHERE,_wdbiGetCell(memtab,row,col,str,rawpos)                                      )
#define wdbiGetColumnDisplayWidth(memtab,col)                                      (LOGWHERE,_wdbiGetColumnDisplayWidth(memtab,col)                                       )
#define wdbiGetColumnPosByName(memtab,colname)                                     (LOGWHERE,_wdbiGetColumnPosByName(memtab,colname)                                      )
#define wdbiGetColumnName(memtab,id,colname)                                       (LOGWHERE,_wdbiGetColumnName(memtab,id,colname)                                        )
#define wdbiGetColumnDataLenByPos(memtab,colid)                                    (LOGWHERE,_wdbiGetColumnDataLenByPos(memtab,colid)                                     )
#define wdbiGetColumnNumber(memtab)                                                (LOGWHERE,_wdbiGetColumnNumber(memtab)                                                 )
#define wdbiGetColumnScale(memtab,colid)                                           (LOGWHERE,_wdbiGetColumnScale(memtab,colid)                                            )
#define wdbiGetColumnTitle(memtab,colid,str,len)                                   (LOGWHERE,_wdbiGetColumnTitle(memtab,colid,str,len)                                    )
#define wdbiGetColumnType(memtab,colid)                                            (LOGWHERE,_wdbiGetColumnType(memtab,colid)                                             )
#define wdbiGetLine(memtab,row,str,rawpos,colsnm)                                  (LOGWHERE,_wdbiGetLine(memtab,row,str,rawpos,colsnm)                                   )
#define wdbiGetCompactLine(memtab,row,str,rawpos,colsnm,clen)                      (LOGWHERE,_wdbiGetLine(memtab,row,str,rawpos,colsnm,clen)                              ) 
#define wdbiGetCompactLen(memtab,row,str,rawpos,colsnm,clen)                       (LOGWHERE,_wdbiGetCompactLen(memtab,row,str,rawpos,colsnm,clen)                        )       
#define wdbiGetCompactTitle(memtab,str,len,colsnm,clen)                            (LOGWHERE,_wdbiGetTitle(memtab,str,len,colsnm,clen)                                    )  
#define wdbiGetMemtableRows(memtab)                                                (LOGWHERE,_wdbiGetMemtableRows(memtab)                                                 )
#define wdbiGetTitle(memtab,str,len,colsnm)                                        (LOGWHERE,_wdbiGetTitle(memtab,str,len,colsnm)                                         )
#define wdbiIsIKSet(memtab)                                                        (LOGWHERE,_wdbiIsIKSet(memtab)                                                         )
#define wdbiOrderByIK(memtab)                                                      (LOGWHERE,_wdbiOrderByIK(memtab)                                                       )
#define wdbiSearchIK(memtab,key)                                                   (LOGWHERE,_wdbiSearchIK(memtab,key)                                                    )
#define wdbiSearchIKLE(memtab,key)                                                 (LOGWHERE,_wdbiSearchIKLE(memtab,key)                                                  )
#define wdbiSetColumnDisplayName(memtab,colnm,str)                                 (LOGWHERE,_wdbiSetColumnDisplayName(memtab,colnm,str)                                  )
#define wdbiSetIKByName(memtab,str)                                                (LOGWHERE,_wdbiSetIKByName(memtab,str)                                                 )
#define wdbiSetSortColumn(memtab,colsnm)                                           (LOGWHERE,_wdbiSetSortColumn(memtab,colsnm)                                            )
#define wdbiReInOrder(memtab)                                                      (LOGWHERE,_wdbiReInOrder(memtab)                                                       )
#define wdbiSetStrValues(memtab,colid,rowstart,rownum,bf)                          (LOGWHERE,_wdbiSetStrValues(memtab,colid,rowstart,rownum,bf)                           )
#define wdbiSetDateValues(memtab,colid,rowstart,rownum,bf)                         (LOGWHERE,_wdbiSetDateValues(memtab,colid,rowstart,rownum,bf)                          )
#define wdbiSetDoubleValues(memtab,colid,rowstart,rownum,bf)                       (LOGWHERE,_wdbiSetDoubleValues(memtab,colid,rowstart,rownum,bf)                        )
#define wdbiSetIntValues(memtab,colid,rowstart,rownum,bf)                         (LOGWHERE,_wdbiSetIntValues(memtab,colid,rowstart,rownum,bf)                          )
#define wdbiSort(memtab)                                                           (LOGWHERE,_wdbiSort(memtab)                                                            )
#define wdbiSortHeap(memtab)                                                       (LOGWHERE,_wdbiSortHeap(memtab)                                                        )
#define wdbiSetGroupSrc(memtab,src)                                                (LOGWHERE,_wdbiSetGroupSrc(memtab,src)                                                 )
#define wdbiSetIKGroupRef(memtab,ref,colnm)                                        (LOGWHERE,_wdbiSetIKGroupRef(memtab,ref,colnm)                                         )
#define wdbiSetGroupSrcCol(memtab,colsnm)                                          (LOGWHERE,_wdbiSetGroupSrcCol(memtab,colsnm)                                           )
#define wdbiSetGroupRefCol(memtab,colsnm)                                          (LOGWHERE,_wdbiSetGroupRefCol(memtab,colsnm)                                           )
#define wdbiSetSrcSumCol(memtab,colsnm)                                            (LOGWHERE,_wdbiSetSrcSumCol(memtab,colsnm)                                             )
#define wdbiGroup(memtab,rowstart,rownum)                                          (LOGWHERE,_wdbiGroup(memtab,rowstart,rownum)                                           )
#define wdbiCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                      (LOGWHERE,_wdbiCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                       )
#define wdbiGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                            (LOGWHERE,_wdbiGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                             )
#define wdbiCopyColumnDefine(memtab,memtabfrom,colsname)                           (LOGWHERE,_wdbiCopyColumnDefine(memtab,memtabfrom,colsname)                            )
#define wdbiExportSomeRows(memtab,pData,startrn,rnnum)                             (LOGWHERE,_wdbiExportSomeRows(memtab,pData,startrn,rnnum)                              )
#define wdbiExport(memtab,pData,_len,_maxrows,_rowct)                              (LOGWHERE,_wdbiExport(memtab,pData,_len,_maxrows,_rowct)                               )
#define wdbiImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)       (LOGWHERE,_wdbiImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)        )
#define wdbiAppendRows(memtab,pData,rnnum)                                         (LOGWHERE,_wdbiAppendRows(memtab,pData,rnnum)                                          )
#define wdbiReset(memtab)                                                          (LOGWHERE,_wdbiReset(memtab)                                                           )
#define wdbiClearIK(memtab)                                                        (LOGWHERE,_wdbiClearIK(memtab)                                                         )
#define wdbiClearSort(memtab)                                                      (LOGWHERE,_wdbiClearSort(memtab)                                                       )
#define wdbiGetMaxRows(memtab)                                                     (LOGWHERE,_wdbiGetMaxRows(memtab)                                                      )
#define wdbiCompact(memtab)                                                        (LOGWHERE,_wdbiCompact(memtab)                                                         )
#define wdbiGetRowLen(memta)                                                       (LOGWHERE,_wdbiGetRowLen(memta)                                                        )
#define wdbiCreateExcelEnv()                                                       (LOGWHERE,_wdbiCreateExcelEnv()                                                        )
#define wdbiSetDir(excel,strTemplate,strReport)                                    (LOGWHERE,_wdbiSetDir(excel,strTemplate,strReport)                                     )
#define wdbiSetMemTable(excel,memtab)                                              (LOGWHERE,_wdbiSetMemTable(excel,memtab)                                               )
#define wdbiLoadTemplate(excel,tempname)                                           (LOGWHERE,_wdbiLoadTemplate(excel,tempname)                                            )
#define wdbiSelectSheet(excel,sheetname)                                           (LOGWHERE,_wdbiSelectSheet(excel,sheetname)                                            )
#define wdbiFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)  (LOGWHERE,_wdbiFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)   )
#define wdbiFillTitle(excel,tocol,torow,fromcol,colnum)                            (LOGWHERE,_wdbiFillTitle(excel,tocol,torow,fromcol,colnum)                             )
#define wdbiSaveAs(excel,filename)                                                 (LOGWHERE,_wdbiSaveAs(excel,filename)                                                  )
#define wdbiGetMemUsed(memtab)                                                     (LOGWHERE,_wdbiGetMemUsed(memtab)                                                      )
#define wdbiSetSortedGroupRef(memtab,mtref, colssrc)                               (LOGWHERE,_wdbiSetSortedGroupRef(memtab,mtref, colssrc)                                )
#define wdbiSearch(memtab,ptr)                                                     (LOGWHERE,_wdbiSearch(memtab,ptr)                                                      )
#define wdbiSearchLE(memtab,ptr)                                                   (LOGWHERE,_wdbiSearchLE(memtab,ptr)                                                    )
#define wdbiValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                    (LOGWHERE,_wdbiValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                     )
#define wdbiDeleteRow(memtab,rown)                                                 (LOGWHERE,_wdbiDeleteRow(memtab,rown)                                                  )
#define wdbiInsertRows(memtab,ptr,colsname,num)                                    (LOGWHERE,_wdbiInsertRows(memtab,ptr,colsname,num)                                     )
#define wdbiBindToStatment(memtab,stmt,colsname,rowst)                             (LOGWHERE,_wdbiBindToStatment(memtab,stmt,colsname,rowst)                              )
#define wdbiTestTable(sess,tablename)                                              (LOGWHERE,_wdbiTestTable(sess,tablename)                                               )
#define wdbiGetRawrnBySort(memtab,ind)                                             (LOGWHERE,_wdbiGetRawrnBySort(memtab,ind)                                              )
#define wdbiGetRawrnByIK(memtab,ind)                                               (LOGWHERE,_wdbiGetRawrnByIK(memtab,ind)                                                )
#define wdbiCompressBf(memtab)                                                     (LOGWHERE,_wdbiCompressBf(memtab)                                                      )
#define wdbiSearchQDel(memtab,key,schopt)                                          (LOGWHERE,_wdbiSearchQDel(memtab,key,schopt)                                           )
#define wdbiIsQDelete(memtab,rownm)                                                (LOGWHERE,_wdbiIsQDelete(memtab,rownm)                                                 )
#define wdbiQDeletedRows(memtab)                                                   (LOGWHERE,_wdbiQDeletedRows(memtab)                                                    )
#define wdbiQDeleteRow(memtab,rownm)                                               (LOGWHERE,_wdbiQDeleteRow(memtab,rownm)                                                )
#define wdbiWaitStmt(stmt)                                                         (LOGWHERE,_wdbiWaitStmt(stmt)                                                          )
#define wdbiWaitMemtable(memtab)                                                   (LOGWHERE,_wdbiWaitMemtable(memtab)                                                    )
#define wdbiWaitLastReturn(handle)                                                 (LOGWHERE,_wdbiWaitLastReturn(handle)                                                  )
#define wdbiWaitTime(handle,time)                                                  (LOGWHERE,_wdbiWaitTime(handle,time)                                                   )
#define wdbiFreshRowNum(memtab)                                                    (LOGWHERE,_wdbiFreshRowNum(memtab)                                                     )
#define wdbiBatchSelect(result,param,colsnm)                                       (LOGWHERE,_wdbiBatchSelect(result,param,colsnm)                                        )
#define wdbiMTPrint(memtab,rownm,colsnm)                                           (LOGWHERE,_wdbiMTPrint(memtab,rownm,colsnm,false)                                      )
#define wdbiMTCompactPrint(memtab,rownm,colsnm)                                    (LOGWHERE,_wdbiMTPrint(memtab,rownm,colsnm,true)                                       )    
#define wdbiGetCurDateTime(date)                                                   (LOGWHERE,_wdbiGetCurDateTime(date)                                                    )
#define wdbiSetDateTime(date,year,mon,day,hour,min,sec)                            (LOGWHERE,_wdbiSetDateTime(date,year,mon,day,hour,min,sec)                             )
#define wdbiSetDate(date,year,mon,day)                                             (LOGWHERE,_wdbiSetDate(date,year,mon,day)                                              )
#define wdbiDateTimeToStr(date,str)                                                (LOGWHERE,_wdbiDateTimeToStr(date,str)                                                 )
#define wdbiGetYear(date)                                                          (LOGWHERE,_wdbiGetYear(date)                                                           )
#define wdbiGetMonth(date)                                                         (LOGWHERE,_wdbiGetMonth(date)                                                          )
#define wdbiGetDay(date)                                                           (LOGWHERE,_wdbiGetDay(date)                                                            )
#define wdbiGetHour(date)                                                          (LOGWHERE,_wdbiGetHour(date)                                                           )
#define wdbiGetMin(date)                                                           (LOGWHERE,_wdbiGetMin(date)                                                            )
#define wdbiGetSec(date)                                                           (LOGWHERE,_wdbiGetSec(date)                                                            )
#define wdbiSetEcho(val)                                                           (LOGWHERE,_wdbiSetEcho(val)                                                            )
#define wdbiIsEcho()                                                               (LOGWHERE,_wdbiIsEcho()                                                                )
#define wdbiMTToTextFile(memtab,fn,rownm,colsnm)                                   (LOGWHERE,_wdbiMTToTextFile(memtab,fn,rownm,colsnm)                                    )
#define wdbiGetMTName(memtab,bf)                                                   (LOGWHERE,_wdbiGetMTName(memtab,bf)                                                    )
#define wdbiReadFromTextFile(memtab,fn,rowst,rownm)                                (LOGWHERE,_wdbiReadFromTextFile(memtab,fn,rowst,rownm)                                 )
#define wdbiFetchAllAt(memtab,st)                                                  (LOGWHERE,_wdbiFetchAllAt(memtab,st)                                                   )
#define wdbiDateDiff(d1,d2)                                                        (LOGWHERE,_wdbiDateDiff(d1,d2)                                                         )
#define wdbiSetOutputToConsole(val)                                                (LOGWHERE,_wdbiSetOutputToConsole(val)                                                 )
#define wdbiSetTraceFile(fn)                                             	   (LOGWHERE,_wdbiSetTraceFile(fn)                                              	  )
#define wdbiSetErrorFile(fn)                                                       (LOGWHERE,_wdbiSetErrorFile(fn)                                                        )
#define wdbiSetLogFile(fn)                                                         (LOGWHERE,_wdbiSetLogFile(fn)                                                          )
#define WDBIInit(appname)                                                          (LOGWHERE,_WDBIInit(appname)                                                           )
#define WDBIQuit()                                                                 (LOGWHERE,_WDBIQuit()                                                                  )
#define wdbiReplaceStmt(memtab,stmt)                                               (LOGWHERE,_wdbiReplaceStmt(memtab,stmt)                                                )
#define wdbiConvertColStrToInt(memtab,colsn,pcol)                                  (LOGWHERE,_wdbiConvertColStrToInt(memtab,colsn,pcol)                                   )
#define wdbiSaveSort(memtab,fp)                                                    (LOGWHERE,_wdbiSaveSort(memtab,fp)                                                     )
#define wdbiLoadSort(memtab,fp)                                                    (LOGWHERE,_wdbiLoadSort(memtab,fp)                                                     )
#define wdbiCopyToMySQL(memtab,startrow,rownum,fp)                                 (LOGWHERE,_wdbiCopyToMySQL(memtab,startrow,rownum,fp)                                  )
#define wdbiCompareSortRow(memtab,row1, row2)                                      (LOGWHERE,_wdbiCompareSortRow(memtab,row1, row2)                                       )
#define wdbiList(pout)                                                             (LOGWHERE,_wdbiList(pout)                                                              )
#define wdbiGetLastError(handle)                                                   (LOGWHERE,_wdbiGetLastError(handle)                                                    )
#define wdbiAddrFresh(memtab,colval,collen,tp)                                     (LOGWHERE,_wdbiAddrFresh(memtab,colval,collen,tp)                                      )
#define wdbiGetColumnInfo(mt,crttab,ismysql)                               	   (LOGWHERE,_wdbiGetColumnInfo(mt,crttab,ismysql)                                     	  )
#define wdbiGetFetchSize(stmt)							   (LOGWHERE,_wdbiGetFetchSize(stmt)							  )
#define wdbiSetFetchSize(stmt,fsize)						   (LOGWHERE,_wdbiSetFetchSize(stmt,fsize)						  )
#endif

DllExport void _wdbilogwhere(const char *fn,int ln);
//Catch level : 0 -- no catch 1--catch WDBIError 2--catch 'WDBIError' and 'int'
//  and 'char *' exceptions
DllExport int _wdbiMainEntrance(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel);
DllExport  int _wdbidestroy(int handle) ;
DllExport int _wdbiDestroyAll();
DllExport void _wdbiBreakAll();
//////////////////////////////////////////////////////////////////
// Session function(6) . 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateSession(const char *username,const char *password,const char *svcname,int dbtype) ;
DllExport  int _wdbiCommit(int sess) ;
DllExport  int _wdbiRollback(int sess) ;
DllExport  int _wdbiSetTerminate(int sess,bool val) ;
DllExport  bool _wdbiIsTerminate(int sess) ;
DllExport  bool _wdbiSetNonBlockMode(int sess) ;

//////////////////////////////////////////////////////////////////
// Statment function (18). 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateStatment(int sess) ;
DllExport  bool _wdbiBindStrByPos(int stmt,int pos,char *ptr,int len) ;
DllExport  bool _wdbiBindDoubleByPos(int stmt,int pos,double *ptr) ;
DllExport  bool _wdbiBindIntByPos(int stmt,int pos,int *ptr) ;
DllExport  bool _wdbiBindDateByPos(int stmt,int pos,char *ptr) ;
DllExport  bool _wdbiDefineStrByPos(int stmt,int pos,char *ptr,int len) ;
DllExport  bool _wdbiDefineDoubleByPos(int stmt,int pos,double *ptr) ;
DllExport  bool _wdbiDefineIntByPos(int stmt,int pos,int *ptr) ;
DllExport  bool _wdbiDefineDateByPos(int stmt,int pos,char *ptr);
DllExport  int _wdbiBreakAndReset(int stmt) ;
DllExport  int _wdbiExecute(int stmt,int times) ;
DllExport  int _wdbiExecuteAt(int stmt,int times,int offset) ;
DllExport  int _wdbiFetch(int stmt,int rows) ;
DllExport  int _wdbiGetStmtColumnsNum(int stmt) ;
DllExport  int _wdbiGetMtColumnsNum(int mt) ;
 //int _wdbiGetColumnsNum(int stmt) ;
DllExport  int _wdbiGetFetchedRows(int stmt) ;
DllExport  int _wdbiGetSession(int stmt) ;
DllExport  unsigned short _wdbiGetStmtType(int stmt) ;
DllExport  bool _wdbiPrepareStmt(int stmt,char *sqlstmt) ;
DllExport  int _wdbiGetCreateTableSQL(int memtab,char *buf,const char *tabname,bool ismysql) ;
//////////////////////////////////////////////////////////////////
// MemTable function (43). 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateMemTable() ;
DllExport  bool _wdbiAddColumn(int memtab,char *name,char *dspname,int ctype,int length,int scale) ;
DllExport  bool _wdbiBuildStmt(int memtab,int stmt,unsigned int rows);
DllExport  BOOL _wdbiBuild(int memtab,unsigned int rows);
DllExport  void _wdbiClear(int memtab) ;
DllExport  BOOL _wdbiAppendToDbTable(int memtab,char *tablename,int sess,bool autocommit) ;
DllExport  unsigned int _wdbiFetchAll(int memtab) ;
DllExport  unsigned int _wdbiFetchFirst(int memtab,unsigned int rows) ;
DllExport  unsigned int _wdbiFetchNext(int memtab,unsigned int rows) ;
DllExport unsigned int _wdbiFetchAt(int memtab,unsigned int rows,int st) ;
DllExport  bool _wdbiGeneTable(int memtab,char *tablename,int sess) ;
DllExport  void _wdbiSetMTName(int memtab,char *mtname) ;
DllExport  int _wdbiGetStrAddrByName(int memtab,char *col,unsigned int rowst,char **pstr,int *celllen) ;
DllExport  int _wdbiGetDateAddrByName(int memtab,char *col,unsigned int rowst,char **pstr) ;
DllExport  int _wdbiGetDoubleAddrByName(int memtab,char *col,unsigned int rowst,double **pstr);
DllExport  int _wdbiGetIntAddrByName(int memtab,char *col,unsigned int rowst,int **pstr) ;
 
DllExport  int _wdbiGetStrValByName(int memtab,char *col,unsigned int rowst,char *pstr) ;
DllExport  int _wdbiGetDateValByName(int memtab,char *col,unsigned int rowst,char *pstr) ;
DllExport  double _wdbiGetDoubleValByName(int memtab,char *col,unsigned int rowst);
DllExport  int _wdbiGetIntValByName(int memtab,char *col,unsigned int rowst) ;

DllExport  int _wdbiGetStrAddrByPos(int memtab,int col,unsigned int rowst,char **pstr,int *celllen) ;
DllExport  int _wdbiGetDateAddrByPos(int memtab,int col,unsigned int rowst,char **pstr) ;
DllExport  int _wdbiGetDoubleAddrByPos(int memtab,int col,unsigned int rowst,double **pstr);
DllExport  int _wdbiGetIntAddrByPos(int memtab,int col,unsigned int rowst,int **pstr) ;
 
DllExport  int _wdbiGetBufferLen(int memtab) ;
DllExport  bool _wdbiGetCell(int memtab,unsigned int row,int col,char *str,bool rawpos) ;
DllExport  int _wdbiGetColumnDisplayWidth(int memtab,int col) ;
DllExport  int _wdbiGetColumnPosByName(int memtab,char *colname);
DllExport  int _wdbiGetColumnName(int memtab,int id,char *colname);
DllExport  int _wdbiGetColumnDataLenByPos(int memtab,int colid);
DllExport  int _wdbiGetColumnNumber(int memtab);
DllExport  int _wdbiGetColumnScale(int memtab,int colid);
DllExport  void _wdbiGetColumnTitle(int memtab,int colid,char *str,int len);
DllExport  short _wdbiGetColumnType(int memtab,int colid);
DllExport  bool _wdbiGetLine(int memtab,unsigned int row,char *str,bool rawpos,char *colsnm,int *clen);
DllExport  int _wdbiGetMemtableRows(int memtab);
DllExport  void _wdbiGetTitle(int memtab,char *str,int len,char *colsnm);
DllExport  bool _wdbiIsIKSet(int memtab) ;
DllExport  bool _wdbiOrderByIK(int memtab);
DllExport  int _wdbiSearchIK(int memtab,int key);
DllExport  int _wdbiSearchIKLE(int memtab,int key);
DllExport  void _wdbiSetColumnDisplayName(int memtab,char *colnm,char *str);
DllExport  bool _wdbiSetIKByName(int memtab,char *str) ;
DllExport  bool _wdbiSetSortColumn(int memtab,char *colsnm) ;
DllExport  void _wdbiReInOrder(int memtab);
DllExport  int _wdbiSetStrValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,char *bf) ;
DllExport  int _wdbiSetDateValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,char *bf) ;
DllExport  int _wdbiSetDoubleValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,double *bf);
DllExport  int _wdbiSetIntValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,int *bf) ;
DllExport  bool _wdbiSort(int memtab) ;
DllExport  bool _wdbiSortHeap(int memtab) ;
//*  Group Functions (6)***************************************/
DllExport  bool _wdbiSetGroupSrc(int memtab,int src) ;
DllExport  bool _wdbiSetIKGroupRef(int memtab,int ref,char *colnm) ;
DllExport  bool _wdbiSetGroupSrcCol(int memtab,char *colsnm) ;
DllExport  bool _wdbiSetGroupRefCol(int memtab,char *colsnm) ;
DllExport  bool _wdbiSetSrcSumCol(int memtab,char *colsnm) ;
DllExport  bool _wdbiGroup(int memtab,int rowstart,int rownum);
DllExport  bool _wdbiCopyRowsTo(int memtab,int memtabto,int toStart,int start,int rowsnum);
DllExport  bool _wdbiGetColumnDesc(int memtab,void **pColDesc,int &cdlen,int &_colnm) ;
DllExport bool _wdbiCopyColumnDefine(int memtab,int memtabfrom,const char *colsname);
DllExport  bool _wdbiExportSomeRows(int memtab,char *pData,int startrn,int rnnum);
DllExport  bool _wdbiExport(int memtab,void *pData,long &_len,int &_maxrows,int &_rowct) ;
DllExport BOOL _wdbiImport(int memtab,void *pData,long _len,void *pColDesc,int _colnm,int _cdlen,int _maxrows,int _rowct);
DllExport  BOOL _wdbiAppendRows(int memtab,char *pData,int rnnum); 
 	//保留表结构，清除索引结构，数据行数置零
DllExport  void _wdbiReset(int memtab) ;
DllExport  void _wdbiClearIK(int memtab) ;
DllExport  void _wdbiClearSort(int memtab) ;
DllExport int _wdbiGetMaxRows(int memtab); 
DllExport  bool _wdbiCompact(int memtab);
DllExport  int _wdbiGetRowLen(int memta);
//////////////////////////////////////////////////////////////////
// ExcelEnv function(7) . 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateExcelEnv() ;
DllExport  void _wdbiSetDir(int excel,char *strTemplate,char *strReport);
DllExport  void _wdbiSetMemTable(int excel,int memtab);
DllExport  bool _wdbiLoadTemplate(int excel,char *tempname);
DllExport  bool _wdbiSelectSheet(int excel,char *sheetname) ;
DllExport  bool _wdbiFillData(int excel,unsigned int tocol, unsigned int torow, unsigned int fromcol, unsigned int fromrow, unsigned int colnum, unsigned int rownum,bool rawpos) ;
DllExport  bool _wdbiFillTitle(int excel,unsigned int tocol,unsigned int torow,unsigned int fromcol,unsigned int colnum);
DllExport  bool _wdbiSaveAs(int excel,char *filename) ;

DllExport  int _wdbiGetMemUsed(int memtab);
DllExport  bool _wdbiSetSortedGroupRef(int memtab,int mtref, char *colssrc);
DllExport  int  _wdbiSearch(int memtab,void **ptr);
DllExport  int  _wdbiSearchLE(int memtab,void **ptr);
DllExport  bool _wdbiValuesSet(int memtab,int mtsrc,char *colto,char *colfrom,bool usedestKey,int op);
DllExport  bool _wdbiDeleteRow(int memtab,int rown) ;
DllExport  bool _wdbiInsertRows(int memtab,void **ptr,char *colsname,int num);
DllExport  bool _wdbiBindToStatment(int memtab,int stmt,char *colsname,int rowst);
DllExport  int _wdbiTestTable(int sess,char *tablename);
DllExport  int _wdbiGetRawrnBySort(int memtab,int ind);
DllExport  int _wdbiGetRawrnByIK(int memtab,int ind);
DllExport  bool _wdbiCompressBf(int memtab);
DllExport  int _wdbiSearchQDel(int memtab,int key,int schopt);
DllExport  bool _wdbiIsQDelete(int memtab,int rownm);
DllExport  int _wdbiQDeletedRows(int memtab);
DllExport  bool _wdbiQDeleteRow(int memtab,int rownm);
DllExport  void _wdbiWaitStmt(int stmt);
DllExport  void _wdbiWaitMemtable(int memtab);
DllExport  int _wdbiWaitLastReturn(int handle);
DllExport  int _wdbiWaitTime(int handle,int time);
DllExport  bool _wdbiFreshRowNum(int memtab);
DllExport  int _wdbiBatchSelect(int result,int param,char *colsnm);
//DllExport void _wdbiMTPrint(int memtab,int rownm,char *colsnm) ;
DllExport void _wdbiMTPrint(int memtab,int rownm,char *colsnm,bool compact) ;
DllExport void _wdbiGetCompactLen(int memtab,int rowstart,int rownum,char *colsnm,int *clen);

DllExport void _wdbiGetCurDateTime(char *date) ;
//Parametes: year(four digits),month(1-12),day(1-31),hour(0-23),minute(0-59),second(0-59).
// return a new datatime handle
DllExport void _wdbiSetDateTime(char *date,int year,int mon,int day,int hour,int min,int sec) ;
//Parameters:year,month,day ; hour,minute,second will be set to zero.
// return a new datetime handle
DllExport void _wdbiSetDate(char *date,int year,int mon,int day);
//Format : yyyy/mm/dd hh24:mi:ss
DllExport void _wdbiDateTimeToStr(char *date,char *str) ;
DllExport int _wdbiGetYear(char *date) ;
DllExport int _wdbiGetMonth(char *date);
DllExport int _wdbiGetDay(char *date) ;
DllExport int _wdbiGetHour(char *date) ;
DllExport int _wdbiGetMin(char *date) ;
DllExport int _wdbiGetSec(char *date) ;
DllExport void _wdbiSetEcho(bool val);
DllExport bool _wdbiIsEcho();
DllExport void _wdbiMTToTextFile(int memtab,char *fn,int rownm,char *colsnm) ;
DllExport void _wdbiGetMTName(int memtab,char *bf);
DllExport int _wdbiReadFromTextFile(int memtab,char *fn,int rowst,int rownm);
DllExport unsigned int _wdbiFetchAllAt(int memtab,int st) ;
DllExport int _wdbiDateDiff(char *d1,char *d2);
DllExport void _wdbiSetOutputToConsole(bool val);
DllExport void _wdbiSetTraceFile(const char *fn); 
DllExport void _wdbiSetErrorFile(const char *fn);
DllExport void _wdbiSetLogFile(const char *fn);
DllExport int errprintf(const char *format,...) ;
DllExport int lgprintf(const char *format,...) ;
DllExport void _WDBIInit(char *appname);
DllExport void _WDBIQuit();
DllExport bool _wdbiReplaceStmt(int memtab,int stmt);
DllExport int _wdbiConvertColStrToInt(int memtab,const char *colsn,int *pcol);
DllExport int _wdbiSaveSort(int memtab,FILE *fp);
DllExport int _wdbiLoadSort(int memtab,FILE *fp);
DllExport void _wdbiCopyToMySQL(int memtab,unsigned int startrow,
			unsigned int rownum,FILE *fp);
DllExport int _wdbiCompareSortRow(int memtab,unsigned int row1, unsigned int row2);
DllExport void _wdbiList(char *pout);
DllExport int _wdbiGetLastError(int handle);
DllExport void _wdbiAddrFresh(int memtab,char **colval,int *collen,int *tp);
DllExport int _wdbiGetColumnInfo(int mt,char *crttab,bool ismysql);
DllExport unsigned int _wdbiGetFetchSize(int stmt);
DllExport void _wdbiSetFetchSize(int stmt,unsigned int fsize);
// End of file _wdbi8intface.h

#endif


