/*
  Copyright (C) 1995-2006 MySQL AB

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation.

  There are special exceptions to the terms and conditions of the GPL
  as it is applied to this software. View the full text of the exception
  in file LICENSE.exceptions in the top-level directory of this software
  distribution.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

/***************************************************************************
 * MYUTIL.H								   *
 *									   *
 * @description: Prototype definations needed by the driver		   *
 *									   *
 * @author     : MySQL AB(monty@mysql.com, venu@mysql.com)		   *
 * @date       : 2001-Sep-22						   *
 * @product    : myodbc3						   *
 *									   *
 ****************************************************************************/
#ifndef __MYUTIL_H__
#define __MYUTIL_H__

/*
  Utility macros
*/

#define if_dynamic_cursor(st) ((st)->stmt_options.cursor_type == SQL_CURSOR_DYNAMIC)
#define if_forward_cache(st) ((st)->stmt_options.cursor_type == SQL_CURSOR_FORWARD_ONLY && \
			     (st)->dbc->flag & FLAG_NO_CACHE )
#define is_connected(dbc)    ((dbc)->mysql.net.vio)
#define trans_supported(db) ((db)->mysql.server_capabilities & CLIENT_TRANSACTIONS)
#define autocommit_on(db) ((db)->mysql.server_status & SERVER_STATUS_AUTOCOMMIT)
#define true_dynamic(flag) (!(flag &FLAG_FORWARD_CURSOR ) && (flag & FLAG_DYNAMIC_CURSOR))
#define reset_ptr(x) if (x) x= 0
#define digit(A) ((int) (A - '0'))
#define option_flag(A,B) ((A)->dbc->flag & B)

#define MYLOG_QUERY(A,B) if ((A)->dbc->flag & FLAG_LOG_QUERY) \
			   query_print((A)->dbc->query_log,(char*) B)

#define MYLOG_DBC_QUERY(A,B) if((A)->flag & FLAG_LOG_QUERY) \
			   query_print((A)->query_log,(char*) B)


#define UTF8_CHARSET_NUMBER 33

/* Wrappers to hide differences in client library versions. */
#if MYSQL_VERSION_ID >= 40100
# define my_int2str(val, dst, radix, upcase) \
    int2str((val), (dst), (radix), (upcase))
#else
# define my_int2str(val, dst, radix, upcase) \
    int2str((val), (dst), (radix))
#endif

#if MYSQL_VERSION_ID >= 50100
typedef unsigned char * DYNAMIC_ELEMENT;
#else
typedef char * DYNAMIC_ELEMENT;
#endif

#if MYSQL_VERSION_ID >= 50100
# define MYODBC_FIELD_STRING(name, len, flags) \
  {(name), NullS, NullS, NullS, NullS, NullS, NullS, (len), 0, 0, 0, 0, 0, 0, \
    0, 0, (flags), 0, UTF8_CHARSET_NUMBER, MYSQL_TYPE_VAR_STRING, NULL}
# define MYODBC_FIELD_SHORT(name, flags) \
  {(name), NullS, NullS, NullS, NullS, NullS, NullS, 5, 5, 0, 0, 0, 0, 0, 0, \
    0, (flags), 0, 0, MYSQL_TYPE_SHORT, NULL}
# define MYODBC_FIELD_LONG(name, flags) \
  {(name), NullS, NullS, NullS, NullS, NullS, NullS, 11, 11, 0, 0, 0, 0, 0, 0, \
    0, (flags), 0, 0, MYSQL_TYPE_LONG, NULL}
#elif MYSQL_VERSION_ID >= 40100
/* we use 3 here as SYSTEM_CHARSET_MBMAXLEN is not defined in v5.0 mysql_com.h */
# define MYODBC_FIELD_STRING(name, len, flags) \
  {(name), NullS, NullS, NullS, NullS, NullS, NullS, (len) * 3, 0, 0, 0, 0, 0, 0, \
    0, 0, (flags), 0, UTF8_CHARSET_NUMBER, MYSQL_TYPE_VAR_STRING}
# define MYODBC_FIELD_SHORT(name, flags) \
  {(name), NullS, NullS, NullS, NullS, NullS, NullS, 5, 5, 0, 0, 0, 0, 0, 0, \
    0, (flags), 0, 0, MYSQL_TYPE_SHORT}
# define MYODBC_FIELD_LONG(name, flags) \
  {(name), NullS, NullS, NullS, NullS, NullS, NullS, 11, 11, 0, 0, 0, 0, 0, 0, \
    0, (flags), 0, 0, MYSQL_TYPE_LONG}
#else
# define MYODBC_FIELD_STRING(name, len, flags) \
  {(name), NullS, NullS, NullS, NullS, (len), 0, (flags), 0, \
    MYSQL_TYPE_VAR_STRING}
# define MYODBC_FIELD_SHORT(name, flags) \
  {(name), NullS, NullS, NullS, NullS, 5, 5, (flags), 0, MYSQL_TYPE_SHORT}
# define MYODBC_FIELD_LONG(name, flags) \
  {(name), NullS, NullS, NullS, NullS, 11, 11, (flags), 0, MYSQL_TYPE_LONG}
#endif

/*
  Utility function prototypes that share among files
*/

SQLRETURN my_SQLExecute(STMT FAR* stmt);
SQLRETURN my_SQLPrepare(SQLHSTMT hstmt,SQLCHAR FAR *szSqlStr,
			SQLINTEGER cbSqlStr);
SQLRETURN SQL_API my_SQLFreeStmt(SQLHSTMT hstmt,SQLUSMALLINT fOption);
SQLRETURN SQL_API my_SQLFreeStmtExtended(SQLHSTMT hstmt,
                    SQLUSMALLINT fOption, uint clearAllResults);
SQLRETURN SQL_API my_SQLAllocStmt(SQLHDBC hdbc,SQLHSTMT FAR *phstmt);
SQLRETURN do_query(STMT FAR *stmt,char *query);
char *insert_params(STMT FAR *stmt);
SQLRETURN odbc_stmt(DBC FAR *dbc, const char *query);
void mysql_link_fields(STMT *stmt,MYSQL_FIELD *fields,uint field_count);
void fix_result_types(STMT *stmt);
char *fix_str(char *to,const char *from,int length);
char *dupp_str(char *from,int length);
SQLRETURN my_pos_delete(STMT FAR *stmt,STMT FAR *stmtParam,
			SQLUSMALLINT irow,DYNAMIC_STRING *dynStr);
SQLRETURN my_pos_update(STMT FAR *stmt,STMT FAR *stmtParam,
			SQLUSMALLINT irow,DYNAMIC_STRING *dynStr);
char *check_if_positioned_cursor_exists(STMT FAR *stmt, STMT FAR **stmtNew);
char *insert_param(DBC *dbc, char *to,PARAM_BIND *param);
char *add_to_buffer(NET *net,char *to,char *from,ulong length);
SQLRETURN copy_lresult(SQLSMALLINT HandleType, SQLHANDLE handle,
		       SQLCHAR FAR *rgbValue, SQLINTEGER cbValueMax,
		       SQLLEN *pcbValue, char *src,
		       long src_length, long max_length,
		       long fill_length,ulong *offset,my_bool binary_data);
SQLRETURN copy_binary_result(SQLSMALLINT HandleType, SQLHANDLE handle,
			     SQLCHAR FAR *rgbValue, SQLINTEGER cbValueMax,
			     SQLLEN *pcbValue, char *src,
			     ulong src_length, ulong max_length,
			     ulong *offset);
SQLRETURN set_dbc_error(DBC FAR *dbc, char *state,const char *message,uint errcode);
#define set_stmt_error myodbc_set_stmt_error
SQLRETURN myodbc_set_stmt_error(STMT *stmt, char *state,const char *message,uint errcode);
SQLRETURN handle_connection_error(STMT *stmt);
void set_mem_error(MYSQL *mysql);
void translate_error(char *save_state,myodbc_errid errid,uint mysql_err);

SQLSMALLINT get_sql_data_type(STMT *stmt, MYSQL_FIELD *field, char *buff);
SQLULEN get_column_size(STMT *stmt, MYSQL_FIELD *field, my_bool actual);
SQLLEN get_decimal_digits(STMT *stmt, MYSQL_FIELD *field);
SQLLEN get_transfer_octet_length(STMT *stmt, MYSQL_FIELD *field);
SQLLEN get_display_size(STMT *stmt, MYSQL_FIELD *field);

#define is_char_sql_type(type) \
  ((type) == SQL_CHAR || (type) == SQL_VARCHAR || (type) == SQL_LONGVARCHAR)
#define is_wchar_sql_type(type) \
  ((type) == SQL_WCHAR || (type) == SQL_WVARCHAR || (type) == SQL_WLONGVARCHAR)
#define is_binary_sql_type(type) \
  ((type) == SQL_BINARY || (type) == SQL_VARBINARY || \
   (type) == SQL_LONGVARBINARY)

#define is_numeric_mysql_type(field) \
  ((field)->type <= MYSQL_TYPE_NULL || (field)->type == MYSQL_TYPE_LONGLONG || \
   (field)->type == MYSQL_TYPE_INT24 || \
   ((field)->type == MYSQL_TYPE_BIT && (field)->length == 1) || \
   (field)->type == MYSQL_TYPE_NEWDECIMAL)

SQLRETURN SQL_API my_SQLBindParameter(SQLHSTMT hstmt,SQLUSMALLINT ipar,
				      SQLSMALLINT fParamType,
				      SQLSMALLINT fCType, SQLSMALLINT fSqlType,
				      SQLULEN cbColDef,
				      SQLSMALLINT ibScale,
				      SQLPOINTER  rgbValue,
				      SQLLEN cbValueMax,
				      SQLLEN *pcbValue);
SQLRETURN SQL_API my_SQLExtendedFetch(SQLHSTMT hstmt, SQLUSMALLINT fFetchType,
				      SQLROWOFFSET irow, SQLULEN *pcrow,
				      SQLUSMALLINT FAR *rgfRowStatus, bool upd_status);
SQLRETURN copy_stmt_error(STMT FAR *src, STMT FAR *dst);
int unireg_to_c_datatype(MYSQL_FIELD *field);
int default_c_type(int sql_data_type);
ulong bind_length(int sql_data_type,ulong length);
my_bool str_to_date(SQL_DATE_STRUCT *rgbValue, const char *str,
                    uint length, int zeroToMin);
my_bool str_to_ts(SQL_TIMESTAMP_STRUCT *ts, const char *str, int zeroToMin);
my_bool str_to_time_st(SQL_TIME_STRUCT *ts, const char *str);
ulong str_to_time_as_long(const char *str,uint length);
void init_getfunctions(void);
void myodbc_init(void);
void myodbc_ov_init(SQLINTEGER odbc_version);
void myodbc_sqlstate2_init(void);
void myodbc_sqlstate3_init(void);
int check_if_server_is_alive(DBC FAR *dbc);
my_bool dynstr_append_quoted_name(DYNAMIC_STRING *str, const char *name);
SQLRETURN set_handle_error(SQLSMALLINT HandleType, SQLHANDLE handle,
			   myodbc_errid errid, const char *errtext, SQLINTEGER errcode);
SQLRETURN set_error(STMT *stmt,myodbc_errid errid, const char *errtext,
		    SQLINTEGER errcode);
SQLRETURN set_conn_error(DBC *dbc,myodbc_errid errid, const char *errtext,
			 SQLINTEGER errcode);
SQLRETURN set_env_error(ENV * env,myodbc_errid errid, const char *errtext,
			SQLINTEGER errcode);
SQLRETURN copy_str_data(SQLSMALLINT HandleType, SQLHANDLE Handle,
			SQLCHAR FAR *rgbValue, SQLSMALLINT cbValueMax,
			SQLSMALLINT FAR *pcbValue,char FAR *src);
SQLRETURN SQL_API my_SQLAllocEnv(SQLHENV FAR * phenv);
SQLRETURN SQL_API my_SQLAllocConnect(SQLHENV henv, SQLHDBC FAR *phdbc);
SQLRETURN SQL_API my_SQLFreeConnect(SQLHDBC hdbc);
SQLRETURN SQL_API my_SQLFreeEnv(SQLHENV henv);
char *extend_buffer(NET *net,char *to,ulong length);
void myodbc_end();
my_bool set_dynamic_result(STMT FAR *stmt);
void set_current_cursor_data(STMT FAR *stmt,SQLUINTEGER irow);
my_bool is_minimum_version(const char *server_version,const char *version,
			   uint length);
int myodbc_strcasecmp(const char *s, const char *t);
int myodbc_casecmp(const char *s, const char *t, uint len);
my_bool reget_current_catalog(DBC FAR *dbc);

ulong myodbc_escape_string(MYSQL *mysql, char *to, ulong to_length,
                           const char *from, ulong length, int escape_id);

/* Functions used when debugging */
void query_print(FILE *log_file,char *query);
FILE *init_query_log(void);
void end_query_log(FILE *query_log);

#ifdef __WIN__
#define cmp_database(A,B) myodbc_strcasecmp((const char *)(A),(const char *)(B))
#else
#define cmp_database(A,B) strcmp((A),(B))
#endif

#endif /* __MYUTIL_H__ */
