/* Copyright (C) 2000-2006 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation				// gcc: Class implementation
#endif

#define MYSQL_SERVER 1
#include "mysql_priv.h"
#include <mysql/plugin.h>
#include <m_ctype.h>
#include <my_bit.h>
#include <myisampack.h>
#include "ha_myisam.h"
#include <stdarg.h>
#include "myisamdef.h"
#include "rt_index.h"
/* Modified by DT project--BEGIN--*/
#include <zlib.h> 
//_syscall0(pid_t,gettid)
//#define dtdbg(a) lgprintf(a)
#define dtdbg(a)
//mytimer blocktm,recordtm,indextm,scantm;
int ldblktms;
dp_cache *dp_cache::p_DP_CACHE=NULL;
/* Modified by DT project--END--*/

ulong myisam_recover_options= HA_RECOVER_NONE;

/* bits in myisam_recover_options */
const char *myisam_recover_names[] =
{ "DEFAULT", "BACKUP", "FORCE", "QUICK", NullS};
TYPELIB myisam_recover_typelib= {array_elements(myisam_recover_names)-1,"",
				 myisam_recover_names, NULL};

const char *myisam_stats_method_names[] = {"nulls_unequal", "nulls_equal",
                                           "nulls_ignored", NullS};
TYPELIB myisam_stats_method_typelib= {
  array_elements(myisam_stats_method_names) - 1, "",
  myisam_stats_method_names, NULL};


/*****************************************************************************
** MyISAM tables
*****************************************************************************/

static handler *myisam_create_handler(handlerton *hton,
                                      TABLE_SHARE *table, 
                                      MEM_ROOT *mem_root)
{
  return new (mem_root) ha_myisam(hton, table);
}

// collect errors printed by mi_check routines

static void mi_check_print_msg(MI_CHECK *param,	const char* msg_type,
			       const char *fmt, va_list args)
{
  THD* thd = (THD*)param->thd;
  Protocol *protocol= thd->protocol;
  uint length, msg_length;
  char msgbuf[MI_MAX_MSG_BUF];
  char name[NAME_LEN*2+2];

  msg_length= my_vsnprintf(msgbuf, sizeof(msgbuf), fmt, args);
  msgbuf[sizeof(msgbuf) - 1] = 0; // healthy paranoia

  DBUG_PRINT(msg_type,("message: %s",msgbuf));

  if (!thd->vio_ok())
  {
    sql_print_error(msgbuf);
    return;
  }

  if (param->testflag & (T_CREATE_MISSING_KEYS | T_SAFE_REPAIR |
			 T_AUTO_REPAIR))
  {
    my_message(ER_NOT_KEYFILE,msgbuf,MYF(MY_WME));
    return;
  }
  length=(uint) (strxmov(name, param->db_name,".",param->table_name,NullS) -
		 name);
  /*
    TODO: switch from protocol to push_warning here. The main reason we didn't
    it yet is parallel repair. Due to following trace:
    mi_check_print_msg/push_warning/sql_alloc/my_pthread_getspecific_ptr.

    Also we likely need to lock mutex here (in both cases with protocol and
    push_warning).
  */
  protocol->prepare_for_resend();
  protocol->store(name, length, system_charset_info);
  protocol->store(param->op_name, system_charset_info);
  protocol->store(msg_type, system_charset_info);
  protocol->store(msgbuf, msg_length, system_charset_info);
  if (protocol->write())
    sql_print_error("Failed on my_net_write, writing to stderr instead: %s\n",
		    msgbuf);
  return;
}


/*
  Convert TABLE object to MyISAM key and column definition

  SYNOPSIS
    table2myisam()
      table_arg   in     TABLE object.
      keydef_out  out    MyISAM key definition.
      recinfo_out out    MyISAM column definition.
      records_out out    Number of fields.

  DESCRIPTION
    This function will allocate and initialize MyISAM key and column
    definition for further use in mi_create or for a check for underlying
    table conformance in merge engine.

    The caller needs to free *recinfo_out after use. Since *recinfo_out
    and *keydef_out are allocated with a my_multi_malloc, *keydef_out
    is freed automatically when *recinfo_out is freed.

  RETURN VALUE
    0  OK
    !0 error code
*/

int table2myisam(TABLE *table_arg, MI_KEYDEF **keydef_out,
                 MI_COLUMNDEF **recinfo_out, uint *records_out)
{
  uint i, j, recpos, minpos, fieldpos, temp_length, length;
  enum ha_base_keytype type= HA_KEYTYPE_BINARY;
  uchar *record;
  KEY *pos;
  MI_KEYDEF *keydef;
  MI_COLUMNDEF *recinfo, *recinfo_pos;
  HA_KEYSEG *keyseg;
  TABLE_SHARE *share= table_arg->s;
  uint options= share->db_options_in_use;
  DBUG_ENTER("table2myisam");
  if (!(my_multi_malloc(MYF(MY_WME),
          recinfo_out, (share->fields * 2 + 2) * sizeof(MI_COLUMNDEF),
          keydef_out, share->keys * sizeof(MI_KEYDEF),
          &keyseg,
          (share->key_parts + share->keys) * sizeof(HA_KEYSEG),
          NullS)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM); /* purecov: inspected */
  keydef= *keydef_out;
  recinfo= *recinfo_out;
  pos= table_arg->key_info;
  for (i= 0; i < share->keys; i++, pos++)
  {
    keydef[i].flag= ((uint16) pos->flags & (HA_NOSAME | HA_FULLTEXT | HA_SPATIAL));
    keydef[i].key_alg= pos->algorithm == HA_KEY_ALG_UNDEF ?
      (pos->flags & HA_SPATIAL ? HA_KEY_ALG_RTREE : HA_KEY_ALG_BTREE) :
      pos->algorithm;
    keydef[i].block_length= pos->block_size;
    keydef[i].seg= keyseg;
    keydef[i].keysegs= pos->key_parts;
    for (j= 0; j < pos->key_parts; j++)
    {
      Field *field= pos->key_part[j].field;
      type= field->key_type();
      keydef[i].seg[j].flag= pos->key_part[j].key_part_flag;

      if (options & HA_OPTION_PACK_KEYS ||
          (pos->flags & (HA_PACK_KEY | HA_BINARY_PACK_KEY |
                         HA_SPACE_PACK_USED)))
      {
        if (pos->key_part[j].length > 8 &&
            (type == HA_KEYTYPE_TEXT ||
             type == HA_KEYTYPE_NUM ||
             (type == HA_KEYTYPE_BINARY && !field->zero_pack())))
        {
          /* No blobs here */
          if (j == 0)
            keydef[i].flag|= HA_PACK_KEY;
          if (!(field->flags & ZEROFILL_FLAG) &&
              (field->type() == MYSQL_TYPE_STRING ||
               field->type() == MYSQL_TYPE_VAR_STRING ||
               ((int) (pos->key_part[j].length - field->decimals())) >= 4))
            keydef[i].seg[j].flag|= HA_SPACE_PACK;
        }
        else if (j == 0 && (!(pos->flags & HA_NOSAME) || pos->key_length > 16))
          keydef[i].flag|= HA_BINARY_PACK_KEY;
      }
      keydef[i].seg[j].type= (int) type;
      keydef[i].seg[j].start= pos->key_part[j].offset;
      keydef[i].seg[j].length= pos->key_part[j].length;
      keydef[i].seg[j].bit_start= keydef[i].seg[j].bit_end=
        keydef[i].seg[j].bit_length= 0;
      keydef[i].seg[j].bit_pos= 0;
      keydef[i].seg[j].language= field->charset()->number;

/* Modified by DT project--BEGIN--*/
 // 增加部分索引字段向MYI文件写入的设置
      keydef[i].seg[j].wide_partlen=pos->key_part[j].wide_partlen ;
 /* Modified by DT project--END--*/
 
       if (field->null_ptr)
      {
        keydef[i].seg[j].null_bit= field->null_bit;
        keydef[i].seg[j].null_pos= (uint) (field->null_ptr-
                                           (uchar*) table_arg->record[0]);
      }
      else
      {
        keydef[i].seg[j].null_bit= 0;
        keydef[i].seg[j].null_pos= 0;
      }
      if (field->type() == MYSQL_TYPE_BLOB ||
          field->type() == MYSQL_TYPE_GEOMETRY)
      {
        keydef[i].seg[j].flag|= HA_BLOB_PART;
        /* save number of bytes used to pack length */
        keydef[i].seg[j].bit_start= (uint) (field->pack_length() -
                                            share->blob_ptr_size);
      }
      else if (field->type() == MYSQL_TYPE_BIT)
      {
        keydef[i].seg[j].bit_length= ((Field_bit *) field)->bit_len;
        keydef[i].seg[j].bit_start= ((Field_bit *) field)->bit_ofs;
        keydef[i].seg[j].bit_pos= (uint) (((Field_bit *) field)->bit_ptr -
                                          (uchar*) table_arg->record[0]);
      }
    }
    keyseg+= pos->key_parts;
  }
  if (table_arg->found_next_number_field)
    keydef[share->next_number_index].flag|= HA_AUTO_KEY;
  record= table_arg->record[0];
  recpos= 0;
  recinfo_pos= recinfo;
  while (recpos < (uint) share->reclength)
  {
    Field **field, *found= 0;
    minpos= share->reclength;
    length= 0;

    for (field= table_arg->field; *field; field++)
    {
      if ((fieldpos= (*field)->offset(record)) >= recpos &&
          fieldpos <= minpos)
      {
        /* skip null fields */
        if (!(temp_length= (*field)->pack_length_in_rec()))
          continue; /* Skip null-fields */
        if (! found || fieldpos < minpos ||
            (fieldpos == minpos && temp_length < length))
        {
          minpos= fieldpos;
          found= *field;
          length= temp_length;
        }
      }
    }
    DBUG_PRINT("loop", ("found: 0x%lx  recpos: %d  minpos: %d  length: %d",
                        (long) found, recpos, minpos, length));
    if (recpos != minpos)
    { // Reserved space (Null bits?)
      bzero((char*) recinfo_pos, sizeof(*recinfo_pos));
      recinfo_pos->type= (int) FIELD_NORMAL;
      recinfo_pos++->length= (uint16) (minpos - recpos);
    }
    if (!found)
      break;

    if (found->flags & BLOB_FLAG)
      recinfo_pos->type= (int) FIELD_BLOB;
    else if (found->type() == MYSQL_TYPE_VARCHAR)
      recinfo_pos->type= FIELD_VARCHAR;
    else if (!(options & HA_OPTION_PACK_RECORD))
      recinfo_pos->type= (int) FIELD_NORMAL;
    else if (found->zero_pack())
      recinfo_pos->type= (int) FIELD_SKIP_ZERO;
    else
      recinfo_pos->type= (int) ((length <= 3 ||
                                 (found->flags & ZEROFILL_FLAG)) ?
                                  FIELD_NORMAL :
                                  found->type() == MYSQL_TYPE_STRING ||
                                  found->type() == MYSQL_TYPE_VAR_STRING ?
                                  FIELD_SKIP_ENDSPACE :
                                  FIELD_SKIP_PRESPACE);
    if (found->null_ptr)
    {
      recinfo_pos->null_bit= found->null_bit;
      recinfo_pos->null_pos= (uint) (found->null_ptr -
                                     (uchar*) table_arg->record[0]);
    }
    else
    {
      recinfo_pos->null_bit= 0;
      recinfo_pos->null_pos= 0;
    }
    (recinfo_pos++)->length= (uint16) length;
    recpos= minpos + length;
    DBUG_PRINT("loop", ("length: %d  type: %d",
                        recinfo_pos[-1].length,recinfo_pos[-1].type));
  }
  *records_out= (uint) (recinfo_pos - recinfo);
  DBUG_RETURN(0);
}


/*
  Check for underlying table conformance

  SYNOPSIS
    check_definition()
      t1_keyinfo       in    First table key definition
      t1_recinfo       in    First table record definition
      t1_keys          in    Number of keys in first table
      t1_recs          in    Number of records in first table
      t2_keyinfo       in    Second table key definition
      t2_recinfo       in    Second table record definition
      t2_keys          in    Number of keys in second table
      t2_recs          in    Number of records in second table
      strict           in    Strict check switch

  DESCRIPTION
    This function compares two MyISAM definitions. By intention it was done
    to compare merge table definition against underlying table definition.
    It may also be used to compare dot-frm and MYI definitions of MyISAM
    table as well to compare different MyISAM table definitions.

    For merge table it is not required that number of keys in merge table
    must exactly match number of keys in underlying table. When calling this
    function for underlying table conformance check, 'strict' flag must be
    set to false, and converted merge definition must be passed as t1_*.

    Otherwise 'strict' flag must be set to 1 and it is not required to pass
    converted dot-frm definition as t1_*.

  RETURN VALUE
    0 - Equal definitions.
    1 - Different definitions.

  TODO
    - compare FULLTEXT keys;
    - compare SPATIAL keys;
    - compare FIELD_SKIP_ZERO which is converted to FIELD_NORMAL correctly
      (should be corretly detected in table2myisam).
*/

int check_definition(MI_KEYDEF *t1_keyinfo, MI_COLUMNDEF *t1_recinfo,
                     uint t1_keys, uint t1_recs,
                     MI_KEYDEF *t2_keyinfo, MI_COLUMNDEF *t2_recinfo,
                     uint t2_keys, uint t2_recs, bool strict)
{
  uint i, j;
  DBUG_ENTER("check_definition");
  if ((strict ? t1_keys != t2_keys : t1_keys > t2_keys))
  {
    DBUG_PRINT("error", ("Number of keys differs: t1_keys=%u, t2_keys=%u",
                         t1_keys, t2_keys));
    DBUG_RETURN(1);
  }
  if (t1_recs != t2_recs)
  {
    DBUG_PRINT("error", ("Number of recs differs: t1_recs=%u, t2_recs=%u",
                         t1_recs, t2_recs));
    DBUG_RETURN(1);
  }
  for (i= 0; i < t1_keys; i++)
  {
    HA_KEYSEG *t1_keysegs= t1_keyinfo[i].seg;
    HA_KEYSEG *t2_keysegs= t2_keyinfo[i].seg;
    if (t1_keyinfo[i].flag & HA_FULLTEXT && t2_keyinfo[i].flag & HA_FULLTEXT)
      continue;
    else if (t1_keyinfo[i].flag & HA_FULLTEXT ||
             t2_keyinfo[i].flag & HA_FULLTEXT)
    {
       DBUG_PRINT("error", ("Key %d has different definition", i));
       DBUG_PRINT("error", ("t1_fulltext= %d, t2_fulltext=%d",
                            test(t1_keyinfo[i].flag & HA_FULLTEXT),
                            test(t2_keyinfo[i].flag & HA_FULLTEXT)));
       DBUG_RETURN(1);
    }
    if (t1_keyinfo[i].flag & HA_SPATIAL && t2_keyinfo[i].flag & HA_SPATIAL)
      continue;
    else if (t1_keyinfo[i].flag & HA_SPATIAL ||
             t2_keyinfo[i].flag & HA_SPATIAL)
    {
       DBUG_PRINT("error", ("Key %d has different definition", i));
       DBUG_PRINT("error", ("t1_spatial= %d, t2_spatial=%d",
                            test(t1_keyinfo[i].flag & HA_SPATIAL),
                            test(t2_keyinfo[i].flag & HA_SPATIAL)));
       DBUG_RETURN(1);
    }
    if (t1_keyinfo[i].keysegs != t2_keyinfo[i].keysegs ||
/* Modified by DT project--BEGIN--*/
// undef algorithm index igore for merged old tables
        (t1_keyinfo[i].key_alg != t2_keyinfo[i].key_alg && (t2_keyinfo[i].key_alg!=HA_KEY_ALG_UNDEF)) )
/* original code
		  t1_keyinfo[i].key_alg != t2_keyinfo[i].key_alg  )
		  */
/* Modified by DT project--END--*/
    {
      DBUG_PRINT("error", ("Key %d has different definition", i));
      DBUG_PRINT("error", ("t1_keysegs=%d, t1_key_alg=%d",
                           t1_keyinfo[i].keysegs, t1_keyinfo[i].key_alg));
      DBUG_PRINT("error", ("t2_keysegs=%d, t2_key_alg=%d",
                           t2_keyinfo[i].keysegs, t2_keyinfo[i].key_alg));
      DBUG_RETURN(1);
    }
    for (j=  t1_keyinfo[i].keysegs; j--;)
    {
      uint8 t1_keysegs_j__type= t1_keysegs[j].type;

      /*
        Table migration from 4.1 to 5.1. In 5.1 a *TEXT key part is
        always HA_KEYTYPE_VARTEXT2. In 4.1 we had only the equivalent of
        HA_KEYTYPE_VARTEXT1. Since we treat both the same on MyISAM
        level, we can ignore a mismatch between these types.
      */
      if ((t1_keysegs[j].flag & HA_BLOB_PART) &&
          (t2_keysegs[j].flag & HA_BLOB_PART))
      {
        if ((t1_keysegs_j__type == HA_KEYTYPE_VARTEXT2) &&
            (t2_keysegs[j].type == HA_KEYTYPE_VARTEXT1))
          t1_keysegs_j__type= HA_KEYTYPE_VARTEXT1; /* purecov: tested */
        else if ((t1_keysegs_j__type == HA_KEYTYPE_VARBINARY2) &&
                 (t2_keysegs[j].type == HA_KEYTYPE_VARBINARY1))
          t1_keysegs_j__type= HA_KEYTYPE_VARBINARY1; /* purecov: inspected */
      }

      if (t1_keysegs_j__type != t2_keysegs[j].type ||
/* Modified by DT project--BEGIN--*/
// latin1 language  igored for merged old tables on gbk default db
          (t1_keysegs[j].language != t2_keysegs[j].language && t2_keysegs[j].language!=8 && t1_keysegs[j].language!=28) ||
/* original code
		  t1_keysegs[j].language != t2_keysegs[j].language  ||
		  */
/* Modified by DT project--End--*/
          t1_keysegs[j].null_bit != t2_keysegs[j].null_bit ||
          t1_keysegs[j].length != t2_keysegs[j].length)
      {
        DBUG_PRINT("error", ("Key segment %d (key %d) has different "
                             "definition", j, i));
        DBUG_PRINT("error", ("t1_type=%d, t1_language=%d, t1_null_bit=%d, "
                             "t1_length=%d",
                             t1_keysegs[j].type, t1_keysegs[j].language,
                             t1_keysegs[j].null_bit, t1_keysegs[j].length));
        DBUG_PRINT("error", ("t2_type=%d, t2_language=%d, t2_null_bit=%d, "
                             "t2_length=%d",
                             t2_keysegs[j].type, t2_keysegs[j].language,
                             t2_keysegs[j].null_bit, t2_keysegs[j].length));

        DBUG_RETURN(1);
      }
    }
  }
  for (i= 0; i < t1_recs; i++)
  {
    MI_COLUMNDEF *t1_rec= &t1_recinfo[i];
    MI_COLUMNDEF *t2_rec= &t2_recinfo[i];
    /*
      FIELD_SKIP_ZERO can be changed to FIELD_NORMAL in mi_create,
      see NOTE1 in mi_create.c
    */
    if ((t1_rec->type != t2_rec->type &&
         !(t1_rec->type == (int) FIELD_SKIP_ZERO &&
           t1_rec->length == 1 &&
           t2_rec->type == (int) FIELD_NORMAL)) ||
        t1_rec->length != t2_rec->length ||
        t1_rec->null_bit != t2_rec->null_bit)
    {
      DBUG_PRINT("error", ("Field %d has different definition", i));
      DBUG_PRINT("error", ("t1_type=%d, t1_length=%d, t1_null_bit=%d",
                           t1_rec->type, t1_rec->length, t1_rec->null_bit));
      DBUG_PRINT("error", ("t2_type=%d, t2_length=%d, t2_null_bit=%d",
                           t2_rec->type, t2_rec->length, t2_rec->null_bit));
      DBUG_RETURN(1);
    }
  }
  DBUG_RETURN(0);
}


extern "C" {

volatile int *killed_ptr(MI_CHECK *param)
{
  /* In theory Unsafe conversion, but should be ok for now */
  return (int*) &(((THD *)(param->thd))->killed);
}

void mi_check_print_error(MI_CHECK *param, const char *fmt,...)
{
  param->error_printed|=1;
  param->out_flag|= O_DATA_LOST;
  va_list args;
  va_start(args, fmt);
  mi_check_print_msg(param, "error", fmt, args);
  va_end(args);
}

void mi_check_print_info(MI_CHECK *param, const char *fmt,...)
{
  va_list args;
  va_start(args, fmt);
  mi_check_print_msg(param, "info", fmt, args);
  va_end(args);
}

void mi_check_print_warning(MI_CHECK *param, const char *fmt,...)
{
  param->warning_printed=1;
  param->out_flag|= O_DATA_LOST;
  va_list args;
  va_start(args, fmt);
  mi_check_print_msg(param, "warning", fmt, args);
  va_end(args);
}

}


ha_myisam::ha_myisam(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg), file(0),
  int_table_flags(HA_NULL_IN_KEY | HA_CAN_FULLTEXT | HA_CAN_SQL_HANDLER |
                  HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
                  HA_DUPLICATE_POS | HA_CAN_INDEX_BLOBS | HA_AUTO_PART_KEY |
                  HA_FILE_BASED | HA_CAN_GEOMETRY | HA_NO_TRANSACTIONS |
                  HA_CAN_INSERT_DELAYED | HA_CAN_BIT_FIELD | HA_CAN_RTREEKEYS |
                  HA_HAS_RECORDS | HA_STATS_RECORDS_IS_EXACT),
   can_enable_indexes(1)
{
		/* Modified by DT project--BEGIN--*/
        vtsf=FALSE;
         
   	THD *thd=current_thd;
	thd->dtmsg[0]=0;

	int pos=-1;
        {
          //_syscall0(pid_t,gettid);
          //pid_t gettid(void);
          //printf("Thread gettid:%d.\n",gettid());
        }
        if(table_share) {
        vsafecall(BuildVtsf(table_share->path.str,table_share->db.str,table_share->table_name.str));
  	    }
  /* Modified by DT project--END--*/
}

handler *ha_myisam::clone(MEM_ROOT *mem_root)
{
  ha_myisam *new_handler= static_cast <ha_myisam *>(handler::clone(mem_root));
  if (new_handler)
    new_handler->file->state= file->state;
  /* Modified by DT project--BEGIN--*/
  new_handler->clonevtsf(this);
  /* Modified by DT project--END--*/
  return new_handler;
}


static const char *ha_myisam_exts[] = {
  ".MYI",
  ".MYD",
  NullS
};

const char **ha_myisam::bas_ext() const
{
  return ha_myisam_exts;
}


const char *ha_myisam::index_type(uint key_number)
{
  return ((table->key_info[key_number].flags & HA_FULLTEXT) ? 
	  "FULLTEXT" :
	  (table->key_info[key_number].flags & HA_SPATIAL) ?
	  "SPATIAL" :
	  (table->key_info[key_number].algorithm == HA_KEY_ALG_RTREE) ?
	  "RTREE" :
	  "BTREE");
}

#ifdef HAVE_REPLICATION
int ha_myisam::net_read_dump(NET* net)
{
  int data_fd = file->dfile;
  int error = 0;

  my_seek(data_fd, 0L, MY_SEEK_SET, MYF(MY_WME));
  for (;;)
  {
    ulong packet_len = my_net_read(net);
    if (!packet_len)
      break ; // end of file
    if (packet_len == packet_error)
    {
      sql_print_error("ha_myisam::net_read_dump - read error ");
      error= -1;
      goto err;
    }
    if (my_write(data_fd, (uchar*)net->read_pos, (uint) packet_len,
		 MYF(MY_WME|MY_FNABP)))
    {
      error = errno;
      goto err;
    }
  }
err:
  return error;
}


int ha_myisam::dump(THD* thd, int fd)
{
  MYISAM_SHARE* share = file->s;
  NET* net = &thd->net;
  uint blocksize = share->blocksize;
  my_off_t bytes_to_read = share->state.state.data_file_length;
  int data_fd = file->dfile;
  uchar *buf = (uchar*) my_malloc(blocksize, MYF(MY_WME));
  if (!buf)
    return ENOMEM;

  int error = 0;
  my_seek(data_fd, 0L, MY_SEEK_SET, MYF(MY_WME));
  for (; bytes_to_read > 0;)
  {
    size_t bytes = my_read(data_fd, buf, blocksize, MYF(MY_WME));
    if (bytes == MY_FILE_ERROR)
    {
      error = errno;
      goto err;
    }

    if (fd >= 0)
    {
      if (my_write(fd, buf, bytes, MYF(MY_WME | MY_FNABP)))
      {
	error = errno ? errno : EPIPE;
	goto err;
      }
    }
    else
    {
      if (my_net_write(net, buf, bytes))
      {
	error = errno ? errno : EPIPE;
	goto err;
      }
    }
    bytes_to_read -= bytes;
  }

  if (fd < 0)
  {
    if (my_net_write(net, (uchar*) "", 0))
      error = errno ? errno : EPIPE;
    net_flush(net);
  }

err:
  my_free((uchar*) buf, MYF(0));
  return error;
}
#endif /* HAVE_REPLICATION */


/* Name is here without an extension */
int ha_myisam::open(const char *name, int mode, uint test_if_locked)
{
  MI_KEYDEF *keyinfo;
  MI_COLUMNDEF *recinfo= 0;
  uint recs;
  uint i;

  /*
    If the user wants to have memory mapped data files, add an
    open_flag. Do not memory map temporary tables because they are
    expected to be inserted and thus extended a lot. Memory mapping is
    efficient for files that keep their size, but very inefficient for
    growing files. Using an open_flag instead of calling mi_extra(...
    HA_EXTRA_MMAP ...) after mi_open() has the advantage that the
    mapping is not repeated for every open, but just done on the initial
    open, when the MyISAM share is created. Everytime the server
    requires to open a new instance of a table it calls this method. We
    will always supply HA_OPEN_MMAP for a permanent table. However, the
    MyISAM storage engine will ignore this flag if this is a secondary
    open of a table that is in use by other threads already (if the
    MyISAM share exists already).
  */
  if (!(test_if_locked & HA_OPEN_TMP_TABLE) && opt_myisam_use_mmap)
    test_if_locked|= HA_OPEN_MMAP;

  if (!(file=mi_open(name, mode, test_if_locked | HA_OPEN_FROM_SQL_LAYER)))
    return (my_errno ? my_errno : -1);
  if (!table->s->tmp_table) /* No need to perform a check for tmp table */
  {
    if ((my_errno= table2myisam(table, &keyinfo, &recinfo, &recs)))
    {
      /* purecov: begin inspected */
      DBUG_PRINT("error", ("Failed to convert TABLE object to MyISAM "
                           "key and column definition"));
      goto err;
      /* purecov: end */
    }
    if (check_definition(keyinfo, recinfo, table->s->keys, recs,
                         file->s->keyinfo, file->s->rec,
                         file->s->base.keys, file->s->base.fields, true))
    {
      /* purecov: begin inspected */
      my_errno= HA_ERR_CRASHED;
      goto err;
      /* purecov: end */
    }
  }
  
  if (test_if_locked & (HA_OPEN_IGNORE_IF_LOCKED | HA_OPEN_TMP_TABLE))
    VOID(mi_extra(file, HA_EXTRA_NO_WAIT_LOCK, 0));

  info(HA_STATUS_NO_LOCK | HA_STATUS_VARIABLE | HA_STATUS_CONST);
  if (!(test_if_locked & HA_OPEN_WAIT_IF_LOCKED))
    VOID(mi_extra(file, HA_EXTRA_WAIT_LOCK, 0));
  if (!table->s->db_record_offset)
    int_table_flags|=HA_REC_NOT_IN_SEQ;
  if (file->s->options & (HA_OPTION_CHECKSUM | HA_OPTION_COMPRESS_RECORD))
    int_table_flags|=HA_HAS_CHECKSUM;

  for (i= 0; i < table->s->keys; i++)
  {
    plugin_ref parser= table->key_info[i].parser;
    if (table->key_info[i].flags & HA_USES_PARSER)
      file->s->keyinfo[i].parser=
        (struct st_mysql_ftparser *)plugin_decl(parser)->info;
    table->key_info[i].block_size= file->s->keyinfo[i].block_length;
  }
    /* Modified by DT project--BEGIN--*/
  if(vtsf) {
	// no_keyread屏蔽了只从索引读数据
	table->no_keyread=1;
	ref_length=sizeof(vtsfref);
  }

// 2006/05/09 修改
// 疑为droptable 后索引消失，但frm文件仍可打开，出现
//  file->s->keyinfo中的信息正确，但table->key_info错误引起core dump(segment fault on table->key_info[i].key_part[j])
//  for(uint i=0;i<file->s->base.keys;i++) {
//    for(uint j=0;j<file->s->keyinfo[i].keysegs;j++) {
//暂时修改为
  for(i=0;i<table->s->keys;i++) {
    for(uint j=0;j<table->key_info[i].key_parts;j++) {
	  file->s->keyinfo[i].seg[j].wide_partlen=table->key_info[i].key_part[j].wide_partlen;
	}
  } 
  if(vtsf) pvtsfFile->attach_child_index_table(table);
  /* Modified by DT project--END--*/
  my_errno= 0;
  goto end;
 err:
  this->close();
 end:
  /*
    Both recinfo and keydef are allocated by my_multi_malloc(), thus only
    recinfo must be freed.
  */
  if (recinfo)
    my_free((uchar*) recinfo, MYF(0));
  return my_errno;
}

int ha_myisam::close(void)
{
  MI_INFO *tmp=file;
  file=0;
  return mi_close(tmp);
}

int ha_myisam::write_row(uchar *buf)
{
  ha_statistic_increment(&SSV::ha_write_count);

/* Modified by DT project--BEGIN--*/
  if(vtsf) return HA_ERR_UNSUPPORTED;
/* Modified by DT project--END--*/
  /* If we have a timestamp column, update it to the current time */
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
    table->timestamp_field->set_time();

  /*
    If we have an auto_increment column and we are writing a changed row
    or a new row, then update the auto_increment value in the record.
  */
  if (table->next_number_field && buf == table->record[0])
  {
    int error;
    if ((error= update_auto_increment()))
      return error;
  }
  return mi_write(file,buf);
}

int ha_myisam::check(THD* thd, HA_CHECK_OPT* check_opt)
{
  if (!file) return HA_ADMIN_INTERNAL_ERROR;
  int error;
  MI_CHECK param;
  MYISAM_SHARE* share = file->s;
  const char *old_proc_info=thd->proc_info;

  thd_proc_info(thd, "Checking table");
  myisamchk_init(&param);
  param.thd = thd;
  param.op_name =   "check";
  param.db_name=    table->s->db.str;
  param.table_name= table->alias;
  param.testflag = check_opt->flags | T_CHECK | T_SILENT;
  param.stats_method= (enum_mi_stats_method)thd->variables.myisam_stats_method;

  if (!(table->db_stat & HA_READ_ONLY))
    param.testflag|= T_STATISTICS;
  param.using_global_keycache = 1;

  if (!mi_is_crashed(file) &&
      (((param.testflag & T_CHECK_ONLY_CHANGED) &&
	!(share->state.changed & (STATE_CHANGED | STATE_CRASHED |
				  STATE_CRASHED_ON_REPAIR)) &&
	share->state.open_count == 0) ||
       ((param.testflag & T_FAST) && (share->state.open_count ==
				      (uint) (share->global_changed ? 1 : 0)))))
    return HA_ADMIN_ALREADY_DONE;

  error = chk_status(&param, file);		// Not fatal
  error = chk_size(&param, file);
  if (!error)
    error |= chk_del(&param, file, param.testflag);
  if (!error)
  if (!error)
  /* Modified by DT project--BEGIN--*/
  /* change line.
   Bypass vtsf key file check
  */ 
	  error = vtsf?0:chk_key(&param, file);
  /* Modified by DT project--END--*/
  /*Original codes before DT changing--BEGIN--*/
  /*
    error = chk_key(&param, file);
  */
  /*Original codes before DT changing--END--*/
  if (!error)
  {
    if ((!(param.testflag & T_QUICK) &&
	 ((share->options &
	   (HA_OPTION_PACK_RECORD | HA_OPTION_COMPRESS_RECORD)) ||
	  (param.testflag & (T_EXTEND | T_MEDIUM)))) ||
	mi_is_crashed(file))
    {
      uint old_testflag=param.testflag;
      param.testflag|=T_MEDIUM;
      if (!(error= init_io_cache(&param.read_cache, file->dfile,
                                 my_default_record_cache_size, READ_CACHE,
                                 share->pack.header_length, 1, MYF(MY_WME))))
      {
       /* Modified by DT project--BEGIN--*/
       //Bypass vtsf key file check
       error |= vtsf?0:chk_data_link(&param, file, param.testflag & T_EXTEND);
       /* Modified by DT project--END--*/
       /*Original codes before DT changing--BEGIN--*/
       /*
        error |= chk_data_link(&param, file, param.testflag & T_EXTEND);
        */
       /*Original codes before DT changing--END--*/
        end_io_cache(&(param.read_cache));
      }
      param.testflag= old_testflag;
    }
  }
  if (!error)
  {
    if ((share->state.changed & (STATE_CHANGED |
				 STATE_CRASHED_ON_REPAIR |
				 STATE_CRASHED | STATE_NOT_ANALYZED)) ||
	(param.testflag & T_STATISTICS) ||
	mi_is_crashed(file))
    {
      file->update|=HA_STATE_CHANGED | HA_STATE_ROW_CHANGED;
      pthread_mutex_lock(&share->intern_lock);
      share->state.changed&= ~(STATE_CHANGED | STATE_CRASHED |
			       STATE_CRASHED_ON_REPAIR);
      if (!(table->db_stat & HA_READ_ONLY))
	error=update_state_info(&param,file,UPDATE_TIME | UPDATE_OPEN_COUNT |
				UPDATE_STAT);
      pthread_mutex_unlock(&share->intern_lock);
      info(HA_STATUS_NO_LOCK | HA_STATUS_TIME | HA_STATUS_VARIABLE |
	   HA_STATUS_CONST);
    }
  }
  else if (!mi_is_crashed(file) && !thd->killed)
  {
    mi_mark_crashed(file);
    file->update |= HA_STATE_CHANGED | HA_STATE_ROW_CHANGED;
  }

  thd_proc_info(thd, old_proc_info);
  return error ? HA_ADMIN_CORRUPT : HA_ADMIN_OK;
}


/*
  analyze the key distribution in the table
  As the table may be only locked for read, we have to take into account that
  two threads may do an analyze at the same time!
*/

int ha_myisam::analyze(THD *thd, HA_CHECK_OPT* check_opt)
{
  int error=0;
  MI_CHECK param;
  MYISAM_SHARE* share = file->s;

  myisamchk_init(&param);
  param.thd = thd;
  param.op_name=    "analyze";
  param.db_name=    table->s->db.str;
  param.table_name= table->alias;
  param.testflag= (T_FAST | T_CHECK | T_SILENT | T_STATISTICS |
                   T_DONT_CHECK_CHECKSUM);
  param.using_global_keycache = 1;
  param.stats_method= (enum_mi_stats_method)thd->variables.myisam_stats_method;

  if (!(share->state.changed & STATE_NOT_ANALYZED))
    return HA_ADMIN_ALREADY_DONE;

  error = chk_key(&param, file);
  if (!error)
  {
    pthread_mutex_lock(&share->intern_lock);
    error=update_state_info(&param,file,UPDATE_STAT);
    pthread_mutex_unlock(&share->intern_lock);
  }
  else if (!mi_is_crashed(file) && !thd->killed)
    mi_mark_crashed(file);
  return error ? HA_ADMIN_CORRUPT : HA_ADMIN_OK;
}


int ha_myisam::restore(THD* thd, HA_CHECK_OPT *check_opt)
{
  HA_CHECK_OPT tmp_check_opt;
  char *backup_dir= thd->lex->backup_dir;
  char src_path[FN_REFLEN], dst_path[FN_REFLEN];
  char table_name[FN_REFLEN];
  int error;
  const char* errmsg;
  DBUG_ENTER("restore");

  VOID(tablename_to_filename(table->s->table_name.str, table_name,
                             sizeof(table_name)));

  if (fn_format_relative_to_data_home(src_path, table_name, backup_dir,
				      MI_NAME_DEXT))
    DBUG_RETURN(HA_ADMIN_INVALID);

  strxmov(dst_path, table->s->normalized_path.str, MI_NAME_DEXT, NullS);
  if (my_copy(src_path, dst_path, MYF(MY_WME)))
  {
    error= HA_ADMIN_FAILED;
    errmsg= "Failed in my_copy (Error %d)";
    goto err;
  }

  tmp_check_opt.init();
  tmp_check_opt.flags |= T_VERY_SILENT | T_CALC_CHECKSUM | T_QUICK;
  DBUG_RETURN(repair(thd, &tmp_check_opt));

 err:
  {
    MI_CHECK param;
    myisamchk_init(&param);
    param.thd= thd;
    param.op_name=    "restore";
    param.db_name=    table->s->db.str;
    param.table_name= table->s->table_name.str;
    param.testflag= 0;
    mi_check_print_error(&param, errmsg, my_errno);
    DBUG_RETURN(error);
  }
}


int ha_myisam::backup(THD* thd, HA_CHECK_OPT *check_opt)
{
  char *backup_dir= thd->lex->backup_dir;
  char src_path[FN_REFLEN], dst_path[FN_REFLEN];
  char table_name[FN_REFLEN];
  int error;
  const char *errmsg;
  DBUG_ENTER("ha_myisam::backup");

  VOID(tablename_to_filename(table->s->table_name.str, table_name,
                             sizeof(table_name)));

  if (fn_format_relative_to_data_home(dst_path, table_name, backup_dir,
				      reg_ext))
  {
    errmsg= "Failed in fn_format() for .frm file (errno: %d)";
    error= HA_ADMIN_INVALID;
    goto err;
  }

  strxmov(src_path, table->s->normalized_path.str, reg_ext, NullS);
  if (my_copy(src_path, dst_path,
	      MYF(MY_WME | MY_HOLD_ORIGINAL_MODES | MY_DONT_OVERWRITE_FILE)))
  {
    error = HA_ADMIN_FAILED;
    errmsg = "Failed copying .frm file (errno: %d)";
    goto err;
  }

  /* Change extension */
  if (fn_format_relative_to_data_home(dst_path, table_name, backup_dir,
                                      MI_NAME_DEXT))
  {
    errmsg = "Failed in fn_format() for .MYD file (errno: %d)";
    error = HA_ADMIN_INVALID;
    goto err;
  }

  strxmov(src_path, table->s->normalized_path.str, MI_NAME_DEXT, NullS);
  if (my_copy(src_path, dst_path,
	      MYF(MY_WME | MY_HOLD_ORIGINAL_MODES | MY_DONT_OVERWRITE_FILE)))
  {
    errmsg = "Failed copying .MYD file (errno: %d)";
    error= HA_ADMIN_FAILED;
    goto err;
  }
  DBUG_RETURN(HA_ADMIN_OK);

 err:
  {
    MI_CHECK param;
    myisamchk_init(&param);
    param.thd=        thd;
    param.op_name=    "backup";
    param.db_name=    table->s->db.str;
    param.table_name= table->s->table_name.str;
    param.testflag =  0;
    mi_check_print_error(&param,errmsg, my_errno);
    DBUG_RETURN(error);
  }
}


int ha_myisam::repair(THD* thd, HA_CHECK_OPT *check_opt)
{
  int error;
  MI_CHECK param;
  ha_rows start_records;

  if (!file) return HA_ADMIN_INTERNAL_ERROR;

  myisamchk_init(&param);
  param.thd = thd;
  param.op_name=  "repair";
  param.testflag= ((check_opt->flags & ~(T_EXTEND)) |
                   T_SILENT | T_FORCE_CREATE | T_CALC_CHECKSUM |
                   (check_opt->flags & T_EXTEND ? T_REP : T_REP_BY_SORT));
  param.sort_buffer_length=  check_opt->sort_buffer_size;
  start_records=file->state->records;
  while ((error=repair(thd,param,0)) && param.retry_repair)
  {
    param.retry_repair=0;
    if (test_all_bits(param.testflag,
		      (uint) (T_RETRY_WITHOUT_QUICK | T_QUICK)))
    {
      param.testflag&= ~T_RETRY_WITHOUT_QUICK;
      sql_print_information("Retrying repair of: '%s' without quick",
                            table->s->path.str);
      continue;
    }
    param.testflag&= ~T_QUICK;
    if ((param.testflag & T_REP_BY_SORT))
    {
      param.testflag= (param.testflag & ~T_REP_BY_SORT) | T_REP;
      sql_print_information("Retrying repair of: '%s' with keycache",
                            table->s->path.str);
      continue;
    }
    break;
  }
  if (!error && start_records != file->state->records &&
      !(check_opt->flags & T_VERY_SILENT))
  {
    char llbuff[22],llbuff2[22];
    sql_print_information("Found %s of %s rows when repairing '%s'",
                          llstr(file->state->records, llbuff),
                          llstr(start_records, llbuff2),
                          table->s->path.str);
  }
  return error;
}

int ha_myisam::optimize(THD* thd, HA_CHECK_OPT *check_opt)
{
  int error;
  if (!file) return HA_ADMIN_INTERNAL_ERROR;
  MI_CHECK param;

  myisamchk_init(&param);
  param.thd = thd;
  param.op_name= "optimize";
  param.testflag= (check_opt->flags | T_SILENT | T_FORCE_CREATE |
                   T_REP_BY_SORT | T_STATISTICS | T_SORT_INDEX);
  param.sort_buffer_length=  check_opt->sort_buffer_size;
  if ((error= repair(thd,param,1)) && param.retry_repair)
  {
    sql_print_warning("Warning: Optimize table got errno %d on %s.%s, retrying",
                      my_errno, param.db_name, param.table_name);
    param.testflag&= ~T_REP_BY_SORT;
    error= repair(thd,param,1);
  }
  return error;
}


int ha_myisam::repair(THD *thd, MI_CHECK &param, bool do_optimize)
{
  int error=0;
  uint local_testflag=param.testflag;
  bool optimize_done= !do_optimize, statistics_done=0;
  const char *old_proc_info=thd->proc_info;
  char fixed_name[FN_REFLEN];
  MYISAM_SHARE* share = file->s;
  ha_rows rows= file->state->records;
  DBUG_ENTER("ha_myisam::repair");

  /*
    Normally this method is entered with a properly opened table. If the
    repair fails, it can be repeated with more elaborate options. Under
    special circumstances it can happen that a repair fails so that it
    closed the data file and cannot re-open it. In this case file->dfile
    is set to -1. We must not try another repair without an open data
    file. (Bug #25289)
  */
  if (file->dfile == -1)
  {
    sql_print_information("Retrying repair of: '%s' failed. "
                          "Please try REPAIR EXTENDED or myisamchk",
                          table->s->path.str);
    DBUG_RETURN(HA_ADMIN_FAILED);
  }

  param.db_name=    table->s->db.str;
  param.table_name= table->alias;
  param.tmpfile_createflag = O_RDWR | O_TRUNC;
  param.using_global_keycache = 1;
  param.thd= thd;
  param.tmpdir= &mysql_tmpdir_list;
  param.out_flag= 0;
  strmov(fixed_name,file->filename);

  // Don't lock tables if we have used LOCK TABLE
  if (!thd->locked_tables && 
      mi_lock_database(file, table->s->tmp_table ? F_EXTRA_LCK : F_WRLCK))
  {
    mi_check_print_error(&param,ER(ER_CANT_LOCK),my_errno);
    DBUG_RETURN(HA_ADMIN_FAILED);
  }

  if (!do_optimize ||
      ((file->state->del || share->state.split != file->state->records) &&
       (!(param.testflag & T_QUICK) ||
	!(share->state.changed & STATE_NOT_OPTIMIZED_KEYS))))
  {
    ulonglong key_map= ((local_testflag & T_CREATE_MISSING_KEYS) ?
			mi_get_mask_all_keys_active(share->base.keys) :
			share->state.key_map);
    uint testflag=param.testflag;
    if (mi_test_if_sort_rep(file,file->state->records,key_map,0) &&
	(local_testflag & T_REP_BY_SORT))
    {
      local_testflag|= T_STATISTICS;
      param.testflag|= T_STATISTICS;		// We get this for free
      statistics_done=1;
      if (thd->variables.myisam_repair_threads>1)
      {
        char buf[40];
        /* TODO: respect myisam_repair_threads variable */
        my_snprintf(buf, 40, "Repair with %d threads", my_count_bits(key_map));
        thd_proc_info(thd, buf);
        error = mi_repair_parallel(&param, file, fixed_name,
            param.testflag & T_QUICK);
        thd_proc_info(thd, "Repair done"); // to reset proc_info, as
                                      // it was pointing to local buffer
      }
      else
      {
        thd_proc_info(thd, "Repair by sorting");
        error = mi_repair_by_sort(&param, file, fixed_name,
            param.testflag & T_QUICK);
      }
    }
    else
    {
      thd_proc_info(thd, "Repair with keycache");
      param.testflag &= ~T_REP_BY_SORT;
      error=  mi_repair(&param, file, fixed_name,
			param.testflag & T_QUICK);
    }
    param.testflag=testflag;
    optimize_done=1;
  }
  if (!error)
  {
    if ((local_testflag & T_SORT_INDEX) &&
	(share->state.changed & STATE_NOT_SORTED_PAGES))
    {
      optimize_done=1;
      thd_proc_info(thd, "Sorting index");
      error=mi_sort_index(&param,file,fixed_name);
    }
    if (!statistics_done && (local_testflag & T_STATISTICS))
    {
      if (share->state.changed & STATE_NOT_ANALYZED)
      {
	optimize_done=1;
	thd_proc_info(thd, "Analyzing");
	error = chk_key(&param, file);
      }
      else
	local_testflag&= ~T_STATISTICS;		// Don't update statistics
    }
  }
  thd_proc_info(thd, "Saving state");
  if (!error)
  {
    if ((share->state.changed & STATE_CHANGED) || mi_is_crashed(file))
    {
      share->state.changed&= ~(STATE_CHANGED | STATE_CRASHED |
			       STATE_CRASHED_ON_REPAIR);
      file->update|=HA_STATE_CHANGED | HA_STATE_ROW_CHANGED;
    }
    /*
      the following 'if', thought conceptually wrong,
      is a useful optimization nevertheless.
    */
    if (file->state != &file->s->state.state)
      file->s->state.state = *file->state;
    if (file->s->base.auto_key)
      update_auto_increment_key(&param, file, 1);
    if (optimize_done)
      error = update_state_info(&param, file,
				UPDATE_TIME | UPDATE_OPEN_COUNT |
				(local_testflag &
				 T_STATISTICS ? UPDATE_STAT : 0));
    info(HA_STATUS_NO_LOCK | HA_STATUS_TIME | HA_STATUS_VARIABLE |
	 HA_STATUS_CONST);
    if (rows != file->state->records && ! (param.testflag & T_VERY_SILENT))
    {
      char llbuff[22],llbuff2[22];
      mi_check_print_warning(&param,"Number of rows changed from %s to %s",
			     llstr(rows,llbuff),
			     llstr(file->state->records,llbuff2));
    }
  }
  else
  {
    mi_mark_crashed_on_repair(file);
    file->update |= HA_STATE_CHANGED | HA_STATE_ROW_CHANGED;
    update_state_info(&param, file, 0);
  }
  thd_proc_info(thd, old_proc_info);
  if (!thd->locked_tables)
    mi_lock_database(file,F_UNLCK);
  DBUG_RETURN(error ? HA_ADMIN_FAILED :
	      !optimize_done ? HA_ADMIN_ALREADY_DONE : HA_ADMIN_OK);
}


/*
  Assign table indexes to a specific key cache.
*/

int ha_myisam::assign_to_keycache(THD* thd, HA_CHECK_OPT *check_opt)
{
  KEY_CACHE *new_key_cache= check_opt->key_cache;
  const char *errmsg= 0;
  int error= HA_ADMIN_OK;
  ulonglong map;
  TABLE_LIST *table_list= table->pos_in_table_list;
  DBUG_ENTER("ha_myisam::assign_to_keycache");

  table->keys_in_use_for_query.clear_all();

  if (table_list->process_index_hints(table))
    DBUG_RETURN(HA_ADMIN_FAILED);
  map= ~(ulonglong) 0;
  if (!table->keys_in_use_for_query.is_clear_all())
    /* use all keys if there's no list specified by the user through hints */
    map= table->keys_in_use_for_query.to_ulonglong();

  if ((error= mi_assign_to_key_cache(file, map, new_key_cache)))
  { 
    char buf[STRING_BUFFER_USUAL_SIZE];
    my_snprintf(buf, sizeof(buf),
		"Failed to flush to index file (errno: %d)", error);
    errmsg= buf;
    error= HA_ADMIN_CORRUPT;
  }

  if (error != HA_ADMIN_OK)
  {
    /* Send error to user */
    MI_CHECK param;
    myisamchk_init(&param);
    param.thd= thd;
    param.op_name=    "assign_to_keycache";
    param.db_name=    table->s->db.str;
    param.table_name= table->s->table_name.str;
    param.testflag= 0;
    mi_check_print_error(&param, errmsg);
  }
  DBUG_RETURN(error);
}


/*
  Preload pages of the index file for a table into the key cache.
*/

int ha_myisam::preload_keys(THD* thd, HA_CHECK_OPT *check_opt)
{
  int error;
  const char *errmsg;
  ulonglong map;
  TABLE_LIST *table_list= table->pos_in_table_list;
  my_bool ignore_leaves= table_list->ignore_leaves;
  char buf[ERRMSGSIZE+20];

  DBUG_ENTER("ha_myisam::preload_keys");

  table->keys_in_use_for_query.clear_all();

  if (table_list->process_index_hints(table))
    DBUG_RETURN(HA_ADMIN_FAILED);

  map= ~(ulonglong) 0;
  /* Check validity of the index references */
  if (!table->keys_in_use_for_query.is_clear_all())
    /* use all keys if there's no list specified by the user through hints */
    map= table->keys_in_use_for_query.to_ulonglong();

  mi_extra(file, HA_EXTRA_PRELOAD_BUFFER_SIZE,
           (void *) &thd->variables.preload_buff_size);

  if ((error= mi_preload(file, map, ignore_leaves)))
  {
    switch (error) {
    case HA_ERR_NON_UNIQUE_BLOCK_SIZE:
      errmsg= "Indexes use different block sizes";
      break;
    case HA_ERR_OUT_OF_MEM:
      errmsg= "Failed to allocate buffer";
      break;
    default:
      my_snprintf(buf, ERRMSGSIZE,
                  "Failed to read from index file (errno: %d)", my_errno);
      errmsg= buf;
    }
    error= HA_ADMIN_FAILED;
    goto err;
  }

  DBUG_RETURN(HA_ADMIN_OK);

 err:
  {
    MI_CHECK param;
    myisamchk_init(&param);
    param.thd= thd;
    param.op_name=    "preload_keys";
    param.db_name=    table->s->db.str;
    param.table_name= table->s->table_name.str;
    param.testflag=   0;
    mi_check_print_error(&param, errmsg);
    DBUG_RETURN(error);
  }
}


/*
  Disable indexes, making it persistent if requested.

  SYNOPSIS
    disable_indexes()
    mode        mode of operation:
                HA_KEY_SWITCH_NONUNIQ      disable all non-unique keys
                HA_KEY_SWITCH_ALL          disable all keys
                HA_KEY_SWITCH_NONUNIQ_SAVE dis. non-uni. and make persistent
                HA_KEY_SWITCH_ALL_SAVE     dis. all keys and make persistent

  IMPLEMENTATION
    HA_KEY_SWITCH_NONUNIQ       is not implemented.
    HA_KEY_SWITCH_ALL_SAVE      is not implemented.

  RETURN
    0  ok
    HA_ERR_WRONG_COMMAND  mode not implemented.
*/

int ha_myisam::disable_indexes(uint mode)
{
  int error;

  if (mode == HA_KEY_SWITCH_ALL)
  {
    /* call a storage engine function to switch the key map */
    error= mi_disable_indexes(file);
  }
  else if (mode == HA_KEY_SWITCH_NONUNIQ_SAVE)
  {
    mi_extra(file, HA_EXTRA_NO_KEYS, 0);
    info(HA_STATUS_CONST);                        // Read new key info
    error= 0;
  }
  else
  {
    /* mode not implemented */
    error= HA_ERR_WRONG_COMMAND;
  }
  return error;
}


/*
  Enable indexes, making it persistent if requested.

  SYNOPSIS
    enable_indexes()
    mode        mode of operation:
                HA_KEY_SWITCH_NONUNIQ      enable all non-unique keys
                HA_KEY_SWITCH_ALL          enable all keys
                HA_KEY_SWITCH_NONUNIQ_SAVE en. non-uni. and make persistent
                HA_KEY_SWITCH_ALL_SAVE     en. all keys and make persistent

  DESCRIPTION
    Enable indexes, which might have been disabled by disable_index() before.
    The modes without _SAVE work only if both data and indexes are empty,
    since the MyISAM repair would enable them persistently.
    To be sure in these cases, call handler::delete_all_rows() before.

  IMPLEMENTATION
    HA_KEY_SWITCH_NONUNIQ       is not implemented.
    HA_KEY_SWITCH_ALL_SAVE      is not implemented.

  RETURN
    0  ok
    !=0  Error, among others:
    HA_ERR_CRASHED  data or index is non-empty. Delete all rows and retry.
    HA_ERR_WRONG_COMMAND  mode not implemented.
*/

int ha_myisam::enable_indexes(uint mode)
{
  int error;

  if (mi_is_all_keys_active(file->s->state.key_map, file->s->base.keys))
  {
    /* All indexes are enabled already. */
    return 0;
  }

  if (mode == HA_KEY_SWITCH_ALL)
  {
    error= mi_enable_indexes(file);
    /*
       Do not try to repair on error,
       as this could make the enabled state persistent,
       but mode==HA_KEY_SWITCH_ALL forbids it.
    */
  }
  else if (mode == HA_KEY_SWITCH_NONUNIQ_SAVE)
  {
    THD *thd=current_thd;
    MI_CHECK param;
    const char *save_proc_info=thd->proc_info;
    thd_proc_info(thd, "Creating index");
    myisamchk_init(&param);
    param.op_name= "recreating_index";
    param.testflag= (T_SILENT | T_REP_BY_SORT | T_QUICK |
                     T_CREATE_MISSING_KEYS);
    param.myf_rw&= ~MY_WAIT_IF_FULL;
    param.sort_buffer_length=  thd->variables.myisam_sort_buff_size;
    param.stats_method= (enum_mi_stats_method)thd->variables.myisam_stats_method;
    param.tmpdir=&mysql_tmpdir_list;
    if ((error= (repair(thd,param,0) != HA_ADMIN_OK)) && param.retry_repair)
    {
      sql_print_warning("Warning: Enabling keys got errno %d on %s.%s, retrying",
                        my_errno, param.db_name, param.table_name);
      /* Repairing by sort failed. Now try standard repair method. */
      param.testflag&= ~(T_REP_BY_SORT | T_QUICK);
      error= (repair(thd,param,0) != HA_ADMIN_OK);
      /*
        If the standard repair succeeded, clear all error messages which
        might have been set by the first repair. They can still be seen
        with SHOW WARNINGS then.
      */
      if (! error)
        thd->clear_error();
    }
    info(HA_STATUS_CONST);
    thd_proc_info(thd, save_proc_info);
  }
  else
  {
    /* mode not implemented */
    error= HA_ERR_WRONG_COMMAND;
  }
  return error;
}


/*
  Test if indexes are disabled.


  SYNOPSIS
    indexes_are_disabled()
      no parameters


  RETURN
    0  indexes are not disabled
    1  all indexes are disabled
   [2  non-unique indexes are disabled - NOT YET IMPLEMENTED]
*/

int ha_myisam::indexes_are_disabled(void)
{
  
  return mi_indexes_are_disabled(file);
}


/*
  prepare for a many-rows insert operation
  e.g. - disable indexes (if they can be recreated fast) or
  activate special bulk-insert optimizations

  SYNOPSIS
    start_bulk_insert(rows)
    rows        Rows to be inserted
                0 if we don't know

  NOTICE
    Do not forget to call end_bulk_insert() later!
*/

void ha_myisam::start_bulk_insert(ha_rows rows)
{
  DBUG_ENTER("ha_myisam::start_bulk_insert");
  THD *thd= current_thd;
  ulong size= min(thd->variables.read_buff_size,
                  (ulong) (table->s->avg_row_length*rows));
  DBUG_PRINT("info",("start_bulk_insert: rows %lu size %lu",
                     (ulong) rows, size));

  /* don't enable row cache if too few rows */
  if (! rows || (rows > MI_MIN_ROWS_TO_USE_WRITE_CACHE))
    mi_extra(file, HA_EXTRA_WRITE_CACHE, (void*) &size);

  can_enable_indexes= mi_is_all_keys_active(file->s->state.key_map,
                                            file->s->base.keys);

  if (!(specialflag & SPECIAL_SAFE_MODE))
  {
    /*
      Only disable old index if the table was empty and we are inserting
      a lot of rows.
      We should not do this for only a few rows as this is slower and
      we don't want to update the key statistics based of only a few rows.
    */
    if (file->state->records == 0 && can_enable_indexes &&
        (!rows || rows >= MI_MIN_ROWS_TO_DISABLE_INDEXES))
      mi_disable_non_unique_index(file,rows);
    else
    if (!file->bulk_insert &&
        (!rows || rows >= MI_MIN_ROWS_TO_USE_BULK_INSERT))
    {
      mi_init_bulk_insert(file, thd->variables.bulk_insert_buff_size, rows);
    }
  }
  DBUG_VOID_RETURN;
}

/*
  end special bulk-insert optimizations,
  which have been activated by start_bulk_insert().

  SYNOPSIS
    end_bulk_insert()
    no arguments

  RETURN
    0     OK
    != 0  Error
*/

int ha_myisam::end_bulk_insert()
{
  mi_end_bulk_insert(file);
  int err=mi_extra(file, HA_EXTRA_NO_CACHE, 0);
  return err ? err : can_enable_indexes ?
                     enable_indexes(HA_KEY_SWITCH_NONUNIQ_SAVE) : 0;
}


bool ha_myisam::check_and_repair(THD *thd)
{
  int error=0;
  int marked_crashed;
  char *old_query;
  uint old_query_length;
  HA_CHECK_OPT check_opt;
  DBUG_ENTER("ha_myisam::check_and_repair");

  check_opt.init();
  check_opt.flags= T_MEDIUM | T_AUTO_REPAIR;
  // Don't use quick if deleted rows
  if (!file->state->del && (myisam_recover_options & HA_RECOVER_QUICK))
    check_opt.flags|=T_QUICK;
  sql_print_warning("Checking table:   '%s'",table->s->path.str);

  old_query= thd->query;
  old_query_length= thd->query_length;
  pthread_mutex_lock(&LOCK_thread_count);
  thd->query=        table->s->table_name.str;
  thd->query_length= table->s->table_name.length;
  pthread_mutex_unlock(&LOCK_thread_count);

  if ((marked_crashed= mi_is_crashed(file)) || check(thd, &check_opt))
  {
    sql_print_warning("Recovering table: '%s'",table->s->path.str);
    check_opt.flags=
      ((myisam_recover_options & HA_RECOVER_BACKUP ? T_BACKUP_DATA : 0) |
       (marked_crashed                             ? 0 : T_QUICK) |
       (myisam_recover_options & HA_RECOVER_FORCE  ? 0 : T_SAFE_REPAIR) |
       T_AUTO_REPAIR);
    if (repair(thd, &check_opt))
      error=1;
  }
  pthread_mutex_lock(&LOCK_thread_count);
  thd->query= old_query;
  thd->query_length= old_query_length;
  pthread_mutex_unlock(&LOCK_thread_count);
  DBUG_RETURN(error);
}

bool ha_myisam::is_crashed() const
{
  return (file->s->state.changed & STATE_CRASHED ||
	  (my_disable_locking && file->s->state.open_count));
}

int ha_myisam::update_row(const uchar *old_data, uchar *new_data)
{
  ha_statistic_increment(&SSV::ha_update_count);
  /* Modified by DT project--BEGIN--*/
  /* add line .
  */
  if(vtsf) return HA_ERR_UNSUPPORTED;
  /* Modified by DT project--END--*/
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
    table->timestamp_field->set_time();
  return mi_update(file,old_data,new_data);
}

int ha_myisam::delete_row(const uchar *buf)
{
  ha_statistic_increment(&SSV::ha_delete_count);
  /* Modified by DT project--BEGIN--*/
  /* add line .
  */
  if(vtsf) return HA_ERR_UNSUPPORTED;
  /* Modified by DT project--END--*/
  return mi_delete(file,buf);
}

int ha_myisam::index_read_map(uchar *buf, const uchar *key,
                              key_part_map keypart_map,
                              enum ha_rkey_function find_flag)
{
  DBUG_ASSERT(inited==INDEX);
  ha_statistic_increment(&SSV::ha_read_key_count);
  /* Modified by DT project--BEGIN--*/
  int error;
  if(vtsf) 
    error=pvtsfFile->index_read(file,buf,key,keypart_map,find_flag);
  else 
    error=mi_rkey(file,buf,active_index, key, keypart_map, find_flag);
  /* Modified by DT project--END--*/
  /*Original codes before DT changing--BEGIN--*/
  /*
  int error=mi_rkey(file, buf, active_index, key, keypart_map, find_flag);
  */
  /*Original codes before DT changing--END--*/
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

int ha_myisam::index_read_idx_map(uchar *buf, uint index, const uchar *key,
                                  key_part_map keypart_map,
                                  enum ha_rkey_function find_flag)
{
  ha_statistic_increment(&SSV::ha_read_key_count);
  /* Modified by DT project--BEGIN--*/
  int error=0;
  if(vtsf) error=pvtsfFile->index_next(file,buf,vtsfFile::INDEX_READ_IDX,key,keypart_map,find_flag,index);
  else  error=mi_rkey(file,buf,index, key, keypart_map, find_flag);  
  /* Modified by DT project--END--*/
  /*Original codes before DT changing--BEGIN--*/
  /*
  int error=mi_rkey(file, buf, index, key, keypart_map, find_flag);
  */
  /*Original codes before DT changing--END--*/
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

int ha_myisam::index_read_last_map(uchar *buf, const uchar *key,
                                   key_part_map keypart_map)
{
  DBUG_ENTER("ha_myisam::index_read_last");
  DBUG_ASSERT(inited==INDEX);
  ha_statistic_increment(&SSV::ha_read_key_count);
    /* Modified by DT project--BEGIN--*/
  int error=vtsf?pvtsfFile->index_next(file,buf,vtsfFile::INDEX_READ_LAST,key,keypart_map):
	  mi_rkey(file,buf,active_index, key, keypart_map, HA_READ_PREFIX_LAST);
  /* Modified by DT project--END--*/
  //修改前
  //int error=mi_rkey(file, buf, active_index, key, keypart_map,
  //                  HA_READ_PREFIX_LAST);
  table->status=error ? STATUS_NOT_FOUND: 0;
  DBUG_RETURN(error);
}

int ha_myisam::index_next(uchar *buf)
{
  DBUG_ASSERT(inited==INDEX);
  ha_statistic_increment(&SSV::ha_read_next_count);
  /* Modified by DT project--BEGIN--*/
  int error=0;
  if(vtsf) error=pvtsfFile->index_next(file,buf,vtsfFile::INDEX_NEXT);
  else	 error=mi_rnext(file,buf,active_index);
  /* Modified by DT project--END--*/
  //修改前
  //int error=mi_rnext(file,buf,active_index);
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

int ha_myisam::index_prev(uchar *buf)
{
  DBUG_ASSERT(inited==INDEX);
  ha_statistic_increment(&SSV::ha_read_prev_count);
    /* Modified by DT project--BEGIN--*/
  int error=vtsf?pvtsfFile->index_next(file,buf,vtsfFile::INDEX_PREV):
	  mi_rprev(file,buf, active_index);
  /* Modified by DT project--END--*/
  //修改前
  //int error=mi_rprev(file,buf, active_index);
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

int ha_myisam::index_first(uchar *buf)
{
  DBUG_ASSERT(inited==INDEX);
  ha_statistic_increment(&SSV::ha_read_first_count);
  /* Modified by DT project--BEGIN--*/
  int error=vtsf?pvtsfFile->index_next(file,buf,vtsfFile::INDEX_FIRST):
	  mi_rfirst(file, buf, active_index);
  /* Modified by DT project--END--*/
  //修改前
  //int error=mi_rfirst(file, buf, active_index);
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

int ha_myisam::index_last(uchar *buf)
{
  DBUG_ASSERT(inited==INDEX);
  ha_statistic_increment(&SSV::ha_read_last_count);
  /* Modified by DT project--BEGIN--*/
  int error=vtsf?pvtsfFile->index_next(file,buf,vtsfFile::INDEX_LAST):
	  mi_rlast(file, buf, active_index);
  /* Modified by DT project--END--*/
  //修改前
  //int error=mi_rlast(file, buf, active_index);
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

int ha_myisam::index_next_same(uchar *buf,
			       const uchar *key __attribute__((unused)),
			       uint length __attribute__((unused)))
{
  int error;
  DBUG_ASSERT(inited==INDEX);
  ha_statistic_increment(&SSV::ha_read_next_count);
  do
  {
  /* Modified by DT project--BEGIN--*/
  //5.1.23rc 检查length要怎么用，4.0.24中length 带入 pvtsfFile->index_next！！
  if(vtsf) error=pvtsfFile->index_next(file,buf,vtsfFile::INDEX_NEXT_SAME,key,length);
  else	 error=mi_rnext_same(file,buf);
  /* Modified by DT project--END--*/
  //修改前
  // error= mi_rnext_same(file,buf);
  } while (error == HA_ERR_RECORD_DELETED);
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}


int ha_myisam::rnd_init(bool scan)
{
   /* Modified by DT project--BEGIN--*/
  THD *thd=current_thd;
  thd->dtmsg[0]=0;
  if(scan)
	  return (vtsf?
	    pvtsfFile->scan_init(file):
          mi_scan_init(file));
  /* Modified by DT project--END--*/
  /*Original codes before DT changing--BEGIN--*/
  /*
  if (scan)
    return mi_scan_init(file);
  */
  /*Original codes before DT changing--END--*/
 return mi_reset(file);                        // Free buffers
}

int ha_myisam::rnd_next(uchar *buf)
{
  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  /* Modified by DT project--BEGIN--*/
  int error=vtsf?pvtsfFile->scan(file,buf):mi_scan(file, buf);
  /* Modified by DT project--END--*/
  /*Original codes before DT changing--BEGIN--*/
  /*
  int error=mi_scan(file, buf);
  */
  /*Original codes before DT changing--END--*/
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

int ha_myisam::restart_rnd_next(uchar *buf, uchar *pos)
{
  return rnd_pos(buf,pos);
}

int ha_myisam::rnd_pos(uchar *buf, uchar *pos)
{
  ha_statistic_increment(&SSV::ha_read_rnd_count);
/* Modified by DT project--BEGIN--*/
  int error=vtsf?
	  pvtsfFile->rrnd(file,buf,pos):mi_rrnd(file, buf, my_get_ptr(pos,ref_length));
  /* Modified by DT project--END--*/
  /*Original codes before DT changing--BEGIN--*/
  /*
  int error=mi_rrnd(file, buf, my_get_ptr(pos,ref_length));
  */
  /*Original codes before DT changing--END--*/
  //4.0.24中下面一行也被注释掉 ： 
  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

void ha_myisam::position(const uchar *record)
{
	  /* Modified by DT project--BEGIN--*/
	if(vtsf) 
		memcpy(ref,pvtsfFile->getCurRef(),sizeof(vtsfref));
	else
	{
		my_off_t row_position= mi_position(file);
		my_store_ptr(ref, ref_length, row_position);
	}
  /* Modified by DT project--END--*/
  /*Original codes before DT changing--BEGIN--*/
  /*
  my_off_t row_position= mi_position(file);
  my_store_ptr(ref, ref_length, row_position);
  */
  /*Original codes before DT changing--END--*/
}

int ha_myisam::info(uint flag)
{
  MI_ISAMINFO misam_info;
  char name_buff[FN_REFLEN];
/* Modified by DT project--BEGIN--*/
/* add line
*/
  if(vtsf) file->state->records=pvtsfFile->GetMaxRows();
/* Modified by DT project--END--*/
  (void) mi_status(file,&misam_info,flag);
  if (flag & HA_STATUS_VARIABLE)
  {
    stats.records=           misam_info.records;
    stats.deleted=           misam_info.deleted;
    stats.data_file_length=  misam_info.data_file_length;
    stats.index_file_length= misam_info.index_file_length;
    stats.delete_length=     misam_info.delete_length;
    stats.check_time=        misam_info.check_time;
    stats.mean_rec_length=   misam_info.mean_reclength;
  }
  if (flag & HA_STATUS_CONST)
  {
    TABLE_SHARE *share= table->s;
    stats.max_data_file_length=  misam_info.max_data_file_length;
    stats.max_index_file_length= misam_info.max_index_file_length;
    stats.create_time= misam_info.create_time;
/* Modified by DT project--BEGIN--*/
// DT destination table use a difference ref_length
// Add lines
    if(vtsf) 
     ref_length=sizeof(vtsfref);
    else
/* Modified by DT project--END--*/
    ref_length= misam_info.reflength;
    share->db_options_in_use= misam_info.options;
    stats.block_size= myisam_block_size;        /* record block size */

    /* Update share */
    if (share->tmp_table == NO_TMP_TABLE)
      pthread_mutex_lock(&share->mutex);
    share->keys_in_use.set_prefix(share->keys);
    share->keys_in_use.intersect_extended(misam_info.key_map);
    share->keys_for_keyread.intersect(share->keys_in_use);
    share->db_record_offset= misam_info.record_offset;
    if (share->key_parts)
      memcpy((char*) table->key_info[0].rec_per_key,
	     (char*) misam_info.rec_per_key,
	     sizeof(table->key_info[0].rec_per_key)*share->key_parts);
    if (share->tmp_table == NO_TMP_TABLE)
      pthread_mutex_unlock(&share->mutex);

   /*
     Set data_file_name and index_file_name to point at the symlink value
     if table is symlinked (Ie;  Real name is not same as generated name)
   */
    data_file_name= index_file_name= 0;
    fn_format(name_buff, file->filename, "", MI_NAME_DEXT,
              MY_APPEND_EXT | MY_UNPACK_FILENAME);
    if (strcmp(name_buff, misam_info.data_file_name))
      data_file_name=misam_info.data_file_name;
    fn_format(name_buff, file->filename, "", MI_NAME_IEXT,
              MY_APPEND_EXT | MY_UNPACK_FILENAME);
    if (strcmp(name_buff, misam_info.index_file_name))
      index_file_name=misam_info.index_file_name;
  }
  if (flag & HA_STATUS_ERRKEY)
  {
    errkey  = misam_info.errkey;
    my_store_ptr(dup_ref, ref_length, misam_info.dupp_key_pos);
  }
  if (flag & HA_STATUS_TIME)
    stats.update_time = misam_info.update_time;
  if (flag & HA_STATUS_AUTO)
    stats.auto_increment_value= misam_info.auto_increment;

  return 0;
}


int ha_myisam::extra(enum ha_extra_function operation)
{
  if ((specialflag & SPECIAL_SAFE_MODE) && operation == HA_EXTRA_KEYREAD)
    return 0;
  return mi_extra(file, operation, 0);
}

int ha_myisam::reset(void)
{
  return mi_reset(file);
}

/* To be used with WRITE_CACHE and EXTRA_CACHE */

int ha_myisam::extra_opt(enum ha_extra_function operation, ulong cache_size)
{
  if ((specialflag & SPECIAL_SAFE_MODE) && operation == HA_EXTRA_WRITE_CACHE)
    return 0;
  return mi_extra(file, operation, (void*) &cache_size);
}

int ha_myisam::delete_all_rows()
{
  return mi_delete_all_rows(file);
}

int ha_myisam::delete_table(const char *name)
{
  return mi_delete_table(name);
}


int ha_myisam::external_lock(THD *thd, int lock_type)
{
  return mi_lock_database(file, !table->s->tmp_table ?
			  lock_type : ((lock_type == F_UNLCK) ?
				       F_UNLCK : F_EXTRA_LCK));
}

THR_LOCK_DATA **ha_myisam::store_lock(THD *thd,
				      THR_LOCK_DATA **to,
				      enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && file->lock.type == TL_UNLOCK)
    file->lock.type=lock_type;
  *to++= &file->lock;
  return to;
}

void ha_myisam::update_create_info(HA_CREATE_INFO *create_info)
{
  ha_myisam::info(HA_STATUS_AUTO | HA_STATUS_CONST);
  if (!(create_info->used_fields & HA_CREATE_USED_AUTO))
  {
    create_info->auto_increment_value= stats.auto_increment_value;
  }
  create_info->data_file_name=data_file_name;
  create_info->index_file_name=index_file_name;
}


int ha_myisam::create(const char *name, register TABLE *table_arg,
		      HA_CREATE_INFO *ha_create_info)
{
  int error;
  uint create_flags= 0, records, i;
  char buff[FN_REFLEN];
  MI_KEYDEF *keydef;
  MI_COLUMNDEF *recinfo;
  MI_CREATE_INFO create_info;
  TABLE_SHARE *share= table_arg->s;
  uint options= share->db_options_in_use;
  DBUG_ENTER("ha_myisam::create");
  for (i= 0; i < share->keys; i++)
  {
    if (table_arg->key_info[i].flags & HA_USES_PARSER)
    {
      create_flags|= HA_CREATE_RELIES_ON_SQL_LAYER;
      break;
    }
  }
  if ((error= table2myisam(table_arg, &keydef, &recinfo, &records)))
    DBUG_RETURN(error); /* purecov: inspected */
  bzero((char*) &create_info, sizeof(create_info));
  create_info.max_rows= share->max_rows;
  create_info.reloc_rows= share->min_rows;
  create_info.with_auto_increment= share->next_number_key_offset == 0;
  create_info.auto_increment= (ha_create_info->auto_increment_value ?
                               ha_create_info->auto_increment_value -1 :
                               (ulonglong) 0);
  create_info.data_file_length= ((ulonglong) share->max_rows *
                                 share->avg_row_length);
  create_info.data_file_name= ha_create_info->data_file_name;
  create_info.index_file_name= ha_create_info->index_file_name;
  create_info.language= share->table_charset->number;

  if (ha_create_info->options & HA_LEX_CREATE_TMP_TABLE)
    create_flags|= HA_CREATE_TMP_TABLE;
  if (ha_create_info->options & HA_CREATE_KEEP_FILES)
    create_flags|= HA_CREATE_KEEP_FILES;
  if (options & HA_OPTION_PACK_RECORD)
    create_flags|= HA_PACK_RECORD;
  if (options & HA_OPTION_CHECKSUM)
    create_flags|= HA_CREATE_CHECKSUM;
  if (options & HA_OPTION_DELAY_KEY_WRITE)
    create_flags|= HA_CREATE_DELAY_KEY_WRITE;

  /* TODO: Check that the following fn_format is really needed */
  error= mi_create(fn_format(buff, name, "", "",
                             MY_UNPACK_FILENAME|MY_APPEND_EXT),
                   share->keys, keydef,
                   records, recinfo,
                   0, (MI_UNIQUEDEF*) 0,
                   &create_info, create_flags);
  my_free((uchar*) recinfo, MYF(0));
  DBUG_RETURN(error);
}


int ha_myisam::rename_table(const char * from, const char * to)
{
  return mi_rename(from,to);
}


void ha_myisam::get_auto_increment(ulonglong offset, ulonglong increment,
                                   ulonglong nb_desired_values,
                                   ulonglong *first_value,
                                   ulonglong *nb_reserved_values)
{
  ulonglong nr;
  int error;
  uchar key[MI_MAX_KEY_LENGTH];

  if (!table->s->next_number_key_offset)
  {						// Autoincrement at key-start
    ha_myisam::info(HA_STATUS_AUTO);
    *first_value= stats.auto_increment_value;
    /* MyISAM has only table-level lock, so reserves to +inf */
    *nb_reserved_values= ULONGLONG_MAX;
    return;
  }

  /* it's safe to call the following if bulk_insert isn't on */
  mi_flush_bulk_insert(file, table->s->next_number_index);

  (void) extra(HA_EXTRA_KEYREAD);
  key_copy(key, table->record[0],
           table->key_info + table->s->next_number_index,
           table->s->next_number_key_offset);
  error= mi_rkey(file, table->record[1], (int) table->s->next_number_index,
                 key, make_prev_keypart_map(table->s->next_number_keypart),
                 HA_READ_PREFIX_LAST);
  if (error)
    nr= 1;
  else
  {
    /* Get data from record[1] */
    nr= ((ulonglong) table->next_number_field->
         val_int_offset(table->s->rec_buff_length)+1);
  }
  extra(HA_EXTRA_NO_KEYREAD);
  *first_value= nr;
  /*
    MySQL needs to call us for next row: assume we are inserting ("a",null)
    here, we return 3, and next this statement will want to insert ("b",null):
    there is no reason why ("b",3+1) would be the good row to insert: maybe it
    already exists, maybe 3+1 is too large...
  */
  *nb_reserved_values= 1;
}


/*
  Find out how many rows there is in the given range

  SYNOPSIS
    records_in_range()
    inx			Index to use
    min_key		Start of range.  Null pointer if from first key
    max_key		End of range. Null pointer if to last key

  NOTES
    min_key.flag can have one of the following values:
      HA_READ_KEY_EXACT		Include the key in the range
      HA_READ_AFTER_KEY		Don't include key in range

    max_key.flag can have one of the following values:  
      HA_READ_BEFORE_KEY	Don't include key in range
      HA_READ_AFTER_KEY		Include all 'end_key' values in the range

  RETURN
   HA_POS_ERROR		Something is wrong with the index tree.
   0			There is no matching keys in the given range
   number > 0		There is approximately 'number' matching rows in
			the range.
*/

ha_rows ha_myisam::records_in_range(uint inx, key_range *min_key,
                                    key_range *max_key)
{
 /* Modified by DT project--BEGIN--*/
	// force bypass key record read.
  //table->used_keys=0;
  /*
  KEY_PART_INFO *key_part;
  uint kpos=0;
  for(key_part=table->key_info[inx].key_part;kpos<table->key_info->key_parts;key_part++,kpos++)
	  if(key_part->wide_partlen>0) table->used_keys=0;
  */
  if(vtsf) 
	return pvtsfFile->records_in_range(inx,
				       min_key, max_key);
  /* Modified by DT project--END--*/
    return (ha_rows) mi_records_in_range(file, (int) inx, min_key, max_key);
}


int ha_myisam::ft_read(uchar *buf)
{
  int error;

  if (!ft_handler)
    return -1;

  thread_safe_increment(table->in_use->status_var.ha_read_next_count,
			&LOCK_status); // why ?

  error=ft_handler->please->read_next(ft_handler,(char*) buf);

  table->status=error ? STATUS_NOT_FOUND: 0;
  return error;
}

uint ha_myisam::checksum() const
{
  return (uint)file->state->checksum;
}


bool ha_myisam::check_if_incompatible_data(HA_CREATE_INFO *info,
					   uint table_changes)
{
  uint options= table->s->db_options_in_use;

  if (info->auto_increment_value != stats.auto_increment_value ||
      info->data_file_name != data_file_name ||
      info->index_file_name != index_file_name ||
      table_changes == IS_EQUAL_NO ||
      table_changes & IS_EQUAL_PACK_LENGTH) // Not implemented yet
    return COMPATIBLE_DATA_NO;

  if ((options & (HA_OPTION_PACK_RECORD | HA_OPTION_CHECKSUM |
		  HA_OPTION_DELAY_KEY_WRITE)) !=
      (info->table_options & (HA_OPTION_PACK_RECORD | HA_OPTION_CHECKSUM |
			      HA_OPTION_DELAY_KEY_WRITE)))
    return COMPATIBLE_DATA_NO;
  return COMPATIBLE_DATA_YES;
}

extern int mi_panic(enum ha_panic_function flag);
int myisam_panic(handlerton *hton, ha_panic_function flag)
{
  return mi_panic(flag);
}

static int myisam_init(void *p)
{
  handlerton *myisam_hton;

  myisam_hton= (handlerton *)p;
  myisam_hton->state= SHOW_OPTION_YES;
  myisam_hton->db_type= DB_TYPE_MYISAM;
  myisam_hton->create= myisam_create_handler;
  myisam_hton->panic= myisam_panic;
  myisam_hton->flags= HTON_CAN_RECREATE | HTON_SUPPORT_LOG_TABLES;
  return 0;
}

struct st_mysql_storage_engine myisam_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(myisam)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &myisam_storage_engine,
  "MyISAM",
  "MySQL AB",
  "Default engine as of MySQL 3.23 with great performance",
  PLUGIN_LICENSE_GPL,
  myisam_init, /* Plugin Init */
  NULL, /* Plugin Deinit */
  0x0100, /* 1.0 */
  NULL,                       /* status variables                */
  NULL,                       /* system variables                */
  NULL                        /* config options                  */
}
mysql_declare_plugin_end;


#ifdef HAVE_QUERY_CACHE
/**
  @brief Register a named table with a call back function to the query cache.

  @param thd The thread handle
  @param table_key A pointer to the table name in the table cache
  @param key_length The length of the table name
  @param[out] engine_callback The pointer to the storage engine call back
    function, currently 0
  @param[out] engine_data Engine data will be set to 0.

  @note Despite the name of this function, it is used to check each statement
    before it is cached and not to register a table or callback function.

  @see handler::register_query_cache_table

  @return The error code. The engine_data and engine_callback will be set to 0.
    @retval TRUE Success
    @retval FALSE An error occured
*/

my_bool ha_myisam::register_query_cache_table(THD *thd, char *table_name,
                                              uint table_name_len,
                                              qc_engine_callback
                                              *engine_callback,
                                              ulonglong *engine_data)
{
  DBUG_ENTER("ha_myisam::register_query_cache_table");
  /*
    No call back function is needed to determine if a cached statement
    is valid or not.
  */
  *engine_callback= 0;

  /*
    No engine data is needed.
  */
  *engine_data= 0;

  if (file->s->concurrent_insert)
  {
    /*
      If a concurrent INSERT has happened just before the currently
      processed SELECT statement, the total size of the table is
      unknown.

      To determine if the table size is known, the current thread's snap
      shot of the table size with the actual table size are compared.

      If the table size is unknown the SELECT statement can't be cached.

      When concurrent inserts are disabled at table open, mi_open()
      does not assign a get_status() function. In this case the local
      ("current") status is never updated. We would wrongly think that
      we cannot cache the statement.
    */
    ulonglong actual_data_file_length;
    ulonglong current_data_file_length;

    /*
      POSIX visibility rules specify that "2. Whatever memory values a
      thread can see when it unlocks a mutex <...> can also be seen by any
      thread that later locks the same mutex". In this particular case,
      concurrent insert thread had modified the data_file_length in
      MYISAM_SHARE before it has unlocked (or even locked)
      structure_guard_mutex. So, here we're guaranteed to see at least that
      value after we've locked the same mutex. We can see a later value
      (modified by some other thread) though, but it's ok, as we only want
      to know if the variable was changed, the actual new value doesn't matter
    */
    actual_data_file_length= file->s->state.state.data_file_length;
    current_data_file_length= file->save_state.data_file_length;

    if (current_data_file_length != actual_data_file_length)
    {
      /* Don't cache current statement. */
      DBUG_RETURN(FALSE);
    }
  }

  /* It is ok to try to cache current statement. */
  DBUG_RETURN(TRUE);
}
#endif


/* Modified by DT project--BEGIN--*/
/* to the end of file*/
vtsfFile::~vtsfFile () {
	//if(current_thd)	
	//	close_cached_tables(current_thd,false,tables);
	    //thd->version--;
		//close_thread_tables(thd,true);
		//delete thd;
//		my_pthread_setspecific_ptr(THR_THD,  0);
		//if(mt) wocidestroy(mt);
		//if(fp) fclose(fp);
//	if(lock && lock->lock_count>0)
//		mysql_unlock_read_tables(current_thd,lock);
	
//	lock=NULL;
	dp_cache *pdc=dp_cache::getInstance();
	pdc->FlushTab(tabid);	
	if(pdtpmap) delete pdtpmap;
//	for(int i=0;i<soleidxnum;i++) 
//	  tables[i].table=NULL;
//	if(tables) delete []tables;
	if(pmycd) delete []pmycd;
}
/*
   	struct tab_desc {
	  int cbsize;
	  int coldesc_len,colnum,rowlen ,rownum;
	  char detail[1];
	};
*/
	bool vtsfFile::checkDM(ulonglong pos) {
		return (havedm && resdmptr!=NULL && (resdmptr[pos/8]&(1<<(pos%8))));
	}
		
	int vtsfFile::getRecord(byte *buf,ulonglong pos,size_t reclen){
		if(!mt) {
			if(resptr==NULL) {
			 ThrowWith("Invalid point address in ::getRecord,fnid:%d,offset:%d,rowpos:%d",
			   cur_ref.fnid,cur_ref.blockoffset,cur_ref.recordid);
			 return 1;
			}
			if(!file.rebuildrow(resptr,(char *)buf,row_num_ib,pos))
			  memcpy(buf,resptr+pos*mysqlrowlen,mysqlrowlen);
			rowct++;
		  	cur_ref.recordid=pos;
			return 0;
		}
		//autotimer atm(&recordtm);
		int i,j; 
		int off=(colct+7)/8;
		memset(buf+off,' ',reclen);
		memset(buf,0,off);
		bool filled=false;
		cur_ref.recordid=pos;
		for(i=0;i<colct;i++) {
			int clen=pmycd[i].clen;
			int slen;
			char *pdst=(char*)buf+off;
			
			switch(pmycd[i].ctype) {
			case COLUMN_TYPE_CHAR	:
				{
				//RebuildDC中已调整过
				//clen--;
				char *src=(char *)(pmycd[i].pbuf+(clen+1)*pos);
				if(*src!=0) {
					//int l=strlen(src);
					//memset(pdst,' ',clen);
					//memcpy(pdst,src,l);//,strlen(src));
					char *lst=(char *)memccpy(pdst,src,0,clen);
					if(lst) *--lst=' ';
				}
				else {
					//memset(pdst,' ',clen);
					setNullBit(buf,i);
				}
				}
				off+=clen;
				break;
			case COLUMN_TYPE_FLOAT	:
				{
				 double v=((double *)pmycd[i].pbuf)[pos];
				 revDouble(&v);
				 memcpy(pdst,&v,sizeof(double));
				}
				clen=sizeof(double);
				off+=clen;
				break;
			case COLUMN_TYPE_NUM	:
				sprintf(pdst,"%.*f",pmycd[i].scale,((double *)pmycd[i].pbuf)[pos]);
				//filled=true;
				//filled=false;
				slen=(uint) strlen(pdst);
				if (slen > clen)
					return 1;
				else
				{
				    //char *to=psrc(char *)buf+off;
					memmove(pdst+(clen-slen),pdst,slen);
					for (j=clen-slen ; j-- > 0 ;)
						*pdst++ = ' ' ;
				}
				off+=clen;
				break;
			case COLUMN_TYPE_INT	:
			 	{
				 	int v=((int *)pmycd[i].pbuf)[pos];
				 	revInt(&v);
				 	memcpy(pdst,&v,sizeof(int));
				}

				//*(int *)pdst=((int *)pmycd[i].pbuf)[pos];//*sizeof(int));
				slen=sizeof(int);
				off+=slen;
				//ResetBit(varflag,varcol);
				//ResetBit(nullflag,col);
				break;
	
				//if(!filled) {
				//	itoa(((int *)pmycd[i].pbuf)[pos],pdst,10);
				//}
			case COLUMN_TYPE_DATE	:
				{
				char *src=pmycd[i].pbuf+7*pos;
				if(*src==0) {
					setNullBit(buf,i);
					//memcpy(pdst,&mdt,sizeof(longlong));
					//off+=sizeof(LONG64);
				}
				else {
					
					//typedef __int64 long long;
					LONG64 mdt=0;
					mdt=LLNY*((src[0]-100)*100+src[1]-100);
					mdt+=LLHMS*src[2];
					mdt+=LL(1000000)*src[3];
					mdt+=LL(10000)*(src[4]-1);
					mdt+=100*(src[5]-1);
					mdt+=src[6]-1;
					memcpy(pdst,&mdt,sizeof(LONG64));
					//rev8B(pdst);
					off+=sizeof(LONG64);
					//rev8B(pdst);//??????
	//				ResetBit(varflag,varcol);
	//				ResetBit(nullflag,col);
				}
				break;
				}
	//
	//			return 1;
			}
		}
		//if(reclen!=off) return 1;
		rowct++;
		return 0;
	}

int vtsfFile::loadBlock(int fnid,int pos,int storesize) {
	//autotimer atm(&blocktm);
        //ldblktms++;
	dp_cache *pdc=dp_cache::getInstance();
	//int blockid,bflen;
	//*暂停cache功能
	//cacheblockid=MAX_UINT;
	
	//CanEdit条件在快速恢复的表中不满足,然而没用cache,对于多分区独立的新索引格式,几乎不可用.
        //if(file.CanEdit()) {
         CheckAndReleaseCache();
         cachebflen=pdtpmap->GetBlockSize();
	 if(pos>0 && mt==0 && pdc->getBlock(&resptr,&resdmptr,tabid,fnid,pos,cacheblockid,cachebflen,havedm))
	 {
		row_num_ib=cachebflen/mysqlrowlen;
		cur_ref.blockoffset=pos;
		cur_ref.recordid=0;
		cur_ref.fnid=fnid-deffnid;
         	return 0;
	 }
	
	//*/
	//resptr=dpcache.get(fnid-deffnid,pos,&cur_ref,&row_num_ib);
	//if(resptr!=NULL) return 0;
	/*
	SvrAdmin *psa=SvrAdmin::GetInstance();
	if(psa==NULL) {
		int retryct=0;
		while(retryct++<20 && psa==NULL) {
		  psa=SvrAdmin::GetInstance();
		  mSleep(500);
		}
		if(psa==NULL) {
      	  		sql_print_error("Fatal error: Can't load DT Block data,SvrAdmin instance always NULL.fnid:%d,pos:%d,size:%d",
	   			fnid,pos,storesize);
      	  		ThrowWith("Fatal error: Can't load DT Block data,SvrAdmin instance always NULL.fnid:%d,pos:%d,size:%d",
	   			fnid,pos,storesize);
	   	}
	}
	*/
	
	//要防止file_mt::ReadMtThread中的Open调用与这里的Open调用冲突
	// pos==-1为连续全表扫描，file_mt::ReadBlock会处理连247续文件读取
	if(file.GetFnid()!=fnid && pos!=-1) { 
		file.Open(pdtpmap->GetFileName(fnid),0,fnid);
	}
	//file.SetDbgCt(rowct);
	//mt=file.ReadBlock(pos,storesize,0,false);
	if(cacheblockid!=MAX_UINT)
	{
		try {
			mt=file.ReadMtOrBlock(pos,storesize,0,&resptr,resptr,cachebflen);
			char *ptemp=NULL;
  	     		havedm=file.getdelmaskbuf(&ptemp);
  	     		if(havedm)
  	     		  memcpy(resdmptr,ptemp,(file.GetBlockRowNum()+7)/8);
		}
		catch(...) {
			pdc->Reset(cacheblockid);
			throw;
		}
	}
  	else 
  	{
  	     mt=file.ReadMtOrBlock(pos,storesize,0,&resptr);
  	     havedm=file.getdelmaskbuf(&resdmptr);
  	}
	if(mt==-1) {
	    mt=0;
	    if(cacheblockid!=MAX_UINT) {
		  //pdc->ReleaseFilling(cacheblockid,tabid,file.GetFnid(),file.GetOldOffset(),file.GetBlockRowNum()*mysqlrowlen);
		  pdc->Reset(cacheblockid);

	    }
	    return HA_ERR_END_OF_FILE;
	}
	if(mt>0) {
		if(!pmycd) {
			colct=wociGetColumnNumber(mt);
			pmycd= new mycd[colct];
			RebuildDC();
		}
		else RebuildDC();
	}
	
	row_num_ib=file.GetBlockRowNum();
	cur_ref.blockoffset=file.GetOldOffset();
	cur_ref.recordid=0;
	cur_ref.fnid=file.GetFnid()-deffnid;
	if(cacheblockid!=MAX_UINT) {
		pdc->ReleaseFilling(cacheblockid,tabid,file.GetFnid(),file.GetOldOffset(),file.GetBlockRowNum()*mysqlrowlen,havedm);
	}
	//if(mt<=0) 
	//	dpcache.push(&cur_ref,resptr,mysqlrowlen*row_num_ib,row_num_ib);
	return 0;
}

int vtsfFile::loadNextBlock() {
	return loadBlock(file.GetFnid(),-1,0);
}

void vtsfFile::CheckAndReleaseCache()
{
  if(cacheblockid!=MAX_UINT) {
	  dp_cache::getInstance()->ReleaseReading(cacheblockid);
	  //一旦释放,绝对不允许再次对缓冲区操作,因此需要加cur_ref初始化代码
	  cacheblockid=MAX_UINT;
	  cur_ref.fnid=MAX_UINT;
	  cur_ref.blockoffset=MAX_UINT;
  }
}

int vtsfFile::raw_scan_init(MI_INFO *info)
{

  DBUG_ENTER("vtsf_scan_init");
  info->nextpos=info->s->pack.header_length;	/* Read first record */
  info->lastinx= -1;				/* Can't forward or backward */
  info->next_block=0;
  CheckAndReleaseCache();
  //blocktm.Clear();
  //scantm.Clear();
  //scantm.Start();
  //recordtm.Clear();

  //一旦释放,绝对不允许再次对缓冲区操作,因此需要加cur_ref初始化代码
  cur_ref.fnid=MAX_UINT;
  cur_ref.blockoffset=MAX_UINT;
  curidxid=-1;
  curidxoff=-1;
  rowct=0;
  subcuridxid=-1;
  idxfieldnum=-1;

  file.Open(pdtpmap->GetFirstFile(),0,0);
  //file.Reset();
  deffnid=file.GetFnid();
  //char cachereport[300]; 
  //dp_cache::getInstance()->RepStatus(cachereport);
  //double blockct=blocktm.GetTime(),recordct=recordtm.GetTime();
  //double indexct=indextm.GetTime();
  /*
  info->lastinx= -1;
  info->current_record= (ulong) ~0L;		// No current record 
  info->update=0;
  */
/*
  TABLE *t_table = tables[1].table; 
  {

	  byte *key=new byte[30];
	  key[0]=0;
  	  strcpy((char *)key+1,"   35009966");
	  t_table->file->index_init(0);
	  //int err=t_table->file->index_read(t_table->record[0], key,
		//	  5, HA_READ_KEY_EXACT);
	  int err=t_table->file->index_read_idx(t_table->record[0], 0,key,
			  12, HA_READ_KEY_EXACT);
//	  if(!err) 
//		  err=t_table->file->index_next_same(t_table->record[0], key,5);
    char buff[MAX_FIELD_WIDTH];
    String str(buff,sizeof(buff));
    Field *field=t_table->field[0];
    field->val_str(&str,&str);
    field=t_table->field[1];
    field->val_str(&str,&str);
	longlong val=field->val_int();
	  uint length=str.length();
	  err=-1;
	  delete [] key;
	  
  }
  t_table->file->index_end();
*/
  DBUG_RETURN(0);
}

int vtsfFile::raw_scan(MI_INFO *info, byte *record)
{
  MYISAM_SHARE *share=info->s;
  ulonglong pos;
  THD *thd=current_thd;
  DBUG_ENTER("vtsf_scan");
  if (current_thd->variables.select_limit==1 && info->nextpos>0) 
	DBUG_RETURN(HA_ERR_END_OF_FILE);
  do {
   pos= ++info->nextpos;//current_record;
   if (pos >= info->next_block)
   {
	  if(loadBlock(deffnid,-1)) {
		info->update= 0;
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	  }
	  info->next_block=row_num_ib;
	  //pos=info->current_record=0;
	  pos=info->nextpos=0;
   }
   info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
   //info->current_hash_ptr=0;			/* Can't use read_next */
   } while(checkDM(pos));
   int err=getRecord(record,pos,info->s->base.pack_reclength);//(size_t) 423);//share->reclength);
//  lmtrn++;
  //if(!err && lmtrn>1000) 
//	  err=137;
//  if(rowct>1000000 && !err) {
//  	err=HA_ERR_END_OF_FILE;
//  	printf("blocktm:%9.4f,recordtm:%9.4f.\n",blocktm.GetTime(),recordtm.GetTime());
//  }
  if(err) return err;

  //memcpy(record,info->current_ptr,(size_t) share->reclength);
  //memcpy(record,info->current_ptr,(size_t) share->reclength);
  DBUG_RETURN(0);
}

void vtsfFile::setNullBit(byte *buf, int colid)
{
	static byte marks[8]={1,2,4,8,16,32,64,128};
	buf[colid/8]|=marks[colid%8];
}

void vtsfFile::RebuildDC()
{
	if(!mt) return;
	for(int i=0;i<colct;i++) {
			pmycd[i].clen=wociGetColumnDataLenByPos(mt,i);
			pmycd[i].scale=wociGetColumnScale(mt,i);
			pmycd[i].ctype=wociGetColumnType(mt,i);
			switch(pmycd[i].ctype) {
			case COLUMN_TYPE_CHAR	:
				wociGetStrAddrByPos(mt,i,0,(char **)&(pmycd[i].pbuf),&pmycd[i].clen);
				pmycd[i].clen--;
				break;
			case COLUMN_TYPE_FLOAT	:
			case COLUMN_TYPE_NUM	:
				wociGetDoubleAddrByPos(mt,i,0,(double **)&pmycd[i].pbuf);
				pmycd[i].clen+=2;
				break;
			case COLUMN_TYPE_INT	:
				wociGetIntAddrByPos(mt,i,0,(int **)&pmycd[i].pbuf);
				pmycd[i].clen++;
				break;
			case COLUMN_TYPE_DATE	:
				wociGetDateAddrByPos(mt,i,0,(char **)&pmycd[i].pbuf);
				break;
			}
		}
}



int vtsfFile::raw_index_read(MI_INFO *info,byte * buf, const byte * key,
			key_part_map keypart_map,
			enum ha_rkey_function find_flag
			__attribute__((unused)))
{
  int error=0;
  MYISAM_SHARE *share=info->s;
  ulonglong pos;
  DBUG_ENTER("vtsf_index_read");
  
//  if (current_thd->limit_found_rows==1) {
  if (current_thd->variables.select_limit==1) {
	return HA_ERR_KEY_NOT_FOUND;
      /*
	  info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
      info->current_hash_ptr=0;			// Can't use read_next 
	  info->next_block=(int)1;
	  pos=info->nextpos=(int)0;
      error=getRecord(buf,pos,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
	  return 0;
	  */
  }
  {
	  extern uint volatile dt_index_read;
	  dt_index_read++;
  }
  bool isdm=false;
  do {
  pos= ++info->nextpos;//current_record;
  if (pos >= info->next_block )
  {
	  if(isdm) {
	  	if(find_flag==HA_READ_AFTER_KEY || find_flag==HA_READ_KEY_EXACT || find_flag==HA_READ_KEY_OR_NEXT ) error=raw_index_next(info,buf,INDEX_NEXT,key,keypart_map,find_flag,curidxid);
	        else if(find_flag==HA_READ_BEFORE_KEY || find_flag==HA_READ_KEY_OR_PREV ) error=raw_index_next(info,buf,INDEX_PREV,key,keypart_map,find_flag,curidxid);
	  	else  // unknown control flow
		error=raw_index_next(info,buf,INDEX_NEXT,key,keypart_map,find_flag,curidxid);
		//return HA_ERR_KEY_NOT_FOUND;
	  	isdm=false;
	  }
	  else {
	   TABLE *t_table = GetTable(0);//tables[curidxoff].table; 
	   error=t_table->file->index_read_map(t_table->record[0], key,
		keypart_map,find_flag);
           if(!error) {
		//char buff[MAX_FIELD_WIDTH];
		//String str(buff,sizeof(buff));
		//longlong fnid=t_table->
		//char buff[MAX_FIELD_WIDTH];
		//索引表公共字段
		//wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"idx_startrow",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"idx_rownum",NULL,COLUMN_TYPE_INT,10,0);
		longlong fnid=t_table->field[idxfieldnum]->val_int()-deffnid;
		longlong blockstart=t_table->field[idxfieldnum+1]->val_int();
		longlong blocksize=t_table->field[idxfieldnum+2]->val_int();
		longlong startrow=t_table->field[idxfieldnum+4]->val_int();
		longlong rownum=t_table->field[idxfieldnum+5]->val_int();
		if(blocksize<0) blocksize=0;
		if( (fnid!=cur_ref.fnid || cur_ref.blockoffset!=blockstart) && loadBlock(fnid+deffnid,(int)blockstart,(int)blocksize)) {
//		if(loadBlock(fnid,(int)blockstart,(int)blocksize)) {
			info->update= 0;
			error=HA_ERR_END_OF_FILE;
		}
		else {
		  info->next_block=(int)startrow+rownum;
		  pos=info->nextpos=(int)startrow;
		  //info->next_block=row_num_ib;
	          info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
//		  info->current_hash_ptr=0;			/* Can't use read_next */
		  if(checkDM(pos)) {
		  	isdm=true;
		  	continue;
		  }
		  error=getRecord(buf,pos,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
		}
	  }
	}
//	t_table->file->index_end();
//	mysql_unlock_read_tables(current_thd,lock);
//	close_thread_table(current_thd,&t_table);
//	t_table->file->extra(HA_EXTRA_NO_CACHE);
//	t_table->file->extra(HA_EXTRA_RESET);
//	mysql_unlock_tables(current_thd, lock);
  }
  else {
      isdm=false;
      info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
//      info->current_hash_ptr=0;			// Can't use read_next 
      if(checkDM(pos)) {
	isdm=true;
	continue;
      }
      error=getRecord(buf,pos,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
  }
  }
  while(isdm);
/*
  if (pos >= info->next_block)
  {
	  if(loadBlock(-1)) {
		info->update= 0;
		DBUG_RETURN(my_errno= HA_ERR_END_OF_FILE);
	  }
	  info->next_block=row_num_ib;
	  pos=info->current_record=0;
      info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
      info->current_hash_ptr=0;			// Can't use read_next 
      error=getRecord(record,pos,(size_t) share->reclength);
  }
  info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
  info->current_hash_ptr=0;			// Can't use read_next 
  int err=getRecord(record,pos,(size_t) share->reclength);
  lmtrn++;
  //if(!err && lmtrn>1000) 
//	  err=137;
  if(err) return err;

  //memcpy(record,info->current_ptr,(size_t) share->reclength);
  //memcpy(record,info->current_ptr,(size_t) share->reclength);
  */
  if(error)	CheckAndReleaseCache();
  if(error==HA_ERR_END_OF_FILE) error=HA_ERR_KEY_NOT_FOUND;
  DBUG_RETURN(error);
}

int vtsfFile::raw_index_next(MI_INFO *info,byte * buf,INDEX_TAG indextag,const byte *key, key_part_map keypart_map, enum ha_rkey_function find_flag,uint idx)
{
  int error=0;
  MYISAM_SHARE *share=info->s;
  longlong pos;
  DBUG_ENTER("vtsf_index_next");
  bool isdm=false;
  do{
  pos= ++info->nextpos;//current_record;
  if (pos >= info->next_block || indextag==INDEX_FIRST)
  {
  	  if(indextag==INDEX_READ_IDX)
  	    index_init(idx,0);
	  TABLE *t_table = GetTable(0);//tables[curidxoff].table; 
	  if(indextag==INDEX_FIRST && isdm) indextag=INDEX_NEXT;
	  if(indextag==INDEX_LAST && isdm) indextag=INDEX_PREV;
	  isdm=false;
	  //t_table->file->index_init(subcuridxid);
		  if(indextag==INDEX_NEXT)
			  error=t_table->file->index_next(t_table->record[0]);
		  else if(indextag==INDEX_NEXT_SAME)
			  // i use keypart_map as key_len ,test!!!!!
			  error=t_table->file->index_next_same(t_table->record[0],key,(uint) keypart_map);
		  else if(indextag==INDEX_FIRST)
			  error=t_table->file->index_first(t_table->record[0]);
		  else if(indextag==INDEX_LAST)
			  error=t_table->file->index_last(t_table->record[0]);
		  else if(indextag==INDEX_PREV)
			  error=t_table->file->index_prev(t_table->record[0]);
		  else if(indextag==INDEX_READ_IDX)
//		  	  error=t_table->file->index_read_idx_map(t_table->record[0],idx,key,keypart_map,find_flag); 
							  	  error=t_table->file->index_read_idx_map(t_table->record[0],subcuridxid,key,keypart_map,find_flag); 
		  else if(indextag==INDEX_READ_LAST)
		  	  error=t_table->file->index_read_last_map(t_table->record[0],key,keypart_map); 
	  if(!error) {
		//索引表公共字段
		//wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"idx_startrow",NULL,COLUMN_TYPE_INT,10,0);
		//wociAddColumn(idxtarget,"idx_rownum",NULL,COLUMN_TYPE_INT,10,0);
		longlong fnid=t_table->field[idxfieldnum]->val_int()-deffnid;
		longlong blockstart=t_table->field[idxfieldnum+1]->val_int();
		longlong blocksize=t_table->field[idxfieldnum+2]->val_int();
		longlong startrow=t_table->field[idxfieldnum+4]->val_int();
		longlong rownum=t_table->field[idxfieldnum+5]->val_int();

		if( (fnid!=cur_ref.fnid || cur_ref.blockoffset!=blockstart) && loadBlock(fnid+deffnid,(int)blockstart,(int)blocksize)) {
//		if(  loadBlock(fnid,(int)blockstart,(int)blocksize)) {
			info->update= 0;
			error=HA_ERR_END_OF_FILE;
		}
		else {
		  info->next_block=(int)startrow+rownum;
		  pos=info->nextpos=(int)startrow;
	          info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
//		  info->current_hash_ptr=0;			// Can't use read_next /
		  if(checkDM(pos)) {
		  	isdm=true;
		  	continue;
		  }
		  error=getRecord(buf,pos,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
		}
	  }
  }
  else {
       isdm=false;
       info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
//      info->current_hash_ptr=0;			// Can't use read_next 
	  if(checkDM(pos)) {
	  	isdm=true;
	  	continue;
	  }
      error=getRecord(buf,pos,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
  }
  }while(isdm);
  if(error)	CheckAndReleaseCache();
  return error;
}


/*
int vtsfFile::raw_index_next(MI_INFO *info,byte * buf,INDEX_TAG indextag)
{
  int error=0;
  MYISAM_SHARE *share=info->s;
  ulong pos;
  DBUG_ENTER("vtsf_index_next");
  pos= ++info->nextpos;//current_record;
  if (pos >= info->next_block)
  {
	  TABLE *t_table = GetTable(0);//tables[curidxoff].table; 
	  int crn=idxcachemt.GetRows();
	  int fnid;
	  int blockstart;
	  int blocksize;
	  int startrow;
	  int rownum;
	  if(indextag==INDEX_FIRST || indextag==INDEX_LAST) {
		    if(indextag==INDEX_FIRST)
			  error=t_table->file->index_first(t_table->record[0]);
		    else if(indextag==INDEX_LAST)
			  error=t_table->file->index_last(t_table->record[0]);
			if(error) return error;
			fnid=t_table->field[idxfieldnum]->val_int()-deffnid;
			blockstart=t_table->field[idxfieldnum+1]->val_int();
			blocksize=t_table->field[idxfieldnum+2]->val_int();
			startrow=t_table->field[idxfieldnum+4]->val_int();
			rownum=t_table->field[idxfieldnum+5]->val_int();
			idxcachemt.Reset();idxcachep=0;
	  }
	  else {
	   if(idxcachep==crn && crn>0 && crn!=idxcachemaxrn) return HA_ERR_END_OF_FILE;
	   if(idxcachep==crn || crn==0) {
		  //if(crn>0) {
			  idxcachemt.Reset();
		  //}
		  crn=0;
		  idxcachep=0;
		  void *insptr[10];
		  insptr[0]=&fnid;
		  insptr[1]=&blockstart;
		  insptr[2]=&blocksize;
		  insptr[3]=&startrow;
		  insptr[4]=&rownum;
		  insptr[5]=NULL;
		  while(true) {
	        //t_table->file->index_init(subcuridxid);
		    if(indextag==INDEX_NEXT)
			  error=t_table->file->index_next(t_table->record[0]);
		    else if(indextag==INDEX_NEXT_SAME)
			  error=t_table->file->index_next_same(t_table->record[0],NULL,0);
		    else if(indextag==INDEX_PREV)
			  error=t_table->file->index_prev(t_table->record[0]);
			if(error && error!=HA_ERR_END_OF_FILE &&error!=HA_ERR_KEY_NOT_FOUND) 
				return error;
	        if(error) break;
			fnid=t_table->field[idxfieldnum]->val_int()-deffnid;
			blockstart=t_table->field[idxfieldnum+1]->val_int();
			blocksize=t_table->field[idxfieldnum+2]->val_int();
			startrow=t_table->field[idxfieldnum+4]->val_int();
			rownum=t_table->field[idxfieldnum+5]->val_int();
			wociInsertRows(idxcachemt,insptr,NULL,1);
			crn++;
			if(crn==idxcachemaxrn) break;
		  } 
		  wociSetSortColumn(idxcachemt,"fnid,blockstart");
		  wociSortHeap(idxcachemt);
	   }
	   int rawrn=wociGetRawrnBySort(idxcachemt,idxcachep);
	   fnid=*idxcachemt.PtrInt(0,rawrn);
	   blockstart=*idxcachemt.PtrInt(1,rawrn);
	   blocksize=*idxcachemt.PtrInt(2,rawrn);
	   startrow=*idxcachemt.PtrInt(3,rawrn);
	   rownum=*idxcachemt.PtrInt(4,rawrn);
       idxcachep++;
	  }
	  if( (fnid!=cur_ref.fnid || cur_ref.blockoffset!=blockstart) && loadBlock(fnid+deffnid,(int)blockstart,(int)blocksize)) {
			info->update= 0;
			error=HA_ERR_END_OF_FILE;
	  }
	  else {
		  info->next_block=(int)startrow+rownum;
		  pos=info->nextpos=(int)startrow;
	          info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
//		  info->current_hash_ptr=0;			// Can't use read_next 
		  error=getRecord(buf,pos,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
	  }
  }
  else {
      info->update= HA_STATE_PREV_FOUND | HA_STATE_NEXT_FOUND | HA_STATE_AKTIV;
//      info->current_hash_ptr=0;			// Can't use read_next 
      error=getRecord(buf,pos,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
  }
  return error;
}

*/

int vtsfFile::raw_heap_extra(MI_INFO *info, enum ha_extra_function function)
{
  DBUG_ENTER("heap_extra");
  if(curidxoff>=0) {
  	pdtpmap->Extra(curidxoff,function);
  	/*
	TABLE *t_table = tables[curidxoff].table; 
	if(t_table)
		t_table->file->extra(function);
	*/
  }
  switch (function) {
  case HA_EXTRA_RESET_STATE:
    info->lastinx= -1;
    info->nextpos= (ulong) ~0L;
    info->next_block=0;
//    info->current_hash_ptr=0;
    info->update=0;
	  scan_init(info);
    break;
  case HA_EXTRA_NO_READCHECK:
    info->opt_flag&= ~READ_CHECK_USED;	/* No readcheck */
    break;
  case HA_EXTRA_READCHECK:
    info->opt_flag|= READ_CHECK_USED;
    break;
  default:
    break;
  }
  DBUG_RETURN(0);
}

int vtsfFile::raw_index_init(uint idx,bool sorted)
{
  //idxcachemt.Reset();
    //scantm.Clear();
  ldblktms=0;
//5.1中会出现不调用scan_init,和extra(reset),直接使用index_init的形式。
//  这种现象在客户端非索引fetch部分数据后，执行按索引查询时可以发现。
    (*minfo)->nextpos= (ulong) ~0L;
    (*minfo)->next_block=0;

  CheckAndReleaseCache();
	curidxid=idx;
	curidxoff=pdtpmap->GetTableOff(idx);//ptaboff[idx];
	subcuridxid=pdtpmap->GetSubIdx(idx)-1;//psubidxid[idx]-1;
	idxfieldnum=pdtpmap->GetFieldNum(idx);//pidxfieldnum[idx];
	return GetTable(0)->file->ha_index_init(subcuridxid,sorted);//tables[curidxoff].table; 
//if (current_thd->limit_found_rows!=1) return 0;
//	if (current_thd->select_limit!=1) return 0;
//	idxused[curidxoff]=true;
//return 0;
}

int vtsfFile::raw_index_end()
{
	int rt=0;
	//if(lock) {
	//	rt=tables[curidxoff].table->file->ha_index_end();
	//	TABLE *t_table = tables[curidxoff].table;
	//	//mysql_unlock_read_tables(current_thd,lock);
	//	//close_thread_table(current_thd,&t_table);
	//}
	//lock=NULL;
	//tables[curidxoff].table->file->rnd_end();
	raw_rnd_end();
	lkbal--;
//	idxused[curidxoff]=false;	
	return rt;
}
 
int vtsfFile::raw_rrnd(MI_INFO *info, byte *buf, byte *pos)
{
	// no delete mask check as this is the second read record from block.
	MYISAM_SHARE *share=info->s;
	int error=0;
	vtsfref *pref=(vtsfref *)pos;
       if(pref->fnid!=cur_ref.fnid || pref->blockoffset!=cur_ref.blockoffset) {
		error=loadBlock(pref->fnid+deffnid,pref->blockoffset);
		if(error) return error;
		error=getRecord(buf,pref->recordid,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
		return error;
	}
	return getRecord(buf,pref->recordid,info->s->base.pack_reclength);//423);//(size_t) share->reclength);
}


int vtsfFile::raw_init(dtp_map *map) {
	char date[20];
  	wociGetCurDateTime(date);
  	int y,m,d;
        DbplusCert::getInstance()->GetExpiredDate(y,m,d);
        if(y>0 && wociGetYear(date)*100+wociGetMonth(date)>y*100+m) 
  	  ThrowWith("您用的" DBPLUS_STR "版本太老，请更新后使用(Your " DBPLUS_STR " is too old,please update it)!");
	pdtpmap=map; 
	if(pdtpmap->IsFromSourceStream()) 
		file.SetStreamName(pdtpmap->GetSourceStream());
	file.Open(pdtpmap->GetFirstFile(),0,0);
        //file.Reset();
        deffnid=file.GetFnid();
        mysqlrowlen=file.GetMySQLRowLen();
	tabid=map->GetTabID();
	CheckAndReleaseCache();
	return 0;
	}


int ha_myisam::rnd_end()
{
	if(vtsf) 
     return pvtsfFile->rnd_end();
  	return 0;
}

int vtsfFile::raw_rnd_end()
{
	//if(lock)
	{
		//TABLE *t_table = tables[curidxoff].table;
		//t_table->file->ha_index_init(subcuridxid);
		//if(curidxoff>=0 && tables[curidxoff].table)
		// tables[curidxoff].table->file->ha_index_end();
		pdtpmap->IndexEnd(curidxoff);
		//if(lock->lock_count>0 && current_thd)
		// mysql_unlock_read_tables(current_thd,lock);
		//close_thread_table(current_thd,&t_table);
	}
	//lock=NULL;
	//pdtpmap->ResetIndexTable();
//	for(int i=0;i<soleidxnum;i++) 
//	  tables[i].table=NULL;
	//scantm.Stop();
	//char str[300];
	//sprintf(str,"blocktm:%9.4f,scantm:%9.4f.\n",blocktm.GetTime(),scantm.GetTime());
	CheckAndReleaseCache();
//	double blockct=blocktm.GetTime();
	return 0;
}

TABLE * vtsfFile::GetTable(int idx)
{
	TABLE_LIST **tables=pdtpmap->GetTables();
	if(curidxoff<0 || curidxoff>pdtpmap->GetTotIdxNum())//totidxnum)
	  	ThrowWith("GetTable exception: invalid index offset %d(total index num %d).!",curidxoff,pdtpmap->GetTotIdxNum());

	//if(lock!=NULL)  // already locked 
	//	return tables[curidxoff]->table;
	//lkbal++;
	//tables[curidxoff].db=current_thd->db;
	//2004/12/16 修改  table->in_use 在sql/field.cc:1974判断。
	// 表被关闭后，table->in_use=0(sql/sql_base.cc:509),但tables[curidxoff].table未置零
	uint counter=0;
	//TABLE_LIST *table_list=tables[curidxoff];
	if(!tables[curidxoff]->table || !tables[curidxoff]->table->in_use) {
		sql_print_error("Fatal error: Can't open dt index table in 'GetTable': %s",current_thd->net.last_error);
		ThrowWith("open dt index table '%s.%s' failed in 'GetTable'!",tables[curidxoff]->db,tables[curidxoff]->table_name);
	}
	return tables[curidxoff]->table;
	// open_tables(current_thd,&table_list,&counter,MYSQL_LOCK_IGNORE_FLUSH);
	 // Mysql 4.1 and above ,use next line:
	 //open_tables(current_thd,&tables[curidxoff],&opend);
	 /*
	TABLE *t_table = tables[curidxoff].table; 
	if(!t_table) {
		//有可能是flush操作还未完成,5秒后重试 ? 不需要,一定有死锁,例如 ,索引表被查询或者fetch中途为 结束(最早的查询测试程序).
		//mSleep(5000);
	        //open_tables(current_thd,&tables[curidxoff]);
		//t_table = tables[curidxoff].table; 
		//if(!t_table) {
		sql_print_error("Fatal error: Can't open dt index table in 'GetTable': %s",current_thd->net.last_error);
		ThrowWith("open dt index table '%s.%s' failed in 'GetTable'!",tables[curidxoff].schema_select_lex->db,tables[curidxoff].table_name);
            	//}
        }
        t_table->reginfo.lock_type=TL_READ;
	bool need_reopen=false;
	if (!(lock=mysql_lock_tables(current_thd,&t_table,1,MYSQL_LOCK_IGNORE_FLUSH,&need_reopen)))
	{
      sql_print_error("Fatal error: Can't lock dt index table: %s",
			  current_thd->net.last_error);
		  ThrowWith("Lock dt index table '%s' failed!",t_table->s->table_name);
	}
	t_table->file->ha_index_init(subcuridxid,FALSE);
	// Mysql 4.1 and above ,use next line:
	//t_table->file->ha_index_init(subcuridxid);
	return t_table;*/
}

ha_rows vtsfFile::raw_records_in_range(int inx,
			   key_range *min_key,
                                    key_range *max_key)
{	
	if (current_thd->variables.select_limit==1 ) 
		return  HA_POS_ERROR;
	  int dtidxoff=pdtpmap->GetTableOff(inx);//ptaboff[inx];
	  int dtcuridxid=pdtpmap->GetSubIdx(inx)-1;//psubidxid[inx]-1;
	  if(dtidxoff<0 || dtidxoff>pdtpmap->GetTotIdxNum())//totidxnum)
	  	ThrowWith("raw_records_in_range exception: invalid index offset %d(total index num %d,request index %d).!",dtidxoff,pdtpmap->GetTotIdxNum(),inx);
	  //unsigned int opend;
	  //if(!tables[dtidxoff].table)
	  //  open_tables(current_thd,&tables[dtidxoff],&opend);
	  raw_index_init(inx,0);
	  TABLE *t_table = GetTable(0); 
	  //TABLE *t_table = tables[dtidxoff].table; 
	  //if(!t_table) {
	  //  sql_print_error("Fatal error: Can't open dt index table in'raw_records_in_range': %s",current_thd->net.last_error);
	  //  ThrowWith("open dt index table '%s.%s' failed in 'raw_records_in_range'!",tables[dtidxoff].db,tables[dtidxoff].real_name);
	  //}
	  //t_table->reginfo.lock_type=TL_READ;
	  //if(lock) //records_in_range应该在语句的最开始被调用，如果每次执行SQL语句都正常
	           //对DT 索引表解锁，则这里lock应该总是NULL。
	           //在下面的代码中已经对lock解锁，因此，records_in_range本身能够保证lock=NULL.
	  // mysql_unlock_tables(current_thd,lock);
	  //if (!(lock=mysql_lock_tables(current_thd,&t_table,1))) //t_table->next强制为NULL,因此只加锁一个DT索引表
	  //{
          // sql_print_error("Fatal error: Can't lock dt index table: %s",
	  //  current_thd->net.last_error);
	  // ThrowWith("Lock dt index table '%s' failed!",t_table->real_name);
	  //}
	  ha_rows rt= t_table->file->records_in_range(dtcuridxid, min_key,
                                    max_key);
	  raw_rnd_end();
	  rt=rt*1.2*GetMaxRows()/t_table->file->records();
	  //mysql_unlock_tables(current_thd,lock); //被测试的DT索引表不一定被选中
	  //lock=NULL;
	  return rt;
}

//由于DT项目在建立调试版的mysqld是仅仅重新编译　.\sql目录的文件，因此，按照DBUG_OFF编译的其它模块可能与此有冲突。
//hash_check就是一个例子，原先的实现是在mysql\hash.c文件，但编译mysql目录是使用DBUG_OFF，因此，未定义。
//在这里定义只是一个补充，如果mysql\*使用无DBUG_OFF的选线编译过，则下面的代码不能再使用无DBUG_OF的选项编译。
//#ifndef DBUG_OFF
//my_bool hash_check(HASH *hash)
//{
// return 0;
/*  int error;
  uint i,rec_link,found,max_links,seek,links,idx;
  uint records,blength;
  HASH_LINK *data,*hash_info;

  records=hash->records; blength=hash->blength;
  data=dynamic_element(&hash->array,0,HASH_LINK*);
  error=0;

  for (i=found=max_links=seek=0 ; i < records ; i++)
  {
    if (hash_rec_mask(hash,data+i,blength,records) == i)
    {
      found++; seek++; links=1;
      for (idx=data[i].next ;
	   idx != NO_RECORD && found < records + 1;
	   idx=hash_info->next)
      {
	if (idx >= records)
	{
	  DBUG_PRINT("error",
		     ("Found pointer outside array to %d from link starting at %d",
		      idx,i));
	  error=1;
	}
	hash_info=data+idx;
	seek+= ++links;
	if ((rec_link=hash_rec_mask(hash,hash_info,blength,records)) != i)
	{
	  DBUG_PRINT("error",
		     ("Record in wrong link at %d: Start %d  Record: 0x%lx  Record-link %d", idx,i,hash_info->data,rec_link));
	  error=1;
	}
	else
	  found++;
      }
      if (links > max_links) max_links=links;
    }
  }
  if (found != records)
  {
    DBUG_PRINT("error",("Found %ld of %ld records"));
    error=1;
  }
  if (records)
    DBUG_PRINT("info",
	       ("records: %ld   seeks: %d   max links: %d   hitrate: %.2f",
		records,seek,max_links,(float) seek / (float) records));
  return error;*/
//}
//#endif


void dtp_map::attach_child_index_table(struct st_table *parent)
	{
		int i;
		for(i=0;i<soledidxnum;i++) {
			if(!tables[i]) {
				if (!(tables[i]= (TABLE_LIST*) alloc_root(&parent->mem_root,
                                          sizeof(TABLE_LIST))))
  			{
    			/* purecov: begin inspected */
    			DBUG_PRINT("error", ("my_malloc error: %d", my_errno));
    			return;
    			/* purecov: end */
  			}
		  	bzero((char*) tables[i], sizeof(TABLE_LIST));

			  /* Set database (schema) name. */
  			tables[i]->db_length= strlen(idxdbname);
  			tables[i]->db= strmake_root(&parent->mem_root, idxdbname, strlen(idxdbname));
  			/* Set table name. */
  			tables[i]->table_name_length= strlen(idxtbname+i*DTP_TABLENAMELEN);
  			tables[i]->table_name= strmake_root(&parent->mem_root, idxtbname+i*DTP_TABLENAMELEN,
                                    tables[i]->table_name_length);
  			/* Convert to lowercase if required. */
  			if (lower_case_table_names && tables[i]->table_name_length)
    			tables[i]->table_name_length= my_casedn_str(files_charset_info,
                                              tables[i]->table_name);
  			/* Set alias. */
  			tables[i]->alias= tables[i]->table_name;

  			/* Initialize table map to 'undefined'. */
  			tables[i]->init_child_def_version();
				tables[i]->next_local=NULL;//tables+off+1;
				tables[i]->table=NULL;
				tables[i]->next_global=NULL;
			}
		  /* Link TABLE_LIST object into the parent list. */
  	   if (!parent->child_last_l)
  	   {
    		/* Initialize parent->child_last_l when handling first child. */
    		parent->child_last_l= &parent->child_l;
  		 }
  		 *parent->child_last_l= tables[i];
  		 tables[i]->prev_global= parent->child_last_l;
  	 	 parent->child_last_l= &tables[i]->next_global;
  	  }
	}
