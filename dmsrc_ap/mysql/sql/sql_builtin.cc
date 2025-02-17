/* Copyright (C) 2006 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include <mysql/plugin.h>

typedef struct st_mysql_plugin builtin_plugin[];

extern builtin_plugin 
  builtin_binlog_plugin, builtin_partition_plugin, builtin_csv_plugin, builtin_federated_plugin, builtin_heap_plugin, builtin_innobase_plugin, builtin_myisam_plugin, builtin_myisammrg_plugin;

struct st_mysql_plugin *mysqld_builtins[]=
{
  builtin_binlog_plugin, builtin_partition_plugin, builtin_csv_plugin, builtin_federated_plugin, builtin_heap_plugin, builtin_innobase_plugin, builtin_myisam_plugin, builtin_myisammrg_plugin,(struct st_mysql_plugin *)0
};

