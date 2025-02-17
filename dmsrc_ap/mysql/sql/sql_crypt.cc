/* Copyright (C) 2000-2001, 2003, 2005 MySQL AB

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



/*
 Functions to handle the encode() and decode() functions
 The strongness of this crypt is large based on how good the random
 generator is.	It should be ok for short strings, but for communication one
 needs something like 'ssh'.
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation				// gcc: Class implementation
#endif

#include "mysql_priv.h"

SQL_CRYPT::SQL_CRYPT(const char *password)
{
  ulong rand_nr[2];
  hash_password(rand_nr,password, strlen(password));
  crypt_init(rand_nr);
}

void SQL_CRYPT::crypt_init(ulong *rand_nr)
{
  uint i;
  randominit(&rand,rand_nr[0],rand_nr[1]);

  for (i=0 ; i<=255; i++)
   decode_buff[i]= (char) i;

  for (i=0 ; i<= 255 ; i++)
  {
    int idx= (uint) (my_rnd(&rand)*255.0);
    char a= decode_buff[idx];
    decode_buff[idx]= decode_buff[i];
    decode_buff[+i]=a;
  }
  for (i=0 ; i <= 255 ; i++)
   encode_buff[(uchar) decode_buff[i]]=i;
  org_rand=rand;
  shift=0;
}


void SQL_CRYPT::encode(char *str,uint length)
{
  for (uint i=0; i < length; i++)
  {
    shift^=(uint) (my_rnd(&rand)*255.0);
    uint idx= (uint) (uchar) str[0];
    *str++ = (char) ((uchar) encode_buff[idx] ^ shift);
    shift^= idx;
  }
}


void SQL_CRYPT::decode(char *str,uint length)
{
  for (uint i=0; i < length; i++)
  {
    shift^=(uint) (my_rnd(&rand)*255.0);
    uint idx= (uint) ((uchar) str[0] ^ shift);
    *str = decode_buff[idx];
    shift^= (uint) (uchar) *str++;
  }
}
