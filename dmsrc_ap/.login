#
# *****************************************************************
# *                                                               *
# *    Copyright Compaq Computer Corporation, 2000                *
# *                                                               *
# *   The software contained on this media  is  proprietary  to   *
# *   and  embodies  the  confidential  technology  of  Compaq    *
# *   Computer Corporation.  Possession, use,  duplication  or    *
# *   dissemination of the software and media is authorized only  *
# *   pursuant to a valid written license from Compaq Computer    *
# *   Corporation.                                                *
# *                                                               *
# *   RESTRICTED RIGHTS LEGEND   Use, duplication, or disclosure  *
# *   by the U.S. Government is subject to restrictions  as  set  *
# *   forth in Subparagraph (c)(1)(ii)  of  DFARS  252.227-7013,  *
# *   or  in  FAR 52.227-19, as applicable.                       *
# *                                                               *
# *****************************************************************
#
#
# HISTORY
#
# @(#)$RCSfile: .login,v $ $Revision: 1.6 $ (DEC) $Date: 2006/10/11 13:10:29 $
#
if ($?path) then
    set path=($HOME/bin $path)
else
    set path=($HOME/bin /usr/bin .)
endif
if ( ! ${?DT} ) then
	stty dec new
	tset -I -Q
endif
set prompt="`hostname`> "
set mail=/usr/spool/mail/$USER
