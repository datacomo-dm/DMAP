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
# HISTORY
# 
# @(#)$RCSfile: .profile,v $ $Revision: 1.6 $ (DEC) $Date: 2006/10/11 13:10:29 $ 
# 
# 
if [ ! "$DT" ]; then
	stty dec
	tset -I -Q
fi
PS1="`hostname`_WGSH> "
MAIL=/usr/spool/mail/$USER
echo $PATH | /bin/grep -q "$HOME/bin" ||
{
	PATH=$HOME/bin:${PATH:-/usr/bin:.}
	export PATH
}

set -o vi
set -o emacs
. login.com
