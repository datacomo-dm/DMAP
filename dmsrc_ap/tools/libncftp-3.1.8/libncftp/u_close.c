/* u_close.c
 *
 * Copyright (c) 2002 Mike Gleason, NcFTP Software.
 * All rights reserved.
 *
 */

#include "syshdrs.h"
#ifdef PRAGMA_HDRSTOP
#	pragma hdrstop
#endif

/* Closes the file supplied, if it isn't a std stream. */
void
CloseFile(FILE **f)
{
	if (*f != NULL) {
		if ((*f != stdout) && (*f != stdin) && (*f != stderr))
			(void) fclose(*f);
		*f = NULL;
	}
}	/* CloseFile */
