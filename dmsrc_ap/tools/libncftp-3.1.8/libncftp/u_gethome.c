/* u_gethome.c
 *
 * Copyright (c) 2002 Mike Gleason, NcFTP Software.
 * All rights reserved.
 *
 */

#include "syshdrs.h"
#ifdef PRAGMA_HDRSTOP
#	pragma hdrstop
#endif

#if (defined(WIN32) || defined(_WINDOWS)) && !defined(__CYGWIN__)
extern void GetSpecialDir(char *dst, size_t size, int whichDir);
#endif

void
GetHomeDir(char *dst, size_t size)
{
#if (defined(WIN32) || defined(_WINDOWS)) && !defined(__CYGWIN__)
	const char *homedrive, *homepath;
	
	homepath = getenv("USERPROFILE");	/* Windows XP */
	if (homepath != NULL) {
		(void) Strncpy(dst, homepath, size);
		return;
	}

	homedrive = getenv("HOMEDRIVE");
	homepath = getenv("HOMEPATH");
	if ((homedrive != NULL) && (homepath != NULL)) {
		(void) Strncpy(dst, homedrive, size);
		(void) Strncat(dst, homepath, size);
		return;
	}

	GetSpecialDir(dst, size, CSIDL_PERSONAL	/* "My Documents" */);
	if (dst[0] != '\0')
		return;

	dst[0] = '\0';
	if (GetWindowsDirectory(dst, size - 1) < 1)
		(void) Strncpy(dst, ".", size);
	else if (dst[1] == ':') {
		dst[2] = '\\';
		dst[3] = '\0';
	}
#else
	struct passwd pw;
	char pwbuf[256];

	if (GetMyPwEnt(&pw, pwbuf, sizeof(pwbuf)) == 0) {
		(void) Strncpy(dst, pw.pw_dir, size);
	} else {
		(void) Strncpy(dst, ".", size);
	}
#endif
}	/* GetHomeDir */
