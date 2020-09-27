/* c_size.c
 *
 * Copyright (c) 2002 Mike Gleason, NcFTP Software.
 * All rights reserved.
 *
 */

#include "syshdrs.h"
#ifdef PRAGMA_HDRSTOP
#	pragma hdrstop
#endif

/* If the remote host supports the SIZE command, we can find out the exact
 * size of a remote file, depending on the transfer type in use.  SIZE
 * returns different values for ascii and binary modes!
 */
int
FTPFileSize(const FTPCIPtr cip, const char *const file, longest_int *const size, const int type)
{
	int result;
	ResponsePtr rp;

	if (cip == NULL)
		return (kErrBadParameter);
	if (strcmp(cip->magic, kLibraryMagic))
		return (kErrBadMagic);

	if ((size == NULL) || (file == NULL))
		return (kErrBadParameter);
	*size = kSizeUnknown;

	result = FTPSetTransferType(cip, type);
	if (result < 0)
		return (result);

	if (cip->hasSIZE == kCommandNotAvailable) {
		cip->errNo = kErrSIZENotAvailable;
		result = kErrSIZENotAvailable;
	} else {
		rp = InitResponse();
		if (rp == NULL) {
			result = kErrMallocFailed;
			cip->errNo = kErrMallocFailed;
			FTPLogError(cip, kDontPerror, "Malloc failed.\n");
		} else {
			result = RCmd(cip, rp, "SIZE %s", file);
			if (result < 0) {
				DoneWithResponse(cip, rp);
				return (result);
			} else if (result == 2) {
#if defined(HAVE_LONG_LONG) && defined(SCANF_LONG_LONG)
				(void) sscanf(rp->msg.first->line, SCANF_LONG_LONG, size);
#elif defined(HAVE_LONG_LONG) && defined(HAVE_STRTOQ)
				*size = (longest_int) strtoq(rp->msg.first->line, NULL, 0);
#else
				(void) sscanf(rp->msg.first->line, "%ld", size);
#endif
				cip->hasSIZE = kCommandAvailable;
				result = kNoErr;
			} else if (UNIMPLEMENTED_CMD(rp->code)) {
				cip->hasSIZE = kCommandNotAvailable;
				cip->errNo = kErrSIZENotAvailable;
				result = kErrSIZENotAvailable;
			} else {
				cip->errNo = kErrSIZEFailed;
				result = kErrSIZEFailed;
			}
			DoneWithResponse(cip, rp);
		}
	}
	return (result);
}	/* FTPFileSize */
