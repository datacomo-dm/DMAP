/* io_put.c
 *
 * Copyright (c) 1996-2002 Mike Gleason, NcFTP Software.
 * All rights reserved.
 *
 */

#include "syshdrs.h"
#ifdef PRAGMA_HDRSTOP
#	pragma hdrstop
#endif

#if (defined(WIN32) || defined(_WINDOWS)) && !defined(__CYGWIN__)
#	define ASCII_TRANSLATION 0
#endif

#ifndef ASCII_TRANSLATION
#	define ASCII_TRANSLATION 1
#endif

#ifndef NO_SIGNALS
#	define NO_SIGNALS 1
#endif

#ifndef O_BINARY
	/* Needed for platforms using different EOLN sequence (i.e. DOS) */
#	ifdef _O_BINARY
#		define O_BINARY _O_BINARY
#	else
#		define O_BINARY 0
#	endif
#endif



int
FTPPutOneF(
	const FTPCIPtr cip,
	const char *const file,
	const char *volatile dstfile,
	int xtype,
	const int fdtouse,
	const int appendflag,
	const char *volatile tmppfx,
	const char *volatile tmpsfx,
	const int resumeflag,
	const int deleteflag,
	const FTPConfirmResumeUploadProc resumeProc)
{
	char *buf, *cp;
	const char *cmd;
	const char *odstfile;
	const char *tdstfile;
	size_t bufSize;
	size_t l;
	int tmpResult, result;
	read_return_t nread;
	write_return_t nwrote;
	volatile int fd;
	char dstfile2[512];
#if ASCII_TRANSLATION
	char *src, *srclim, *dst;
	write_size_t ntowrite;
	char inbuf[256], crlf[4];
	int lastch_of_prev_block, lastch_of_cur_block;
#endif
	int fstatrc, statrc;
	longest_int startPoint = 0;
	struct Stat st;
	time_t mdtm;
#if !defined(NO_SIGNALS)
	int sj;
	volatile FTPSigProc osigpipe;
	volatile FTPCIPtr vcip;
	volatile int vfd, vfdtouse;
#endif	/* NO_SIGNALS */
	volatile int vzaction;
	int zaction = kConfirmResumeProcSaidBestGuess;
	int sameAsRemote;
	
	if (cip->buf == NULL) {
		FTPLogError(cip, kDoPerror, "Transfer buffer not allocated.\n");
		cip->errNo = kErrNoBuf;
		return (cip->errNo);
	}

	cip->usingTAR = 0;
	if (fdtouse < 0) {
		fd = Open(file, O_RDONLY|O_BINARY, 0);
		if (fd < 0) {
			FTPLogError(cip, kDoPerror, "Cannot open local file %s for reading.\n", file);
			cip->errNo = kErrOpenFailed;
			return (cip->errNo);
		}
	} else {
		fd = fdtouse;
	}

	fstatrc = Fstat(fd, &st);
	if ((fstatrc == 0) && (S_ISDIR(st.st_mode))) {
		if (fdtouse < 0) {
			(void) close(fd);
		}
		FTPLogError(cip, kDontPerror, "%s is a directory.\n", (file != NULL) ? file : "that");
		cip->errNo = kErrOpenFailed;
		return (cip->errNo);
	}

	/* For Put, we can't recover very well if it turns out restart
	 * didn't work, so check beforehand.
	 */
	if ((resumeflag == kResumeYes) || (resumeProc != kNoFTPConfirmResumeUploadProc)) {
		FTPCheckForRestartModeAvailability(cip); 
	}

	if (fdtouse < 0) {
		AutomaticallyUseASCIIModeDependingOnExtension(cip, dstfile, &xtype);
		mdtm = kModTimeUnknown;
		if ((cip->progress != (FTPProgressMeterProc) 0) || (resumeflag == kResumeYes) || (resumeProc != kNoFTPConfirmResumeUploadProc)) {
			(void) FTPFileSizeAndModificationTime(cip, dstfile, &startPoint, xtype, &mdtm);
		}

		if (appendflag == kAppendYes) {
			zaction = kConfirmResumeProcSaidAppend;
		} else if (
				(cip->hasREST == kCommandNotAvailable) ||
				(xtype != kTypeBinary) ||
				(fstatrc < 0)
		) {
			zaction = kConfirmResumeProcSaidOverwrite;
		} else if (resumeflag == kResumeYes) {
			zaction = kConfirmResumeProcSaidBestGuess;
		} else {
			zaction = kConfirmResumeProcSaidOverwrite;
		}

		statrc = -1;
		if ((mdtm != kModTimeUnknown) || (startPoint != kSizeUnknown)) {
			/* Then we know the file exists.  We will
			 * ask the user what to do, if possible, below.
			 */
			statrc = 0;
		} else if ((resumeProc != kNoFTPConfirmResumeUploadProc) && (cip->hasMDTM != kCommandAvailable) && (cip->hasSIZE != kCommandAvailable)) {
			/* We already checked if the file had a filesize
			 * or timestamp above, but if the server indicated
			 * it did not support querying those directly,
			 * we now need to try to determine if the file
			 * exists in a few other ways.
			 */
			statrc = FTPFileExists2(cip, dstfile, 0, 0, 0, 1, 1);
		}

		sameAsRemote = 0;
		if (
			(resumeProc != kNoFTPConfirmResumeUploadProc) &&
			(statrc == 0)
		) {
			tdstfile = dstfile;
			zaction = (*resumeProc)(cip, file, (longest_int) st.st_size, st.st_mtime, &tdstfile, startPoint, mdtm, &startPoint);
			dstfile = tdstfile;
		}

		if (zaction == kConfirmResumeProcSaidCancel) {
			/* User wants to cancel this file and any
			 * remaining in batch.
			 */
			cip->errNo = kErrUserCanceled;
			return (cip->errNo);
		}

		if (zaction == kConfirmResumeProcSaidBestGuess) {
			if ((mdtm != kModTimeUnknown) && (st.st_mtime > (mdtm + 1))) {
				/* Local file is newer than remote,
				 * overwrite the remote file instead
				 * of trying to resume it.
				 *
				 * Note:  Add one second fudge factor
				 * for Windows' file timestamps being
				 * imprecise to one second.
				 */
				zaction = kConfirmResumeProcSaidOverwrite; 
			} else if ((longest_int) st.st_size == startPoint) {
				/* Already sent file, done. */
				zaction = kConfirmResumeProcSaidSkip; 
				sameAsRemote = 1;
			} else if ((startPoint != kSizeUnknown) && ((longest_int) st.st_size > startPoint)) {
				zaction = kConfirmResumeProcSaidResume; 
			} else {
				zaction = kConfirmResumeProcSaidOverwrite; 
			}
		}

		if (zaction == kConfirmResumeProcSaidSkip) {
			/* Nothing done, but not an error. */
			if (fdtouse < 0) {
				(void) close(fd);
			}
			if (deleteflag == kDeleteYes) {
				if (unlink(file) < 0) {
					cip->errNo = kErrLocalDeleteFailed;
					return (cip->errNo);
				}
			}
			if (sameAsRemote != 0) {
				cip->errNo = kErrRemoteSameAsLocal;
				return (cip->errNo);
			}
			return (kNoErr);
		} else if (zaction == kConfirmResumeProcSaidResume) {
			/* Resume; proc set the startPoint. */
			if ((longest_int) st.st_size == startPoint) {
				/* Already sent file, done. */
				if (fdtouse < 0) {
					(void) close(fd);
				}

				if (deleteflag == kDeleteYes) {
					if (unlink(file) < 0) {
						cip->errNo = kErrLocalDeleteFailed;
						return (cip->errNo);
					}
				}
				return (kNoErr);
			} else if (Lseek(fd, (off_t) startPoint, SEEK_SET) != (off_t) -1) {
				cip->startPoint = startPoint;
			}
		} else if (zaction == kConfirmResumeProcSaidAppend) {
			/* append: leave startPoint at zero, we will append everything. */
			cip->startPoint = startPoint = 0;
		} else /* if (zaction == kConfirmResumeProcSaidOverwrite) */ {
			/* overwrite: leave startPoint at zero */
			cip->startPoint = startPoint = 0;
		}
	} else if (appendflag == kAppendYes) {
		zaction = kConfirmResumeProcSaidAppend;
	}

	FTPSetUploadSocketBufferSize(cip);

#ifdef NO_SIGNALS
	vzaction = zaction;
#else	/* NO_SIGNALS */
	vcip = cip;
	vfdtouse = fdtouse;
	vfd = fd;
	vzaction = zaction;
	osigpipe = (volatile FTPSigProc) signal(SIGPIPE, BrokenData);

	gGotBrokenData = 0;
	gCanBrokenDataJmp = 0;

#ifdef HAVE_SIGSETJMP
	sj = sigsetjmp(gBrokenDataJmp, 1);
#else
	sj = setjmp(gBrokenDataJmp);
#endif	/* HAVE_SIGSETJMP */

	if (sj != 0) {
		(void) signal(SIGPIPE, (FTPSigProc) osigpipe);
		if (vfdtouse < 0) {
			(void) close(vfd);
		}
		FTPShutdownHost(vcip);
		vcip->errNo = kErrRemoteHostClosedConnection;
		return(vcip->errNo);
	}
	gCanBrokenDataJmp = 1;
#endif	/* NO_SIGNALS */

	if (vzaction == kConfirmResumeProcSaidAppend) {
		cmd = "APPE";
		tmppfx = "";	/* Can't use that here. */
		tmpsfx = "";
	} else {
		cmd = "STOR";
		if (tmppfx == NULL)
			tmppfx = "";
		if (tmpsfx == NULL)
			tmpsfx = "";
	}

	odstfile = dstfile;
	if ((tmppfx[0] != '\0') || (tmpsfx[0] != '\0')) {
		cp = strrchr(dstfile, '/');
		if (cp == NULL)
			cp = strrchr(dstfile, '\\');
		if (cp == NULL) {
			(void) STRNCPY(dstfile2, tmppfx);
			(void) STRNCAT(dstfile2, dstfile);
			(void) STRNCAT(dstfile2, tmpsfx);
		} else {
			cp++;
			l = (size_t) (cp - dstfile);
			(void) STRNCPY(dstfile2, dstfile);
			dstfile2[l] = '\0';	/* Nuke stuff after / */
			(void) STRNCAT(dstfile2, tmppfx);
			(void) STRNCAT(dstfile2, cp);
			(void) STRNCAT(dstfile2, tmpsfx);
		}
		dstfile = dstfile2;
	}

	tmpResult = FTPStartDataCmd(
		cip,
		kNetWriting,
		xtype,
		startPoint,
		"%s %s",
		cmd,
		dstfile
	);

	if (tmpResult < 0) {
		cip->errNo = tmpResult;
		if (fdtouse < 0) {
			(void) close(fd);
		}
#if !defined(NO_SIGNALS)
		(void) signal(SIGPIPE, (FTPSigProc) osigpipe);
#endif	/* NO_SIGNALS */
		return (cip->errNo);
	}

	if ((startPoint != 0) && (cip->startPoint == 0)) {
		/* Remote could not or would not set the start offset
		 * to what we wanted.
		 *
		 * So now we have to undo our seek.
		 */
		if (Lseek(fd, (off_t) 0, SEEK_SET) != (off_t) 0) {
			cip->errNo = kErrLseekFailed;
			if (fdtouse < 0) {
				(void) close(fd);
			}
#if !defined(NO_SIGNALS)
			(void) signal(SIGPIPE, (FTPSigProc) osigpipe);
#endif	/* NO_SIGNALS */
			return (cip->errNo);
		}
		startPoint = 0;
	}

	result = kNoErr;
	buf = cip->buf;
	bufSize = cip->bufSize;

	FTPInitIOTimer(cip);
	if ((fstatrc == 0) && (S_ISREG(st.st_mode) != 0)) {
		cip->expectedSize = (longest_int) st.st_size;
		cip->mdtm = st.st_mtime;
	}
	cip->lname = file;	/* could be NULL */
	cip->rname = odstfile;
	if (fdtouse >= 0)
		cip->useProgressMeter = 0;
	FTPStartIOTimer(cip);

	/* Note: On Windows, we don't have to do anything special
	 * for ASCII mode, since Net ASCII's end-of-line sequence
	 * corresponds to the same thing used for DOS/Windows.
	 */

#if ASCII_TRANSLATION
	if (xtype == kTypeAscii) {
		/* ascii */
		lastch_of_prev_block = 0;
		for (;;) {
#if !defined(NO_SIGNALS)
			gCanBrokenDataJmp = 0;
#endif	/* NO_SIGNALS */
			nread = read(fd, inbuf, (read_size_t) sizeof(inbuf));
			if (nread < 0) {
				if (errno == EINTR) {
					continue;
				} else {
					result = kErrReadFailed;
					cip->errNo = kErrReadFailed;
					FTPLogError(cip, kDoPerror, "Local read failed.\n");
				}
				break;
			} else if (nread == 0) {
				break;
			}
			cip->bytesTransferred += (longest_int) nread;

#if !defined(NO_SIGNALS)
			gCanBrokenDataJmp = 1;
#endif	/* NO_SIGNALS */
			src = inbuf;
			srclim = src + nread;
			lastch_of_cur_block = srclim[-1];
			if (lastch_of_cur_block == '\r') {
				srclim[-1] = '\0';
				srclim--;
				nread--;
				if (nread == 0) {
					lastch_of_prev_block = lastch_of_cur_block;
					break;
				}
			}
			dst = cip->buf;		/* must be 2x sizeof inbuf or more. */

			if (*src == '\n') {
				src++;
				*dst++ = '\r';
				*dst++ = '\n';
			} else if (lastch_of_prev_block == '\r') {
				/* Raw CR at end of last block,
				 * no LF at the start of this block.
				 */
				*dst++ = '\r';
				*dst++ = '\n';
			}

			/* Prepare the buffer, converting end-of-lines
			 * to CR+LF format as required by protocol.
			 */
			while (src < srclim) {
				if (*src == '\r') {
					if (src[1] == '\n') {
						/* CR+LF pair */
						*dst++ = *src++;
						*dst++ = *src++;
					} else {
						/* raw CR */
						*dst++ = *src++;
						*dst++ = '\n';
					}
				} else if (*src == '\n') {
					/* LF only; expected for UNIX text. */
					*dst++ = '\r';
					*dst++ = *src++;
				} else {
					*dst++ = *src++;
				}
			}
			lastch_of_prev_block = lastch_of_cur_block;

			ntowrite = (write_size_t) (dst - cip->buf);
			cp = cip->buf;

#if !defined(NO_SIGNALS)
			if (cip->xferTimeout > 0)
				(void) alarm(cip->xferTimeout);
#endif	/* NO_SIGNALS */
			do {
				if (! WaitForRemoteOutput(cip)) {	/* could set cancelXfer */
					cip->errNo = result = kErrDataTimedOut;
					FTPLogError(cip, kDontPerror, "Remote write timed out.\n");
					goto brk;
				}
				if (cip->cancelXfer > 0) {
					FTPAbortDataTransfer(cip);
					result = cip->errNo = kErrDataTransferAborted;
					goto brk;
				}

#ifdef NO_SIGNALS
				nwrote = (write_return_t) SWrite(cip->dataSocket, cp, (size_t) ntowrite, (int) cip->xferTimeout, kNoFirstSelect);
				if (nwrote < 0) {
					if (nwrote == kTimeoutErr) {
						cip->errNo = result = kErrDataTimedOut;
						FTPLogError(cip, kDontPerror, "Remote write timed out.\n");
					} else if (errno == EPIPE) {
						cip->errNo = result = kErrSocketWriteFailed;
						errno = EPIPE;
						FTPLogError(cip, kDoPerror, "Lost data connection to remote host.\n");
					} else if (errno == EINTR) {
						continue;
					} else {
						cip->errNo = result = kErrSocketWriteFailed;
						FTPLogError(cip, kDoPerror, "Remote write failed.\n");
					}
					(void) shutdown(cip->dataSocket, 2);
					goto brk;
				}
#else	/* NO_SIGNALS */
				nwrote = write(cip->dataSocket, cp, ntowrite);
				if (nwrote < 0) {
					if ((gGotBrokenData != 0) || (errno == EPIPE)) {
						cip->errNo = result = kErrSocketWriteFailed;
						errno = EPIPE;
						FTPLogError(cip, kDoPerror, "Lost data connection to remote host.\n");
					} else if (errno == EINTR) {
						continue;
					} else {
						cip->errNo = result = kErrSocketWriteFailed;
						FTPLogError(cip, kDoPerror, "Remote write failed.\n");
					}
					(void) shutdown(cip->dataSocket, 2);
					goto brk;
				}
#endif	/* NO_SIGNALS */
				cp += nwrote;
				ntowrite -= (write_size_t) nwrote;
			} while (ntowrite != 0);
			FTPUpdateIOTimer(cip);
		}

		if (lastch_of_prev_block == '\r') {
			/* Very rare, but if the file's last byte is a raw CR
			 * we need to write out one more line since we
			 * skipped it earlier.
			 */
			crlf[0] = '\r';
			crlf[1] = '\n';
			crlf[2] = '\0';
			cp = crlf;
			ntowrite = 2;

			do {
				if (! WaitForRemoteOutput(cip)) {	/* could set cancelXfer */
					cip->errNo = result = kErrDataTimedOut;
					FTPLogError(cip, kDontPerror, "Remote write timed out.\n");
					goto brk;
				}
				if (cip->cancelXfer > 0) {
					FTPAbortDataTransfer(cip);
					result = cip->errNo = kErrDataTransferAborted;
					goto brk;
				}

#ifdef NO_SIGNALS
				nwrote = (write_return_t) SWrite(cip->dataSocket, cp, (size_t) ntowrite, (int) cip->xferTimeout, kNoFirstSelect);
				if (nwrote < 0) {
					if (nwrote == kTimeoutErr) {
						cip->errNo = result = kErrDataTimedOut;
						FTPLogError(cip, kDontPerror, "Remote write timed out.\n");
					} else if (errno == EPIPE) {
						cip->errNo = result = kErrSocketWriteFailed;
						errno = EPIPE;
						FTPLogError(cip, kDoPerror, "Lost data connection to remote host.\n");
					} else if (errno == EINTR) {
						continue;
					} else {
						cip->errNo = result = kErrSocketWriteFailed;
						FTPLogError(cip, kDoPerror, "Remote write failed.\n");
					}
					(void) shutdown(cip->dataSocket, 2);
					goto brk;
				}
#else	/* NO_SIGNALS */
				nwrote = write(cip->dataSocket, cp, ntowrite);
				if (nwrote < 0) {
					if ((gGotBrokenData != 0) || (errno == EPIPE)) {
						cip->errNo = result = kErrSocketWriteFailed;
						errno = EPIPE;
						FTPLogError(cip, kDoPerror, "Lost data connection to remote host.\n");
					} else if (errno == EINTR) {
						continue;
					} else {
						cip->errNo = result = kErrSocketWriteFailed;
						FTPLogError(cip, kDoPerror, "Remote write failed.\n");
					}
					(void) shutdown(cip->dataSocket, 2);
					goto brk;
				}
#endif	/* NO_SIGNALS */
				cp += nwrote;
				ntowrite -= (write_size_t) nwrote;
			} while (ntowrite != 0);
			FTPUpdateIOTimer(cip);
		}
	} else
#endif	/* ASCII_TRANSLATION */
	{
		/* binary */
		for (;;) {
#if !defined(NO_SIGNALS)
			gCanBrokenDataJmp = 0;
#endif	/* NO_SIGNALS */
			cp = buf;
			nread = read(fd, cp, (read_size_t) bufSize);
			if (nread < 0) {
				if (errno == EINTR) {
					continue;
				} else {
					result = kErrReadFailed;
					cip->errNo = kErrReadFailed;
					FTPLogError(cip, kDoPerror, "Local read failed.\n");
				}
				break;
			} else if (nread == 0) {
				break;
			}
			cip->bytesTransferred += (longest_int) nread;

#if !defined(NO_SIGNALS)
			gCanBrokenDataJmp = 1;
			if (cip->xferTimeout > 0)
				(void) alarm(cip->xferTimeout);
#endif	/* NO_SIGNALS */
			do {
				if (! WaitForRemoteOutput(cip)) {	/* could set cancelXfer */
					cip->errNo = result = kErrDataTimedOut;
					FTPLogError(cip, kDontPerror, "Remote write timed out.\n");
					goto brk;
				}
				if (cip->cancelXfer > 0) {
					FTPAbortDataTransfer(cip);
					result = cip->errNo = kErrDataTransferAborted;
					goto brk;
				}

#ifdef NO_SIGNALS
				nwrote = (write_return_t) SWrite(cip->dataSocket, cp, (size_t) nread, (int) cip->xferTimeout, kNoFirstSelect);
				if (nwrote < 0) {
					if (nwrote == kTimeoutErr) {
						cip->errNo = result = kErrDataTimedOut;
						FTPLogError(cip, kDontPerror, "Remote write timed out.\n");
					} else if (errno == EPIPE) {
						cip->errNo = result = kErrSocketWriteFailed;
						errno = EPIPE;
						FTPLogError(cip, kDoPerror, "Lost data connection to remote host.\n");
					} else if (errno == EINTR) {
						continue;
					} else {
						cip->errNo = result = kErrSocketWriteFailed;
						FTPLogError(cip, kDoPerror, "Remote write failed.\n");
					}
					(void) shutdown(cip->dataSocket, 2);
					cip->dataSocket = -1;
					goto brk;
				}
#else	/* NO_SIGNALS */
				nwrote = write(cip->dataSocket, cp, (write_size_t) nread);
				if (nwrote < 0) {
					if ((gGotBrokenData != 0) || (errno == EPIPE)) {
						cip->errNo = result = kErrSocketWriteFailed;
						errno = EPIPE;
						FTPLogError(cip, kDoPerror, "Lost data connection to remote host.\n");
					} else if (errno == EINTR) {
						continue;
					} else {
						cip->errNo = result = kErrSocketWriteFailed;
						FTPLogError(cip, kDoPerror, "Remote write failed.\n");
					}
					(void) shutdown(cip->dataSocket, 2);
					cip->dataSocket = -1;
					goto brk;
				}
#endif	/* NO_SIGNALS */
				cp += nwrote;
				nread -= nwrote;
			} while (nread > 0);
			FTPUpdateIOTimer(cip);
		}
	}
brk:

	/* This looks very bizarre, since
	 * we will be checking the socket
	 * for readability here!
	 *
	 * The reason for this is that we
	 * want to be able to timeout a
	 * small put.  So, we close the
	 * write end of the socket first,
	 * which tells the server we're
	 * done writing.  We then wait
	 * for the server to close down
	 * the whole socket (we know this
	 * when the socket is ready for
	 * reading an EOF), which tells
	 * us that the file was completed.
	 */
	(void) shutdown(cip->dataSocket, 1);
	(void) WaitForRemoteInput(cip);

#if !defined(NO_SIGNALS)
	gCanBrokenDataJmp = 0;
	if (cip->xferTimeout > 0)
		(void) alarm(0);
#endif	/* NO_SIGNALS */
	tmpResult = FTPEndDataCmd(cip, 1);
	if ((tmpResult < 0) && (result == kNoErr)) {
		cip->errNo = result = kErrSTORFailed;
	}
	FTPStopIOTimer(cip);

	if (fdtouse < 0) {
		/* If they gave us a descriptor (fdtouse >= 0),
		 * leave it open, otherwise we opened it, so
		 * we need to dispose of it.
		 */
		(void) Fstat(fd, &st);
		(void) close(fd);
		fd = -1;
	}

	if (result == kNoErr) {
		/* The store succeeded;  If we were
		 * uploading to a temporary file,
		 * move the new file to the new name.
		 */
		cip->numUploads++;

		if ((tmppfx[0] != '\0') || (tmpsfx[0] != '\0')) {
			if ((result = FTPRename(cip, dstfile, odstfile)) < 0) {
				/* May fail if file was already there,
				 * so delete the old one so we can move
				 * over it.
				 */
				if (FTPDelete(cip, odstfile, kRecursiveNo, kGlobNo) == kNoErr) {
					result = FTPRename(cip, dstfile, odstfile);
					if (result < 0) {
						FTPLogError(cip, kDontPerror, "Could not rename %s to %s: %s.\n", dstfile, odstfile, FTPStrError(cip->errNo));
					}
				} else {
					FTPLogError(cip, kDontPerror, "Could not delete old %s, so could not rename %s to that: %s\n", odstfile, dstfile, FTPStrError(cip->errNo));
				}
			}
		}

		if (FTPUtime(cip, odstfile, st.st_atime, st.st_mtime, st.st_ctime) != kNoErr) {
			if (cip->errNo != kErrUTIMENotAvailable)
				FTPLogError(cip, kDontPerror, "Could not preserve times for %s: %s.\n", odstfile, FTPStrError(cip->errNo));
		}

		if ((result == kNoErr) && (deleteflag == kDeleteYes)) {
			if (unlink(file) < 0) {
				result = cip->errNo = kErrLocalDeleteFailed;
			}
		}
	}

#if !defined(NO_SIGNALS)
	(void) signal(SIGPIPE, (FTPSigProc) osigpipe);
#endif	/* NO_SIGNALS */
	return (result);
}	/* FTPPutOneF */
