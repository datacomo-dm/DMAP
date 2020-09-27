/* simpleget.c */

#ifdef HAVE_CONFIG_H
#	include <config.h>
#endif

#include <ncftp.h>				/* Library header. */

main_void_return_t
main(int argc, char **argv)
{
	int result;
	int es;
	FTPConnectionInfo connInfo;
	FTPLibraryInfo libInfo;

	InitWinsock();	/* Calls WSAStartup() on Windows; No-Op on UNIX */
	if (argc < 3) {
		fprintf(stderr, "Usage:   simpleget <host> <pathname>\n");
		fprintf(stderr, "Example: simpleget ftp.freebsd.org /pub/FreeBSD/README.TXT\n");
		DisposeWinsock();
		exit(5);
	}

	result = FTPInitLibrary(&libInfo);
	if (result < 0) {
		fprintf(stderr, "simpleget: init library error %d (%s).\n", result, FTPStrError(result));
		DisposeWinsock();
		exit(4);
	}

	result = FTPInitConnectionInfo(&libInfo, &connInfo, kDefaultFTPBufSize);
	if (result < 0) {
		fprintf(stderr, "simpleget: init connection info error %d (%s).\n", result, FTPStrError(result));
		DisposeWinsock();
		exit(3);
	}

	strncpy(connInfo.host, argv[1], sizeof(connInfo.host) - 1);
	strncpy(connInfo.user, "anonymous", sizeof(connInfo.user) - 1);
	connInfo.debugLog = stdout;	/* Print the whole FTP conversation. */

	if ((result = FTPOpenHost(&connInfo)) < 0) {
		FTPPerror(&connInfo, result, 0, "Could not open", connInfo.host);
		es = 2;
	} else {
		/* At this point, we should close the host when
		 * were are finished.
		 */
		result = FTPGetFiles3(
				&connInfo,
				argv[2],
				".",
				kRecursiveNo,
				kGlobYes,
				kTypeBinary,
				kResumeYes,
				kAppendNo,
				kDeleteNo,
				kTarNo,
				kNoFTPConfirmResumeDownloadProc,
				0
			);

		if (result < 0) {
			FTPPerror(&connInfo, result, kErrCouldNotStartDataTransfer, "Could not get", argv[2]);
			es = 1;
		} else {
			es = 0;
		}

		FTPCloseHost(&connInfo);
	}

	DisposeWinsock();	/* Calls WSACleanup() on Windows; No-Op on UNIX */
	exit(es);
}	/* main */
