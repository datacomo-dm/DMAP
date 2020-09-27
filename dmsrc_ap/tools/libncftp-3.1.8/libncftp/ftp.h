/* FTP.h
 *
 * Copyright (c) 1996-2004 Mike Gleason, NcFTP Software.
 * All rights reserved.
 *
 */

/* FTP.c */
int OpenControlConnection(const FTPCIPtr cip, char *host, unsigned int port);
void CloseDataConnection(const FTPCIPtr cip);
int SetStartOffset(const FTPCIPtr cip, longest_int restartPt);
int OpenDataConnection(const FTPCIPtr cip, int mode);
int AcceptDataConnection(const FTPCIPtr cip);
void HangupOnServer(const FTPCIPtr cip);
void SendTelnetInterrupt(const FTPCIPtr cip);
