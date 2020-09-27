# Microsoft Developer Studio Project File - Name="wdbi" - Package Owner=<4>
# Microsoft Developer Studio Generated Build File, Format Version 6.00
# ** DO NOT EDIT **

# TARGTYPE "Win32 (x86) Dynamic-Link Library" 0x0102

CFG=wdbi - Win32 Mem_Debug
!MESSAGE This is not a valid makefile. To build this project using NMAKE,
!MESSAGE use the Export Makefile command and run
!MESSAGE 
!MESSAGE NMAKE /f "wdbi.mak".
!MESSAGE 
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "wdbi.mak" CFG="wdbi - Win32 Mem_Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "wdbi - Win32 Release" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "wdbi - Win32 Debug" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "wdbi - Win32 ODBC_Release" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "wdbi - Win32 ODBC_Debug" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "wdbi - Win32 Mem_Release" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "wdbi - Win32 Mem_Debug" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE 

# Begin Project
# PROP AllowPerConfigDependencies 0
# PROP Scc_ProjName ""
# PROP Scc_LocalPath ""
CPP=cl.exe
MTL=midl.exe
RSC=rc.exe

!IF  "$(CFG)" == "wdbi - Win32 Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "Release"
# PROP BASE Intermediate_Dir "Release"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "Release"
# PROP Intermediate_Dir "Release"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MT /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /Yu"stdafx.h" /FD /c
# ADD CPP /nologo /MD /W3 /GX /O2 /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /FD /c
# SUBTRACT CPP /YX /Yc /Yu
# ADD BASE MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x804 /d "NDEBUG"
# ADD RSC /l 0x804 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /machine:I386
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib oci.lib ociw32.lib /nologo /dll /machine:I386 /out:"../bin/wdbi.dll" /implib:"../lib/wdbi.lib" /libpath:"c:\oracle\ora92\oci\lib\msvc"
# SUBTRACT LINK32 /pdb:none

!ELSEIF  "$(CFG)" == "wdbi - Win32 Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "Debug"
# PROP BASE Intermediate_Dir "Debug"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir "Debug"
# PROP Intermediate_Dir "Debug"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MTd /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /Yu"stdafx.h" /FD /GZ /c
# ADD CPP /nologo /MTd /Gm /GX /ZI /Od /I "e:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /FR /FD /GZ /c
# SUBTRACT CPP /YX /Yc /Yu
# ADD BASE MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x804 /d "_DEBUG"
# ADD RSC /l 0x804 /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /debug /machine:I386 /pdbtype:sept
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib oci.lib ociw32.lib /nologo /dll /pdb:"../lib/wdbid.pdb" /debug /machine:I386 /out:"../bin/wdbid.dll" /implib:"../lib/wdbid.lib" /pdbtype:sept /libpath:"e:\oracle\ora92\oci\lib\msvc"
# SUBTRACT LINK32 /pdb:none

!ELSEIF  "$(CFG)" == "wdbi - Win32 ODBC_Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "wdbi___Win32_ODBC_Release"
# PROP BASE Intermediate_Dir "wdbi___Win32_ODBC_Release"
# PROP BASE Ignore_Export_Lib 0
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "wdbi___Win32_ODBC_Release"
# PROP Intermediate_Dir "wdbi___Win32_ODBC_Release"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MD /W3 /GX /O2 /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /FD /c
# SUBTRACT BASE CPP /YX /Yc /Yu
# ADD CPP /nologo /MD /W3 /GX /O2 /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /D "NO_OCI" /FD /c
# SUBTRACT CPP /YX /Yc /Yu
# ADD BASE MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x804 /d "NDEBUG"
# ADD RSC /l 0x804 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib oci.lib ociw32.lib /nologo /dll /machine:I386 /libpath:"c:\oracle\ora92\oci\lib\msvc"
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /machine:I386 /out:"../bin/wdbi_odbc.dll" /implib:"../lib/wdbi_odbc.lib"
# SUBTRACT LINK32 /pdb:none

!ELSEIF  "$(CFG)" == "wdbi - Win32 ODBC_Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "wdbi___Win32_ODBC_Debug"
# PROP BASE Intermediate_Dir "wdbi___Win32_ODBC_Debug"
# PROP BASE Ignore_Export_Lib 0
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir "wdbi___Win32_ODBC_Debug"
# PROP Intermediate_Dir "wdbi___Win32_ODBC_Debug"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MTd /Gm /GX /ZI /Od /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /FR /FD /GZ /c
# SUBTRACT BASE CPP /YX /Yc /Yu
# ADD CPP /nologo /MTd /Gm /GX /ZI /Od /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /D "NO_OCI" /FR /FD /GZ /c
# SUBTRACT CPP /YX /Yc /Yu
# ADD BASE MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x804 /d "_DEBUG"
# ADD RSC /l 0x804 /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib oci.lib ociw32.lib /nologo /dll /pdb:"../lib/wdbid.pdb" /debug /machine:I386 /out:"../bin/wdbid.dll" /implib:"../lib/wdbid.lib" /pdbtype:sept /libpath:"c:\oracle\ora92\oci\lib\msvc"
# SUBTRACT BASE LINK32 /pdb:none
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /pdb:"../lib/wdbid.pdb" /debug /machine:I386 /out:"../bin/wdbi_odbcd.dll" /implib:"../lib/wdbi_odbcd.lib" /pdbtype:sept
# SUBTRACT LINK32 /pdb:none

!ELSEIF  "$(CFG)" == "wdbi - Win32 Mem_Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "wdbi___Win32_Mem_Release"
# PROP BASE Intermediate_Dir "wdbi___Win32_Mem_Release"
# PROP BASE Ignore_Export_Lib 0
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "wdbi___Win32_Mem_Release"
# PROP Intermediate_Dir "wdbi___Win32_Mem_Release"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MD /W3 /GX /O2 /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /FD /c
# SUBTRACT BASE CPP /YX /Yc /Yu
# ADD CPP /nologo /MD /W3 /GX /O2 /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /D "NO_ODBC" /D "NO_OCI" /FD /c
# SUBTRACT CPP /YX /Yc /Yu
# ADD BASE MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x804 /d "NDEBUG"
# ADD RSC /l 0x804 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib oci.lib ociw32.lib /nologo /dll /machine:I386 /libpath:"c:\oracle\ora92\oci\lib\msvc"
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /machine:I386 /out:"../bin/wdbi_mem.dll" /implib:"../lib/wdbi_mem.lib"
# SUBTRACT LINK32 /pdb:none

!ELSEIF  "$(CFG)" == "wdbi - Win32 Mem_Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "wdbi___Win32_Mem_Debug"
# PROP BASE Intermediate_Dir "wdbi___Win32_Mem_Debug"
# PROP BASE Ignore_Export_Lib 0
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir "wdbi___Win32_Mem_Debug"
# PROP Intermediate_Dir "wdbi___Win32_Mem_Debug"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MTd /Gm /GX /ZI /Od /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /FR /FD /GZ /c
# SUBTRACT BASE CPP /YX /Yc /Yu
# ADD CPP /nologo /MTd /Gm /GX /ZI /Od /I "c:\oracle\ora92\oci\include" /I "..\inc" /I "." /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "WDBI_EXPORTS" /D "NO_EXCEL" /D "NO_ODBC" /D "NO_OCI" /FR /FD /GZ /c
# SUBTRACT CPP /YX /Yc /Yu
# ADD BASE MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x804 /d "_DEBUG"
# ADD RSC /l 0x804 /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib oci.lib ociw32.lib /nologo /dll /pdb:"../lib/wdbid.pdb" /debug /machine:I386 /out:"../bin/wdbid.dll" /implib:"../lib/wdbid.lib" /pdbtype:sept /libpath:"c:\oracle\ora92\oci\lib\msvc"
# SUBTRACT BASE LINK32 /pdb:none
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /pdb:"../lib/wdbid.pdb" /debug /machine:I386 /out:"../bin/wdbi_memd.dll" /implib:"../lib/wdbi_memd.lib" /pdbtype:sept
# SUBTRACT LINK32 /pdb:none

!ENDIF 

# Begin Target

# Name "wdbi - Win32 Release"
# Name "wdbi - Win32 Debug"
# Name "wdbi - Win32 ODBC_Release"
# Name "wdbi - Win32 ODBC_Debug"
# Name "wdbi - Win32 Mem_Release"
# Name "wdbi - Win32 Mem_Debug"
# Begin Group "Source Files"

# PROP Default_Filter "cpp;c;cxx;rc;def;r;odl;idl;hpj;bat"
# Begin Source File

SOURCE=.\dataTable.cpp
# End Source File
# Begin Source File

SOURCE=.\dataTableLink.cpp
# End Source File
# Begin Source File

SOURCE=.\memTable.cpp
# End Source File
# Begin Source File

SOURCE=.\StdAfx.cpp
# ADD CPP /Yc"stdafx.h"
# End Source File
# Begin Source File

SOURCE=.\wdbi_global.cpp
# End Source File
# Begin Source File

SOURCE=.\wdbi_int.cpp
# End Source File
# Begin Source File

SOURCE=.\wdbi_main.cpp
# End Source File
# Begin Source File

SOURCE=.\wdbiError.cpp
# End Source File
# Begin Source File

SOURCE=.\wociSession.cpp
# End Source File
# Begin Source File

SOURCE=.\wociStatement.cpp
# End Source File
# Begin Source File

SOURCE=.\wodbcSession.cpp
# End Source File
# Begin Source File

SOURCE=.\wodbcStatement.cpp
# End Source File
# End Group
# Begin Group "Header Files"

# PROP Default_Filter "h;hpp;hxx;hm;inl"
# Begin Source File

SOURCE=.\StdAfx.h
# End Source File
# Begin Source File

SOURCE=.\wdbi.h
# End Source File
# Begin Source File

SOURCE=..\inc\wdbi_inc.h
# End Source File
# Begin Source File

SOURCE=.\wdbiint.h
# End Source File
# End Group
# Begin Group "Resource Files"

# PROP Default_Filter "ico;cur;bmp;dlg;rc2;rct;bin;rgs;gif;jpg;jpeg;jpe"
# End Group
# Begin Source File

SOURCE=.\ReadMe.txt
# End Source File
# End Target
# End Project
