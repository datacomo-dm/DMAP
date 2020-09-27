/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#ifndef RSI_UPDATER_H
#define RSI_UPDATER_H

#define RSI_UPDATER_VERSION "3.2"

#include <iostream>
#include <string>

#include "core/RSI_Framework.h"

class UpdaterParameters;

class RSIUpdater
{
public:
	RSIUpdater(const UpdaterParameters& updater_params);

public:
	int Run();

private:
	void 		WriteToLog(const std::string& report);
    RSIndexID	ParseFileName(const std::string & fn);
    int 		CreateMetadataFile();
    int 		RebuildRsiDir();

private:
	std::string	kn_folder_path;
	std::string	data_dir;
	int			version_to;
	int 		version_from;
	std::string metadata_file_path;
};

#endif
