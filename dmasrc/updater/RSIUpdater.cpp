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

#include <iostream>
#include <fstream>
#include <boost/bind.hpp>
#include <boost/filesystem/operations.hpp>

#include "RSIUpdater.h"
#include "UpdaterParameters.h"
#include "UpdaterMethod.h"
#include "system/RCException.h"
#include "system/Channel.h"
#include "system/ChannelOut.h"
#include "system/StdOut.h"
#include "system/IBFile.h"
#include "system/IBFileSystem.h"

using namespace std;
using namespace boost;
using namespace boost::filesystem;


RSIUpdater::RSIUpdater(const UpdaterParameters& updater_params)
	:	kn_folder_path(updater_params.KNFolderPath()),  data_dir(updater_params.DataDirPath()), version_to(updater_params.VersionTo())
{
	metadata_file_path = kn_folder_path + RSI_METADATAFILE ;

	if(DoesFileExist(metadata_file_path.c_str()))
		version_from = RSI_Manager::GetRSIVersion(kn_folder_path);
	else
		version_from = 1;

	if (version_from < 0)
		throw FileRCException("Unable to obtain actual version name.");

	WriteToLog("Starting RSI Manager update process...");
}

void RSIUpdater::WriteToLog(const string& report)
{
	UpdaterParameters::LogOutput() << lock << report << unlock;
}

int RSIUpdater::Run()
{
	int res = -1;
	bool success = false;
	string welcome = string("Recognized existing version: ") + RSI_Manager::GetRSIVersionName(version_from) + string(", target version: ") + RSI_Manager::GetRSIVersionName(version_to) + string(".");
	if(version_from != version_to) {
		welcome += string(" Continuing Update process...");
		WriteToLog(welcome);

		if(version_from == 1 && version_to == 2) {
			if(UpdaterMethod((function0<int>)((boost::bind(&RSIUpdater::CreateMetadataFile, this))), "Creating metadata file")() >= 0)
				success = true;
		} else if(version_from == 2 && version_to == 1 ) {
			if(	UpdaterMethod((function0<int>)((boost::bind(&RSIUpdater::RebuildRsiDir, this))), "Rebuilding rsi_dir file")() >= 0 &&
				UpdaterMethod((function0<void>)((boost::bind(&RemoveFile, metadata_file_path.c_str(), 1))), "Removing metadata file")() >= 0
			)
				success = true;
		}
		if(success) {
			WriteToLog("Update process succeeded.");
			res = 1;
		}
	} else {
		WriteToLog(welcome);
		WriteToLog("Update process abandoned.");
		res = 0;
	}
	return res;
}

RSIndexID RSIUpdater::ParseFileName(const string & fn)
{
	try {
		size_t pos1 = fn.find('.', 0);
		size_t pos2 = fn.find('.', pos1 + 1);
		size_t pos3 = fn.find('.', pos2 + 1);
		string typ = fn.substr(0, pos1);
		RSIndexType type;
		if(typ == "HIST")
			type = RSI_HIST;
		else if(typ == "CMAP")
			type = RSI_CMAP;
		else if(typ == "JPP")
			type = RSI_JPP;
		else
			return RSIndexID();

		int tab = lexical_cast<int>(fn.substr(pos1 + 1, pos2 - pos1 - 1));
		int col = lexical_cast<int>(fn.substr(pos2 + 1, pos3 - pos2 - 1));
		if(type == RSI_HIST || type == RSI_CMAP)
			return RSIndexID(type, tab, col);

		if(typ == "JPP"){
			size_t pos4 = fn.find('.', pos3 + 1);
			size_t pos5 = fn.find('.', pos4 + 1);
			int tab2 = lexical_cast<int>(fn.substr(pos3 + 1, pos4 - pos3 - 1));
			int col2 = lexical_cast<int>(fn.substr(pos4 + 1, pos5 - pos4 - 1));
			return RSIndexID(RSI_JPP, tab, col, tab2, col2);
		}
	} catch(...) {
		throw FileRCException(string("Unable to parse KN file name: ") + fn);
	}
	return RSIndexID();
}

int RSIUpdater::CreateMetadataFile()
{
	return RSI_Manager::CreateMetadataFile(metadata_file_path, version_to);
}

int RSIUpdater::RebuildRsiDir()
{
	vector<RSIndexID> pool;
	RSIndexID emptyRSIndexID;
	directory_iterator end_itr;
	for(directory_iterator itr(kn_folder_path); itr != end_itr; ++itr){
		string fn = lexical_cast<string>( itr->path().filename() );
		if(fn.substr(0, 5) != "HIST." && fn.substr(0, 5) != "CMAP." && fn.substr(0, 4) != "JPP.")
			continue;

		RSIndexID res = ParseFileName(fn);
		if(!(res == emptyRSIndexID))
			pool.push_back(res);

	}
	sort(pool.begin(), pool.end());
	string path_tmp = kn_folder_path + RSI_DEFFILE + "_tmp";
	try {
		ofstream fnew(path_tmp.c_str(), ios_base::out);
		copy(pool.begin(), pool.end(), ostream_iterator<RSIndexID> (fnew, "\n"));
		fnew.close();
	} catch(std::exception& e){
		throw SystemRCException(string("Unable to create rsi_dir file. ") + e.what());
	}

	try {
		RenameFile(path_tmp.c_str(), (kn_folder_path + RSI_DEFFILE).c_str());
	} catch (RCException& e) {
		throw SystemRCException(string("Unable to rename rsi_dir.tmp file. ") + e.what());
	}
	return 1;
}
