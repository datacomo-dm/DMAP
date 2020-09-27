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



#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem/operations.hpp>

#include "UpdaterParameters.h"
#include "RSIUpdater.h"
#include "core/RSI_Framework.h"
#include "system/Channel.h"
#include "system/ChannelOut.h"
#include "system/StdOut.h"
#include "system/FileOut.h"

using namespace std;
using namespace boost;
using namespace boost::filesystem;

boost::shared_ptr<ChannelOut>	UpdaterParameters::channel = boost::shared_ptr<ChannelOut>(new StdOut());
boost::shared_ptr<Channel>		UpdaterParameters::log_output = boost::shared_ptr<Channel>(new Channel(UpdaterParameters::channel.get(), true));

UpdaterParameters::UpdaterParameters()
	:	print_help(false), print_version(false),
		desc("IB Updater version: " + string(RSI_UPDATER_VERSION) + ", options")
{
	desc.add_options()
		("help", 											"Display help message and exit.")
		("version,V", 										"Display version information and exit.")
		("datadir",		po::value<string>(&data_dir), 		"Absolute path to data directory.")
		("knfolder",	po::value<string>(&kn_folder),		"Absolute path to KNFolder directory.")
		("migrate-to",	po::value<string>(&migrate_to),		GetMigrateToHelpMessage().c_str())
		("log-file",	po::value<string>(&log_file_name),	"Print output to log file.")
	;
}

int UpdaterParameters::VersionTo() const
{
	return RSI_Manager::GetRSIVersionCode(migrate_to);
}

void UpdaterParameters::PrintHelpMessage(std::ostream& os) const
{
	os << desc << "\n";
}

void UpdaterParameters::PrintVersionMessage(std::ostream& os) const
{
	os << "IB Updater version: " << RSI_UPDATER_VERSION << "\n";
}

string UpdaterParameters::GetMigrateToHelpMessage() const
{
	std::string message("Version to which migration should be done.\nAvailable versions: ");
	RSI_Manager::versions_map_type::iterator iter = RSI_Manager::versions_map.begin();
	RSI_Manager::versions_map_type::iterator end = RSI_Manager::versions_map.end();
	for(; iter != end; ++iter)
		message += (iter->first + ((iter + 1) == end ? "" : ", "));
	return message;
}

void UpdaterParameters::PrepareLogOutput()
{
	 if(vm.count("log-file")) {
		string updater_logfile_path = data_dir + log_file_name;
		channel = boost::shared_ptr<ChannelOut>(new FileOut(updater_logfile_path.c_str()));
		log_output = boost::shared_ptr<Channel>(new Channel(channel.get(), true));
	 }
}

void UpdaterParameters::PrepareParameters()
{
	if(!IsPrintHelpParamSet() && !IsVersionParamSet()) {
		if(!vm.count("datadir"))
			throw SystemRCException("No data directory specified.");
		else if (!exists(data_dir) || !is_directory(data_dir))
			throw SystemRCException("Specified data directory does not exist.");
		else if((*data_dir.rbegin()) != '/' && (*data_dir.rbegin()) != '\\')
			 data_dir += "/";

		if(!vm.count("knfolder"))
			throw SystemRCException("No kn-folder specified.");
		else if(!exists(kn_folder) || !is_directory(kn_folder))
			throw SystemRCException("Specified kn-folder does not exist.");
		else if(((*kn_folder.rbegin()) != '/' && (*kn_folder.rbegin()) != '\\'))
			kn_folder += "/";

		if(!vm.count("migrate-to"))
			throw SystemRCException("No target version specified.");
		else {
			trim(migrate_to);
			if (RSI_Manager::GetRSIVersionCode(migrate_to) < 0)
				throw SystemRCException("Unknown version identifier. Type 'updater --help' for available versions.");
		}
		PrepareLogOutput();
	}
}

auto_ptr<UpdaterParameters> UpdaterParameters::CreateUpdaterParameters(int argc, char** argv)
{
	std::auto_ptr<UpdaterParameters> up = std::auto_ptr<UpdaterParameters>(new UpdaterParameters());

	try {
		po::store(po::parse_command_line(argc, argv, up->desc), up->vm);
		po::notify(up->vm);

		up->PrepareParameters();

	} catch (...) {
		throw;
	}
	return up;
}
