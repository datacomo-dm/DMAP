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

#ifndef UPDATER_PARAMETERS_H
#define UPDATER_PARAMETERS_H

#include <iostream>
#include <string>
#include <memory>
#include <boost/shared_ptr.hpp>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

class ChannelOut;
class Channel;

class UpdaterParameters
{
private:
	UpdaterParameters();

public:
	const std::string&	DataDirPath() const 		{ return data_dir; }
	const std::string&	KNFolderPath() const 		{ return kn_folder; }
	int 				VersionTo() const;

	bool				IsPrintHelpParamSet() const	{ return vm.count("help"); }
	bool				IsVersionParamSet()	const	{ return vm.count("version"); }

	void 				PrintHelpMessage(std::ostream& os) const;
	void 				PrintVersionMessage(std::ostream& os) const;

private:
	std::string 		GetMigrateToHelpMessage() const;
	void 				PrepareLogOutput();
	void 				PrepareParameters();

private:
	std::string	data_dir;
	std::string	kn_folder;
	std::string migrate_to;
	bool        print_help;
	bool		print_version;

	std::string log_file_name;

	po::variables_map vm;
	po::options_description desc;

	static boost::shared_ptr<ChannelOut>	channel;
	static boost::shared_ptr<Channel>		log_output;

public:
	static Channel&	LogOutput()	{ return *log_output; }
	static std::auto_ptr<UpdaterParameters> CreateUpdaterParameters(int argc, char** argv);
};

#endif
