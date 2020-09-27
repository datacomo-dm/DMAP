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

#ifndef DATALOADER_H_
#define DATALOADER_H_

#include <boost/utility.hpp>
#include <vector>

#include "core/bintools.h"
#include "common/CommonDefinitions.h"
#include "compress/tools.h"
#include "system/IOParameters.h"
#include "common/bhassert.h"
#include "types/RCDataTypes.h"
#include "loader/NewValuesSetBase.h"

class RCAttrLoad;
class DataParser;
class Buffer;
class NewValuesSetCopy;

class DataLoaderImpl : public boost::noncopyable
{
	typedef std::vector<std::pair<int, RCAttrLoad*> > ThreadsHandles;
public:
	virtual _uint64	Proceed();
	
	virtual _uint64	Proceed(NewValuesSetBase **nvs);
	// for insert only
	_uint64 ProceedEnd();
	virtual ~DataLoaderImpl();
	uint GetPackrowSize() const { return packrow_size; }
	bool DoSort() const { return enable_sort; }
	uint GetSortSize() const { return sort_size; }
	
public:
	DataLoaderImpl(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop);
	void WritePackIndex();

protected:
	virtual int		LoadData();
	virtual void	Abort();
	virtual std::vector<uint>*  RowOrder(NewValuesSetCopy **nvs, uint size) { return NULL; }
	virtual void OmitAttrs( NewValuesSetCopy **) {return;}
	
public:
	typedef std::vector<std::pair<uint,bool> > SortOrderType;

	static std::auto_ptr<DataLoader>
			CreateDataLoader(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop);
	static std::auto_ptr<DataLoader>
			CreateDataLoader(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, 
								SortOrderType &, int, std::vector<unsigned int> &);
protected:

	std::vector<RCAttrLoad*> 	attrs;
	Buffer*		 				buffer;
	IOParameters 				iop;
	std::auto_ptr<DataParser> 	parser;
	_uint64 					no_loaded_rows;
	uint						packrow_size;
	uint                        sort_size;
	SortOrderType               sort_order;
	bool						enable_sort;
};

#endif /*DATALOADER_H_*/
