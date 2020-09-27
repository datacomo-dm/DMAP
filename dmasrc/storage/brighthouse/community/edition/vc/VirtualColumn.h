/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * VirtualColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in VirtualColumn object may not exist physically, they can be computed on demand.
 *
 */


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


#ifndef VIRTUALCOLUMN_H_INCLUDED
#define VIRTUALCOLUMN_H_INCLUDED

#include "vc/VirtualColumnBase.h"
#include "core/PackGuardian.h"
#include "core/Filter.h"
class MIIterator;

/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * VirtualColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in VirtualColumn object may not exist physically, they can be computed on demand.
 *
 */
#define PREFETCH_BUFL (MAX_PREFETCHS+2)
class VirtualColumn : public VirtualColumnBase
{
	RSValue *rsvalue;
	std::vector<_int64 *> prefetchnos;
	int prefetch_limit;
public:
	VirtualColumn(ColumnType const& col_type, MultiIndex* mind) : VirtualColumnBase(col_type, mind), pguard(this) {

		rsvalue=NULL;
		prefetch_limit=MAX_PREFETCH;
	}
	VirtualColumn(VirtualColumn const& vc) : VirtualColumnBase(vc), pguard(this) {

		rsvalue=NULL;
		prefetch_limit=MAX_PREFETCH;
	}
	~VirtualColumn();
	void SetThreads(const MIIterator& mit,int threadn) {
		pguard.SetThreads(mit,threadn);
	}
    virtual bool IsExpressionColumn(){ return false;}
	virtual void LockSourcePacks(const MIIterator& mit,int th_no);
	virtual void LockSourcePacks(const MIIterator& mit);
	void UnlockSourcePacks();
	void UpdatePrefetchBuf(const MIIterator& mit);
	void Prefetch(const MIIterator& mit);
	void AllocPrefetching() {
		int i=0;
		for(std::vector<_int64 *>::iterator it=prefetchnos.begin(); it != prefetchnos.end(); ++it) {
			delete [] *it;
		}
		prefetchnos.clear();
		for(vector<VirtualColumn::VarMap>::const_iterator iter = var_map.begin(); iter != var_map.end(); iter++) {
			_int64 *bf=new _int64[PREFETCH_BUFL];
			memset(bf,0,PREFETCH_BUFL*sizeof(_int64));
			prefetchnos.push_back(bf);
		}
	}
   void InitPrefetching(MIIterator& mit, int attrs = -1,int nth=1) {		
		pguard.SetThreads(mit,nth);
		AllocPrefetching();
		if(attrs<6)
			prefetch_limit=MAX_PREFETCH;
		else {
			//最低10个预读取深度
			double reducerate=attrs/5.0;
			prefetch_limit=MAX_PREFETCH/reducerate,10;
			if(prefetch_limit<10) 
				prefetch_limit=10;
		}
		rsvalue=NULL;
		Prefetch(mit);
  }
	void SetPrefetchFilter(RSValue* r_filter = NULL) {
		rsvalue=r_filter;
	}
	void StopPrefetching() {
		pguard.WaitPreload();
		for(std::vector<_int64 *>::iterator it=prefetchnos.begin(); it != prefetchnos.end(); ++it) {
			delete [] *it;
		}
		prefetchnos.clear();
		prefetch_limit=MAX_PREFETCH;
		rsvalue=NULL;
	}

protected:
	VCPackGuardian pguard;		//IEE version

};

#endif /* not VIRTUALCOLUMN_H_INCLUDED */


