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

#include "edition/vc/VirtualColumn.h"
#include "core/MIIterator.h"

VirtualColumn::~VirtualColumn()
	{
		pguard.UnlockAll();
		StopPrefetching();
	}

void VirtualColumn::LockSourcePacks(const MIIterator& mit)
{

		if(mit.IsPackMttLoad()) {
			UpdatePrefetchBuf(mit);
			pguard.LockPackrow(mit,prefetchnos);
		}
		else {
			std::vector<_int64 *> prefetches;
			pguard.LockPackrow(mit,prefetches);
		}
	
}

void VirtualColumn::LockSourcePacks(const MIIterator& mit,int th_no)
{
    if(mit.IsPackMttLoad()) {
        UpdatePrefetchBuf(mit);
        pguard.LockPackrow(mit,prefetchnos,th_no);
    }
    else {
        std::vector<_int64 *> prefetches;
        pguard.LockPackrow(mit,prefetches,th_no);
    }
}

void VirtualColumn::UnlockSourcePacks()
{
	pguard.WaitPreload();
	pguard.UnlockAll();
}

void VirtualColumn::Prefetch(const MIIterator& mit) {
	UpdatePrefetchBuf(mit);
	pguard.PreloadPack(mit,prefetchnos);
}

void VirtualColumn::UpdatePrefetchBuf(const MIIterator& mit) 
{
	if(prefetchnos.size()!=var_map.size()) {
		AllocPrefetching();
	}
	int vct=0;
	bool updated=false;
	static double prefetch_filltime=0;
	static int prefetch_filltimes=0;
	mytimer tmfill;
	tmfill.Start();
	for(std::vector<VirtualColumn::VarMap>::const_iterator iter = var_map.begin(); iter != var_map.end(); iter++,vct++) {
		_int64 *aheads=prefetchnos[vct];
		_int64 &curahead=aheads[0];
		_int64 &procblocks=aheads[1];
		_int64 *ahead=aheads+2;
		_int64 lastpackrow=-1;
		if(iter->dim<0) continue;
		_int64 curpackrow= mit.GetCurPackrow(iter->dim);
		if(curpackrow<0) continue;
		if(curahead-procblocks<prefetch_limit/2) {//已处理超过一半，需要补充数据
			int remain=curahead-procblocks;
			//get last filled preload pack or first pack
			_int64 lastpreloaded=ahead[max((int)curahead-1,0)];
			if(lastpreloaded==0) 
				lastpreloaded=curpackrow-1;
			//wait for processing.
			if(lastpreloaded>=mit.GetNextPackrow(iter->dim,prefetch_limit)) continue;
			if(lastpreloaded<curpackrow) //已经无效？
				remain=0;
			if(remain>0)
				memmove(ahead,ahead+procblocks,remain*sizeof(_int64));
			curahead=remain;procblocks=0;
			RSValue ah_roughval;
			// 删除不需要预处理的列表
			if(remain>0 && *ahead<curpackrow) {
				for(int ap=0;ap<remain;ap++)
				  if(ahead[ap]>=curpackrow)
				  	{
				  		memmove(ahead,ahead+ap,(curahead-ap)*sizeof(_int64));
				  		curahead-=ap;
						remain-=ap;
				  		break;
				  	}
			}
			_int64 tocheck=curpackrow;
			//look ahead position
			int aheadp=0;

			//find first preload pack position for appending to list
			while(tocheck<=lastpreloaded  && aheadp<prefetch_limit && tocheck>=0) {
				tocheck=mit.GetNextPackrow(iter->dim,++aheadp);
			}
			//tocheck shoud be just equal to checkby while curahead>0
			while(curahead<prefetch_limit && aheadp<prefetch_limit*2) {
				_int64 toahead=mit.GetNextPackrow(iter->dim,aheadp++);
				//TODO: check validation like this?
				if(toahead<0||lastpackrow>=toahead ) break;
				ah_roughval=	RS_SOME;
				if(rsvalue && toahead >= 0)
					ah_roughval = rsvalue[(int)toahead];
				else
					ah_roughval = RS_SOME;
				if(ah_roughval == RS_NONE) {
					continue;
				} 
				//RS_ALL will need prefetch block to accelarating
				else
				{
					ahead[(int)curahead++]=toahead;
					updated = true;
				}
				lastpackrow=toahead;
			}
		}
	}

	if(updated) {
		tmfill.Stop();
		prefetch_filltime+=tmfill.GetTime();
		prefetch_filltimes++;
		if(prefetch_filltimes==200) {
			if(rccontrol.isOn() && PRELOAD_OPER_LOG>=1) {
				rclog << lock << "Fill prefetch queue 200 times using "<<prefetch_filltime<<"s."<<unlock;
			}
			prefetch_filltimes=0;
			prefetch_filltime=0;
		}
	}
}
