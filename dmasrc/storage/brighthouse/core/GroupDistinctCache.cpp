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

#include "GroupDistinctCache.h"

GroupDistinctCache::GroupDistinctCache() : CacheableItem("JW","GDC")
{
	t = NULL;
	t_write = NULL;
	cur_pos = NULL;
	cur_obj = 0;
	buf_size = 0;
	no_obj = 0;
	orig_no_obj = 0;
	width = 0;
	upper_byte_limit = 0;
	cur_write_pos = NULL;
}

GroupDistinctCache::~GroupDistinctCache()
{
	dealloc(t);
	dealloc(t_write);
}

void GroupDistinctCache::Initialize(int byte_width)		// size  - number of objects,
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(no_obj > 0 && byte_width > 0);
	upper_byte_limit = 32 * MBYTE;					// 32 MB max.
	width = byte_width;
	buf_size = no_obj;
	if(no_obj > 32 * GBYTE)
		no_obj = 32 * GBYTE;				// upper reasonable size: 32 bln rows (= up to 640 GB on disk)
											// this limitation should be in future released by actual disk limits
	if(buf_size * width > upper_byte_limit) {
		buf_size = upper_byte_limit / width;
		CI_SetDefaultSize(upper_byte_limit);
		t = (unsigned char*)alloc(upper_byte_limit, BLOCK_TEMPORARY);
	}
	else
		t = (unsigned char*)alloc(buf_size * width, BLOCK_TEMPORARY);	// no need to cache on disk
	cur_pos = t;
	cur_obj = 0;
	cur_write_pos = t;
	cur_write_obj = 0;
}

void GroupDistinctCache::SetCurrentValue(unsigned char *val, int w)
{
	if(t == NULL) 		// not initialized yet!
		Initialize(w);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(cur_pos && w == width);		// cur_pos==NULL  =>  no more place
	memcpy(cur_pos, val, width);
}

void GroupDistinctCache::Reset()							// rewind iterator, start from object 0
{
	no_obj = orig_no_obj;
	Rewind();
}

void GroupDistinctCache::Rewind()							// rewind iterator, start from object 0
{
	if(no_obj > 0 && buf_size > 0) {
		if(cur_obj >= buf_size) {
			CI_Put(int(cur_obj / buf_size), t);					// save the last block
			CI_Get(0, t);									// load the first block
		}
		cur_pos = t;					// may be NULL, will be initialized on the first SetCurrentValue
		cur_write_pos = t_write;
	} else {
		cur_pos = NULL;					// invalid from the beginning
		cur_write_pos = NULL;
	}
	cur_obj = 0;
	cur_write_obj = 0;
}

bool GroupDistinctCache::NextRead()			// go to the next position, return false if out of scope
{
	cur_obj++;
	if(cur_obj >= no_obj) {
		cur_obj--;
		cur_pos = NULL;					// indicator of buffer overflow
		return false;
	}
	if(cur_obj % buf_size == 0) {			// a boundary between buffers
		CI_Get(int(cur_obj / buf_size), t);		// load the current block
		cur_pos = t;
	} else
		cur_pos += width;
	return true;
}

bool GroupDistinctCache::NextWrite()	// go to the next position, return false if out of scope
{
	cur_obj++;
	if(cur_obj >= no_obj) {
		cur_obj--;
		cur_pos = NULL;					// indicator of buffer overflow
		return false;
	}
	if(cur_obj % buf_size == 0) {			// a boundary between buffers
		CI_Put(int((cur_obj - 1) / buf_size), t);		// save the previous block
		cur_pos = t;
	} else
		cur_pos += width;
	return true;
}

void GroupDistinctCache::MarkCurrentAsPreserved()
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(cur_obj >= cur_write_obj);
	if(t_write == NULL) {
		t_write = (unsigned char*)alloc(upper_byte_limit, BLOCK_TEMPORARY);	// switch writing to the new buffer
		cur_write_pos = t_write;
	}
	if(cur_obj > cur_write_obj) {
		memcpy(cur_write_pos, cur_pos, width);
	}
	cur_write_obj++;
	if(cur_write_obj % buf_size == 0) {			// a boundary between buffers
		CI_Put(int((cur_write_obj - 1) / buf_size), t_write);		// save the previous block
		cur_write_pos = t_write;
	} else
		cur_write_pos += width;
}

void GroupDistinctCache::SwitchToPreserved()				// switch to values marked as preserved
{
	no_obj = cur_write_obj;
	if(cur_write_obj > buf_size) {			// more than one buffer
		CI_Put(int(cur_write_obj / buf_size), t_write);		// save the last block
		CI_Get(0, t);									// load the first block
		cur_obj = 0;		// preventing reloading anything on Rewind()
	} else {
		unsigned char *p = t;		// switch buffer names
		t = t_write;
		t_write = p;
		cur_obj = cur_write_obj;
	}
}
