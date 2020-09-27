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

#include "RSI_Histogram.h"

using namespace std;

#define RSI_HIST_INT_RES 32

RSIndex_Hist::RSIndex_Hist()				// create an empty index
{
	t = NULL;
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	start_pack = 0;
	end_pack = 0;
	fixed = false;
	int_res = RSI_HIST_INT_RES;
}

RSIndex_Hist::~RSIndex_Hist()
{
	if (t) dealloc(t);
	t = NULL;
}

void RSIndex_Hist::Clear()
{
	if (t) dealloc(t);
	t = NULL;
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	fixed = false;
	int_res = RSI_HIST_INT_RES;
	start_pack = 0;
	end_pack = 0;
}

void RSIndex_Hist::Create(_int64 _no_obj, bool _fixed)
{
	if (t) dealloc(t);
	fixed	= _fixed;
	no_obj	= _no_obj;
	no_pack	= bh::common::NoObj2NoPacks(no_obj);
	no_pack_declared = (no_pack / 30 + 1) * 30;
	t = (unsigned int*)alloc(no_pack_declared * int_res * sizeof(int), BLOCK_TEMPORARY, true);
	if (!t)	throw OutOfMemoryRCException();
	memset(t, 255, no_pack_declared * int_res * sizeof(int)); // Note: fill with value 255 (all ones), because if a pack is not updated, it should be treated as "ones only"
	start_pack = 0;
	end_pack = no_pack - 1;
}

void RSIndex_Hist::AppendKNs(int new_no_pack)
{
	int new_no_pack_declared = (new_no_pack / 30 + 1) * 30; // now the minimal size of hist is below 4 KB (including header)
	if(new_no_pack < no_pack) {
		assert(0);
		no_pack = new_no_pack;
		// TODO: shrink buffers, what about start_pack?
		start_pack = 0;
		end_pack = no_pack - 1;
		return;
	}

	if(t) // Note: fill with value 255 (all ones), because if a pack is not updated, it should be treated as "ones only"
	{
		if(new_no_pack_declared > no_pack_declared) {
			size_t size = (new_no_pack_declared - start_pack) * int_res * sizeof(int);
			t = (unsigned int*) rc_realloc(t, size, BLOCK_TEMPORARY);
			memset(t + (no_pack_declared - start_pack) * int_res, 255, (new_no_pack_declared - no_pack_declared)
					* int_res * sizeof(int));
		}
	} else {
		t = (unsigned int*) alloc((new_no_pack_declared - start_pack) * int_res * sizeof(int), BLOCK_TEMPORARY);
		if(!t)
			throw OutOfMemoryRCException();
		memset(t, 255, (new_no_pack_declared - start_pack) * int_res * sizeof(int));
	}
	no_pack = new_no_pack;
	no_pack_declared = new_no_pack_declared;
	// start_pack remains unchanged
	end_pack = no_pack - 1;

}

void RSIndex_Hist::Update(_int64 _new_no_obj)		// enlarge buffers for the new number of objects
{
	int new_no_pack = bh::common::NoObj2NoPacks(_new_no_obj);
	//DMA-520 previous partition loading not started at lastpack
	if(new_no_pack < no_pack) return;
	AppendKNs(new_no_pack);
	no_obj  = _new_no_obj;
}

bool RSIndex_Hist::UpToDate(_int64 cur_no_obj, int pack)	// true, if this pack is up to date (i.e. either cur_obj==no_obj, or pack is not the last pack in the index)
{
	return (cur_no_obj == no_obj || ((pack < no_pack - 1) && (pack < (cur_no_obj / MAX_PACK_ROW_SIZE))));
}

void RSIndex_Hist::CopyPack(int pack,const void* pbuff,const int buff_len){    // ¿½±´RSI×ÊÔ´,ADD BY  Liujs
    assert(int_res * sizeof(int) == buff_len);
    
	if(t && pack >= start_pack && pack <= end_pack)
		memcpy(t + (pack - start_pack) * int_res, pbuff, buff_len);	// used before actual update
}

void RSIndex_Hist::ClearPack(int pack)
{
//	assert(pack >= start_pack && pack <= end_pack);			// too restrictive?
	if(t && pack >= start_pack && pack <= end_pack)
		memset(t + (pack - start_pack) * int_res, 0, int_res * sizeof(int));	// used before actual update
}

void RSIndex_Hist::CopyRepresentation(void *buf, int pack)
{
	if(t == NULL || pack > end_pack)
		AppendKNs(pack);

	memcpy(t + (pack - start_pack) * int_res, buf, int_res * sizeof(int)); // used before actual update
}

int RSIndex_Hist::Load(IBFile *frs_index, int current_read_loc)
{
	// Old file format:
	// Header:	<no_obj><no_pack><int_res><fixed>		- 8+4+1+1 = 14 bytes
	//			<table_t>								- no_pack*int_res*sizeof(int) bytes
	// File format:
	// Header0:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// Header1:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// LastPack0: int_res * sizeof(int) bytes
	// LastPack1: int_res * sizeof(int) bytes
	// <table_t>: (no_pack - 1) * int_res * sizeof(int) bytes

	if (t) dealloc(t);

	// Check which version of file is being loaded

	int bytes = (int)frs_index->Seek(8, SEEK_SET);
	bytes = frs_index->ReadExact(&no_pack, 4);
	bytes = (int)frs_index->Seek(0, SEEK_END);

	bool zero = current_read_loc == 0;
	unsigned char header[15];
	bytes = (int)frs_index->Seek(zero ? 0 : 15, SEEK_SET);
	bytes = frs_index->ReadExact(header, 15);
	no_obj	= *(_int64 *)(header + 1);
	no_pack	= *(int *)(header + 9);
	no_pack_declared = (no_pack / 30 + 1) * 30;
	//int_res = int(header[13]);
	int_res = RSI_HIST_INT_RES;
	fixed	= (header[14] > 0);
	start_pack = 0;
	end_pack = no_pack - 1;
	if(zero) // skip Header1
		bytes = (int)frs_index->Seek(15, SEEK_CUR);
	else // skip LastPack0
		bytes = (int)frs_index->Seek(int_res * sizeof(int), SEEK_CUR);
	if(no_pack * int_res > 0) {
		// allocate memory for no_pack_declared packs
		t = (unsigned int *)alloc(no_pack_declared * int_res * sizeof(int), BLOCK_TEMPORARY, true);
		if (!t)	throw OutOfMemoryRCException();
		// read last pack
		bytes = frs_index->ReadExact(t + (no_pack - 1) * int_res, int_res * sizeof(int));
		if(zero) // skip LastPack1
			bytes = (int)frs_index->Seek(int_res * sizeof(int), SEEK_CUR);
		// read all packs but last
		bytes = frs_index->ReadExact(t, (no_pack - 1) * int_res * sizeof(int));
		memset(t + no_pack * int_res,
			255,
			(no_pack_declared - no_pack) * int_res * sizeof(int)
			);
	}
	else
		t = NULL;
	return 0;
}

int RSIndex_Hist::LoadLastOnly(IBFile *frs_index, int current_read_loc)
{
	// If file is in old format all packs must be loaded!

	// Old file format:
	// Header:	<no_obj><no_pack><int_res><fixed>		- 8+4+1+1 = 14 bytes
	//			<table_t>								- no_pack*int_res*sizeof(int) bytes
	// File format:
	// Header0:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// Header1:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// LastPack0: int_res * sizeof(int) bytes
	// LastPack1: int_res * sizeof(int) bytes
	// <table_t>: (no_pack - 1) * int_res * sizeof(int) bytes

	// Check which version of file is being loaded

	int bytes = (int)frs_index->Seek(8, SEEK_SET);
	bytes = frs_index->ReadExact(&no_pack, 4);
	bytes = (int)frs_index->Seek(0, SEEK_END);

	// new format

	if (t) dealloc(t);
	t = NULL;

	unsigned char header[15];
	bool zero = current_read_loc == 0;
	bytes = (int)frs_index->Seek(zero ? 0 : 15, SEEK_SET);
	bytes = frs_index->ReadExact(header, 15);
	no_obj	= *(_int64 *)(header + 1);
	no_pack	= *(int *)(header + 9);
	no_pack_declared = (no_pack / 30 + 1) * 30;
	//int_res = int(header[13]);
	int_res = RSI_HIST_INT_RES;

	fixed	= (header[14] > 0);
	if(no_pack * int_res > 0) {
		end_pack = start_pack = no_pack - 1;
		if(zero) // skip Header1
			bytes = (int)frs_index->Seek(15, SEEK_CUR);
		else // skip LastPack0
			bytes = (int)frs_index->Seek(int_res * sizeof(int), SEEK_CUR);
		t = (unsigned int *)alloc((no_pack_declared - start_pack) * int_res * sizeof(int), BLOCK_TEMPORARY, true);
		if (!t)	throw OutOfMemoryRCException();
		// read last pack
		memset(t , 255,	(no_pack_declared - start_pack) * int_res * sizeof(int));
		bytes = frs_index->ReadExact(t, int_res * sizeof(int));
	}
	return 0;
}

int RSIndex_Hist::Save(IBFile *frs_index, int current_save_loc)
{
	// File format:
	// Header0:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// Header1:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// LastPack0: int_res * sizeof(int) bytes
	// LastPack1: int_res * sizeof(int) bytes
	// <table_t>: (no_pack - 1) * int_res * sizeof(int) bytes
	unsigned char header[15];
	int bytes;
	header[0]				= 1u; // version number
	*(_int64 *)(header + 1)	= no_obj;
	*(int *  )(header + 9)	= no_pack;
	header[13]				= (unsigned char)int_res;
	header[14]				= (fixed ? 1 : 0);

	bool zero = current_save_loc == 0;

	bytes = (int)frs_index->Seek(zero ? 0 : 15, SEEK_SET);
	bytes = frs_index->WriteExact(header, 15);

	if(zero) // skip Header1
		bytes = (int)frs_index->Seek(15, SEEK_CUR);
	else // skip LastPack0
		bytes = (int)frs_index->Seek(int_res * sizeof(int), SEEK_CUR);

	if(t) {
		// write LastPack
		bytes = frs_index->WriteExact(t + (no_pack - start_pack - 1) * int_res, int_res * sizeof(int) );
		if(zero) // skip LastPack1
			bytes = (int)frs_index->Seek(int_res * sizeof(int), SEEK_CUR);

		// write all the other packs
		int offset = start_pack * int_res * sizeof(int); // position (32bit) of last pack
		bytes = (int)frs_index->Seek(offset, SEEK_CUR);
		bytes = frs_index->WriteExact(t, (no_pack - start_pack - 1) * int_res * sizeof(int));
	}
	else
		bytes = 0;
	if((unsigned int)bytes < (no_pack - start_pack - 1) * int_res * sizeof(int))
		return 1;
	return 0;
}

int RSIndex_Hist::NoOnes(int pack, int width)
{
	if(pack < start_pack || pack > end_pack)
		return 0;
	int d = 0;
	int start_int = (pack - start_pack) * int_res;
	int stop_int = start_int;
	if( width >= int_res*32 )
		stop_int += int_res;							// normal mode
	else
		stop_int += width/32;							// exact mode
	for(int i = start_int; i < stop_int; i++)
		d += CalculateBinSum( t[i] );
	int bits_left = width%32;
	if( stop_int-start_int<int_res && bits_left>0 ) {	// there are bits left in exact mode
		unsigned int last_t = t[ stop_int ];
		unsigned int mask = ~((unsigned int)0xFFFFFFFF << bits_left);	// 0x0000FFFF, i.e. bits_left ones
		last_t &= mask;
		d += CalculateBinSum( last_t );
	}
	return d;
}

//////////////////////////////////////////////////////////////////

RSValue RSIndex_Hist::IsValue(_int64 min_v, _int64 max_v, int pack, _int64 pack_min, _int64 pack_max)
{
	// Results:		RS_NONE - there is no objects having values between min_v and max_v (including)
	//				RS_SOME - some objects from this pack may have values between min_v and max_v
	//				RS_ALL	- all objects from this pack do have values between min_v and max_v
	if(pack < start_pack || pack > end_pack)
		return RS_SOME;
	if(IntervalTooLarge(pack_min, pack_max) || IntervalTooLarge(min_v, max_v))
		return RS_SOME;
	int min_bit = 0, max_bit = 0;
	if(!fixed) {	// floating point
		double dmin_v = *(double*)(&min_v);
		double dmax_v = *(double*)(&max_v);
		double dpack_min = *(double*)(&pack_min);
		double dpack_max = *(double*)(&pack_max);
		assert(dmin_v <= dmax_v);
		if(dmax_v < dpack_min || dmin_v > dpack_max)
			return RS_NONE;
		if(dmax_v >= dpack_max && dmin_v <= dpack_min)
			return RS_ALL;
		if(dmax_v >= dpack_max || dmin_v <= dpack_min)
			return RS_SOME;			// pack_min xor pack_max are present
		// now we know that (max_v<pack_max) and (min_v>pack_min) and there is only RS_SOME or RS_NONE answer possible
		double interval_len = (dpack_max - dpack_min) / double(32 * int_res);
		min_bit = int((dmin_v - dpack_min) / interval_len);
		max_bit = int((dmax_v - dpack_min) / interval_len);
	} else {
		assert(min_v <= max_v);
		if(max_v < pack_min || min_v > pack_max)
			return RS_NONE;
		if(max_v >= pack_max && min_v <= pack_min)
			return RS_ALL;
		if(max_v >= pack_max || min_v <= pack_min)
			return RS_SOME;			// pack_min xor pack_max are present
		// now we know that (max_v<pack_max) and (min_v>pack_min) and there is only RS_SOME or RS_NONE answer possible
		if(ExactMode(pack_min, pack_max)) {		// exact mode
			min_bit = int(min_v - pack_min - 1);			// translate into [0,...]
			max_bit = int(max_v - pack_min - 1);
		} else {			// interval mode
			double interval_len = (pack_max - pack_min - 1) / double(32 * int_res);
			min_bit = int((min_v - pack_min - 1) / interval_len);
			max_bit = int((max_v - pack_min - 1) / interval_len);
		}
	}
	assert(min_bit >= 0);
	if(max_bit >= 32 * int_res)
		return RS_SOME;		// it may happen for extremely large numbers ( >2^52 )
	for(int i = min_bit; i <= max_bit; i++)	{
		if(( (t[(pack - start_pack) * int_res + i / 32] >> (i % 32)) & 0x00000001 ) != 0)
			return RS_SOME;
	}
	return RS_NONE;
}

void RSIndex_Hist::PutValue(_int64 v, int pack, _int64 pack_min, _int64 pack_max)		// set information that value v does exist in this pack
{
	if(pack < start_pack || pack > end_pack)
		return;
	if( v == NULL_VALUE_64 || IntervalTooLarge(pack_min, pack_max) )
		return;			// Note: use ClearPack() first! Default histogram has all ones.
	int bit = -1;
	if(fixed == false) {		// floating point
		double dv = *(double*)(&v);
		double dpack_min = *(double*)(&pack_min);
		double dpack_max = *(double*)(&pack_max);
		assert(dv >= dpack_min && dv <= dpack_max);
		if(dv == dpack_min || dv == dpack_max)
			return;
		double interval_len = (dpack_max - dpack_min) / double(32 * int_res);
		bit = int( (dv - dpack_min) / interval_len );
	} else {
		assert(v >= pack_min && v <= pack_max);
		if(v == pack_min || v == pack_max)
			return;
		if(ExactMode(pack_min, pack_max))	{	// exact mode
			bit = int(v - pack_min - 1);			// translate into [0,...]
		} else {			// interval mode
			double interval_len = _uint64(pack_max - pack_min - 1) / double(32 * int_res);
			bit = int( _uint64(v - pack_min - 1) / interval_len );
		}
	}
	if(bit >= 32 * int_res)
		return;		// it may happen for extremely large numbers ( >2^52 )
	assert(bit >= 0);
	t[(pack - start_pack) * int_res + bit / 32] |= ( 0x00000001u << (bit % 32) );
}

bool RSIndex_Hist::Intersection(				   int pack,  _int64 pack_min,  _int64 pack_max,
								RSIndex_Hist *sec, int pack2, _int64 pack_min2, _int64 pack_max2)
{
	// we may assume that min-max of packs was already checked
	if(!fixed || !sec->fixed || int_res != sec->int_res)
		return true;	// not implemented - intersection possible
	if( IntervalTooLarge(pack_min, pack_max) || IntervalTooLarge(pack_min2, pack_max2))
		return true;

	if(	sec->IsValue(pack_min, pack_min, pack2,pack_min2,pack_max2) != RS_NONE ||
		sec->IsValue(pack_max, pack_max, pack2,pack_min2,pack_max2) != RS_NONE ||
		     IsValue(pack_min2,pack_min2,pack, pack_min, pack_max)  != RS_NONE ||
		     IsValue(pack_max2,pack_max2,pack, pack_min, pack_max)  != RS_NONE )
		return true;	// intersection found (extreme values)

	if(ExactMode(pack_min, pack_max)  &&  ExactMode(pack_min2, pack_max2)) {		// exact mode
		int bit1, bit2;
		_int64 min_v = ( pack_min < pack_min2 ? pack_min2 + 1 : pack_min + 1 );		// these values are possible
		_int64 max_v = ( pack_max > pack_max2 ? pack_max2 - 1 : pack_max - 1 );
		for(_int64 v = min_v; v <= max_v; v++) {
			bit1 = int(v - pack_min  - 1);
			bit2 = int(v - pack_min2 - 1);
			if( ( ((	 t[(pack  -      start_pack) *      int_res + bit1 / 32] >> (bit1 % 32)) & 0x00000001)!=0 ) &&
				( ((sec->t[(pack2 - sec->start_pack) * sec->int_res + bit2 / 32] >> (bit2 % 32)) & 0x00000001)!=0 ) )
					return true;	// intersection found
		}
		return false;	// no intersection possible - all values do not math the second histogram
	}
	// TODO: all other possibilities, not only the exact cases
	return true;		// we cannot foreclose intersecting
}

//void RSIndex_Hist::Display(uint pack)
//{
//	if(pack < start_pack || pack > end_pack || no_pack == 0) {
//		cout << "Invalid pack: " << pack << endl;
//		return;
//	}
//	for(uint i = 0; i < 32; i++) {
//		cout << i << ": ";
//		uint v = t[(pack - start_pack) * int_res + i];
//		for (int j = 0; j < 32; j++) {
//			cout << (v >> j) % 2;
//		}
//		cout << endl;
//	}
//	cout << endl;
//}

/*
 * /pre Index is locked for update
 * Append a KN by the contents of the given KN
 * */

void RSIndex_Hist::AppendKN(int pack, RSIndex_Hist* hist, uint no_values_to_add)
{
	uint no_objs_to_add_to_last_DP = uint(no_obj % MAX_PACK_ROW_SIZE ? MAX_PACK_ROW_SIZE - no_obj % MAX_PACK_ROW_SIZE : 0);
	if(pack < hist->no_pack) {
		AppendKNs(no_pack + 1);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(pack != no_pack - 1);
		if(pack != no_pack - 1)
			memcpy(&t[(no_pack - 1 - start_pack) * int_res], &hist->t[(pack - hist->start_pack) * int_res], int_res * sizeof(int));
		no_obj  += no_objs_to_add_to_last_DP + no_values_to_add;
	}
}
