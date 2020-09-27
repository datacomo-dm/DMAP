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

#include "RSI_CMap.h"

using namespace std;

RSIndex_CMap::RSIndex_CMap(int no_pos): no_positions(no_pos)				// create an empty index
{
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	start_pack = 0;
	end_pack = 0;
}

RSIndex_CMap::~RSIndex_CMap()
{
}

void RSIndex_CMap::Clear()
{
	t.reset();
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	no_positions = 64;
	start_pack = 0;
	end_pack = 0;
}

void RSIndex_CMap::Create(_int64 _no_obj, int no_pos)
{
	t.reset();
	no_obj	= _no_obj;
	no_positions = no_pos;
	no_pack = bh::common::NoObj2NoPacks(no_obj);
	no_pack_declared = (no_pack / 30 + 1) * 30;
	size_t size = no_pack_declared * no_positions * 32; // * 256 / 8 = 32

	t = MMGuard<uint>((uint*)alloc(size, BLOCK_TEMPORARY), *this);
	memset(t.get(), 255, size);
	start_pack = 0;
	end_pack = no_pack - 1;

	//// below loop is for testing purpose only, should be deleted
	//for(int i = 0; i < no_pack; i++) {
	//	UnSet(i, 0, 0);
	//	UnSet(i, 255, 63);
	//}
}

void RSIndex_CMap::AppendKNs(int new_no_pack) {
	int new_no_pack_declared = (new_no_pack / 30 + 1) * 30; // now the minimal size of hist is below 4 KB (including header)
	if(new_no_pack < no_pack) {		// note that there is common situation to have new_no_pack == no_pack
		assert(0);
		// TODO: shrink buffers, what about start_pack?
		no_pack = new_no_pack;
		start_pack = 0;
		end_pack = no_pack - 1;
		return;
	}

	if(no_pack > 0) {
		if(new_no_pack_declared > no_pack_declared) {
			size_t size = (new_no_pack_declared - start_pack) * no_positions * 32; // * 256 / 8 = 32
			t = MMGuard<uint>((uint*)rc_realloc(t.release(), size, BLOCK_TEMPORARY), *this);
			memset(t.get() + (no_pack_declared - start_pack) * no_positions * 32 / sizeof(int), // because t is of type uint we divide by sizeof(int)
				255,
				(new_no_pack_declared - no_pack_declared) * no_positions * 32
			);
		}
	} else {
		size_t size = new_no_pack_declared * no_positions * 32; // * 256 / 8 = 32
		//t.get() = (unsigned int*)alloc(size, BLOCK_TEMPORARY);
		t = MMGuard<uint>((uint*)alloc(size, BLOCK_TEMPORARY), *this);
		memset(t.get(), 255, size);
	}
	no_pack = new_no_pack;
	no_pack_declared = new_no_pack_declared;
	// start_pack remains unchanged
	end_pack = no_pack - 1;

}

void RSIndex_CMap::CopyRepresentation(void *buf, int pack)
{
	if( t.get() == NULL || pack > no_pack)
		AppendKNs(pack);
	memcpy(t.get() + (pack - start_pack) * no_positions  * 32 / sizeof(int), buf, no_positions * 32);	// used before actual update
}

void RSIndex_CMap::Update(_int64 _new_no_obj)		// enlarge buffers for the new number of objects
{
	uint new_no_pack = bh::common::NoObj2NoPacks(_new_no_obj);
	//DMA-520 previous partition loading not started at lastpack
	if(new_no_pack < no_pack) return;
	AppendKNs(new_no_pack);
	no_obj  = _new_no_obj;
}

bool RSIndex_CMap::UpToDate(_int64 cur_no_obj, int pack) // true, if this pack is up to date (i.e. either cur_obj==no_obj, or pack is not the last pack in the index)
{
	return (cur_no_obj == no_obj || ((pack < no_pack - 1) && (pack < (cur_no_obj / MAX_PACK_ROW_SIZE))));
}

void RSIndex_CMap::CopyPack(int pack,const void* pbuff,const int buff_len)    // 拷贝RSI资源,ADD BY  Liujs
{
	assert(pack >= start_pack && pack <= end_pack);
	if(t.get() && pack >= start_pack && pack <= end_pack)
		memcpy(t.get() + (pack - start_pack) * no_positions  * 32 / sizeof(int), pbuff, buff_len);
}

void RSIndex_CMap::ClearPack(int pack)
{
	assert(pack >= start_pack && pack <= end_pack);
	if(t.get() && pack >= start_pack && pack <= end_pack)
		memset(t.get() + (pack - start_pack) * no_positions  * 32 / sizeof(int), 0, no_positions * 32);
	// below code is for testing purpose only, should be commented
	//Set(pack, 0, 0);
	//Set(pack, 255, 63);
}

//void RSIndex_CMap::SetPack(int pack)
//{
//	assert(pack >= start_pack && pack <= end_pack);
//	if(t && pack >= start_pack && pack <= end_pack)
//		memset(t + (pack - start_pack) * no_positions * 32 / sizeof(int), 255, no_positions * 32);
//}

int RSIndex_CMap::Load(IBFile* frs_index, int current_read_loc)
{
	// current_read_loc: 0 or 1, determines which version of header and last pack should be loaded
	// File format:
	// Header0:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// Header1:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// LastPack0:	no_positions * 256 / 8 bytes
	// LastPack1:	no_positions * 256 / 8 bytes
	// <table_t>									- (no_pack - 1) * no_positions * 256 / 8 bytes

	t.reset();

	bool zero = current_read_loc == 0;
	unsigned char header[14];
	int bytes;
	bytes = (int)frs_index->Seek(zero ? 0 : 14, SEEK_SET);
	bytes = frs_index->ReadExact(header, 14);
	no_obj	= *(_int64 *)(header + 1);
	no_pack	= *(uint *)(header + 9);
	no_pack_declared = (no_pack / 30 + 1) * 30;
	no_positions = uint(header[13]);
	start_pack = 0;
	end_pack = no_pack - 1;
	if(zero) // skip Header1
		bytes = (int)frs_index->Seek(14, SEEK_CUR);
	else // skip LastPack0
		bytes = (int)frs_index->Seek(no_positions * 32, SEEK_CUR);
	if(no_pack * no_positions > 0) {
		t = MMGuard<uint>((uint*)alloc(no_pack_declared * no_positions * 32, BLOCK_TEMPORARY), *this);
		// read last pack
		bytes = frs_index->ReadExact(t.get() + (no_pack - 1) * no_positions * 32 / sizeof(int), no_positions * 32);
		if(zero) // skip LastPack1
			bytes = (int)frs_index->Seek(no_positions * 32, SEEK_CUR);
		// read all packs but last
		bytes = frs_index->ReadExact(t.get(), (no_pack - 1) * no_positions * 32);
		memset(t.get() + no_pack * no_positions * 32 / sizeof(int), 255, (no_pack_declared - no_pack) * no_positions * 32);
	}
	return 0;
}

int RSIndex_CMap::LoadLastOnly(IBFile* frs_index, int current_read_loc)
{
	// function reads the header and allocates memory for future packs.
	// Last pack is not read from disc since it will be generated from scratch
	// all values of last pack are set to 255
	//
	// current_read_loc: 0 or 1, determines which version of header and last pack should be loaded
	// File format:
	// Header0:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// Header1:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// LastPack0:	no_positions * 256 / 8 bytes
	// LastPack1:	no_positions * 256 / 8 bytes
	// <table_t>									- (no_pack - 1) * no_positions * 256 / 8 bytes

	t.reset();
	unsigned char header[14];

	bool zero = current_read_loc == 0;

	int bytes;
	bytes = (int)frs_index->Seek(zero ? 0 : 14, SEEK_SET);
	bytes = frs_index->ReadExact(header, 14);
	no_obj	= *(_int64 *)(header + 1);
	no_pack	= *(int *)(header + 9);
	no_pack_declared = (no_pack / 30 + 1) * 30;
	no_positions = uint(header[13]);
	if(no_pack * no_positions > 0) {
		end_pack = start_pack = no_pack - 1;
		if(zero) // skip Header1
			bytes = (int)frs_index->Seek(14, SEEK_CUR);
		else // skip LastPack0
			bytes = (int)frs_index->Seek(no_positions * 32, SEEK_CUR);
		t = MMGuard<uint>((uint*)alloc((no_pack_declared - start_pack) * no_positions * 32, BLOCK_TEMPORARY), *this);
		// read last pack
		bytes = frs_index->ReadExact(t.get(), no_positions * 32);
		BHASSERT(bytes == no_positions * 32,"corrupt cmap file");
	}
	return 0;
}

// file structure : 
//                    Header0  : committed data index
//                    Header1 : not committed dataindex
//                   LastPack0:  committed last pack index
//                   LastPack1:  not committed lastpack index
//|..file header0(14 bytes)...fileheader1(14bytes)...|Lastpack0...Lastpack1...firstpack......updatestart_pack.......................no_pack|
//   ^-----currentsave_loc(Header0 or 1)                                                                     ^----save packs updated  ---------^
int RSIndex_CMap::Save(IBFile* frs_index, int current_save_loc)
{
	// File format:
	// Header0:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// Header1:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// LastPack0:	no_positions * 256 / 8 bytes
	// LastPack1:	no_positions * 256 / 8 bytes
	// <table_t>									- (no_pack - 1) * no_positions * 256 / 8 bytes
	unsigned char header[14];
	int bytes;
	header[0]				= 1u; // version number
	*(_int64*)(header + 1)	= no_obj;
	*(int*) (header + 9)	= no_pack;
	header[13]				= (unsigned char)no_positions;

	bool zero = current_save_loc == 0;

	bytes = (int)frs_index->Seek(zero ? 0 : 14, SEEK_SET);
	bytes = frs_index->WriteExact(header, 14);

	if(zero) // skip Header1
		bytes = (int)frs_index->Seek(14, SEEK_CUR);
	else // skip LastPack0
		bytes = (int)frs_index->Seek(no_positions * 32, SEEK_CUR);

	if(t.get()) {
		// write LastPack
		bytes = frs_index->WriteExact(t.get() + (no_pack - start_pack - 1) * no_positions * 32 / sizeof(int), no_positions * 32);
		if(zero) // skip LastPack1
			bytes = (int)frs_index->Seek(no_positions * 32, SEEK_CUR);

		// write all the other packs
		int offset = start_pack * no_positions * 32; // position (32bit) of last pack
		bytes = (int)frs_index->Seek(offset, SEEK_CUR);
		bytes = frs_index->WriteExact(t.get(), (no_pack - start_pack - 1) * no_positions * 32);
	} else
		bytes = 0;
	if(bytes < (no_pack - start_pack - 1) * no_positions * 32)
		return 1;
	return 0;
}

int RSIndex_CMap::NoOnes(int pack, uint pos)
{
	assert(pack >= start_pack && pack <= end_pack);
	int d = 0;
	// change beg's type from int to long to refer 64 bits offset
	long beg = 0;
	for(int i = 0; i < 32 / sizeof(int); i++) {
		beg = (pack - start_pack) * no_positions * 32 / sizeof(int) + pos * 32 / sizeof(int);
		d += CalculateBinSum(t.get()[beg + i]);
	}
	return d;
}

//////////////////////////////////////////////////////////////////

RSValue RSIndex_CMap::IsValue(RCBString min_v, RCBString max_v, int pack)
{
	// Results:		RS_NONE - there is no objects having values between min_v and max_v (including)
	//				RS_SOME - some objects from this pack may have values between min_v and max_v
	//				RS_ALL	- all objects from this pack do have values between min_v and max_v

	assert(pack >= start_pack && pack <= end_pack);

	if(min_v == max_v) {
		uint size = min_v.size() < no_positions ? (uint)min_v.size() : no_positions;
		for(uint pos = 0; pos < size; pos++) {
			if(!IsSet(pack, (unsigned char)min_v[pos], pos))
				return RS_NONE;
		}
		return RS_SOME;
	} else {
		// TODO: to be further optimized	
		unsigned char f=0, l=255;
		if(min_v.len>0) f = (unsigned char)min_v[0];		// min_v.len == 0 usually means -inf
		if(max_v.len>0) l = (unsigned char)max_v[0];
		if( !IsAnySet(pack, f, l, 0) ) // 只比较第一个就可以,不是min的每一个字符都比max的小,add by liujs
			return RS_NONE;
        
		/*
		else if( IsAnySet(pack, f + 1, l - 1, 0) ) {						// not needed - RS_ALL is already determined by pack min and max
			if( !IsAnySet(pack, 0, f, 0) && !IsAnySet(pack, l, 255, 0) )
				return RS_ALL;
			else
				return RS_SOME;
		}*/
		return RS_SOME;
	}
}

RSValue	RSIndex_CMap::IsLike(RCBString pattern, int pack, char escape_character)
{
	// we can exclude cases: "ala%..." and "a_l_a%..."
	char *p = pattern.val;		// just for short...
	uint pos = 0;
	while(pos < pattern.len && pos < (uint)no_positions) {
		if(p[pos] == '%' || p[pos] == escape_character)
			break;
		if(p[pos] != '_' && !IsSet(pack, p[pos], pos))
			return RS_NONE;
		pos++;
	}
	return RS_SOME;
}

void RSIndex_CMap::PutValue(RCBString& v, int pack)		// set information that value v does exist in this pack
{
	if(v.IsNullOrEmpty())
		return;
	uint size = v.size() < no_positions ? (uint)v.size() : no_positions;

	assert(pack >= start_pack && pack <= end_pack);
	for(uint i = 0; i < size; i++) {
		Set(pack, (unsigned char)v[i], i);
		//t[pack * no_positions * 32 / sizeof(int) + i * 32 / sizeof(int) + c / 32] |= ( 0x00000001u << (c % 32) );
	}
}

//bool RSIndex_CMap::Intersection(int pack, RSIndex_CMap *sec, int pack2)
//{
//	// we may assume that min-max of packs was already checked
//	bool set1, set2;
//	bool any_set = false;
//	for(int v = 0; v <= 255; v++) {
//		// if there is at least one common char on first position return true
//		set1 = IsSet(pack, v, 0);
//		set2 = sec->IsSet(pack2, v, 0);
//		if( set1 && set2 )
//			return true;
//		if( set1 || set2 )
//			any_set = true;
//	}
//	if( !any_set )
//		return true;	// special case: no characters at all in both packs (e.g. CMAP empty because of common prefix)
//	return false;		// no intersection possible - all values do not match the second histogram
//}
//
//void RSIndex_CMap::Display(int pack, unsigned char c)
//{
//	if(pack < start_pack || pack > end_pack) {
//		cout << "Invalid pack: " << pack << endl;
//		return;
//	}
//	uint int_pack_size = no_positions * 32 / sizeof(int);
//	uint int_col_size = 32 / sizeof(int);
//	cout << c << ": ";
//	for(uint i = 0; i < no_positions; i++)
//		cout << ( (int)IsSet(pack, c, i) ) << ( (i + 1) % 8 == 0 ? " " : "");
//	cout << endl;
//}

bool RSIndex_CMap::IsSet(int pack, unsigned char c, uint pos)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(t.get());
	//uint int_pack_size = no_positions * 32 / sizeof(int);
	//uint int_col_size = 32 / sizeof(int);
	//uint n = t[pack * int_pack_size + pos * int_col_size + c / 32];
	assert(pack >= start_pack && pack <= end_pack);
	return ( (t.get()[(pack - start_pack) * no_positions * 32 / sizeof(int) + pos * 32 / sizeof(int) + c / 32] >> (c % 32)) % 2 == 1 ) ;
}

// true, if there is at least one 1 in [first, last]
bool RSIndex_CMap::IsAnySet(int pack, unsigned char first, unsigned char last, uint pos)
{	// TODO: could be done more efficiently
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(first <= last);
	for(int c = first; c <= last; c++)
		if(IsSet(pack, c, pos))
			return true;
	return false;
}

void RSIndex_CMap::Set(int pack, unsigned char c, uint pos)
{
	//uint int_pack_size = no_positions * 32 / sizeof(int);
	//uint int_col_size = 32 / sizeof(int);
	assert(pack >= start_pack && pack <= end_pack);
	t.get()[(pack - start_pack) * no_positions * 32 / sizeof(int) + pos * 32 / sizeof(int) + c / 32] |= ( 0x00000001u << (c % 32) );
}

//void RSIndex_CMap::UnSet(int pack, unsigned char c, uint pos)
//{
//	//uint int_pack_size = no_positions * 32 / sizeof(int);
//	//uint int_col_size = 32 / sizeof(int);
//	assert(pack >= start_pack && pack <= end_pack);
//	t[(pack - start_pack) * no_positions * 32 / sizeof(int) + pos * 32 / sizeof(int) + c / 32] |= ( 0x00000000u << (c % 32) );
//}

void RSIndex_CMap::AppendKN(int pack, RSIndex_CMap* cmap, uint no_values_to_add)
{
	uint no_objs_to_add_to_last_DP = uint(no_obj % MAX_PACK_ROW_SIZE ? MAX_PACK_ROW_SIZE - no_obj % MAX_PACK_ROW_SIZE : 0);
	if(pack < cmap->no_pack) {
		AppendKNs(no_pack + 1);
		memcpy(&t.get()[(no_pack -1 - start_pack) * no_positions * 32 / sizeof(int)],
			   &cmap->t.get()[(pack - cmap->start_pack) * no_positions * 32 / sizeof(int)], no_positions * 32);
		no_obj  += no_objs_to_add_to_last_DP + no_values_to_add;
	}
}



