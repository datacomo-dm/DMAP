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

#include "RCAttr.h"
#include "RCAttrPack.h"
#include "bintools.h"
#include "compress/NumCompressor.h"
#include "WinTools.h"
#include "system/MemoryManagement/MMGuard.h"
#include "zlib.h"


AttrPackN_Zip::AttrPackN_Zip(PackCoordinate pc, AttributeType attr_type, int inserting_mode, DataCache* owner)
	:	AttrPackN(pc, attr_type, inserting_mode,owner)
{
}

/*
AttrPackN_Zip::AttrPackN_Zip(const AttrPackN_Zip &apn)
{
	assert(!is_only_compressed);
	value_type = apn.value_type;
	bit_rate = apn.bit_rate;
	max_val = apn.max_val;
	optimal_mode = apn.optimal_mode;
	data_full = 0;
	if(no_obj && data_full) {
		data_full = alloc(value_type * no_obj, BLOCK_UNCOMPRESSED);
		memcpy(data_full,get_data_full, value_type * no_obj);
	}
}
*/
std::auto_ptr<AttrPack> AttrPackN_Zip::Clone() const
{
	return std::auto_ptr<AttrPack>(new AttrPackN_Zip(*this) );
}

template<typename etype> void AttrPackN_Zip::_RemoveNullsAndCompress(etype e,char* tmp_comp_buffer, uint & tmp_cb_len, _uint64 & maxv)
{
    MMGuard<etype> tmp_data;
	if(no_nulls > 0) {
		tmp_data = MMGuard<etype>((etype*) (alloc((no_obj - no_nulls) * sizeof(etype), BLOCK_TEMPORARY)), *this);
		for(uint i = 0, d = 0; i < no_obj; i++) {
			if(!IsNull(i))
				tmp_data[d++] = ((etype*) (data_full))[i];
		}
	} 
	else
	{
		tmp_data = MMGuard<etype>((etype*)data_full, *this, false);
	}

	uLongf _tmp_tmp_cb_len = tmp_cb_len;
	int rt = compress2((Bytef*)tmp_comp_buffer,(uLongf*)&_tmp_tmp_cb_len,(Bytef*)tmp_data.get(),(uLong)((no_obj - no_nulls) * sizeof(etype)),/*Z_DEFAULT_COMPRESSION*/Z_BEST_SPEED);
	if(rt!=Z_OK) 
    {					
        rclog << lock << "ERROR: AttrPackN_Zip RemoveNullsAndCompress compress2 error." << unlock;
		BHASSERT(0, "ERROR: AttrPackN_Zip RemoveNullsAndCompress compress2 error..");
    }	
	tmp_cb_len = _tmp_tmp_cb_len;
}

template<typename etype> void AttrPackN_Zip::_DecompressAndInsertNulls(etype e,uint *& cur_buf)
{
	uint _buf_len = *cur_buf;
	uLongf _tmp_dest_len = value_type * no_obj;
	Bytef *_pbuf = (Bytef*)(cur_buf+3);
    int rt = uncompress((Bytef*)data_full,(uLongf *)&_tmp_dest_len,_pbuf,_buf_len);
	if(rt!=Z_OK) 
    {					
        rclog << lock << "ERROR: AttrPackN_Zip uncompress error." << unlock;
		BHASSERT(0, "ERROR: AttrPackN_Zip uncompress error..");
    }	
    etype *d = ((etype*)(data_full)) + no_obj - 1;
    etype *s = ((etype*)(data_full)) + no_obj - no_nulls - 1;
    for(int i = no_obj - 1;d > s;i--){
        if(IsNull(i))
            --d;
        else
            *(d--) = *(s--);
    }
}

// 数值类型数据压缩，将0的个数压缩后，放入compressed_buf前面，非0的数据压缩后放入compressed_buf缓存的后面
CompressionStatistics AttrPackN_Zip::Compress(DomainInjectionManager& dim)		// Create new optimal compressed buf. basing on full data.
{
	MEASURE_FET("AttrPackN::Compress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	std::stringstream s1;
	s1 << "aN[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
	FETOperator fet1(s1.str());
#endif

	/////////////////////////////////////////////////////////////////////////
	uint *cur_buf = NULL;
	uint buffer_size = 0;
	MMGuard<char> tmp_comp_buffer;

	uint tmp_cb_len = 0;
	SetModeDataCompressed();

    // 直接压缩data_full是不是更简单呢?
	_uint64 maxv = 0;
	if(data_full) {		// else maxv remains 0
		_uint64 cv = 0;
		for(uint o = 0; o < no_obj; o++) {
			if(!IsNull(o)) {
				cv = (_uint64)GetVal64(o);
				if(cv > maxv)
					maxv = cv;
			}
		}
	}

    // 1>  remove nulls and compress 
	if(maxv != 0) {
		//BHASSERT(last_set + 1 == no_obj - no_nulls, "Expression evaluation failed!");

		if(value_type == UCHAR) {			
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uchar) + 20;
			uchar e;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			_RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else if(value_type == USHORT) {
			tmp_cb_len = (no_obj - no_nulls) * sizeof(ushort) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			ushort e;
			_RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else if(value_type == UINT) {
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uint) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			uint e;
			_RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else {
			tmp_cb_len = (no_obj - no_nulls) * sizeof(_uint64) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			_uint64 e;
			_RemoveNullsAndCompress(e,tmp_comp_buffer.get(), tmp_cb_len, maxv);
		}
		buffer_size += tmp_cb_len;
	}
	buffer_size += 12;   // 12 = sizeof(tmp_cb_len) + sizeof(maxv)

	////////////////////////////////////////////////////////////////////////////////////
	// 2> compress nulls
	uint null_buf_size = ((no_obj+7)/8);
	MMGuard<uchar> comp_null_buf;
	//IBHeapAutoDestructor del((void*&)comp_null_buf, *this);
	if(no_nulls > 0) {
		if(ShouldNotCompress()) {
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((null_buf_size + 2) * sizeof(char), BLOCK_TEMPORARY), *this);
			uint cnbl = null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun
			
			int rt = 0;
			uLongf _tmp_null_buf_size= null_buf_size;
			rt = compress2((Bytef*)comp_null_buf.get(), (uLongf *)&_tmp_null_buf_size, (Bytef*) nulls, ((no_obj + 7) / 8),/*Z_DEFAULT_COMPRESSION*/Z_BEST_SPEED); 
			if(Z_OK == rt) 
			{
				null_buf_size = _tmp_null_buf_size;
				SetModeNullsCompressed();
			}
			else 
			{
				ResetModeNullsCompressed();
				rclog << lock << "ERROR: compress2 return error. (N)." << unlock;
				BHASSERT(0, "ERROR: compress2 return error. (N f).");					
			}
		}
		buffer_size += null_buf_size + 2;
	}
	dealloc(compressed_buf);
	compressed_buf = 0;
	compressed_buf= (uchar*)alloc(buffer_size*sizeof(uchar), BLOCK_COMPRESSED);

	if(!compressed_buf) rclog << lock << "Error: out of memory (" << buffer_size << " bytes failed). (29)" << unlock;
	memset(compressed_buf, 0, buffer_size);
	cur_buf = (uint*)compressed_buf;
	if(no_nulls > 0) 
	{
		if(null_buf_size > 8192)
			throw DatabaseRCException("Unexpected bytes found (AttrPackN_Zip::Compress).");
#ifdef _MSC_VER
		__assume(null_buf_size <= 8192);
#endif
		*(ushort*) compressed_buf = (ushort) null_buf_size;
#pragma warning(suppress: 6385)
		memcpy(compressed_buf + 2, comp_null_buf.get(), null_buf_size);
		cur_buf = (uint*) (compressed_buf + null_buf_size + 2);
	}

	////////////////////////////////////////////////////////////////////////////////////

	*cur_buf = tmp_cb_len;
	*(_uint64*) (cur_buf + 1) = maxv;  
	memcpy(cur_buf + 3, tmp_comp_buffer.get(), tmp_cb_len);
	comp_buf_size = buffer_size;
	compressed_up_to_date = true;

	CompressionStatistics stats;
	stats.new_no_obj = NoObjs();
	return stats;
}

void AttrPackN_Zip::Uncompress(DomainInjectionManager& dim)		// Create full_data basing on compressed buf.
{
	MEASURE_FET("AttrPackN::Uncompress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto1(string("aN[") + boost::lexical_cast<string>(pc_column( GetPackCoordinate())) + "].Compress(...)");
	NotifyDataPackDecompression(GetPackCoordinate());
#endif
	is_only_compressed = false;
	if(IsModeNoCompression())
		return;
	assert(compressed_buf);
	uint *cur_buf=(uint*)compressed_buf;
	if(data_full == NULL && bit_rate > 0 && value_type * no_obj)
		data_full = alloc(value_type * no_obj, BLOCK_UNCOMPRESSED);

	///////////////////////////////////////////////////////////////
	// decompress nulls
	if(no_nulls > 0) 
	{
		uint null_buf_size = 0;
		if(nulls == NULL)
			nulls = (uint*) alloc(2048 * sizeof(uint), BLOCK_UNCOMPRESSED);

		if(no_obj < 65536)
			memset(nulls, 0, 8192);
		null_buf_size = (*(ushort*) cur_buf);
		if(null_buf_size > 8192)
			throw DatabaseRCException("Unexpected bytes found in data pack (AttrPackN::Uncompress).");
		if(!IsModeNullsCompressed() ) // no nulls compression
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else {
			int rt = 0;
			uLongf _tmp_no_nulls = no_nulls;
			rt = uncompress((Bytef*)nulls,(uLongf*)&_tmp_no_nulls,(Bytef*)(((uchar*)cur_buf) + 2),(uLongf)null_buf_size);
            if( Z_OK != rt)
            {
				rclog << lock << "ERROR: buffer overrun by uncompress." << unlock;
				BHASSERT(0, "ERROR: uncompress return != Z_OK .");
            }
			// For tests:
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
			uint nulls_counted = 0;
			for(uint i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				throw DatabaseRCException("AttrPackN::Uncompress uncompressed wrong number of nulls.");
#endif
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
	}

	////////////////////////////////////////////////////////////////////////////////////
	// decompress non null object
	if(!IsModeValid()) 
	{
		rclog << lock << "Unexpected byte in data pack (AttrPackN_Zip)." << unlock;
		throw DatabaseRCException("Unexpected byte in data pack (AttrPackN_Zip).");
	} 
	else 
	{
		if(IsModeDataCompressed() && bit_rate > 0 && *(_uint64*) (cur_buf + 1) != (_uint64) 0) {
			if(value_type == UCHAR) {
				uchar e;
				_DecompressAndInsertNulls(e,cur_buf);
			} else if(value_type == USHORT) {
				ushort e;
				_DecompressAndInsertNulls(e,cur_buf);
			} else if(value_type == UINT) {
				uint e;
				_DecompressAndInsertNulls(e,cur_buf);
			} else {
				_uint64 e;
				_DecompressAndInsertNulls(e,cur_buf);
			}
		} else if(bit_rate > 0) {
			for(uint o = 0; o < no_obj; o++)
				if(!IsNull(int(o)))
					SetVal64(o, 0);
		}
	}

	compressed_up_to_date = true;	
	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;

#ifdef FUNCTIONS_EXECUTION_TIMES
	SizeOfUncompressedDP += (rc_msize(data_full) + rc_msize(nulls));
#endif
}
