#include <string>
#include <vector>

#include "TableTest.h"
#include "DIMParameters.h"
#include "TestOfDeleteMaskConsistency.h"
#include "TestOfNoObjInColumnsEquality.h"
#include "TestOfNoObjFirstColumnVsDelMask.h"
#include "TestOfLookups.h"
#include "TestOfMaxLookupVsDPN.h"
#include "TestOfKNsConsistencyLight.h"
#include "TestOfPackFileOverlapping.h"
#include "core/RCAttr.h"
#include "core/Filter.h"
#include "core/RCAttrPack.h"
#include "edition/local.h"

#include "boost/filesystem.hpp"
#include "boost/shared_ptr.hpp"


using namespace std;
using namespace boost::filesystem;
using namespace boost;

TableTest::TableTest(string& table_path)
: table_path(table_path)
{
	vector<string> tp_dec = TableTest::ParseTablePath(table_path);
	data_dir = tp_dec[0];
	database_name = tp_dec[1];
	table_name = tp_dec[2].substr(0, tp_dec[2].find('.'));
	column_name = string("");
//	ab_switch = (ABSwitcher()).GetState(table_path);
}

TableTest::~TableTest(void)
{
}

bool TableTest::IsEmptyDelMask() {
	return (rc_table->GetDeleteMask()==0);
}

bool TableTest::IsEmptyRCTable() {
	return (rc_table==0);
}

Filter& TableTest::GetDeleteMask() {
	return *(rc_table->GetDeleteMask());
}

RCTable& TableTest::GetRCTable() {
	return *rc_table;
}

//string TableTest::GetColumnName()
//{
//	if(column_number==-1)
//		return string("");
//	else
//		return string(rc_table->AttrName(column_number));
//}


void TableTest::Init(const string& kn_folder, const string& ext_tool)
{	
	try {
		vector<DTCollation> charsets;
		rc_table = shared_ptr<RCTable>(new RCTable(table_path.c_str(), charsets, 0));
	} catch (...) {
		rc_table.reset();
	}

	// delete mask loading
	string table_path = data_dir + DIR_SEPARATOR_CHAR + database_name + DIR_SEPARATOR_CHAR + table_name + string(".bht");
	string ab_switch_path = table_path + DIR_SEPARATOR_CHAR + string("ab_switch");
	string del_mask_alt_path = table_path + DIR_SEPARATOR_CHAR + string("del_mask_alt.flt");
	string del_mask_path = table_path + DIR_SEPARATOR_CHAR + string("del_mask.flt");
	try {
		if(exists(ab_switch_path)) {
			if(exists(del_mask_alt_path))
				rc_table->SetDeleteMask(shared_ptr<SavableFilter>(new SavableFilter(del_mask_alt_path.c_str())));
		} else {
			if(exists(del_mask_path))
				rc_table->SetDeleteMask(shared_ptr<SavableFilter>(new SavableFilter(del_mask_path.c_str())));
		}
	} catch (...) {
		rc_table.reset();
	}

	// tests preparation
	t.push_back(new TestOfDeleteMaskConsistency(table_path, this));
	t.push_back(new TestOfNoObjInColumnsEquality(table_path, this));
	t.push_back(new TestOfNoObjFirstColumnVsDelMask(table_path, this));
	if(rc_table) {
		for(uint i = 0; i<rc_table->NoAttrs(); i++) {
			if(rc_table && rc_table->IsLookup(i)) {
				t.push_back(new TestOfLookups(table_path, this, i));
				t.push_back(new TestOfMaxLookupVsDPN(table_path, this, i));
			}
			if(kn_folder!="")
				t.push_back(new TestOfKNsConsistencyLight(table_path, this, i, kn_folder));
			t.push_back(new TestOfPackFileOverlapping(table_path, this, i));
		}
	} 
}

void TableTest::InitColumn(const string& col_name)
{
}

pair<int, string> TableTest::Run() throw(DatabaseRCException)
{
	DIMParameters::LogOutput() << "For db: " << database_name << "  tab: " << table_name << endl;
	int res = 0;
	for(vector<DataIntegrityTest*>::iterator it = t.begin(); it != t.end(); ++it)
	{
		//(*it)->RunDataIntegrityTest();		
		
		int res_t = (*it)->RunDataIntegrityTest();		
		if(res_t > res)
			res = res_t;
	}
	//return res;
	if (res == 1)
		return make_pair<int, string>(1, string(""));
	else if(res == 0)
		return make_pair<int, string>(0, string("All tests were abandoned."));
	else
		return make_pair<int, string>(2, string("Corruption for table ")+table_name+string(" found."));
}

vector<string> TableTest::ParseTablePath(const string& path_to_parse)
{
	assert((*(path_to_parse.rbegin())) != '\\' && (*(path_to_parse.rbegin())) != '/');
	vector<string> res;
	size_t i_last0 = path_to_parse.find_last_of("\\/");	
	size_t i_last1 = path_to_parse.find_last_of("\\/", i_last0 - 1);	
	res.push_back(path_to_parse.substr(0, i_last1));							// data_dir
	res.push_back(path_to_parse.substr(i_last1+1, i_last0 - i_last1 - 1));	// database name
	res.push_back(path_to_parse.substr(i_last0+1));							// table name (with '.bht')
	return res;
}

int TableTest::ExistsProperDelMaskInFolder()
{
	// 0 = none of ab_switch, del_mask_alt, del_mask exists in folder (e.g. ICE table)
	// 1 = no proper file for delete mask
	// 2 = proper delete mask exists

	string ab_switch_path = table_path + DIR_SEPARATOR_CHAR + string("ab_switch");
	string del_mask_alt_path = table_path + DIR_SEPARATOR_CHAR + string("del_mask_alt.flt");
	string del_mask_path = table_path + DIR_SEPARATOR_CHAR + string("del_mask.flt");

	if(exists(ab_switch_path)) {
		if(exists(del_mask_alt_path))
			return 2;
		else
			return 1;
	}
	else {
		if(exists(del_mask_path))
			return 2;
		else
			if(exists(del_mask_alt_path))
				return 1;
			else
				return 0;
	}
}
