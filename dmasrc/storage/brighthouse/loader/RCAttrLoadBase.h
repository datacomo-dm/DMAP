/* Copyright (C) 2005-2008 Infobright Inc. */

#ifndef LOADER_RCATTRLOADBASE_H_INCLUDED
#define LOADER_RCATTRLOADBASE_H_INCLUDED

#include <boost/optional.hpp>
#include <boost/any.hpp>
#include "common/mysql_gate.h"
#include "core/RCAttr.h"
#include "system/ib_system.h"



class NewValuesSetBase;

class RCAttrLoadBase : public RCAttr
{
public:
	RCAttrLoadBase(int a_num,int t_num,std::string const& a_path,int conn_mode=0,unsigned int s_id=0, bool loadapindex=true, DTCollation collation = DTCollation()) throw(DatabaseRCException);
	RCAttrLoadBase(int a_num, AttributeType a_type, int a_field_size, int a_dec_places, uint param, DTCollation collation, bool compress_lookups, std::string const& path_ );
	virtual ~RCAttrLoadBase();
	void LoadPackInfoForLoader();
	void LoadPackIndexForLoader(const char *partname,int sessionid,bool ismerge=false,int from_truncate_part = 0);
	void LoadPartionForLoader(uint connid,int sessionid,bool appendmode=false);
	void ExpandDPNArrays();
	
	void SavePartitionPackIndex(DMThreadData *tl);
	/*!
	 * \param pack_already_prepared: false - prepare DP, true - new DP was prepared/created
	 */
	int LoadData(NewValuesSetBase* nvs, bool force_values_coping, bool force_saveing_pack = false, bool pack_already_prepared = false);
	bool PreparePackForLoad();

	void UnlockPackFromUse(unsigned pack_no);
	void LockPackForUse(unsigned pack_no);
	void LoadPack(uint n);
	virtual int Save(bool for_insert = false) = 0;
	DPN& CreateNewPackage(bool for_mysqld_insert = true,int for_mysqld_merge = 0);
    static DWORD WINAPI WINAPI SavePackThread(void *params);
    void UpdateRSI_Hist(int pack, int no_objs);
    void UpdateRSI_CMap(int pack, int no_objs, bool new_prefix = true);
    void threadJoin(int n);
    void threadCancel(int n);
    inline void WaitForSaveThreads() { DoWaitForSaveThreads(); }
    inline int SavePack(int n,bool reset_pack = true) { return DoSavePack(n,reset_pack); }

    void	InitKNsForUpdate();

	void SetPackrowSize(uint packrow_size) { this->packrow_size = packrow_size; }
	uint GetPackrowSize() const { return packrow_size; }

	int64	GetNoOutliers() const;
	void	AddOutliers(int64 no_outliers);

protected:
	void LoadPackInherited(int n);
    virtual int DoSavePack(int n,bool reset_pack = true) =0;
    virtual void DoWaitForSaveThreads() =0;
    void LogWarnigs();
    RCDataTypePtr GetMinValuePtr(int pack);
    RCDataTypePtr GetMaxValuePtr(int pack);
    void GetMinMaxValuesPtrs(int pack, RCDataTypePtr & out_min, RCDataTypePtr & out_max);
    void SetPackMax(uint pack, RCBString& s);
    void SetPackMin(uint pack, RCBString& s);
    inline uint RoundUpTo8Bytes(RCBString& s);

    DPN		LoadDataPackN(const DPN& source_dpn, NewValuesSetBase* nvs, _int64& load_min, _int64& load_max, int& load_nulls);
    bool 	UpdateGlobalMinAndMaxForPackN(const DPN& dpn);
    bool	UpdateGlobalMinAndMaxForPackN(int no_obj, _int64 load_min, _int64 load_max, int load_nulls); // returns true if the new data lie outside of the current i_min-i_max
    void	UpdateUniqueness(const DPN& old_dpn, DPN& new_dpn, _int64 load_min, _int64 load_max, int load_nulls, bool is_exclusive, NewValuesSetBase* nvs);
    void	UpdateUniqueness(DPN& dpn, bool is_exclusive);
    bool 	HasRepetitions(DPN & new_dpn, const DPN & old_dpn, int load_obj, int load_nulls, NewValuesSetBase *nvs);


private:
    RCBString MinS(Filter *f);
    RCBString MaxS(Filter *f);
    inline void CompareAndSetCurrentMin(RCBString tstmp, RCBString & min, bool set);
    inline void CompareAndSetCurrentMax(RCBString tstmp, RCBString & min);

public:
    // �ֲ�ʽ�����������ݽ��кϲ�װ�����
    typedef struct node_sorted_data_file{
        std::string dpn_name;
        std::string rsi_name;
        int pack_no;
        std::vector<std::string> pack_name_lst;
        node_sorted_data_file(){
            pack_no = 0;
            pack_name_lst.clear();
        }        
    }node_sorted_data_file,*node_sorted_data_file_ptr;

protected:
    // ����rsi,pack = NoPack()-packs_omitted    
    void CopyRSI_Hist(int pack, const char* p_rsi_buff,const int rsi_buff_size);
    void CopyRSI_CMap(int pack, const char* p_rsi_buff,const int rsi_buff_size);
    
    // ��ȡ׼��װ��������ļ��б���Ϣ
    int get_sorted_data_file_info(const int attr_num,
                                    const std::string sessionid,
                                    const std::string sorted_data_path,
                                    std::vector<node_sorted_data_file>& sorted_data_file_lst);

    // ��ȡ���������ļ��б�(�ڵ����ɺ������İ�����,DMA-1319)
    int get_sorted_ldb_file_info(const int attr_num,
                                    const std::string sessionid,
                                    const std::string sorted_data_path,
                                    std::vector<std::string>& ldb_file_lst);
    

    // ��ȡ���нڵ�dpn��pack��Ŀ
    int get_pack_num_from_file_info(std::vector<node_sorted_data_file>& sorted_data_file_lst);

    // ͨ��dpn�ļ���ȡ�������ݰ���
    int get_sorted_packno(const std::string& sorted_dpn_file);

    // ��pack�ж�ȡһ����ֵ�������ݰ�
    int read_one_packn(IBFile & pack_handle,char* p_pack_buff,uint& pack_buff_size,int& read_pack_size);

    // ��pack�ж�ȡһ���ַ��������ݰ�
    int read_one_packs(IBFile & pack_handle,char* p_pack_buff,uint& pack_buff_size,int& read_pack_size);

    // �ϲ�һ�����ݰ��Ĺ���(һ������ݰ�)
    int load_one_packs(AttrPack* one_pack,const bool first_merge,bool pack_already_prepared = false);

    // �ϲ�һ��packn���ݰ�(ȫnull,����ֵ��ͬ��)
    int load_one_spec_packs(const int no_nulls,const bool first_merge,bool pack_already_prepared = false);

    // �ϲ�һ��packn���ݰ�(һ������ݰ�)
    int load_one_packn(AttrPack* one_pack,
                        const _int64 local_min,
                        const _int64 local_max,
                        const bool first_merge,
                        bool pack_already_prepared = false);

    // ����dpn�е�pack�����ڴ�
    int load_current_pack(const int current_pack);

    // �ϲ�һ��packn���ݰ�(ȫnull,����ֵ��ͬ��)
    int load_one_spec_packn(const int load_no_nulls,
                        const int load_no_objs,
                        const _int64 load_min, 
                        const _int64 load_max,
                        const bool first_merge,
                        bool pack_already_prepared = false);
    

    // ����һ��dpn(һ������ݰ�)
    DPN load_data_packn(const DPN& source_dpn, 
                        AttrPack* one_pack, 
                        const _int64 local_min,
                        const _int64 local_max,
                        _int64& load_min, 
                        _int64& load_max, 
                        int& load_nulls);

    // ����һ��dpn(ȫnull,����ֵ��ͬ��)
    DPN load_data_spec_packn(const DPN& source_dpn, 
                        const int load_no_nulls,
                        const int load_no_objs,
                        const _int64 load_min, 
                        const _int64 load_max);
    
    // �ֲ�ʽ�����������ݽ��кϲ�װ�����
    int merge_table_from_sorted_data(const std::string& sorted_data_path,
                                    const std::string& sorted_sessionid,
                                    const std::string& datapart_name,
                                    const _int64& rownums);
    // ɾ��������������
    bool truncate_partition_from_mysql(const std::string& datapart_name,std::vector<int> & del_pack_file_list);

        
    // ���ػ����ڲ��ύ����,xxx.ntrunc�ļ�������
    bool commit_long_session_merge_data(const std::string & datapart_name,std::vector<int> & del_pack_file_list);

    // �ϲ�leveldb�İ���������
    int  merge_packindex_data(const std::string& sorted_data_path,
                                    const std::string& sorted_sessionid,
                                    const bool bltsession);

    
public:
	
    // �ϲ��̻Ự����
    int merge_short_session_data(const std::string& sorted_data_path,
                                    const std::string& sorted_sessionid,
                                    const std::string& datapart_name,
                                    const _int64& rownums);
    
    // �ϲ����Ự����
    int merge_long_session_data(const std::string& sorted_data_path,
                                    const std::string& sorted_sessionid,
                                    const std::string& datapart_name,
                                    const _int64& rownums);

protected:
    bool illegal_nulls;
    uint packrow_size;

private:
    int64 no_outliers;
    mutable IBMutex no_outliers_mutex;
};

#endif /* not LOADER_RCATTRLOADBASE_H_INCLUDED */
