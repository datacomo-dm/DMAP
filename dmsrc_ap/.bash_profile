# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs
DATAMERGER_HOME=/data/dt2/app/dma
LANG=zh_CN
#ORACLE_BASE=/app/ora
#ORACLE_HOME=$ORACLE_BASE/product/10.1.0/db_1
PATH=$PATH:$DATAMERGER_HOME/bin:./:$DATAMERGER_HOME/oci
#ORACLE_TERM=vt100
#NLS_LANG="AMERICAN_AMERICA.ZHS16CGB231280"
NLS_LANG="SIMPLIFIED CHINESE_CHINA.ZHS16GBK"
#ORA_NLS33=$ORACLE_HOME/ocommon/nls/admin/data
LD_LIBRARY_PATH=$DATAMERGER_HOME/lib:$DATAMERGER_HOME/oci
#uac p 0
ulimit -c unlimited
ulimit -u 16382 
ulimit -n 65534
ulimit -d 4000000
#ulimit -s 22920
#ORACLE_SID=dtagt
export PATH LANG NLS_LANG LD_LIBRARY_PATH DATAMERGER_HOME
unset USERNAME
 export WDBI_LOGPATH=$DATAMERGER_HOME/wdbilog/
 export DP_LOOP=0
 export DP_MTLIMIT=100
 export DP_INDEXNUM=50
 export DP_ECHO=1
 export DP_VERBOSE=0
 export DP_DSN=dp
 export DP_MUSERNAME=root
 export DP_MPASSWORD=JTLXOWOOP1VXWRX5XZT1Z7HADOIPJOLFBN
 export DP_WAITTIME=60
 export DP_THREADNUM=1
 export DP_CORELEVEL=2
 export DP_BLOCKSIZE=1000
 export DP_LOADTIDXSIZE=50

