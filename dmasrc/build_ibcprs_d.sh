# 调试版本libibcompress_d.a生成
echo "编译目录必须在:/home/dm6/dmdsrc/build/community/debug/infobright 中"
echo "当前目录`pwd`" 
echo 

# 编译ibcompress/ibdecompress库
echo "rm libibcompress_d.a"
rm libibcompress_d.a
echo

# 清除：RCDataType.o,RCDateTime.o
echo "1. 清除：RCDataType.o,RCDateTime.o"

echo "rm storage/brighthouse/types/RCDataType.o"
rm storage/brighthouse/types/RCDataType.o
echo

echo "rm storage/brighthouse/types/RCDateTime.o"
rm storage/brighthouse/types/RCDateTime.o
echo

# 重新编译RCDataType.o
echo "2.重新编译RCDataType.o"
g++ -g3 -ggdb3 -O0 -fno-inline -std=c++0x -DPURE_LIBRARY  -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated   -g3 -ggdb3 -O0 -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I/data/dt2/dm6/dmdsrc/src/storage/brighthouse -I/data/dt2/dm6/dmdsrc/src/storage/brighthouse/community -I../.. -I/data/dt2/dm6/dmdsrc/vendor/mysql/include -I/data/dt2/dm6/dmdsrc/build/community/debug/vendor/include -I/data/dt2/dm6/dmdsrc/vendor/mysql/sql -I/data/dt2/dm6/dmdsrc/vendor/mysql/regex -I/data/dt2/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I/data/dt2/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  /data/dt2/dm6/dmdsrc/src/storage/brighthouse/types/RCDataType.cpp -c -o storage/brighthouse/types/RCDataType.o

# 重新编译RCDateTime.o
echo "3.重新编译RCDateTime.o"
g++ -g3 -ggdb3 -O0 -fno-inline -std=c++0x -DPURE_LIBRARY -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated   -g3 -ggdb3 -O0 -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I/data/dt2/dm6/dmdsrc/src/storage/brighthouse -I/data/dt2/dm6/dmdsrc/src/storage/brighthouse/community -I../.. -I/data/dt2/dm6/dmdsrc/vendor/mysql/include -I/data/dt2/dm6/dmdsrc/build/community/debug/vendor/include -I/data/dt2/dm6/dmdsrc/vendor/mysql/sql -I/data/dt2/dm6/dmdsrc/vendor/mysql/regex -I/data/dt2/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I/data/dt2/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  /data/dt2/dm6/dmdsrc/src/storage/brighthouse/types/RCDateTime.cpp -c -o storage/brighthouse/types/RCDateTime.o

# 重新编译RCBString.o
echo "4.重新编译RCBString.o"
g++ -g3 -ggdb3 -O0 -fno-inline -std=c++0x -DPURE_LIBRARY -DlibIBCompress_DEF -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS   -m64 -fprefetch-loop-arrays   -fno-strict-aliasing -fno-merge-constants -fno-gcse  -Wno-deprecated   -g3 -ggdb3 -O0 -fno-inline -D_THREAD_SAFE -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -m64 -fprefetch-loop-arrays -fno-strict-aliasing -fno-merge-constants -fno-gcse -Wno-deprecated -pthread -I/usr/local/boost_1_42_0/include -Wmissing-include-dirs -DHAVE_CONFIG_H -D__BH_COMMUNITY__ -DMYSQL_SERVER -I. -I/data/dt2/dm6/dmdsrc/src/storage/brighthouse -I/data/dt2/dm6/dmdsrc/src/storage/brighthouse/community -I../.. -I/data/dt2/dm6/dmdsrc/vendor/mysql/include -I/data/dt2/dm6/dmdsrc/build/community/debug/vendor/include -I/data/dt2/dm6/dmdsrc/vendor/mysql/sql -I/data/dt2/dm6/dmdsrc/vendor/mysql/regex -I/data/dt2/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/include -I/data/dt2/dm6/dmdsrc/vendor/mysql/extra/yassl/taocrypt/mySTL  /data/dt2/dm6/dmdsrc/src/storage/brighthouse/types/RCBString.cpp -c -o storage/brighthouse/types/RCBString.o

# 连接生成库
echo
echo "5.生成静态库libibcompress_d.a"
echo "ar crv libibcompress_d.a ./storage/brighthouse/compress/*.o ./storage/brighthouse/core/QuickMath.o  ./storage/brighthouse/system/TextUtils.o ./storage/brighthouse/types/*.o"
ar crv libibcompress_d.a ./storage/brighthouse/compress/*.o ./storage/brighthouse/core/QuickMath.o  ./storage/brighthouse/system/TextUtils.o ./storage/brighthouse/types/*.o
echo

echo "cp libibcompress_d.a /tmp"
cp libibcompress_d.a /tmp
