#include "ibfilewrapper.h"
#include <stdio.h>
#include <stdlib.h>

const char* pmsg = "adfdump: 1).将整理完成的数据文件转换成可以直接装入的inforbright文件\n"
                   "adfdump [source datafile(in)] [ibload datafile(out)]\n"
                   "example : adfdump /data/data/dt2/app/dma/data/dm_test.tb_test1_1_1_1.dat /tmp/dm_test.tb_test1_1_1_1.bin\n\n"
                   "adfdump: 2).校验整理完成后的数据文件是否正确\n"
                   "adfdump [source datafile(in)] [startrow(in)] [endrow(in)]\n"
                   "example : adfdump /data/data/dt2/app/dma/data/dm_test.tb_test1_1_1_1.dat 0 100\n";

int main(int argc,char **argv)
{
    if(argc<3 || argc>4) {
        printf(pmsg);
        return 1;
    }
    char *filename=argv[1];
    IBDataFile ibf;
    ibf.SetFileName(filename);
    try {
        if(argc==3) {
            FILE *fout=fopen(argv[2],"w+b");
            printf("Begin dump data file ...\n");
            ibf.Pipe(fout,NULL);
            printf("Compete.\n");
            fclose(fout);
        }
        if(argc==4) {
            int rowseek=atoi(argv[2]);
            int rowend=atoi(argv[3]);
            FILE *fcheck=fopen(argv[1],"rb");
            int rowsize=0;
            int rows=0;
            long offset=0;

            while(rows++<rowend) {
                if(fread(&rowsize,2,1,fcheck)<1) break;
                if(rows-1==rowseek)
                    printf("row %d located at :%ld(%x).\n",rowseek,offset,offset);
                offset+=rowsize+2;
                if(fseek(fcheck,offset,SEEK_SET)==-1)
                    break;
            }
            if(rows<rowend) {
                printf("Seek failed! last offset: row %d at %ld(%lx).\n",rows,offset,offset);
            } else    printf("row %d located at :%ld(%x).\n",rowend,offset,offset);
            fclose(fcheck);
        }
    } catch (char *errmsg) {
        printf("Error ocurs:%s.\n",errmsg);
        return 2;
    }

    return 0;
}


