#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#ifndef TIMEOFDAY
#define TIMEOFDAY CLOCK_PROCESS_CPUTIME_ID
#endif

class mytimer {
#ifdef WIN32 
	LARGE_INTEGER fcpu,st,ct;
#else
	struct timespec st,ct;
#endif
public:
	mytimer() {
#ifdef WIN32
	 QueryPerformanceFrequency(&fcpu);
	 QueryPerformanceCounter(&st);
	 ct.QuadPart=0;
#else
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
#endif
	}
	void Clear() {
#ifdef WIN32
		ct.QuadPart=0;
#else
		memset(&ct,0,sizeof(timespec));
#endif
	}
	void Start() {
#ifdef WIN32
	 QueryPerformanceCounter(&st);
#else
	 clock_gettime(TIMEOFDAY,&st);
#endif
	}
	void Stop() {
#ifdef WIN32
	 LARGE_INTEGER ed;
	 QueryPerformanceCounter(&ed);
	 ct.QuadPart+=(ed.QuadPart-st.QuadPart);
	 st.QuadPart=ed.QuadPart;
#else
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec+=ed.tv_sec-st.tv_sec;
	ct.tv_nsec+=ed.tv_nsec-st.tv_nsec;
#endif
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
#ifdef WIN32
		return (double)ct.QuadPart/fcpu.QuadPart;
#else
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
#endif
	}
};

int nlp=0,blp=0;
#define MASK_SLASH 0x01  //斜率1 '/'
#define MASK_COLUMN 0x02  //本列 '|'
#define MASK_BACKSLASH 0x04 //斜率-1 '\'
#define MASK_ALL 0x07  //全部条件
//取本行下一个有效列
int *ckArray;
char *mask;
char *maskIdx[30];
int qsum;
inline int NextColumn(int line,int pos) {
	//nlp++;
	pos++;
	if(pos>qsum)
		return -1;
	char *mpos=maskIdx[line];
	int rt=-1;
	for(;pos<=qsum;pos++)
		if(!mpos[pos]) {
			return pos;
		}
	return rt;
}
//构造下一行掩码
// line:行号
// column:列号
// pcol:前次列号
// qsum: 维度
// void BuildMask(char *mask,int line,int column,int pcol,int qsum) {
inline void BuildMask(int line,int column,int pcol) {
	if(line==qsum) return;
	//char *pos=mask+line*(qsum+1);
	char *pos=maskIdx[line];
	char *posn=pos+(qsum+1);
	//恢复掩码
	if(pcol!=0) {
	 pos[pcol]=0;
	 posn[pcol]&=(~MASK_COLUMN);
	 posn[pcol-1]&=(~MASK_SLASH);
	 if(pcol<qsum) 
	   posn[pcol+1]&=(~MASK_BACKSLASH);
	}
	if(column==0)  return;
	pos[column]=MASK_ALL;
	if(pcol!=0) {
	    posn[column]|=MASK_COLUMN;
	    posn[column-1]|=MASK_SLASH;
	    if(column<qsum) posn[column+1]|=MASK_BACKSLASH;
	    return;
	}
	int i;
	memset(posn,0,(qsum+1));
	for(i=1;i<=qsum;i++) {
		if(pos[i]&MASK_SLASH && i>1)		posn[i-1]|=MASK_SLASH;
		if(pos[i]&MASK_COLUMN)   			posn[i]|=MASK_COLUMN;
		if(pos[i]&MASK_BACKSLASH && i<qsum) posn[i+1]|=MASK_BACKSLASH;
	}
/*	memset(posn,0,(qsum+1));
	for(i=1;i<=qsum;i++) {
		if(pos[i]&MASK_SLASH && i>1)		posn[i-1]|=MASK_SLASH;
		if(pos[i]&MASK_COLUMN)   			posn[i]|=MASK_COLUMN;
		if(pos[i]&MASK_BACKSLASH && i<qsum) posn[i+1]|=MASK_BACKSLASH;
	}
*///	blp++;
}

int main(void){ 
	int countSum,queenSum,printCount,*queenArray,queenPosition=0; 
	int maskCount;
	int checksum=0;
	int tempArray[20]={6666,1,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}; 
	countSum=1; 
	ckArray=tempArray;
	queenArray=tempArray; 
	printf("input you queenSum here:"); 
	/*scanf("%d",&queenSum); 
	fflush(stdin); 
	if(queenSum<4){ 
	printf("the %d queen's sum is 0\n",queenSum); 
	return; 
	} 
	*/
	printf("\n");
	queenSum=14;
	char *maskarray=new char [(queenSum+1)*(queenSum+1)];
	memset(maskarray,0,(queenSum+1)*(queenSum+1));
	
	mask=maskarray;
	for(maskCount=1;maskCount<=queenSum;maskCount++)
	  maskIdx[maskCount]=mask+maskCount*(queenSum+1);
	qsum=queenSum;
	//1行1列，前次为0
	BuildMask(1,1,0);
	mytimer tm;
	tm.Start();
	double lp=0;
	double slp=0;
	for(;;){ 
		lp+=1;
		if(countSum<queenSum){ // middle try
			if(countSum==1&&*(queenArray+countSum)==1) {
				countSum++; 
				BuildMask(countSum,queenArray[countSum],0);
				++countSum;
				int pcol=0;
				//int ncol=NextColumn(maskarray,countSum,pcol,queenSum);
				int ncol=NextColumn(countSum,pcol);
				while(countSum>0 && ncol==-1) {
					BuildMask(countSum,0,*(queenArray+countSum));
					*(queenArray+countSum--)=0; 
					pcol=*(queenArray+countSum);
					ncol=NextColumn(countSum,pcol);
					if(ncol!=-1) break;
				}
				if(countSum==0) 
					break;
				*(queenArray+countSum)=ncol;
				BuildMask(countSum,ncol,pcol);
			}
				++countSum;
				int pcol=0;
				int ncol=NextColumn(countSum,pcol);
				while(countSum>0 && ncol==-1) {
					BuildMask(countSum,0,*(queenArray+countSum));
					*(queenArray+countSum--)=0; 
					pcol=*(queenArray+countSum);
					ncol=NextColumn(countSum,pcol);
					if(ncol!=-1) break;
					ncol=-1;
				}
				if(countSum==0) 
					break;
				*(queenArray+countSum)=ncol;
				BuildMask(countSum,ncol,pcol);
		} 
		else{ //a complete items try
			slp+=1;
			queenPosition++; 
			{
				static int skip=1;
				static int inturn[20]={0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}; 
				if(memcmp(inturn+1,queenArray+1,queenSum*sizeof(int))==0) {
					queenPosition=(queenPosition-1)*2;
					break;
				}
				for(printCount=1;printCount<=queenSum;printCount++) 
					inturn[printCount]=queenSum-*(queenArray+printCount)+1;
				for(printCount=1;printCount<=queenSum;printCount++) 
					checksum+=*(queenArray+printCount);
				for(printCount=1;printCount<=queenSum;printCount++) 
					checksum+=*(inturn+printCount);
				if(queenPosition==skip) {
					skip+=(skip*2);
					//printf("Position %d\n",queenPosition);
					for(printCount=1;printCount<=queenSum;printCount++) 
						printf("%3d",*(queenArray+printCount)); 
					//for(printCount=1;printCount<=queenSum;printCount++) 
					//	printf("%3d",queenSum-*(queenArray+printCount)+1); 
					
					printf("\n"); 
				}
			}
			*(queenArray+countSum)=0;
			BuildMask(countSum,0,*(queenArray+countSum));
			int pcol=*(queenArray+(--countSum));
			int ncol=NextColumn(countSum,pcol);
			while(countSum>0 && ncol==-1) {
			    BuildMask(countSum,0,*(queenArray+countSum));
				*(queenArray+countSum--)=0; 
				pcol=*(queenArray+countSum);
				ncol=NextColumn(countSum,pcol);
				if(ncol!=-1) break;
			}
			if(countSum==0) 
				break;
			*(queenArray+countSum)=ncol;
			BuildMask(countSum,ncol,pcol);
		} 
	} 
	delete []maskarray;
	tm.Stop();
	printf("the %d queen's sum is %d,lp %f,slp:%f,time:%.4fseconds,checksum:%d,nlp:%d,blp:%d.\n",queenSum,queenPosition,lp,slp,tm.GetTime(),checksum,nlp,blp); 
	return 0;
}

