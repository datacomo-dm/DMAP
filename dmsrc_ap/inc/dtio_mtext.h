
int exCopyMt(int _mt) {
	int mt=wociCreateMemTable();
 	wociCopyColumnDefine(mt,_mt,NULL);
	wociCopyRowsTo(mt,_mt,0,0,wociGetMemtableRows(_mt));
	return mt;
}

void exSetDouble(int mt,const char *col,int off,double &v) {
	double *pv;
	wociGetStrAddrByName(mt,col,of,&pv);
	*pv=v;
}
