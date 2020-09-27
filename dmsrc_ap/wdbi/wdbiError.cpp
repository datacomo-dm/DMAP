#include "wdbi.h"


void WDBIError::SetErrCode(int ec)
{
	errcode=ec;
}

void WDBIError::SetEcho(bool val)
{
 echo=val;
}

void WDBIError::SetTrace(bool val)
{
	trace=val;
}

