#ifndef FWD_H_INCLUDED
#define FWD_H_INCLUDED 1

#include "edition/local_fwd.h"

typedef boost::shared_ptr<RCTable> RCTablePtr;

class ConnectionInfo;

class TableLock;
typedef boost::shared_ptr<TableLock>	TableLockPtr;
typedef boost::weak_ptr<TableLock>		TableLockWeakPtr;

class JustATable;
typedef boost::shared_ptr<JustATable>	JustATablePtr;
typedef boost::weak_ptr<JustATable>		JustATableWeakPtr;

class Filter;
typedef boost::shared_ptr<Filter> FilterPtr;

class TempTable;
typedef boost::shared_ptr<TempTable>	TempTablePtr;
typedef boost::weak_ptr<TempTable>		TempTableWeakPtr;

class AttrPack;
typedef boost::shared_ptr<AttrPack> AttrPackPtr;

#endif /* #ifndef FWD_H_INCLUDED */

