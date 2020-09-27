/****************************************************************************
** CDSNList meta object code from reading C++ file 'CDSNList.h'
**
** Created: Mon Jul 13 18:29:53 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CDSNList.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CDSNList::className() const
{
    return "CDSNList";
}

QMetaObject *CDSNList::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CDSNList( "CDSNList", &CDSNList::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CDSNList::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CDSNList", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CDSNList::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CDSNList", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CDSNList::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QListView::staticMetaObject();
    static const QUMethod slot_0 = {"Add", 0, 0 };
    static const QUMethod slot_1 = {"Edit", 0, 0 };
    static const QUMethod slot_2 = {"Delete", 0, 0 };
    static const QUParameter param_slot_3[] = {
	{ "itm", &static_QUType_ptr, "QListViewItem", QUParameter::In }
    };
    static const QUMethod slot_3 = {"DoubleClick", 1, param_slot_3 };
    static const QMetaData slot_tbl[] = {
	{ "Add()", &slot_0, QMetaData::Public },
	{ "Edit()", &slot_1, QMetaData::Public },
	{ "Delete()", &slot_2, QMetaData::Public },
	{ "DoubleClick(QListViewItem*)", &slot_3, QMetaData::Public }
    };
    metaObj = QMetaObject::new_metaobject(
	"CDSNList", parentObject,
	slot_tbl, 4,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CDSNList.setMetaObject( metaObj );
    return metaObj;
}

void* CDSNList::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CDSNList" ) )
	return this;
    return QListView::qt_cast( clname );
}

bool CDSNList::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: Add(); break;
    case 1: Edit(); break;
    case 2: Delete(); break;
    case 3: DoubleClick((QListViewItem*)static_QUType_ptr.get(_o+1)); break;
    default:
	return QListView::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CDSNList::qt_emit( int _id, QUObject* _o )
{
    return QListView::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CDSNList::qt_property( int id, int f, QVariant* v)
{
    return QListView::qt_property( id, f, v);
}

bool CDSNList::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
