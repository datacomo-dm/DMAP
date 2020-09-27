/****************************************************************************
** CFileList meta object code from reading C++ file 'CFileList.h'
**
** Created: Mon Jul 13 18:29:56 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CFileList.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CFileList::className() const
{
    return "CFileList";
}

QMetaObject *CFileList::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CFileList( "CFileList", &CFileList::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CFileList::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CFileList", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CFileList::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CFileList", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CFileList::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = LView::staticMetaObject();
    static const QUMethod slot_0 = {"Add", 0, 0 };
    static const QUMethod slot_1 = {"Edit", 0, 0 };
    static const QUMethod slot_2 = {"Delete", 0, 0 };
    static const QUMethod slot_3 = {"NewDir", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "Add()", &slot_0, QMetaData::Public },
	{ "Edit()", &slot_1, QMetaData::Public },
	{ "Delete()", &slot_2, QMetaData::Public },
	{ "NewDir()", &slot_3, QMetaData::Public }
    };
    metaObj = QMetaObject::new_metaobject(
	"CFileList", parentObject,
	slot_tbl, 4,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CFileList.setMetaObject( metaObj );
    return metaObj;
}

void* CFileList::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CFileList" ) )
	return this;
    return LView::qt_cast( clname );
}

bool CFileList::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: Add(); break;
    case 1: Edit(); break;
    case 2: Delete(); break;
    case 3: NewDir(); break;
    default:
	return LView::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CFileList::qt_emit( int _id, QUObject* _o )
{
    return LView::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CFileList::qt_property( int id, int f, QVariant* v)
{
    return LView::qt_property( id, f, v);
}

bool CFileList::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
