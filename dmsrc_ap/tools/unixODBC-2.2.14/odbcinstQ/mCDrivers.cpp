/****************************************************************************
** CDrivers meta object code from reading C++ file 'CDrivers.h'
**
** Created: Mon Jul 13 18:29:51 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CDrivers.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CDrivers::className() const
{
    return "CDrivers";
}

QMetaObject *CDrivers::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CDrivers( "CDrivers", &CDrivers::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CDrivers::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CDrivers", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CDrivers::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CDrivers", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CDrivers::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QWidget::staticMetaObject();
    static const QUMethod slot_0 = {"Add", 0, 0 };
    static const QUMethod slot_1 = {"Edit", 0, 0 };
    static const QUMethod slot_2 = {"Delete", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "Add()", &slot_0, QMetaData::Public },
	{ "Edit()", &slot_1, QMetaData::Public },
	{ "Delete()", &slot_2, QMetaData::Public }
    };
    metaObj = QMetaObject::new_metaobject(
	"CDrivers", parentObject,
	slot_tbl, 3,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CDrivers.setMetaObject( metaObj );
    return metaObj;
}

void* CDrivers::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CDrivers" ) )
	return this;
    return QWidget::qt_cast( clname );
}

bool CDrivers::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: Add(); break;
    case 1: Edit(); break;
    case 2: Delete(); break;
    default:
	return QWidget::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CDrivers::qt_emit( int _id, QUObject* _o )
{
    return QWidget::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CDrivers::qt_property( int id, int f, QVariant* v)
{
    return QWidget::qt_property( id, f, v);
}

bool CDrivers::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
