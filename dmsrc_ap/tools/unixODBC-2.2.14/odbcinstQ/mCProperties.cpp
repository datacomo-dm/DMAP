/****************************************************************************
** CProperties meta object code from reading C++ file 'CProperties.h'
**
** Created: Mon Jul 13 18:30:04 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CProperties.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CProperties::className() const
{
    return "CProperties";
}

QMetaObject *CProperties::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CProperties( "CProperties", &CProperties::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CProperties::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CProperties", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CProperties::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CProperties", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CProperties::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = MWindow::staticMetaObject();
    static const QUMethod slot_0 = {"pbOk_Clicked", 0, 0 };
    static const QUMethod slot_1 = {"pbCancel_Clicked", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "pbOk_Clicked()", &slot_0, QMetaData::Protected },
	{ "pbCancel_Clicked()", &slot_1, QMetaData::Protected }
    };
    static const QUMethod signal_0 = {"Ok", 0, 0 };
    static const QUMethod signal_1 = {"Cancel", 0, 0 };
    static const QMetaData signal_tbl[] = {
	{ "Ok()", &signal_0, QMetaData::Protected },
	{ "Cancel()", &signal_1, QMetaData::Protected }
    };
    metaObj = QMetaObject::new_metaobject(
	"CProperties", parentObject,
	slot_tbl, 2,
	signal_tbl, 2,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CProperties.setMetaObject( metaObj );
    return metaObj;
}

void* CProperties::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CProperties" ) )
	return this;
    return MWindow::qt_cast( clname );
}

// SIGNAL Ok
void CProperties::Ok()
{
    activate_signal( staticMetaObject()->signalOffset() + 0 );
}

// SIGNAL Cancel
void CProperties::Cancel()
{
    activate_signal( staticMetaObject()->signalOffset() + 1 );
}

bool CProperties::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: pbOk_Clicked(); break;
    case 1: pbCancel_Clicked(); break;
    default:
	return MWindow::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CProperties::qt_emit( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->signalOffset() ) {
    case 0: Ok(); break;
    case 1: Cancel(); break;
    default:
	return MWindow::qt_emit(_id,_o);
    }
    return TRUE;
}
#ifndef QT_NO_PROPERTIES

bool CProperties::qt_property( int id, int f, QVariant* v)
{
    return MWindow::qt_property( id, f, v);
}

bool CProperties::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
