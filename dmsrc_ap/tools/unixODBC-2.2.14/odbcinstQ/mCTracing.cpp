/****************************************************************************
** CTracing meta object code from reading C++ file 'CTracing.h'
**
** Created: Mon Jul 13 18:30:15 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CTracing.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CTracing::className() const
{
    return "CTracing";
}

QMetaObject *CTracing::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CTracing( "CTracing", &CTracing::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CTracing::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CTracing", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CTracing::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CTracing", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CTracing::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QWidget::staticMetaObject();
    static const QUMethod slot_0 = {"setDefault", 0, 0 };
    static const QUMethod slot_1 = {"apply", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "setDefault()", &slot_0, QMetaData::Public },
	{ "apply()", &slot_1, QMetaData::Public }
    };
    metaObj = QMetaObject::new_metaobject(
	"CTracing", parentObject,
	slot_tbl, 2,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CTracing.setMetaObject( metaObj );
    return metaObj;
}

void* CTracing::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CTracing" ) )
	return this;
    return QWidget::qt_cast( clname );
}

bool CTracing::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: setDefault(); break;
    case 1: apply(); break;
    default:
	return QWidget::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CTracing::qt_emit( int _id, QUObject* _o )
{
    return QWidget::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CTracing::qt_property( int id, int f, QVariant* v)
{
    return QWidget::qt_property( id, f, v);
}

bool CTracing::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
