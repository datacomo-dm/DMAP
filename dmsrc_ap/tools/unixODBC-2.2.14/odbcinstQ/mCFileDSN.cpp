/****************************************************************************
** CFileDSN meta object code from reading C++ file 'CFileDSN.h'
**
** Created: Mon Jul 13 18:29:55 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CFileDSN.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CFileDSN::className() const
{
    return "CFileDSN";
}

QMetaObject *CFileDSN::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CFileDSN( "CFileDSN", &CFileDSN::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CFileDSN::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CFileDSN", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CFileDSN::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CFileDSN", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CFileDSN::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QWidget::staticMetaObject();
    static const QUMethod slot_0 = {"NewDir", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "NewDir()", &slot_0, QMetaData::Public }
    };
    metaObj = QMetaObject::new_metaobject(
	"CFileDSN", parentObject,
	slot_tbl, 1,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CFileDSN.setMetaObject( metaObj );
    return metaObj;
}

void* CFileDSN::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CFileDSN" ) )
	return this;
    return QWidget::qt_cast( clname );
}

bool CFileDSN::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: NewDir(); break;
    default:
	return QWidget::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CFileDSN::qt_emit( int _id, QUObject* _o )
{
    return QWidget::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CFileDSN::qt_property( int id, int f, QVariant* v)
{
    return QWidget::qt_property( id, f, v);
}

bool CFileDSN::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
