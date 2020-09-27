/****************************************************************************
** CUserDSN meta object code from reading C++ file 'CUserDSN.h'
**
** Created: Mon Jul 13 18:30:16 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CUserDSN.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CUserDSN::className() const
{
    return "CUserDSN";
}

QMetaObject *CUserDSN::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CUserDSN( "CUserDSN", &CUserDSN::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CUserDSN::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CUserDSN", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CUserDSN::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CUserDSN", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CUserDSN::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QWidget::staticMetaObject();
    metaObj = QMetaObject::new_metaobject(
	"CUserDSN", parentObject,
	0, 0,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CUserDSN.setMetaObject( metaObj );
    return metaObj;
}

void* CUserDSN::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CUserDSN" ) )
	return this;
    return QWidget::qt_cast( clname );
}

bool CUserDSN::qt_invoke( int _id, QUObject* _o )
{
    return QWidget::qt_invoke(_id,_o);
}

bool CUserDSN::qt_emit( int _id, QUObject* _o )
{
    return QWidget::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CUserDSN::qt_property( int id, int f, QVariant* v)
{
    return QWidget::qt_property( id, f, v);
}

bool CUserDSN::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
