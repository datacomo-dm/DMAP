/****************************************************************************
** CCredits meta object code from reading C++ file 'CCredits.h'
**
** Created: Mon Jul 13 18:29:47 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CCredits.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CCredits::className() const
{
    return "CCredits";
}

QMetaObject *CCredits::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CCredits( "CCredits", &CCredits::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CCredits::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CCredits", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CCredits::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CCredits", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CCredits::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QDialog::staticMetaObject();
    metaObj = QMetaObject::new_metaobject(
	"CCredits", parentObject,
	0, 0,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CCredits.setMetaObject( metaObj );
    return metaObj;
}

void* CCredits::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CCredits" ) )
	return this;
    return QDialog::qt_cast( clname );
}

bool CCredits::qt_invoke( int _id, QUObject* _o )
{
    return QDialog::qt_invoke(_id,_o);
}

bool CCredits::qt_emit( int _id, QUObject* _o )
{
    return QDialog::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CCredits::qt_property( int id, int f, QVariant* v)
{
    return QDialog::qt_property( id, f, v);
}

bool CCredits::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
