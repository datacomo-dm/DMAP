/****************************************************************************
** CODBCConfig meta object code from reading C++ file 'CODBCConfig.h'
**
** Created: Mon Jul 13 18:30:00 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CODBCConfig.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CODBCConfig::className() const
{
    return "CODBCConfig";
}

QMetaObject *CODBCConfig::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CODBCConfig( "CODBCConfig", &CODBCConfig::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CODBCConfig::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CODBCConfig", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CODBCConfig::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CODBCConfig", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CODBCConfig::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = TDialog::staticMetaObject();
    metaObj = QMetaObject::new_metaobject(
	"CODBCConfig", parentObject,
	0, 0,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CODBCConfig.setMetaObject( metaObj );
    return metaObj;
}

void* CODBCConfig::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CODBCConfig" ) )
	return this;
    return TDialog::qt_cast( clname );
}

bool CODBCConfig::qt_invoke( int _id, QUObject* _o )
{
    return TDialog::qt_invoke(_id,_o);
}

bool CODBCConfig::qt_emit( int _id, QUObject* _o )
{
    return TDialog::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CODBCConfig::qt_property( int id, int f, QVariant* v)
{
    return TDialog::qt_property( id, f, v);
}

bool CODBCConfig::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
