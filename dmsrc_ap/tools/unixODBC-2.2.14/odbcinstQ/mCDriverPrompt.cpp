/****************************************************************************
** CDriverPrompt meta object code from reading C++ file 'CDriverPrompt.h'
**
** Created: Mon Jul 13 18:29:49 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CDriverPrompt.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CDriverPrompt::className() const
{
    return "CDriverPrompt";
}

QMetaObject *CDriverPrompt::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CDriverPrompt( "CDriverPrompt", &CDriverPrompt::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CDriverPrompt::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CDriverPrompt", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CDriverPrompt::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CDriverPrompt", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CDriverPrompt::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QDialog::staticMetaObject();
    static const QUMethod slot_0 = {"pbCancel_Clicked", 0, 0 };
    static const QUMethod slot_1 = {"pbOk_Clicked", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "pbCancel_Clicked()", &slot_0, QMetaData::Protected },
	{ "pbOk_Clicked()", &slot_1, QMetaData::Protected }
    };
    metaObj = QMetaObject::new_metaobject(
	"CDriverPrompt", parentObject,
	slot_tbl, 2,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CDriverPrompt.setMetaObject( metaObj );
    return metaObj;
}

void* CDriverPrompt::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CDriverPrompt" ) )
	return this;
    return QDialog::qt_cast( clname );
}

bool CDriverPrompt::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: pbCancel_Clicked(); break;
    case 1: pbOk_Clicked(); break;
    default:
	return QDialog::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CDriverPrompt::qt_emit( int _id, QUObject* _o )
{
    return QDialog::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CDriverPrompt::qt_property( int id, int f, QVariant* v)
{
    return QDialog::qt_property( id, f, v);
}

bool CDriverPrompt::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
