/****************************************************************************
** CPropertiesFrame meta object code from reading C++ file 'CPropertiesFrame.h'
**
** Created: Mon Jul 13 18:30:06 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CPropertiesFrame.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CPropertiesFrame::className() const
{
    return "CPropertiesFrame";
}

QMetaObject *CPropertiesFrame::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CPropertiesFrame( "CPropertiesFrame", &CPropertiesFrame::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CPropertiesFrame::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CPropertiesFrame", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CPropertiesFrame::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CPropertiesFrame", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CPropertiesFrame::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QDialog::staticMetaObject();
    static const QUMethod slot_0 = {"doOk", 0, 0 };
    static const QUMethod slot_1 = {"doCancel", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "doOk()", &slot_0, QMetaData::Protected },
	{ "doCancel()", &slot_1, QMetaData::Protected }
    };
    metaObj = QMetaObject::new_metaobject(
	"CPropertiesFrame", parentObject,
	slot_tbl, 2,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CPropertiesFrame.setMetaObject( metaObj );
    return metaObj;
}

void* CPropertiesFrame::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CPropertiesFrame" ) )
	return this;
    return QDialog::qt_cast( clname );
}

bool CPropertiesFrame::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: doOk(); break;
    case 1: doCancel(); break;
    default:
	return QDialog::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CPropertiesFrame::qt_emit( int _id, QUObject* _o )
{
    return QDialog::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CPropertiesFrame::qt_property( int id, int f, QVariant* v)
{
    return QDialog::qt_property( id, f, v);
}

bool CPropertiesFrame::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
