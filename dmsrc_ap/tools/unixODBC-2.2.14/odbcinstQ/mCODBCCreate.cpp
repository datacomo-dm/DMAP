/****************************************************************************
** CODBCCreate meta object code from reading C++ file 'CODBCCreate.h'
**
** Created: Mon Jul 13 18:30:02 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CODBCCreate.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CODBCCreate::className() const
{
    return "CODBCCreate";
}

QMetaObject *CODBCCreate::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CODBCCreate( "CODBCCreate", &CODBCCreate::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CODBCCreate::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CODBCCreate", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CODBCCreate::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CODBCCreate", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CODBCCreate::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = Wiz::staticMetaObject();
    static const QUMethod slot_0 = {"fds_click", 0, 0 };
    static const QUMethod slot_1 = {"uds_click", 0, 0 };
    static const QUMethod slot_2 = {"sds_click", 0, 0 };
    static const QUMethod slot_3 = {"file_click", 0, 0 };
    static const QUMethod slot_4 = {"ad_click", 0, 0 };
    static const QUParameter param_slot_5[] = {
	{ 0, &static_QUType_ptr, "LViewItem", QUParameter::In }
    };
    static const QUMethod slot_5 = {"dl_click", 1, param_slot_5 };
    static const QUParameter param_slot_6[] = {
	{ "title", &static_QUType_QString, 0, QUParameter::In }
    };
    static const QUMethod slot_6 = {"page_change", 1, param_slot_6 };
    static const QUParameter param_slot_7[] = {
	{ "text", &static_QUType_QString, 0, QUParameter::In }
    };
    static const QUMethod slot_7 = {"file_changed", 1, param_slot_7 };
    static const QMetaData slot_tbl[] = {
	{ "fds_click()", &slot_0, QMetaData::Public },
	{ "uds_click()", &slot_1, QMetaData::Public },
	{ "sds_click()", &slot_2, QMetaData::Public },
	{ "file_click()", &slot_3, QMetaData::Public },
	{ "ad_click()", &slot_4, QMetaData::Public },
	{ "dl_click(LViewItem*)", &slot_5, QMetaData::Public },
	{ "page_change(const QString&)", &slot_6, QMetaData::Public },
	{ "file_changed(const QString&)", &slot_7, QMetaData::Public }
    };
    metaObj = QMetaObject::new_metaobject(
	"CODBCCreate", parentObject,
	slot_tbl, 8,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CODBCCreate.setMetaObject( metaObj );
    return metaObj;
}

void* CODBCCreate::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CODBCCreate" ) )
	return this;
    return Wiz::qt_cast( clname );
}

bool CODBCCreate::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: fds_click(); break;
    case 1: uds_click(); break;
    case 2: sds_click(); break;
    case 3: file_click(); break;
    case 4: ad_click(); break;
    case 5: dl_click((LViewItem*)static_QUType_ptr.get(_o+1)); break;
    case 6: page_change((const QString&)static_QUType_QString.get(_o+1)); break;
    case 7: file_changed((const QString&)static_QUType_QString.get(_o+1)); break;
    default:
	return Wiz::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CODBCCreate::qt_emit( int _id, QUObject* _o )
{
    return Wiz::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CODBCCreate::qt_property( int id, int f, QVariant* v)
{
    return Wiz::qt_property( id, f, v);
}

bool CODBCCreate::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES


const char *CODBCAdvanced::className() const
{
    return "CODBCAdvanced";
}

QMetaObject *CODBCAdvanced::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CODBCAdvanced( "CODBCAdvanced", &CODBCAdvanced::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CODBCAdvanced::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CODBCAdvanced", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CODBCAdvanced::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CODBCAdvanced", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CODBCAdvanced::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QDialog::staticMetaObject();
    static const QUMethod slot_0 = {"ad_ok", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "ad_ok()", &slot_0, QMetaData::Public }
    };
    metaObj = QMetaObject::new_metaobject(
	"CODBCAdvanced", parentObject,
	slot_tbl, 1,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CODBCAdvanced.setMetaObject( metaObj );
    return metaObj;
}

void* CODBCAdvanced::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CODBCAdvanced" ) )
	return this;
    return QDialog::qt_cast( clname );
}

bool CODBCAdvanced::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: ad_ok(); break;
    default:
	return QDialog::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CODBCAdvanced::qt_emit( int _id, QUObject* _o )
{
    return QDialog::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CODBCAdvanced::qt_property( int id, int f, QVariant* v)
{
    return QDialog::qt_property( id, f, v);
}

bool CODBCAdvanced::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
