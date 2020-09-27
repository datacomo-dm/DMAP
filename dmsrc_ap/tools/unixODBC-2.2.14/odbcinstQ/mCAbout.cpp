/****************************************************************************
** CAboutDiagram meta object code from reading C++ file 'CAbout.h'
**
** Created: Mon Jul 13 18:29:46 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CAbout.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CAboutDiagram::className() const
{
    return "CAboutDiagram";
}

QMetaObject *CAboutDiagram::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CAboutDiagram( "CAboutDiagram", &CAboutDiagram::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CAboutDiagram::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CAboutDiagram", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CAboutDiagram::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CAboutDiagram", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CAboutDiagram::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QWidget::staticMetaObject();
    static const QUMethod slot_0 = {"pbODBCConfig_Clicked", 0, 0 };
    static const QUMethod slot_1 = {"pbODBC_Clicked", 0, 0 };
    static const QUMethod slot_2 = {"pbDatabase_Clicked", 0, 0 };
    static const QUMethod slot_3 = {"pbDriverManager_Clicked", 0, 0 };
    static const QUMethod slot_4 = {"pbDriver_Clicked", 0, 0 };
    static const QUMethod slot_5 = {"pbODBCDrivers_Clicked", 0, 0 };
    static const QUMethod slot_6 = {"pbApplication_Clicked", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "pbODBCConfig_Clicked()", &slot_0, QMetaData::Protected },
	{ "pbODBC_Clicked()", &slot_1, QMetaData::Protected },
	{ "pbDatabase_Clicked()", &slot_2, QMetaData::Protected },
	{ "pbDriverManager_Clicked()", &slot_3, QMetaData::Protected },
	{ "pbDriver_Clicked()", &slot_4, QMetaData::Protected },
	{ "pbODBCDrivers_Clicked()", &slot_5, QMetaData::Protected },
	{ "pbApplication_Clicked()", &slot_6, QMetaData::Protected }
    };
    metaObj = QMetaObject::new_metaobject(
	"CAboutDiagram", parentObject,
	slot_tbl, 7,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CAboutDiagram.setMetaObject( metaObj );
    return metaObj;
}

void* CAboutDiagram::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CAboutDiagram" ) )
	return this;
    return QWidget::qt_cast( clname );
}

bool CAboutDiagram::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: pbODBCConfig_Clicked(); break;
    case 1: pbODBC_Clicked(); break;
    case 2: pbDatabase_Clicked(); break;
    case 3: pbDriverManager_Clicked(); break;
    case 4: pbDriver_Clicked(); break;
    case 5: pbODBCDrivers_Clicked(); break;
    case 6: pbApplication_Clicked(); break;
    default:
	return QWidget::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CAboutDiagram::qt_emit( int _id, QUObject* _o )
{
    return QWidget::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CAboutDiagram::qt_property( int id, int f, QVariant* v)
{
    return QWidget::qt_property( id, f, v);
}

bool CAboutDiagram::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES


const char *CAbout::className() const
{
    return "CAbout";
}

QMetaObject *CAbout::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CAbout( "CAbout", &CAbout::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CAbout::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CAbout", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CAbout::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CAbout", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CAbout::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QWidget::staticMetaObject();
    static const QUMethod slot_0 = {"pbCredits_Clicked", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "pbCredits_Clicked()", &slot_0, QMetaData::Protected }
    };
    metaObj = QMetaObject::new_metaobject(
	"CAbout", parentObject,
	slot_tbl, 1,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CAbout.setMetaObject( metaObj );
    return metaObj;
}

void* CAbout::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CAbout" ) )
	return this;
    return QWidget::qt_cast( clname );
}

bool CAbout::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: pbCredits_Clicked(); break;
    default:
	return QWidget::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CAbout::qt_emit( int _id, QUObject* _o )
{
    return QWidget::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CAbout::qt_property( int id, int f, QVariant* v)
{
    return QWidget::qt_property( id, f, v);
}

bool CAbout::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
