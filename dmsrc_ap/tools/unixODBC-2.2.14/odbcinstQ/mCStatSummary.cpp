/****************************************************************************
** CStatSummary meta object code from reading C++ file 'CStatSummary.h'
**
** Created: Mon Jul 13 18:30:09 2009
**      by: The Qt MOC ($Id: qt/moc_yacc.cpp   3.3.6   edited Mar 8 17:43 $)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#undef QT_NO_COMPAT
#include "CStatSummary.h"
#include <qmetaobject.h>
#include <qapplication.h>

#include <private/qucomextra_p.h>
#if !defined(Q_MOC_OUTPUT_REVISION) || (Q_MOC_OUTPUT_REVISION != 26)
#error "This file was generated using the moc from 3.3.6. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

const char *CStatSummary::className() const
{
    return "CStatSummary";
}

QMetaObject *CStatSummary::metaObj = 0;
static QMetaObjectCleanUp cleanUp_CStatSummary( "CStatSummary", &CStatSummary::staticMetaObject );

#ifndef QT_NO_TRANSLATION
QString CStatSummary::tr( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CStatSummary", s, c, QApplication::DefaultCodec );
    else
	return QString::fromLatin1( s );
}
#ifndef QT_NO_TRANSLATION_UTF8
QString CStatSummary::trUtf8( const char *s, const char *c )
{
    if ( qApp )
	return qApp->translate( "CStatSummary", s, c, QApplication::UnicodeUTF8 );
    else
	return QString::fromUtf8( s );
}
#endif // QT_NO_TRANSLATION_UTF8

#endif // QT_NO_TRANSLATION

QMetaObject* CStatSummary::staticMetaObject()
{
    if ( metaObj )
	return metaObj;
    QMetaObject* parentObject = QWidget::staticMetaObject();
    static const QUMethod slot_0 = {"showStats", 0, 0 };
    static const QMetaData slot_tbl[] = {
	{ "showStats()", &slot_0, QMetaData::Protected }
    };
    metaObj = QMetaObject::new_metaobject(
	"CStatSummary", parentObject,
	slot_tbl, 1,
	0, 0,
#ifndef QT_NO_PROPERTIES
	0, 0,
	0, 0,
#endif // QT_NO_PROPERTIES
	0, 0 );
    cleanUp_CStatSummary.setMetaObject( metaObj );
    return metaObj;
}

void* CStatSummary::qt_cast( const char* clname )
{
    if ( !qstrcmp( clname, "CStatSummary" ) )
	return this;
    return QWidget::qt_cast( clname );
}

bool CStatSummary::qt_invoke( int _id, QUObject* _o )
{
    switch ( _id - staticMetaObject()->slotOffset() ) {
    case 0: showStats(); break;
    default:
	return QWidget::qt_invoke( _id, _o );
    }
    return TRUE;
}

bool CStatSummary::qt_emit( int _id, QUObject* _o )
{
    return QWidget::qt_emit(_id,_o);
}
#ifndef QT_NO_PROPERTIES

bool CStatSummary::qt_property( int id, int f, QVariant* v)
{
    return QWidget::qt_property( id, f, v);
}

bool CStatSummary::qt_static_property( QObject* , int , int , QVariant* ){ return FALSE; }
#endif // QT_NO_PROPERTIES
