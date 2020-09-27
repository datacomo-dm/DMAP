#!/bin/sh

if [ -f rcmd.c ] ; then
	cd ..
fi
wd=`pwd`
for f in libncftp sio Strn doc ; do
	if [ ! -f "$f" ] && [ ! -d "$f" ] ; then
		echo "Missing directory $f ?" 1>&2
		exit 1
	fi
	( cd "$f" ; if [ -f Makefile ] ; then make clean ; fi )
done

TMPDIR=/tmp
TAR=""
TARFLAGS=""

if [ "$#" -lt 2 ] ; then
	TARDIR="ncftp"
	STGZFILE="$TARDIR.tar.gz"
else
	TARDIR="$1"
	STGZFILE="$2"
	if [ "$#" -eq 4 ] ; then
		# I.e., called from Makefile
		TAR="$3"
		TARFLAGS="$4"
	fi
fi

if [ -x /usr/bin/bzip2 ] ; then
	BZIP="/usr/bin/bzip2"
elif [ -x /usr/local/bin/bzip2 ] ; then
	BZIP="/usr/local/bin/bzip2"
else
	BZIP=":"
fi
SBGZFILE=`echo "$STGZFILE" | sed 's/\.tar\.gz/.tar.bz2/g'`
ZIPFILE=`echo "$STGZFILE" | sed 's/\.tar\.gz/.zip/g'`
rm -rf $TMPDIR/TAR
mkdir -p -m755 $TMPDIR/TAR/$TARDIR 2>/dev/null

umask 022
chmod 755 configure sh/* install-sh 2>/dev/null

find . -depth -follow -type f | sed '
/sio\/configure$/d
/Strn\/configure$/d
/\.o$/d
/\.so$/d
/\.a$/d
/\.lib$/d
/\.ncb$/d
/\.pdb$/d
/\.idb$/d
/\.pch$/d
/\.gch$/d
/\.ilk$/d
/\.res$/d
/\.aps$/d
/\.opt$/d
/\.plg$/d
/\.obj$/d
/\.exe$/d
/\.zip$/d
/\.gz$/d
/\.bz2$/d
/\.tgz$/d
/\.tar$/d
/\.swp$/d
/\.orig$/d
/\.rej$/d
/\/\._/d
/\/Makefile\.bin$/p
/\.bin$/d
/\/bin/d
/\/core$/d
/\/ccdv$/d
/\/[Rr]elease$/d
/\/[Dd]ebug$/d
/\/sio\/.*\//d
/shit/d
/\/upload/d
/\/config\.guess$/p
/\/config\.sub$/p
/\/config\.h\.in$/p
/\/config\./d
/\/Makefile$/d
/\/out\./d
/\/OLD/d
/\/old/d' | cut -c3- | tee "$wd/doc/manifest.txt" | cpio -Lpdm $TMPDIR/TAR/$TARDIR

chmod -R a+rX "$TMPDIR/TAR/$TARDIR"
( cd "$TMPDIR/TAR/$TARDIR" ; ln -s doc/README.txt README.txt )

if [ "$TAR" = "" ] || [ "$TARFLAGS" = "" ] ; then
	x=`tar --help 2>&1 | sed -n 's/.*owner=NAME.*/owner=NAME/g;/owner=NAME/p'`
	case "$x" in
		*owner=NAME*)
			TARFLAGS="-c --owner=bin --group=bin --verbose -f"
			TAR=tar
			;;
		*)
			TARFLAGS="cvf"
			TAR=tar
			x2=`gtar --help 2>&1 | sed -n 's/.*owner=NAME.*/owner=NAME/g;/owner=NAME/p'`
			case "$x2" in
				*owner=NAME*)
					TARFLAGS="-c --owner=bin --group=bin --verbose -f"
					TAR=gtar
					;;
			esac
			;;
	esac
fi

( cd $TMPDIR/TAR ; $TAR $TARFLAGS - $TARDIR | gzip -c > $STGZFILE )
cp $TMPDIR/TAR/$STGZFILE .

if [ "$BZIP" != ":" ] ; then
	( cd $TMPDIR/TAR ; $TAR $TARFLAGS - $TARDIR | $BZIP -c > $SBGZFILE )
	cp $TMPDIR/TAR/$SBGZFILE .
fi

( cd $TMPDIR/TAR ; zip -r -9 -v $ZIPFILE $TARDIR )
cp $TMPDIR/TAR/$ZIPFILE .

chmod 644 $STGZFILE $SBGZFILE $ZIPFILE 2>/dev/null
rm -rf $TMPDIR/TAR
ls -l $STGZFILE $SBGZFILE $ZIPFILE 2>/dev/null
exit 0
