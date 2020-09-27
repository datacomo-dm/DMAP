Welcome to the FreeBSD archive!
-------------------------------

Here you will find the official releases of FreeBSD, along with
the ports and packages collection.  For those who have World Wide
Web access, we encourage you to visit the FreeBSD home page at:

   http://www.freebsd.org/



Contents of this directory:
---------------------------
releases/${ARCH}/*-RELEASE/
    The official FreeBSD releases.  The latest releases are:

	releases/i386/4.4-RELEASE
	releases/alpha/4.4-RELEASE

    See README.TXT files in these directories for more information.
    The releases/${ARCH}/ISO-IMAGES directory also contains ISO 9660
    (+ rockridge extentions) installation CD images for the latest
    releases.

snapshots/${ARCH}/*-YYMMDD-SNAP/
    Any "snapshot" tests of an upcoming release.  YYMMDD represents
    the year, month and day of the snapshot.  These are more generally
    found on ftp://current.freebsd.org/pub/FreeBSD and
    ftp://releng3.freebsd.org/pub/FreeBSD, this directory generally
    (and intermittently) containing only "special" snapshots for testing
    purposes.

FreeBSD-current/
    This contains the source code for FreeBSD-current, which is
    the active development version of FreeBSD.  It is *strongly*
    advised that you be familiar with UNIX development in general
    and FreeBSD in particular before running the code from this
    directory.  This branch is currently where 4.0 snapshot
    releases are being generated from.

FreeBSD-stable/
    This contains the source code for FreeBSD-stable, the stable
    code branch of FreeBSD.  This branch is currently where the
    mainstream (3.x) releases are being generated from.

development/FreeBSD-CVS/
    This contains the FreeBSD CVS repository.  It is intended for
    use by FreeBSD (or related project) developers.

distfiles/
    Original distribution files for the ports collection (see below).

doc/
    Documentation for FreeBSD including the FAQ and the FreeBSD
    handbook.  Both these documents are available in hypertext
    form from http://www.freebsd.org/

FreeBSD-stable/packages/
FreeBSD-current/packages/
    The FreeBSD packages collection for each of the 2 active branches.
    These are pre-compiled applications ready to install with the
    pkg_add command.

FreeBSD-stable/ports/
FreeBSD-current/ports/
    The FreeBSD ports collection.  These directories contain
    makfiles, patches and configuration scripts necessary to make
    the applications in the distfiles/ directory compile and run
    on FreeBSD.  If your FreeBSD machine is connected to the
    Internet, you need not download the application source code
    from distfiles/ because the makefile will automatically fetch
    it for you.

tools/
    A collection of useful tools for people installing FreeBSD.
    This includes MS-DOS tools such as RAWRITE used for
    making installation disks, FIPS for splitting an MS-DOS
    partition and a couple boot managers to allow easy booting on
    computers with more than one operating system installed.



Mirror Sites:
-------------
The mirroring of FreeBSD distributions from this location is handled by
mapping each FreeBSD mirror into a common "namespace" which can be said
to follow this rule:

	ftp://ftp[n][.domain].freebsd.org/pub/FreeBSD

Where "n" is an optional, logical site number (when you have more than one
FTP server for a domain) and ".domain" is an optional domain, specifying
which particular region of the world you're interested in.  Examples:

	ftp://ftp3.freebsd.org/pub/FreeBSD	[3rd logical ftp mirror]
	ftp://ftp.fr.freebsd.org/pub/FreeBSD	[primary French mirror]
	ftp://ftp4.de.freebsd.org/pub/FreeBSD	[4th logical German mirror]

Logical site assignments are dynamic, with the "fastest, best connected"
mirrors having the lowest logical numbers.  The DNS administrators are
expected to keep this true as mirror sites are created or retired.


New Mirrors:
------------
If you create a new mirror of these FreeBSD distributions and wish to
make it more generally available, you should send mail to
hostmaster@freebsd.org and ask that your site be added to the
global logical namespace map (ftp://ftp[n].freebsd.org/pub/FreeBSD).

If the mirror is in a subdomain then mail must ALSO be sent to
hostmaster@domain.freebsd.org (where domain is your country's domain
designator) since each country is responsible for and "owns"
its own local DNS administration for domain.freebsd.org.

It also goes without saying that should you shut down a mirror
after so registering it, you're expect to send notice to these
addresses again so that the namespace can be updated.

Finally, you must make sure that all FreeBSD distributions are available
under the pub/FreeBSD hierarchy, as they are at ftp.freebsd.org.  This
can be a symbolic link or an actual directory, just so long as
the URL ``ftp://ftp[n][.domain].freebsd.org/pub/FreeBSD'' works
for your site (with appropriate values for "n" and ".domain").

That is an important requirement for automating the process of locating
and loading distributions from FreeBSD mirrors.

o What if my country does not have its own freebsd.org subdomain?

If that is your situation, you might well consider becoming
the DNS administrator for your country.  That person is responsible
for the day-to-day administration of ``somedomain.freebsd.org''
and should both be skilled in DNS setup and maintainence *and*
be contactable most days of the year since, essentially, any ftp or
www site in that country will be relying on the DNS administrator
to register and maintain the name space for the entire nation.

If you think you're qualified and there is no existing freebsd.org
subdomain for your country, please send email to the FreeBSD Hostmaster
(hostmaster@freebsd.org) and include details on your DNS server's IP
address and contact information for the DNS administrator.  As mentioned
above, you should also make sure that the hostmaster@yourdomain.freebsd.org
alias reaches the DNS administator since that address will be used by others
to report problems or submit new entries to the regional subdomain.
