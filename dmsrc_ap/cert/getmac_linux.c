/*
 * mac_addr_sys.c
 *
 * Return the MAC (ie, ethernet hardware) address by using system specific
 * calls.
 *
 * compile with: gcc -c -D "OS" mac_addr_sys.c
 * with "OS" is one of Linux, AIX, HPUX 
 */

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef Linux
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <linux/if.h>
#endif

#ifdef HPUX
#include <netio.h>
#endif

#ifdef AIX
#include <sys/ndd_var.h>
#include <sys/kinfo.h>
#endif

long mac_addr_sys ( u_char *addr)
{
/* implementation for Linux */
#ifdef Linux
    struct ifreq ifr;
    struct ifreq *IFR;
    struct ifconf ifc;
    char buf[1024];
    int s, i;
    int ok = 0;

    s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s==-1) {
        return -1;
    }

    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = buf;
    ioctl(s, SIOCGIFCONF, &ifc);
 
    IFR = ifc.ifc_req;
    for (i = ifc.ifc_len / sizeof(struct ifreq); --i >= 0; IFR++) {

        strcpy(ifr.ifr_name, IFR->ifr_name);
        if (ioctl(s, SIOCGIFFLAGS, &ifr) == 0) {
            if (! (ifr.ifr_flags & IFF_LOOPBACK)) {
                if (ioctl(s, SIOCGIFHWADDR, &ifr) == 0) {
                    ok = 1;
                    break;
                }
            }
        }
    }

    close(s);
    if (ok) {
        bcopy( ifr.ifr_hwaddr.sa_data, addr, 6);
    }
    else {
        return -1;
    }
    return 0;
#endif

/* implementation for HP-UX */
#ifdef HPUX

#define LAN_DEV0 "/dev/lan0"

    int		fd;
    struct fis	iocnt_block;
    int		i;
    char	net_buf[sizeof(LAN_DEV0)+1];
    char	*p;

    (void)sprintf(net_buf, "%s", LAN_DEV0);
    p = net_buf + strlen(net_buf) - 1;

    /* 
     * Get 802.3 address from card by opening the driver and interrogating it.
     */
    for (i = 0; i < 10; i++, (*p)++) {
        if ((fd = open (net_buf, O_RDONLY)) != -1) {
			iocnt_block.reqtype = LOCAL_ADDRESS;
			ioctl (fd, NETSTAT, &iocnt_block);
			close (fd);

            if (iocnt_block.vtype == 6)
                break;
        }
    }

    if (fd == -1 || iocnt_block.vtype != 6) {
        return -1;
    }

	bcopy( &iocnt_block.value.s[0], addr, 6);
	return 0;

#endif /* HPUX */

/* implementation for AIX */
#ifdef AIX

    int size;
    struct kinfo_ndd *nddp;

    size = getkerninfo(KINFO_NDD, 0, 0, 0);
    if (size <= 0) {
        return -1;
    }
    nddp = (struct kinfo_ndd *)malloc(size);
          
    if (!nddp) {
        return -1;
    }
    if (getkerninfo(KINFO_NDD, nddp, &size, 0) < 0) {
        free(nddp);
        return -1;
    }
    bcopy(nddp->ndd_addr, addr, 6);
    free(nddp);
    return 0;
#endif

/* Not implemented platforms */
	return -1;
}

long getmac(char *addr)
{
    u_char raddr[6];
    int i;
    if(mac_addr_sys( raddr)!=0)
     return -1;
    addr[0]=0;
    for (i=0; i<6; ++i) {
    	sprintf(addr+strlen(addr),"%02x", raddr[i]);
    }
    return 0;
}
/***********************************************************************/
/*
 * Main (only for testing)
 */
#ifdef MAIN
int main( int argc, char **argv)
{
    long stat;
    int i;
    char addr[36];

    stat = getmac( addr);
    if (0 == stat) {
        printf( "MAC address =%s\n ",addr);
    }
    else {
        fprintf( stderr, "can't get MAC address\n");
        exit( 1);
    }
    return 0;
}
#endif
