#include <stdio.h>
#include <string.h>
#include "udp.h"
#include "mfs.h"
#include <sys/select.h>
#include <sys/time.h>

struct sockaddr_in addrSnd, addrRcv;
int sd;
int connected = 0;
char* server_host = NULL;
int server_port = -1;

int UDP_Send(UDP_Packet *st, UDP_Packet *re)
{   
    int sd = UDP_Open(20000);
    if(sd < -1){
        printf("Failed to open socket\n");
        return sd;
    }

    int rc = UDP_FillSockAddr(&addrSnd, server_host, server_port);
    if (rc != 0){
        printf("Failed to set up server address\n");
        return rc;
    }

    fd_set readfds;
    struct timeval t;
    t.tv_sec = 3;
    t.tv_usec = 0;
    int limit = 5;
    while(limit){
        FD_ZERO(&readfds);
        FD_SET(sd, &readfds);
        UDP_Write(sd, &addrSnd, (char*)st, sizeof(UDP_Packet));
        if(select(sd+1, &readfds, NULL, NULL, &t)){
            int rc = UDP_Read(sd, &addrRcv, (char*)re, sizeof(UDP_Packet));
            if(rc > 0){
                UDP_Close(sd);
                return 0;
            }
        else {
            limit --;
        }
        }
    }
    return -1;
}

int MFS_Init(char *hostname, int port)
{   
    server_host = strdup(hostname);
    server_port = port;
    connected = 1;
    return 0;
}

int MFS_Lookup(int pinum, char *name)
{
    if(!connected)
    return -1;
    if(strlen(name) >= FILE_NAME_SIZE || name == NULL)
    return -1;

    UDP_Packet st;
    UDP_Packet re;

    st.inum = pinum;
    st.req = LOOKUP;

    if(strlen(name) + 1 > 28){
        return -1;
    }
    strcpy(st.name, name);

    if(UDP_Send(&st, &re) < 0){
        return -1;
    }
	else {
        if(re.inum >= 0){
            return re.inum;
        } else {
            return -1;
        }
    }
}

int MFS_Stat(int inum, MFS_Stat_t *m)
{
    if(!connected)
    return -1;

    UDP_Packet st;
    UDP_Packet re;
    st.inum = inum;
    st.req = STAT;
    if(UDP_Send(&st, &re) < 0){
        return -1;
    }
	else {
        if(re.rc == 0){
            m->size = re.size;
            m->type = re.type;
            return 0;
        } else {
            return -1;
        }
    }
}

int MFS_Write(int inum, char *buffer, int offset, int nbytes)
{
    if(!connected)
    return -1;

    UDP_Packet st;
    UDP_Packet re;
    st.inum = inum;
    st.req = WRITE;
    st.offset = offset;
    if(nbytes > MFS_BLOCK_SIZE){
        return -1;
    }

    for(int i = 0; i < nbytes; i++){
        st.buffer[i] = buffer[i];
    }

    st.buffer_size = nbytes;

    if(UDP_Send(&st, &re) < 0){
        return -1;
    }
	else {
        if(re.rc == 0){
            return 0;
        } else {
            return -1;
        }
    }
}

int MFS_Read(int inum, char *buffer, int offset, int nbytes)
{
    if(!connected)
    return -1;

    UDP_Packet st;
    UDP_Packet re;
    st.inum = inum;
    st.req = READ;
    st.offset = offset;

    if(nbytes > MFS_BLOCK_SIZE){
        return -1;
    }

    st.buffer_size = nbytes;

    if(UDP_Send(&st, &re) < 0){
        return -1;
    }
	else {
        if(re.rc == 0){
            for(int i = 0; i < nbytes; i++){
                buffer[i] = re.buffer[i];
            }
            return 0;
        } else {
            return -1;
        }
    }
}

int MFS_Creat(int pinum, int type, char *name)
{
    if(!connected)
    return -1;
    UDP_Packet st;
    UDP_Packet re;
    st.inum = pinum;
    st.req = CREAT;
    st.type = type;
    if(strlen(name) + 1 > 28){
        return -1;
    }
    strcpy(st.name, name);
    if(UDP_Send(&st, &re) < 0){
        return -1;
    }
	else {
        if(re.rc == 0){
            return 0;
        } else {
            return -1;
        }
    }
}

int MFS_Unlink(int pinum, char *name)
{
    if(!connected)
    return -1;

    UDP_Packet st;
    UDP_Packet re;
    st.inum = pinum;
    st.req = UNLINK;

    if(strlen(name) + 1 > 28){
        return -1;
    }

    strcpy(st.name, name);
    if(UDP_Send(&st, &re) < 0){
        return -1;
    }
	else {
        if(re.rc == 0){
            return 0;
        } else {
            return -1;
        }
    }
}

int MFS_Shutdown()
{   
    if(!connected)
    return -1;

    UDP_Packet st;
    UDP_Packet re;
    st.req = SHUTDOWN;
  
    if(UDP_Send(&st, &re) < 0){
        return -1;
    }
	else {
        if(re.rc == 0){
            return 0;
        } else {
            return -1;
        }
    }
}

