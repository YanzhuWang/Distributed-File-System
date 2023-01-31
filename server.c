#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <signal.h>
#include "udp.h"
#include "mfs.h"
#include "ufs.h"

int fd;
int sd;
void *fs_img;
inode_t *inode_table;
super_t *superblock;
struct stat fs;
unsigned int *inode_bitmap;
unsigned int *data_bitmap;

unsigned int get_bit(unsigned int *, int);
void set_bit(unsigned int *, int);
int server_init(int, char*);
int server_lookup(int, char*);
int server_stat(int, UDP_Packet *);
int server_write(int, char*, int, int);
int server_read(int, char* , int, int);
int server_create(int, int, char*);
int server_unlink(int, char*);
int server_shutdown();

void intHandler(int dummy)
{
    UDP_Close(sd);
    exit(130);
}

unsigned int get_bit(unsigned int *bitmap, int position) {
   int index = position / 32;
   int offset = 31 - (position % 32);
   return (bitmap[index] >> offset) & 0x1;
}

void set_bit(unsigned int *bitmap, int position) {
   int index = position / 32;
   int offset = 31 - (position % 32);
    bitmap[index] = bitmap[index] | (0x1 << offset);
}

void clean_bit(unsigned int *bitmap, int position) {
   int index = position / 32;
   int offset = 31 - (position % 32);
   bitmap[index] = bitmap[index] & ~(0x1 << offset);
}

int get_dataBlock() {
    int data_loc = -1;

        for(int i = 0; i < superblock->num_data; i++){
            if(get_bit(data_bitmap, i) == 0){
                data_loc = i;
                set_bit(data_bitmap, i);
                break;
            }
        }

        if(data_loc < 0){
            return -1;
        }

    return data_loc;
}

int get_inode() {
    int inum = -1;
    for(int i = 0; i < superblock->num_inodes; i++){
        if(get_bit(inode_bitmap, i) == 0){
            inum = i;
            set_bit(inode_bitmap, i);
            break;
        }
    }

    if(inum < 0){
        return -1;
    }

    return inum;
}

int server_lookup(int pinum, char* name)
{   
    if(pinum < 0 || pinum >= superblock->num_inodes)
        return -1;
    if(get_bit(inode_bitmap, pinum) != 1)
        return -1;
    if(inode_table[pinum].type != MFS_DIRECTORY)
        return -1;

    int size = inode_table[pinum].size;
    if (size < sizeof(MFS_DirEnt_t)){
        return -1;
    }

    for(int i = 0; i < DIRECT_PTRS; i++){
        if (inode_table[pinum].direct[i] == -1){
            break;
        }
     
        MFS_DirEnt_t* dict_entry = (MFS_DirEnt_t *)(fs_img + inode_table[pinum].direct[i] * UFS_BLOCK_SIZE);
        for(int j = 0; j < 128; j++){
            if(dict_entry[j].inum == -1)
                break;

            if(strcmp(name, dict_entry[j].name) == 0){
                return dict_entry[j].inum;
            }
        }
    }

    return -1;
}

int server_stat(int inum, UDP_Packet *reply)
{
    if(inum < 0 || inum >= superblock->num_inodes)
        return -1;

    if(get_bit(inode_bitmap, inum) != 1)
        return -1;

    reply->size = inode_table[inum].size;
    reply->type = inode_table[inum].type;
    
    return 0;
}

int server_read(int inum, char* buffer, int offset, int nbytes)
{
    if (nbytes <= 0 || nbytes > UFS_BLOCK_SIZE){
        return -1;
    }

    if(inum < 0 || inum >= superblock->num_inodes)
        return -1;

    if(get_bit(inode_bitmap, inum) != 1)
        return -1;

    if(offset < 0 || (offset + nbytes) > inode_table[inum].size)
        return -1;
    
    if(inode_table[inum].type == MFS_DIRECTORY){
        if(offset % sizeof(MFS_DirEnt_t) != 0)
            return -1;

        if(nbytes % sizeof(MFS_DirEnt_t) != 0)
            return -1;
    }

    int position_dataBlock = offset / UFS_BLOCK_SIZE;
    int position_insideBlock = offset % UFS_BLOCK_SIZE;
    int position_nextBlock = (offset + nbytes - 1) / UFS_BLOCK_SIZE;
    char *position_block = fs_img + inode_table[inum].direct[position_dataBlock] * UFS_BLOCK_SIZE + position_insideBlock;
    char *postion_next = 0;
    int byte_1 = nbytes;
    int byte_2 = 0;
    if(position_nextBlock > position_dataBlock){
        byte_1 = (position_dataBlock + 1) * UFS_BLOCK_SIZE - offset;
        byte_2 = nbytes - byte_1;
        postion_next = fs_img + inode_table[inum].direct[position_nextBlock] * UFS_BLOCK_SIZE;
    }

    for(int i = 0; i < byte_1; i++){
        buffer[i] = position_block[i];
    }

    for(int i = 0; i < byte_2; i++){
        buffer[byte_1 + i] = postion_next[i];
    }

    return 0;
}

int server_write(int inum, char* buffer, int offset, int nbytes)
{
    if (nbytes <= 0 || nbytes > UFS_BLOCK_SIZE){
        return -1;
    }

    if(inum < 0 || inum >= superblock->num_inodes)
        return -1;

    if(get_bit(inode_bitmap, inum) != 1)
        return -1;

    if(inode_table[inum].type != MFS_REGULAR_FILE)
        return -1;

    if(offset < 0 || offset > inode_table[inum].size || offset > DIRECT_PTRS * UFS_BLOCK_SIZE || (offset + nbytes) > DIRECT_PTRS * UFS_BLOCK_SIZE)
        return -1;

    if((offset + nbytes) > inode_table[inum].size){
        inode_table[inum].size = (offset + nbytes);
    }

    int position_dataBlock = offset / UFS_BLOCK_SIZE;
    int position_insideBlock = offset % UFS_BLOCK_SIZE;
    int position_nextBlock = (offset + nbytes - 1) / UFS_BLOCK_SIZE;
    char *position_next;
    int byte_1 = nbytes;
    int byte_2 = 0;
    char *position_block;
    if(position_dataBlock == position_nextBlock){
        if(inode_table[inum].direct[position_dataBlock] == -1){
            int data_loc = get_dataBlock();
            inode_table[inum].direct[position_dataBlock] = data_loc + superblock->data_region_addr;
        }
        position_block = fs_img + inode_table[inum].direct[position_dataBlock] * UFS_BLOCK_SIZE + position_insideBlock;
        for(int i = 0; i < byte_1; i++){
            position_block[i] = buffer[i];
        }
    } else {
        byte_1 = (position_dataBlock + 1) * UFS_BLOCK_SIZE - offset;
        byte_2 = nbytes - byte_1;
        position_block = fs_img + inode_table[inum].direct[position_dataBlock] * UFS_BLOCK_SIZE + position_insideBlock;
        for(int i = 0; i < byte_1; i++){
            position_block[i] = buffer[i];
        }

        if(inode_table[inum].direct[position_nextBlock] == -1){
            int data_loc = get_dataBlock();
            inode_table[inum].direct[position_nextBlock] = data_loc + superblock->data_region_addr;
        }
        position_next = fs_img + inode_table[inum].direct[position_nextBlock] * UFS_BLOCK_SIZE;
        for(int i = 0; i < byte_2; i++){
            position_block[i] = buffer[i + byte_1];
        }
    }
    write(fd, fs_img, fs.st_size);
    lseek(fd, 0, SEEK_SET);
    fsync(fd);
    return 0;

}

int server_create(int pinum, int type, char* name){
    int dict_data_loc = -2;
    int rc = server_lookup(pinum, name);
    if(rc > 0)
        return 0;

    if(inode_table[pinum].type != MFS_DIRECTORY)
        return -1;
    
    int inum = get_inode();
    if(inum == -1)
        return -1;

    inode_table[inum].type = type;

    if(type == MFS_REGULAR_FILE){
        inode_table[inum].size = 0;
        for (int i = 0; i < DIRECT_PTRS; i++){
            inode_table[inum].direct[i] = -1;
        }
    } else {
        inode_table[inum].size = 2 * sizeof(MFS_DirEnt_t);

        int dict_data_loc = get_dataBlock();

        if(dict_data_loc == -1){
            clean_bit(inode_bitmap, inum);
            return -1;
        }

        inode_table[inum].direct[0] = superblock->data_region_addr + dict_data_loc;

        for (int i = 1; i < DIRECT_PTRS; i++){
            inode_table[inum].direct[i] = -1;
        }

        MFS_DirEnt_t* dict_entry = (MFS_DirEnt_t *)(fs_img + inode_table[inum].direct[0] * UFS_BLOCK_SIZE);
        dict_entry[0].inum = inum;
        strcpy(dict_entry[0].name, ".");
        dict_entry[1].inum = pinum;
        strcpy(dict_entry[1].name, "..");

        for (int i = 2; i < 128; i++)
	        dict_entry[i].inum = -1;
    }
    
    int off = (inode_table[pinum].size - 1) / UFS_BLOCK_SIZE;
    int remain = (inode_table[pinum].size) % UFS_BLOCK_SIZE;
    if(remain != 0){
        MFS_DirEnt_t* dict_entry = (MFS_DirEnt_t *)(fs_img + inode_table[pinum].direct[off] * UFS_BLOCK_SIZE);
        int found = -1;
        for(int i = 0; i < 128; i++){
            if(dict_entry[i].inum == -1){
                found = i;
                break;
            }
        }
        dict_entry[found].inum = inum;
        strcpy(dict_entry[found].name, name);
    }else{
        off = off + 1;
        int data_loc = get_dataBlock();
        if(data_loc == -1){
            clean_bit(inode_bitmap, inum);
            if(dict_data_loc != -2)
                clean_bit(data_bitmap, dict_data_loc);
            return -1;
        }
        inode_table[pinum].direct[off] = data_loc;
        MFS_DirEnt_t* dict_entry = (MFS_DirEnt_t *)(fs_img + inode_table[pinum].direct[off] * UFS_BLOCK_SIZE);
        dict_entry[0].inum = inum;
        strcpy(dict_entry[0].name, name);
        for(int i = 1; i < 128; i++){
            dict_entry[i].inum = -1;
        }
    }

    inode_table[pinum].size += sizeof(MFS_DirEnt_t);
    write(fd, fs_img, fs.st_size);
    lseek(fd, 0, SEEK_SET);
    fsync(fd);
    return 0;
}

int server_unlink(int pinum, char* name)
{   
    int inum = -1;
    if(pinum < 0 || pinum >= superblock->num_inodes)
        return -1;

    if(get_bit(inode_bitmap, pinum) != 1)
        return -1;
    
    if(strlen(name) + 1 >= FILE_NAME_SIZE)
        return -1;
         
    if(inode_table[pinum].type != MFS_DIRECTORY)
        return -1;
    
    int size = inode_table[pinum].size;
    if (size < sizeof(MFS_DirEnt_t)){
        return 0;
    }

    for(int i = 0; i < DIRECT_PTRS; i++){
        if (inode_table[pinum].direct[i] == -1){
            break;
        }
     
        MFS_DirEnt_t* dict_entry = (MFS_DirEnt_t *)(fs_img + inode_table[pinum].direct[i] * UFS_BLOCK_SIZE);
        for(int j = 0; j < 128; j++){
            if(dict_entry[j].inum == -1)
                break;

            if(strcmp(name, dict_entry[j].name) == 0){
                inum = dict_entry[j].inum;
                dict_entry[j].inum = -1;
                inode_table[pinum].size-= sizeof(MFS_DirEnt_t);
                break;
            }
        }
    }

    if(inum == -1)
        return 0;

    if(inode_table[inum].type == MFS_DIRECTORY){
        if(inode_table[inum].size > 2 * sizeof(MFS_DirEnt_t))
            return -1;  
        
    }

    for(int i = 0; i < DIRECT_PTRS; i++){
        if (inode_table[inum].direct[i] == -1){
            break;
        }
     
        int data_loc = inode_table[inum].direct[i] - superblock->data_region_addr;
        clean_bit(data_bitmap, data_loc);
    }

    clean_bit(inode_bitmap, inum); 
    write(fd, fs_img, fs.st_size);
    lseek(fd, 0, SEEK_SET);
    fsync(fd);
    return 0;
}

int server_shutdown()
{
    fsync(fd);
    munmap(fs_img, fs.st_size);
    close(fd);
    return 0;
}

int server_init(int portnum, char* fsi_path)
{   
    signal(SIGINT, intHandler);
    fd = open(fsi_path, O_RDWR, S_IRWXU);
    if(fd < 0){
        perror("image does not exist\n");
        return -1;
    }
    
    fstat(fd, &fs);
    fs_img = mmap(NULL, fs.st_size, MAP_PRIVATE, PROT_READ | PROT_WRITE, fd, 0);
    superblock = (super_t *)fs_img;
    inode_bitmap = (unsigned int *)(fs_img + superblock->inode_bitmap_addr * UFS_BLOCK_SIZE);
    data_bitmap = (unsigned int *)(fs_img + superblock->data_bitmap_addr * UFS_BLOCK_SIZE);
    inode_table = (inode_t *)(fs_img + superblock->inode_region_addr * UFS_BLOCK_SIZE);

    sd = UDP_Open(portnum);
    if(sd < 0){
        perror("Cannot connect to portnum\n");
        close(fd);
        return -1;
    }

    UDP_Packet message;
    UDP_Packet reply;

    while (1) {
        struct sockaddr_in addr;
	    if(UDP_Read(sd, &addr, (char *)&message, sizeof(UDP_Packet)) < 1)
            continue;

        if(message.req == LOOKUP){
            reply.inum = server_lookup(message.inum, message.name);
            reply.req = RESPONSE;
            UDP_Write(sd, &addr, (char *)&reply, sizeof(UDP_Packet));
        }

        if(message.req == STAT){
            reply.rc = server_stat(message.inum, &reply);
            reply.req = RESPONSE;
            UDP_Write(sd, &addr, (char *)&reply, sizeof(UDP_Packet));
        }
        
        if(message.req == WRITE){
            reply.rc = server_write(message.inum, message.buffer, message.offset, message.buffer_size);
            reply.req = RESPONSE;
            UDP_Write(sd, &addr, (char *)&reply, sizeof(UDP_Packet));
        }

        if(message.req == READ){
            reply.rc = server_read(message.inum, reply.buffer, message.offset, message.buffer_size);
            reply.req = RESPONSE;
            UDP_Write(sd, &addr, (char *)&reply, sizeof(UDP_Packet));
        }
   
        if(message.req == CREAT){
            reply.rc = server_create(message.inum, message.type, message.name);
            reply.req = RESPONSE;
            UDP_Write(sd, &addr, (char *)&reply, sizeof(UDP_Packet));
        }
        
        if(message.req == UNLINK) {
            reply.rc = server_unlink(message.inum, message.name);
            reply.req = RESPONSE;
            UDP_Write(sd, &addr, (char *)&reply, sizeof(UDP_Packet));
        }
        
        if(message.req == SHUTDOWN) {
            reply.req = RESPONSE;
            reply.rc = 0;
            UDP_Write(sd, &addr, (char *)&reply, sizeof(UDP_Packet));
            return server_shutdown();
        } 
    }
    return 0; 
}

int main(int argc, char *argv[]) 
{
    if(argc != 3){
        perror("Correct format: server [portnum] [file-system-image]\n");
        exit(1);
    }

    int rc = server_init(atoi(argv[1]), argv[2]);
    if (rc < 0){
        exit(1);
    }

    exit(0);
    return 0; 
}
    


 