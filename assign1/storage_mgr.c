//  CS 525 assignment

#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"

struct stat st = {0};
#define PATH_DIR1 "/tmp/database"
#define DELIM "/"
#define DEFAULT_MODE 0777

int main() {
    SM_FileHandle *fHandle;
    initStorageManager();
    createPageFile ("HelloWorld.txt");
    openPageFile("HelloWorld.txt", fHandle);
    closePageFile(fHandle);
}

void initStorageManager (void){
    printf("Initialization of storage manager\n");
    if ( stat( PATH_DIR1, &st) == -1) {
        int mk = mkdir( PATH_DIR1 , DEFAULT_MODE);
        if(mk == 0) {
            //Exit??
        }
    }
}

RC createPageFile (char *fileName){
    int f;
    char sel;
    char filepath[256];
    snprintf(filepath, sizeof filepath, "%s%s%s", PATH_DIR1, DELIM, fileName);
    
    if( access( filepath, F_OK ) != -1 ) {
        printf("File Exists!!\nOver-write file (y/n): ");
        scanf("%c",&sel);
        if(sel=='n')
            return RC_OK;
        else {
            unlink(filepath);
        }
    }
    
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
    
    f = creat(filepath, mode);
    return RC_OK;
    
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){

    int f;
    char filepath[256];
    snprintf(filepath, sizeof filepath, "%s%s%s", PATH_DIR1, DELIM, fileName);
    
    if ((f = open(filepath, O_APPEND, O_RDONLY)) > 0) {
        struct stat fileStat;
        stat(fileName,&fileStat);
        long fileSize = fileStat.st_size;
        
        fHandle->fileName = fileName;
        fHandle->totalNumPages = fileSize/PAGE_SIZE;
        fHandle->curPagePos = ftell(f)/PAGE_SIZE;
        fHandle->mgmtInfo = f;

    }
    return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle){
    char filepath[256];
    snprintf(filepath, sizeof filepath, "%s%s%s", PATH_DIR1, DELIM, fHandle->fileName);
    close(filepath);
    return RC_OK;
}

RC destroyPageFile (char *fileName){
    char filepath[256];
    snprintf(filepath, sizeof filepath, "%s%s%s", PATH_DIR1, DELIM, fileName);
    unlink(filepath);
    return RC_OK;
}
