#include "storage_mgr.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>


/* Get the file descriptor from the file handle */
int getFd(SM_FileHandle *fHandle) {
  return *((int*)fHandle->mgmtInfo);
}


/* Set the file descriptor from the file handle */
void setFD(SM_FileHandle *fHandle, int fd) {
  fHandle->mgmtInfo = malloc(sizeof(int));
  *((int*)fHandle->mgmtInfo) = fd;
}

/* Check if the file handle is initialize */
int isFHandleInit(SM_FileHandle *fHandle) {
  return (fHandle->mgmtInfo != NULL);
}


/* A flag to determine if the storage manager is initialized */
int initialized = 0;

/*
  initialize the storage manager
  create the database directory to some pre configured path
  change directory to that path
  this function will crash whenever there is an error, because if something went wrong, nothing can be done afterward
*/
void initStorageManager (void) {
  if (initialized) {
    return;
  }
  struct stat state;
  int ret;
  // folder does not Exists, create it
  if (stat(PATH_DIR, &state) == -1) {
    ret = mkdir(PATH_DIR , DEFAULT_MODE);
    if(ret == -1) {
      return throwError();
    }
  }
  ret = chdir(PATH_DIR);
  if (ret == -1) {
    return throwError();
  }
  initialized = 1;
}


/* Create a file and write an empty block to it */
RC createPageFile (char *fileName) {
  initStorageManager();
  int fd;

  /* in test file, it expects to overwrite the file.
  if(access(fileName, F_OK) != -1) {
    return RC_FILE_ALREADY_EXISTS;
  }
  */

  /*
   * S_IRUSR : Read permission to user ,S_IWUSR : Write permissions to users
   * S_IRGRP : Read permission to group ,S_IRGRP : Write permission to group
   * S_IROTH : Read permission to others ,S_IWOTH : Write permission to others
  */
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH | O_TRUNC;
  fd = creat(fileName, mode);
  if (fd == -1) {
    return RC_FS_ERROR;
  }

  // write an empty block to file upon creation
  SM_PageHandle memPage = (char *) calloc(PAGE_SIZE, sizeof(char));
  size_t writeSize;
  writeSize = pwrite(fd, memPage, (size_t) PAGE_SIZE, (off_t) 0);
  free(memPage);
  if (writeSize == -1) {
    return RC_FS_ERROR;
  }
  // close the file, since it will be reopened when needed
  int ret = close(fd);
  if(ret == -1) {
    return RC_FS_ERROR;
  }
  return RC_OK;
}


/* Open a file and copy it's properties to the file handle */
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
  initStorageManager();
  if(access(fileName, F_OK) == -1) {
    return RC_FILE_NOT_FOUND;
  }
  int fd;
  fd = open(fileName, O_RDWR);
  if (fd == -1) {
    return RC_FS_ERROR;
  }

  struct stat state;
  stat(fileName, &state);
  long file_size = state.st_size;
  fHandle->fileName = fileName;
  fHandle->totalNumPages = (int) (file_size/PAGE_SIZE);
  fHandle->curPagePos = 0;
  setFD(fHandle, fd);
  return RC_OK;
}

/* Close an opened file */
RC closePageFile (SM_FileHandle *fHandle) {
  if (!isFHandleInit(fHandle)) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  int ret = close(getFd(fHandle));
  free(fHandle->mgmtInfo);
  if (ret == -1) {
    return RC_FS_ERROR;
  }
  return RC_OK;
}


/* Remove a file by it's name */
RC destroyPageFile (char *fileName) {
  if(access(fileName, F_OK) == -1) {
    return RC_FILE_NOT_FOUND;
  }
  int ret = unlink(fileName);
  if (ret == -1) {
    return RC_FS_ERROR;
  }
  return RC_OK;
}


/* Read a block from a file by the block number */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (!isFHandleInit(fHandle)) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  if (pageNum < 0) {
    return RC_BLOCK_POSITION_ERROR;
  }
  if (pageNum >= fHandle->totalNumPages) {
    RC err = ensureCapacity(pageNum, fHandle);
    if (err != RC_OK) {
      return err;
    }
  }
  size_t readSize;
  // Choosing pread/pwrite is more stable in performance and faster than seek+read/write and it is multi thread friendly
  readSize = pread(getFd(fHandle), memPage, (size_t) PAGE_SIZE, (off_t) pageNum * PAGE_SIZE);
  if (readSize == -1) {
    return RC_FS_ERROR;
  }
  fHandle->curPagePos = pageNum + 1;
  return RC_OK;
}


/* get the current block position */
int getBlockPos (SM_FileHandle *fHandle) {
  if (!isFHandleInit(fHandle)) {
    // here can not return RC_FILE_HANDLE_NOT_INIT, because it is integer value, -1 means no fHandle
    return -1;
  }
  return fHandle->curPagePos;
}


/* Read the first Block of the File */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(0, fHandle, memPage);
}


/* Read the block that is previous to current position */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->curPagePos - 1;
  return readBlock(pageNum, fHandle, memPage);
}


/* Read the block that is currently pointed to by the page position pointer */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(fHandle->curPagePos, fHandle, memPage);
}


/* Read the block that is next to current position */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->curPagePos + 1;
  return readBlock(pageNum, fHandle, memPage);
}


/* Read the last block in the file */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->totalNumPages - 1;
  return readBlock(pageNum, fHandle, memPage);
}


/* write a block to a file by the block number */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (!isFHandleInit(fHandle)) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  // here is only > so that we can append to the end of the file but we can not skip a block
  if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
    return RC_BLOCK_POSITION_ERROR;
  }
  size_t writeSize;
  // Choosing pread/pwrite is more stable in performance and faster than seek+read/write and it is multi thread friendly
  writeSize = pwrite(getFd(fHandle), memPage, (size_t) PAGE_SIZE, (off_t) pageNum * PAGE_SIZE);
  if (writeSize == -1) {
    return RC_WRITE_FAILED;
  }
  fHandle->curPagePos = pageNum + 1;
  // means we added new block
  if (pageNum == fHandle->totalNumPages) {
    fHandle->totalNumPages++;
  }
  return RC_OK;
}


/* write the block that is currently pointed to by the page position pointer */
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return writeBlock(fHandle->curPagePos, fHandle, memPage);
}


/* Extend the file by adding new empty block */
RC appendEmptyBlock (SM_FileHandle *fHandle) {
  SM_PageHandle memPage = (char *) calloc(PAGE_SIZE, sizeof(char));
  RC err = writeBlock(fHandle->totalNumPages, fHandle, memPage);
  free(memPage);
  return err;
}


/* Ensure ensure the that the file is extended to some number of pages */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
  RC ret;
  while (numberOfPages > fHandle->totalNumPages) {
    if ((ret = appendEmptyBlock(fHandle)) != RC_OK) {
      return ret;
    }
  }
  return RC_OK;
}
