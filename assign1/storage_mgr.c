#include "storage_mgr.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>


int getFd(SM_FileHandle *fHandle) {
  return *((int*)fHandle->mgmtInfo);
}

void setFD(SM_FileHandle *fHandle, int fd) {
  fHandle->mgmtInfo = malloc(sizeof(int));
  *((int*)fHandle->mgmtInfo) = fd;
}

int isFHandleInit(SM_FileHandle *fHandle) {
  return (fHandle->mgmtInfo != NULL);
}

int initialized = 0;

// This function must crash any error
void initStorageManager (void) {
  if (initialized) {
    return;
  }
  struct stat state;
  int ret;
  if (stat(PATH_DIR, &state) == -1) { // folder does not Exists
    ret = mkdir(PATH_DIR , DEFAULT_MODE);
    if(ret == -1) {
      return throw_error();
    }
  }
  ret = chdir(PATH_DIR);
  if (ret == -1) {
    return throw_error();
  }
  initialized = 1;
}


RC createPageFile (char *fileName) {
  initStorageManager();
  int fd;
  /* in test file, it expects to overwrite the file.
  if(access(fileName, F_OK) != -1) {
    return RC_FILE_ALREADY_EXISTS;
  }
  */
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
  fd = creat(fileName, mode);
  if (fd == -1) {
    return RC_FS_ERROR;
  }
  // write an empty block to file upon creation
  SM_PageHandle memPage = (char *) calloc(PAGE_SIZE, sizeof(char));
  size_t writeSize;
  writeSize = pwrite(fd, memPage, (size_t) PAGE_SIZE, (off_t) 0);
  if (writeSize == -1) {
    return RC_FS_ERROR;
  }
  return RC_OK;
}


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
  fHandle->totalNumPages = file_size/PAGE_SIZE;
  fHandle->curPagePos = 0;
  setFD(fHandle, fd);
  return RC_OK;
}


RC closePageFile (SM_FileHandle *fHandle) {
  if (fHandle->mgmtInfo == NULL) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  int ret = close(getFd(fHandle));
  if (ret == -1) {
    return RC_FS_ERROR;
  }
  return RC_OK;
}


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


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (!isFHandleInit(fHandle)) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
    return RC_BLOCK_POSITION_ERROR;
  }
  memPage = malloc(sizeof(char) * PAGE_SIZE);
  size_t readSize;
  readSize = pread(getFd(fHandle), memPage, (size_t) PAGE_SIZE, (off_t) pageNum * PAGE_SIZE);
  if (readSize == -1) {
    return RC_FS_ERROR;
  }
  fHandle->curPagePos = pageNum + 1;
  return RC_OK;
}


int getBlockPos (SM_FileHandle *fHandle) {
  if (!isFHandleInit(fHandle)) {
    return -1; // here can not return RC_FILE_HANDLE_NOT_INIT, because it is integer value, -1 means no fHandle
  }
  return fHandle->curPagePos;
}


RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(0, fHandle, memPage);
}


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->curPagePos - 1;
  return readBlock(pageNum, fHandle, memPage);
}


RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return readBlock(fHandle->curPagePos, fHandle, memPage);
}


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->curPagePos + 1;
  return readBlock(pageNum, fHandle, memPage);
}


RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  int pageNum = fHandle->totalNumPages - 1;
  return readBlock(pageNum, fHandle, memPage);
}


RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
  if (!isFHandleInit(fHandle)) {
    return RC_FILE_HANDLE_NOT_INIT;
  }
  // here is only > so that we can append to the end of the file but we can not skip a block
  if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
    return RC_BLOCK_POSITION_ERROR;
  }
  size_t writeSize;
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


RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
  return writeBlock(fHandle->curPagePos, fHandle, memPage);
}


RC appendEmptyBlock (SM_FileHandle *fHandle) {
  SM_PageHandle memPage = (char *) calloc(PAGE_SIZE, sizeof(char));
  RC ret;
  return writeBlock(fHandle->totalNumPages, fHandle, memPage);
}


RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
  RC ret;
  while (numberOfPages > fHandle->totalNumPages) {
    if ((ret = appendEmptyBlock(fHandle)) != RC_OK) {
      return ret;
    }
  }
  return RC_OK;
}
