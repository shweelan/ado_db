#include "contest.h"

#include "dberror.h"

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

/* set up record, buffer, pagefile, and index managers */
RC
setUpContest (int numPages)
{
  initStorageManager();
  initRecordManager(NULL);
  return RC_OK;
}

/* shutdown record, buffer, pagefile, and index managers */
RC
shutdownContest (void)
{
  shutdownRecordManager();
  return RC_OK;
}

/* return the total number of I/O operations used after setUpContest */
long
getContestIOs (BM_BufferPool *const bm)
{
  int numR = getNumReadIO(bm);
  int numW = getNumWriteIO(bm);
  printf("\n#Num of readIO: %d, #Num of Bytes Read: %f MB\n", numR, (float)(numR * PAGE_SIZE) / (1024 * 1024));
  printf("#Num of writeIO: %d, #Num of Bytes Written: %f MB\n", numW, (float)(numW * PAGE_SIZE) / (1024 * 1024));
  return numR + numW;
}
