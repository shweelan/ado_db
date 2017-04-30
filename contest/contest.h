#ifndef CONTEST_H
#define CONTEST_H

#include "dberror.h"
#include "buffer_mgr.h"

extern RC setUpContest (int numPages);
extern RC shutdownContest (void);

extern long getContestIOs (BM_BufferPool *const bm);

#endif
