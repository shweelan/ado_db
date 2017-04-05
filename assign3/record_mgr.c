#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "expr.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <float.h>
#include <math.h>



// helpers
//Creates new string and copies the value
char *copyString(char *_string) {
  char *string = newStr(strlen(_string));
  strcpy(string, _string);
  return string;
}

//Pin the page
RC atomicPinPage(BM_BufferPool *bm, BM_PageHandle **resultPage, int pageIdx) {
  BM_PageHandle *page = new(BM_PageHandle);
  RC err = pinPage(bm, page, pageIdx);
  if (err) {
    free(page);
  }
  *(resultPage) = page;
  return err;
}

//Calls the unpin method.
RC atomicUnpinPage(BM_BufferPool *bm, BM_PageHandle *page, bool dirty) {
  RC err;
  if (dirty) {
    if ((err = markDirty(bm, page))) {
      free(page);
      return err;
    }
  }
  err = unpinPage(bm, page);
  free(page);
  return err;
}


void freeSchemaObjects(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int *keyAttrs) {
  int i;
  for (i = 0; i < numAttr; i++) {
    free(attrNames[i]);
  }
  free(attrNames);
  free(dataTypes);
  free(typeLength);
  free(keyAttrs);
}


int getSchemaStringLength(char *string) {
  int length;
  int formatLen = 8;
  char intString[formatLen + 1];
  memcpy(&intString[0], string, formatLen);
  intString[formatLen] = '\0';
  length = (int) strtol(intString, NULL, 16);
  return length;
}


char * stringifySchema(Schema *schema) {
  char *string = newStr(PAGE_SIZE - 1); // -1 because newStr adds 1 byte for \0 terminator
  int formatLen = 8;
  char *format = "%08x";
  char intString[formatLen + 1];// +1 for \0 terminator
  sprintf(&intString[0], format, 0); // placeholder for schema length
  strcat(string, intString);
  strcat(string, DELIMITER);
  sprintf(&intString[0], format, schema->numAttr);
  strcat(string, intString);
  strcat(string, DELIMITER);
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    strcat(string, schema->attrNames[i]);
    strcat(string, DELIMITER);
    sprintf(&intString[0], format, schema->dataTypes[i]);
    strcat(string, intString);
    strcat(string, DELIMITER);
    sprintf(&intString[0], format, schema->typeLength[i]);
    strcat(string, intString);
    strcat(string, DELIMITER);
  }
  sprintf(&intString[0], format, schema->keySize);
  strcat(string, intString);
  strcat(string, DELIMITER);
  i = 0;
  for (i = 0; i < schema->keySize; i++) {
    sprintf(&intString[0], format, schema->keyAttrs[i]);
    strcat(string, intString);
    if (i < schema->keySize - 1) { // no delimiter for the last value
      strcat(string, DELIMITER);
    }
  }
  int length = (int) strlen(string);
  length++; // +1 for \0 terminator
  if (length > PAGE_SIZE) {
    // TODO throw error
  }
  sprintf(&intString[0], format, length);
  memcpy(string, &intString, formatLen);
  return string;
}

/*
 *Method to parse Schema (convert to string)
 */
Schema * parseSchema(char *_string) {
  int length = getSchemaStringLength(_string);
  char *string = newCharArr(length);
  memcpy(string, _string, length);
  char *token;
  token = strtok(string, DELIMITER); // ignore it's already the length of schema
  token = strtok(NULL, DELIMITER);
  int numAttr = (int) strtol(token, NULL, 16);
  char **attrNames = newArray(char *, numAttr);
  DataType *dataTypes = newArray(DataType, numAttr);
  int *typeLength = newIntArr(numAttr);
  int i;
  for (i = 0; i < numAttr; i++) {
    token = strtok(NULL, DELIMITER);
    attrNames[i] = copyString(token);

    token = strtok(NULL, DELIMITER);
    dataTypes[i] = (DataType) strtol(token, NULL, 16);

    token = strtok(NULL, DELIMITER);
    typeLength[i] = (int) strtol(token, NULL, 16);
  }
  token = strtok(NULL, DELIMITER);
  int keySize = (int) strtol(token, NULL, 16);
  int *keyAttrs = newIntArr(keySize);
  for (i = 0; i < keySize; i++) {
    token = strtok(NULL, DELIMITER);
    keyAttrs[i] = (int) strtol(token, NULL, 16);
  }
  Schema *s = createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs);
  freeSchemaObjects(numAttr, attrNames, dataTypes, typeLength, keyAttrs);
  free(string);
  return s;
}

//Method to get pointer position
int getAttrStartingPosition(Schema *schema, int attrNum) {
  int position = 0;
  int i;
  for (i = 0; i < attrNum; i++) {
    switch (schema->dataTypes[i]) {
      case DT_INT:
        position += sizeof(int);
        break;
      case DT_FLOAT:
        position += sizeof(float);
        break;
      case DT_STRING:
        position += schema->typeLength[i] + 1; // +1 for \0 terminator
        break;
      case DT_BOOL:
        position += sizeof(bool);
        break;
    }
  }
  return position;
}

//Prints the schema
void printSchema(Schema *schema) {
  char del;
  printf("{\n\tnumAttr : %d,\n\tattrs : [\n", schema->numAttr);
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    del = (i < schema->numAttr - 1) ? ',' : ' ';
    printf("\t\t[%s,%d,%d]%c\n", schema->attrNames[i], (int) schema->dataTypes[i], schema->typeLength[i], del);
  }
  printf("\t],\n\tkeySize : %d,\n\tkeyAttrs : [\n", schema->keySize);
  for (i = 0; i < schema->keySize; i++) {
    del = (i < schema->keySize - 1) ? ',' : ' ';
    printf("\t\t%d%c\n", schema->keyAttrs[i], del);
  }
  printf("\t]\n}\n\n");
}

//Prints the record
void printRecord(Schema *schema, Record * record) {
  char del;
  Value *val;
  int i;
  RC err;
  printf("\n{\n");
  for (i = 0; i < schema->numAttr; i ++) {
    del = (i < schema->numAttr - 1) ? ',' : ' ';
    if ((err = getAttr(record, schema, i, &val))) {
      // TODO throw err
    }
    switch (val->dt) {
      case DT_INT:
        printf("\t%s : %d%c\n", schema->attrNames[i], val->v.intV, del);
        break;
      case DT_FLOAT:
        printf("\t%s : %g%c\n", schema->attrNames[i], val->v.floatV, del);
        break;
      case DT_BOOL:
        if (val->v.boolV) {
          printf("\t%s : true%c\n", schema->attrNames[i], del);
        }
        else {
          printf("\t%s : false%c\n", schema->attrNames[i], del);
        }
        break;
      case DT_STRING:
        printf("\t%s : \"%s\"%c\n", schema->attrNames[i], val->v.stringV, del);
        free(val->v.stringV);
        break;
    }
    free(val);
  }
  printf("}\n\n");
}

//Gets size of record in Bytes
int getRecordSizeInBytes(Schema *schema, bool withTerminators) {
  int size = 0;
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    switch (schema->dataTypes[i]) {
      case DT_INT:
        size += sizeof(int);
        break;
      case DT_FLOAT:
        size += sizeof(float);
        break;
      case DT_STRING:
        size += schema->typeLength[i];
        if (withTerminators) {
          size++; // +1 for \0 terminator
        }
        break;
      case DT_BOOL:
        size += sizeof(bool);
        break;
    }
  }
  return size;
}

bool isSetBit(char *bitMap, int bitIdx) {
  int byteIdx = bitIdx / NUM_BITS;
  int bitSeq = NUM_BITS - (bitIdx % NUM_BITS);
  if (bitMap[byteIdx] & (1 << (bitSeq - 1))) {
    return true;
  }
  return false;
}

//Used to set memory indicators
void setBit(char *bitMap, int bitIdx) {
  int byteIdx = bitIdx / NUM_BITS;
  int bitSeq = NUM_BITS - (bitIdx % NUM_BITS);
  bitMap[byteIdx] = bitMap[byteIdx] | 1 << (bitSeq - 1);
}

//Used to reset memory indicators
void unsetBit(char *bitMap, int bitIdx) {
  int byteIdx = bitIdx / NUM_BITS;
  int bitSeq = NUM_BITS - (bitIdx % NUM_BITS);
  bitMap[byteIdx] = bitMap[byteIdx] & ~(1 << (bitSeq - 1));
}

//Used to get free memory locations
int getUnsetBitIndex(char *bitMap, int bitmapSize) {
  int bytesCount = 0;
  unsigned char byte;
  while(bytesCount < bitmapSize) {
    byte = bitMap[bytesCount];
    if (byte < UCHAR_MAX) {
      int i = NUM_BITS;
      while(byte & (1 << (i - 1))) {
        --i;
      }
      return (bytesCount * NUM_BITS) + (NUM_BITS - i);
    }
    bytesCount++;
  }
  return -1;
}


RC getEmptyPage(RM_TableData *rel, int *pageNum) {
  RC err;
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  BM_PageHandle *page;
  // page 1 free pages bitmap
  if ((err = atomicPinPage(bm, &page, 1))) {
    return err;
  }

  int res = getUnsetBitIndex(page->data, PAGE_SIZE);
  if (res == -1) {
    printf("!!!!!!!!!!!!!!!!!!!! PANIC !! PAGES OVERFLOW\n");
    exit(0);
  }
  *(pageNum) = res;
  return atomicUnpinPage(bm, page, false);
}


RC changePageFillBit(RM_TableData *rel, int pageNum, bool bitVal) {
  if (pageNum > NUM_BITS * PAGE_SIZE - 1) {
    printf("!!!!!!!!!!!!!!!!!!!! PANIC !! PAGES OVERFLOW\n");
    exit(0);
  }
  RC err;
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  BM_PageHandle *page;
  // page 1 free pages bitmap
  if ((err = atomicPinPage(bm, &page, 1))) {
    return err;
  }

  if (bitVal) { // mark as full
    setBit(page->data, pageNum);
  }
  else {
    unsetBit(page->data, pageNum);
  }
  return atomicUnpinPage(bm, page, true); // true for markDirty
}


//functionality


RC initRecordManager (void *mgmtData) {
  initStorageManager();
  return RC_OK;
}


RC shutdownRecordManager () {
  return RC_OK;
}

//Create table functionality
//Tables are stored Page2 onwards
RC createTable (char *name, Schema *schema) {
  RC err;
  if ((err = createPageFile(name))) {
    return err;
  }

  // No need to ensureCapacity, because creating a file already ensures one block, we dont need more for now.
  char *schemaString = stringifySchema(schema);
  SM_FileHandle fileHandle;
  if ((err = openPageFile(name, &fileHandle))) {
    return err;
  }
  if ((err = writeBlock(0, &fileHandle, schemaString))) {
    free(schemaString);
    return err;
  }
  free(schemaString);
  if ((err = closePageFile(&fileHandle))) {
    return err;
  }
  return RC_OK;
}

//Opens the table, close the table after completing operations
RC openTable (RM_TableData *rel, char *name) {
  RC err;
  BM_BufferPool *bm = new(BM_BufferPool);
  BM_PageHandle *pageHandle;
  if ((err = initBufferPool(bm, name, PER_TBL_BUF_SIZE, RS_LRU, NULL))) {
    free(bm);
    return err;
  }
  if ((err = atomicPinPage(bm, &pageHandle, 0))) {
    free(bm);
    return err;
  }
  Schema *schema = parseSchema(pageHandle->data);
  rel->name = copyString(name);
  rel->schema = schema;
  int recordSize = getRecordSizeInBytes(schema, true);
  int pageSize = PAGE_SIZE - PAGE_HEADER_LEN;
  rel->maxSlotsPerPage = floor((pageSize * NUM_BITS) / (recordSize * NUM_BITS + 1)); // pageSize = (X * recordSize) + (X / NUM_BITS)
  rel->slotsBitMapSize = ceil(((float) rel->maxSlotsPerPage) / NUM_BITS);
  rel->recordByteSize = recordSize;
  rel->mgmtData = bm;
  return atomicUnpinPage(bm, pageHandle, false); // false for not markDirty
}


int getNumTuples (RM_TableData *rel) {
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  forceFlushPool(bm);
  PoolMgmtInfo *pmi = (PoolMgmtInfo *)(bm->mgmtData);
  SM_FileHandle *fHandle = pmi->fHandle;
  int totalNumDataPages = fHandle->totalNumPages - TABLE_HEADER_PAGES_LEN;
  BM_PageHandle *page;
  int i = 0;
  int res = 0;
  short totalSlots;
  while(i < totalNumDataPages) {
    if (atomicPinPage(bm, &page, i + TABLE_HEADER_PAGES_LEN)) {
      return -1;
    }
    memcpy(&totalSlots, page->data, sizeof(short));
    res += totalSlots;
    if (atomicUnpinPage(bm, page, false)) {
      return -1;
    }
    i++;
  }
  return res;
}

//Close the table, and releases the bufferPool and schema
RC closeTable (RM_TableData *rel) {
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
	RC err;
  if ((err = shutdownBufferPool(bm))) {
    return err;
  }
  free(rel->name);
  if ((err = freeSchema(rel->schema))) {
    return err;
  }
  free(rel->mgmtData);
  return RC_OK;
}


/*
 *Create schema function
 *For each table there will be a file.
 *The first page (page#0) of the file is the schema informations about that table.
 *The second page (page#1) of the file will be a bit map for tracing the pages that have at least one empty slot.
 */
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
  Schema *schema = new(Schema);
  schema->numAttr = numAttr;
  schema->attrNames = newArray(char *, numAttr);
  schema->dataTypes = newArray(DataType, numAttr);
  schema->typeLength = newIntArr(numAttr);
  int i;
  int len;
  for (i = 0; i < numAttr; i++) {
    schema->attrNames[i] = copyString(attrNames[i]);
    schema->dataTypes[i] = dataTypes[i];
    switch (dataTypes[i]) {
      case DT_INT:
        len = sizeof(int);
        break;
      case DT_FLOAT:
        len = sizeof(float);
        break;
      case DT_BOOL:
        len = sizeof(bool);
        break;
      case DT_STRING:
        len = typeLength[i];
        break;
    }
    schema->typeLength[i] = len;
  }
  schema->keySize = keySize;
  schema->keyAttrs = newIntArr(keySize);
  for (i = 0; i < keySize; i++) {
    schema->keyAttrs[i] = keys[i];
  }
  return schema;
}

//free schema
RC freeSchema (Schema *schema) {
  freeSchemaObjects(schema->numAttr, schema->attrNames, schema->dataTypes, schema->typeLength, schema->keyAttrs);
  free(schema);
  return RC_OK;
}


int getRecordSize (Schema *schema) {
  return getRecordSizeInBytes(schema, false);
}


RC deleteTable (char *name) {
  return destroyPageFile(name);
}


RC createRecord (Record **record, Schema *schema) {
  int size = getRecordSizeInBytes(schema, true);
  Record *r = new(Record);
  r->data = newCharArr(size);
  *record = r;
  return RC_OK;
}


RC freeRecord (Record *record) {
  free(record->data);
  free(record);
  return RC_OK;
}

//Set the valuess in record
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
  int position = getAttrStartingPosition(schema, attrNum);
  char *ptr = record->data;
  ptr += position;
  int size;
  switch(value->dt) {
    case DT_INT:
      if (value->v.intV > INT_MAX || value->v.intV < INT_MIN) {
        return RC_RM_LIMIT_EXCEEDED;
      }
      size = sizeof(int);
      memcpy(ptr, &value->v.intV, size);
      break;
    case DT_FLOAT:
      if (value->v.floatV > FLT_MAX || value->v.floatV < FLT_MIN) {
        return RC_RM_LIMIT_EXCEEDED;
      }
      size = sizeof(float);
      memcpy(ptr, &value->v.floatV, size);
      break;
    case DT_STRING:
      if (strlen(value->v.stringV) > schema->typeLength[attrNum]) {
        return RC_RM_LIMIT_EXCEEDED;
      }
      size = schema->typeLength[attrNum] + 1; // +1 for \0 terminator
      memcpy(ptr, value->v.stringV, size);
      ptr[size - 1] = '\0'; // for safety
      break;
    case DT_BOOL:
      if (value->v.boolV != true && value->v.boolV != false) {
        return RC_RM_LIMIT_EXCEEDED;
      }
      size = sizeof(bool);
      memcpy(ptr, &value->v.boolV, size);
      break;
    default :
      return RC_RM_UNKOWN_DATATYPE;
  }
  return RC_OK;
}


RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
  Value *val = new(Value);
  int position = getAttrStartingPosition(schema, attrNum);
  int size;
  char *ptr = record->data;
  ptr += position;
  val->dt = schema->dataTypes[attrNum];
  switch (schema->dataTypes[attrNum]) {
    case DT_INT:
      size = sizeof(int);
      memcpy(&val->v.intV, ptr, size);
      break;
    case DT_FLOAT:
      size = sizeof(float);
      memcpy(&val->v.floatV, ptr, size);
      break;
    case DT_STRING:
      size = schema->typeLength[attrNum];
      val->v.stringV = newStr(size);
      memcpy(val->v.stringV, ptr, size + 1); // +1 for \0 terminator
      break;
    case DT_BOOL:
      size = sizeof(bool);
      memcpy(&val->v.boolV, ptr, size);
      break;
  }
  *value = val;
  return RC_OK;
}

//Insert the record into the table mentioned in the relation
//Method sets the RID in the record for future use
RC insertRecord (RM_TableData *rel, Record *record) {
  RC err;
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  BM_PageHandle *page;
  int pageNum = 0;
  int slotNum;
  short totalSlots;
  if ((err = getEmptyPage(rel, &pageNum))) {
    return err;
  }
  if ((err = atomicPinPage(bm, &page, pageNum + TABLE_HEADER_PAGES_LEN))) {
    return err;
  }
  char *ptr = page->data;
  memcpy(&totalSlots, ptr, sizeof(short));
  ptr = page->data + PAGE_HEADER_LEN;
  slotNum = getUnsetBitIndex(ptr, rel->slotsBitMapSize);
  if (slotNum < 0 || slotNum >= rel->maxSlotsPerPage) { // -1 means not found and it is a problem
    printf("!!!!!!!!!!!!!!!!!!!! PANIC !! SOME ERROR HAPPENED WITH PAGE#%d SLOT#%d WITH TOTAL %d\n", pageNum, slotNum, totalSlots);
    exit(0);
  }

  int recordSize = rel->recordByteSize;
  int position = PAGE_HEADER_LEN + rel->slotsBitMapSize + (recordSize * slotNum);
  ptr = page->data + position;
  memcpy(ptr, record->data, recordSize);

  ptr = page->data + PAGE_HEADER_LEN;
  totalSlots++;
  memcpy(page->data, &totalSlots, sizeof(short)); // writing the totalSlots
  setBit(ptr, slotNum); // setting the slot bit
  if ((err = atomicUnpinPage(bm, page, true))) { // true for markDirty
    return err;
  }

  if (totalSlots >= rel->maxSlotsPerPage) {
    if ((err = changePageFillBit(rel, pageNum, true))) { // true set it as full
      return err;
    }
  }
  record->id.page = pageNum;
  record->id.slot = slotNum;
  return RC_OK;
}

//get the record details
RC getRecord (RM_TableData *rel, RID id, Record *record) {
  RC err;
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  BM_PageHandle *page;
  int pageNum = id.page;
  int slotNum = id.slot;
  if ((err = atomicPinPage(bm, &page, pageNum + TABLE_HEADER_PAGES_LEN))) {
    return err;
  }
  char *ptr = page->data;
  ptr += PAGE_HEADER_LEN;

  if (!isSetBit(ptr, slotNum)) {
    atomicUnpinPage(bm, page, false); // used instead of atomicUnpinPage because we need to free page all the time
    return RC_RM_NO_SUCH_TUPLE;
  }
  int recordSize = rel->recordByteSize;
  int position = PAGE_HEADER_LEN + rel->slotsBitMapSize + (recordSize * slotNum);
  ptr = page->data; // pointer to bytes array
  ptr += position;
  memcpy(record->data, ptr, recordSize);
  return atomicUnpinPage(bm, page, false); // false for not markDirty
}

//functions deletes the record in rid
RC deleteRecord (RM_TableData *rel, RID id) {
  RC err;
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  BM_PageHandle *page;
  int pageNum = id.page;
  int slotNum = id.slot;

  if ((err = atomicPinPage(bm, &page, pageNum + TABLE_HEADER_PAGES_LEN))) {
    return err;
  }

  char *ptr = page->data + PAGE_HEADER_LEN;

  if (!isSetBit(ptr, slotNum)) {
    atomicUnpinPage(bm, page, false); // used instead of atomicUnpinPage because we need to free page all the time
    return RC_RM_NO_SUCH_TUPLE;
  }
  unsetBit(ptr, slotNum);
  short totalSlots;
  ptr = page->data;
  memcpy(&totalSlots, ptr, sizeof(short)); // may use later
  totalSlots--;
  memcpy(ptr, &totalSlots, sizeof(short));
  if ((err = atomicUnpinPage(bm, page, true))) { // true for markDirty
    return err;
  }
  if (totalSlots == rel->maxSlotsPerPage - 1) { // means it was full page, mark it as unfull now
    err = changePageFillBit(rel, pageNum, false); // false set it as unfull
  }
  return RC_OK;
}

//updates the record
RC updateRecord (RM_TableData *rel, Record *record) {
  RC err;
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  BM_PageHandle *page;
  int pageNum = record->id.page;
  int slotNum = record->id.slot;

  if ((err = atomicPinPage(bm, &page, pageNum + TABLE_HEADER_PAGES_LEN))) {
    return err;
  }

  char *ptr = page->data + PAGE_HEADER_LEN;

  if (!isSetBit(ptr, slotNum)) {
    atomicUnpinPage(bm, page, false); // used instead of atomicUnpinPage because we need to free page all the time
    return RC_RM_NO_SUCH_TUPLE;
  }
  int recordSize = rel->recordByteSize;
  int position = PAGE_HEADER_LEN + rel->slotsBitMapSize + (recordSize * slotNum);
  ptr = page->data + position;
  memcpy(ptr, record->data, recordSize);
  return atomicUnpinPage(bm, page, true);
}

//start scan
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
  BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
  forceFlushPool(bm);
  ScanMgmtInfo *smi = new(ScanMgmtInfo);
  PoolMgmtInfo *pmi = (PoolMgmtInfo *)(bm->mgmtData);
  SM_FileHandle *fHandle = pmi->fHandle;
  int totalNumDataPages = fHandle->totalNumPages - TABLE_HEADER_PAGES_LEN;
  smi->page = 0;
  smi->slot = -1; // next Slot is 0
  smi->totalPages = totalNumDataPages;
  smi->maxSlots = rel->maxSlotsPerPage;
  smi->condition = cond;
  scan->rel = rel;
  scan->mgmtData = smi;
  return RC_OK;
}


//used in conjuction with start scan to get next recod in scan
RC next (RM_ScanHandle *scan, Record *record) {
  ScanMgmtInfo *smi = (ScanMgmtInfo *)(scan->mgmtData);
  Value *val;
  RC err;
  if (++smi->slot >= smi->maxSlots) {
    smi->slot = 0;
    if(++smi->page >= smi->totalPages) {
      return RC_RM_NO_MORE_TUPLES;
    }
  }
  record->id.page = smi->page;
  record->id.slot = smi->slot;

  err = getRecord(scan->rel, record->id, record);
  if (err) {
    if (err == RC_RM_NO_SUCH_TUPLE) {
      return next(scan, record);
    }
    return err;
  }
  if ((err = evalExpr(record, scan->rel->schema, smi->condition, &val))) {
    return err;
  }
	if (val->v.boolV == 1) {
    free(val);
    return RC_OK;
  }
  free(val);
  return next(scan, record);
}

//close the scan
RC closeScan (RM_ScanHandle *scan) {
  ScanMgmtInfo *smi = (ScanMgmtInfo *)(scan->mgmtData);
  free(smi);
	return RC_OK;
}
