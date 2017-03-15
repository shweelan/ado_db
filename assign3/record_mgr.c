#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include <string.h>
#include <stdlib.h>


// helpers


char *copyString(char *_string) {
  char *string = newStr(strlen(_string)); // TODO free it[count, multiple maybe]
  strcpy(string, _string);
  return string;
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


char * stringifySchema(Schema *schema) {
  char *string = newCharArr(PAGE_SIZE); // TODO free it[count, multiple maybe]
  char intString[9];
  char *format = "%08x";
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
  return string;
}


Schema * parseSchema(char *_string) {
  char *string = copyString(_string); // TODO free it, Done below
  char *token;
  token = strtok(string, DELIMITER);
  int numAttr = (int) strtol(token, NULL, 16);
  char **attrNames = newArray(char *, numAttr); // TODO free it, Done in freeSchemaObjects
  DataType *dataTypes = newArray(DataType, numAttr); // TODO free it, Done in freeSchemaObjects
  int *typeLength = newIntArr(numAttr); // TODO free it, Done in freeSchemaObjects
  int i;
  for (i = 0; i < numAttr; i++) {
    token = strtok(NULL, DELIMITER);
    attrNames[i] = copyString(token); // TODO free it, Done in freeSchemaObjects

    token = strtok(NULL, DELIMITER);
    dataTypes[i] = (DataType) strtol(token, NULL, 16);

    token = strtok(NULL, DELIMITER);
    typeLength[i] = (int) strtol(token, NULL, 16);
  }
  token = strtok(NULL, DELIMITER);
  int keySize = (int) strtol(token, NULL, 16);
  int *keyAttrs = newIntArr(keySize); // TODO free it, Done in freeSchemaObjects
  for (i = 0; i < keySize; i++) {
    token = strtok(NULL, DELIMITER);
    keyAttrs[i] = (int) strtol(token, NULL, 16);
  }
  Schema *s = createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs);
  freeSchemaObjects(numAttr, attrNames, dataTypes, typeLength, keyAttrs);
  free(string);
  return s;
}


void printSchema(Schema *schema) {
  char del = ',';
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


//functionality


RC initRecordManager (void *mgmtData) {
  // TODO
  initStorageManager();
  return RC_OK;
}


RC shutdownRecordManager () {
  // TODO
  return RC_OK;
}


RC createTable (char *name, Schema *schema) {
  // TODO check if not already exists. // TODO downcase the name
  RC err;
  if ((err = createPageFile(name))) {
    return err; // TODO THROW maybe because nothing can be dont after this point
  }

  // No need to ensureCapacity, because creating a file already ensures one block, we dont need more for now.
  // TODO check if schemaString is less than pageSize, else (find a new solution)
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
  printSchema(schema);
  return RC_OK;
}


RC openTable (RM_TableData *rel, char *name) {
  // TODO checl all errors and free resources on error or throw it. // TODO downcase name
  RC err;
  BM_BufferPool *bm = new(BM_BufferPool); // TODO free it
  BM_PageHandle *pageHandle = new(BM_PageHandle); // TODO free it, Done below
  if ((err = initBufferPool(bm, name, PER_TBL_BUF_SIZE, RS_LRU, NULL))) {
    return err;
  }
  if ((err = pinPage(bm, pageHandle, 0))) {
    return err;
  }
  Schema *schema = parseSchema(pageHandle->data);
  rel->name = copyString(name); // TODO free it, Done in closeTable
  rel->schema = schema;
  rel->mgmtData = bm;
  if ((err = unpinPage(bm, pageHandle))) {
    return err;
  }
  free(pageHandle);
  printSchema(rel->schema);
  return RC_OK;
}


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


Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
  Schema *schema = new(Schema); // TODO free it, Done in freeSchema
  schema->numAttr = numAttr;
  schema->attrNames = newArray(char *, numAttr); // TODO free it, Done in freeSchema
  schema->dataTypes = newArray(DataType, numAttr); // TODO free it, Done in freeSchema
  schema->typeLength = newIntArr(numAttr); // TODO free it, Done in freeSchema
  int i;
  for (i = 0; i < numAttr; i++) {
    schema->attrNames[i] = copyString(attrNames[i]); // TODO free it, Done in freeSchema
    schema->dataTypes[i] = dataTypes[i];
    schema->typeLength[i] = typeLength[i];
  }
  schema->keySize = keySize;
  schema->keyAttrs = newIntArr(keySize); // TODO free it, Done in freeSchema
  for (i = 0; i < keySize; i++) {
    schema->keyAttrs[i] = keys[i];
  }
  return schema;
}


RC freeSchema (Schema *schema) {
  freeSchemaObjects(schema->numAttr, schema->attrNames, schema->dataTypes, schema->typeLength, schema->keyAttrs);
  free(schema);
  return RC_OK;
}


int getRecordSize (Schema *schema) {
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
        break;
      case DT_BOOL:
        size += sizeof(bool);
        break;
    }
  }
  return size;
}


RC deleteTable (char *name) {
  RC err = destroyPageFile(name);
  // TODO more descriptive error
  return err;
}


int main(int argc, char *argv[]) {
  int a = 4;
  char **b = newArray(char *, a);
  b[0] = "SH";
  b[1] = "WE";
  b[2] = "EL";
  b[3] = "AN";
  DataType *c = newArray(DataType, a);
  c[0] = c[1] = c[2] = c[3] = 1;
  int *d = newIntArr(a);
  d[0] = 10;
  d[1] = 30;
  d[2] = 20;
  d[3] = 5;
  int e = 2;
  int *f = newIntArr(e);
  f[0] = 1;
  f[1] = 3;
  printf("\n\n");
  Schema *s = createSchema(a, b, c, d, e, f);
  initRecordManager(NULL);
  createTable("shweelan", s);
  RM_TableData *rel = new(RM_TableData);
  openTable(rel, "shweelan");
  closeTable(rel);
  deleteTable("shweelan");
  printf("first schema : ");
  printSchema(s);
  char *ss = stringifySchema(s);
  printf("first schema string : %s\n", ss);
  printf("first schema record size : %d\n\n", getRecordSize(s));
  Schema *ns = parseSchema(ss);
  printf("second schema : ");
  printSchema(ns);
  char *nss = stringifySchema(ns);
  printf("second schema string : %s\n", nss);
  printf("second schema record size : %d\n\n", getRecordSize(ns));
  printf("\n");
  freeSchema(s);
  freeSchema(ns);
  free(ss);
  free(nss);
  free(b);
  free(c);
  free(d);
  free(f);
  return 0;
}
