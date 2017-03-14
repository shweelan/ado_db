#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include <string.h>
#include <stdlib.h>


// helpers

char * stringifySchema(Schema *schema) {
  char *string = (char *) malloc(sizeof(char) * PAGE_SIZE); // TODO free it[count]
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


Schema * parseSchema(char *string) {
  char *token; // TODO check memory leakage
  token = strtok(string, DELIMITER);
  int numAttr = (int) strtol(token, NULL, 16);
  char **attrNames = (char **) malloc(sizeof(char *) * numAttr); // TODO free it, Done in freeSchema
  DataType *dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr); // TODO free it, Done in freeSchema
  int *typeLength = (int *) malloc(sizeof(int) * numAttr); // TODO free it, Done in freeSchema
  int i;
  for (i = 0; i < numAttr; i++) {
    token = strtok(NULL, DELIMITER);
    attrNames[i] = (char *) malloc(sizeof(char) * (strlen(token) + 1)); // +1 for \0 terminator // TODO free it, Done in freeSchema
    strcpy(attrNames[i], token);

    token = strtok(NULL, DELIMITER);
    dataTypes[i] = (DataType) strtol(token, NULL, 16);

    token = strtok(NULL, DELIMITER);
    typeLength[i] = (int) strtol(token, NULL, 16);
  }
  token = strtok(NULL, DELIMITER);
  int keySize = (int) strtol(token, NULL, 16);
  int *keyAttrs = (int *) malloc(sizeof(int) * keySize); // TODO free it, Done in freeSchema
  for (i = 0; i < keySize; i++) {
    token = strtok(NULL, DELIMITER);
    keyAttrs[i] = (int) strtol(token, NULL, 16);
  }
  return createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs);
}


void printSchema(Schema *schema) {
  char del = ',';
  printf("{\nnumAttr : %d,\n\tattrs : [\n", schema->numAttr);
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    del = (i < schema->numAttr - 1) ? ',' : ' ';
    printf("\t\t[%s,%d,%d]%c\n", schema->attrNames[i], (int) schema->dataTypes[i], schema->typeLength[i], del);
  }
  printf("\t],\n\tkeySize : %d,\n\t : [\n", schema->keySize);
  for (i = 0; i < schema->keySize; i++) {
    del = (i < schema->keySize - 1) ? ',' : ' ';
    printf("\t\t%d%c\n", schema->keyAttrs[i], del);
  }
  printf("\t]\n}\n\n");
}

//functionality


RC initRecordManager (void *mgmtData) {
  // TODO
  return RC_OK;
}


RC shutdownRecordManager () {
  // TODO
  return RC_OK;
}


RC createTable (char *name, Schema *schema) {
  // TODO check if not already exists.
  RC err;
  if ((err = createPageFile(name))) {
    return err; // TODO THROW maybe because nothing can be dont after this point
  }

  // No need to ensureCapacity, because creating a file already ensures one block.
  // TODO check if schemaString is less than pageSize, else (find a new solution)
  char *schemaString = stringifySchema(schema);
  SM_FileHandle fileHandle;
  if ((err = openPageFile(name, &fileHandle))) {
    return err;
  }
  if ((err = writeBlock(0, &fileHandle, schemaString))) {
    return err;
  }
  if ((err = closePageFile(&fileHandle))) {
    return err;
  }
  return RC_OK;
}


RC openTable (RM_TableData *rel, char *name) {
  return RC_OK;
}


Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
  Schema *schema = (Schema *) malloc(sizeof(Schema)); // TODO free it, Done in freeSchema
  schema->numAttr = numAttr;
  schema->attrNames = attrNames;
  schema->dataTypes = dataTypes;
  schema->typeLength = typeLength;
  schema->keySize = keySize;
  schema->keyAttrs = keys;
  return schema;
}


RC freeSchema (Schema *schema) {
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    free(schema->attrNames[i]);
  }
  free(schema->attrNames);
  free(schema->dataTypes);
  free(schema->typeLength);
  free(schema->keyAttrs);
  free(schema);
  return RC_OK;
}


int main(int argc, char *argv[]) {
  int a = 4;
  char **b = (char **) malloc(sizeof(char *) * a);
  b[0] = (char *) malloc(sizeof(char) * 2);
  strcpy(b[0], "SH");
  b[1] = (char *) malloc(sizeof(char) * 2);
  strcpy(b[1] , "WE");
  b[2] = (char *) malloc(sizeof(char) * 2);
  strcpy(b[2] , "EL");
  b[3] = (char *) malloc(sizeof(char) * 2);
  strcpy(b[3] , "AN");
  DataType *c = (DataType *) malloc(sizeof(DataType) * a);
  c[0] = c[1] = c[2] = c[3] = 1;
  int *d = (int *) malloc(sizeof(int) * a);
  d[0] = 10;
  d[1] = 30;
  d[2] = 20;
  d[3] = 5;
  int e = 2;
  int *f = (int *) malloc(sizeof(int) * e);
  f[0] = 1;
  f[1] = 3;
  printf("\n\n");
  Schema *s = createSchema(a, b, c, d, e, f);
  printf("first schema : ");
  printSchema(s);
  char *ss = stringifySchema(s);
  printf("first schema string : %s\n\n", ss);
  Schema *ns = parseSchema(ss);
  printf("second schema : ");
  printSchema(ns);
  char *nss = stringifySchema(ns);
  printf("second schema string : %s\n\n", nss);
  printf("\n");
  freeSchema(s);
  freeSchema(ns);
  free(ss);
  free(nss);
  return 0;
}
