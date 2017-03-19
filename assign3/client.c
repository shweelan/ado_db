#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "record_mgr.h"
#include "dberror.h"
#include "const.h"

#define MAX_NAME 256
#define MAX_NUM_ATTR 16
#define MAX_STRING 7000


char *copyStr(char *_string) {
  char *string = newStr(strlen(_string)); // TODO free it[count, multiple maybe]
  strcpy(string, _string);
  return string;
}


void showError(char *msg) {
  printf("ERROR! : %s!\n", msg);
}


void processCommand(char *input) {
  RC err;
  if (strcmp("CT", input) == 0) {
    char *token = newStr(PAGE_SIZE);
    printf("Enter table name! (MAX_LEN=%d) : ", MAX_NAME - 1); // -1 for \0 terminator
    scanf("%s", token);
    if (strlen(token) > MAX_NAME - 1) {
      return showError("MAX EXCEEDED");
    }
    char *tableName = newStr(MAX_NAME);
    printf("Enter number of attributes! (MAX=%d) : ", MAX_NUM_ATTR);
    scanf("%s", token);
    int numAttrs = (int) strtol(token, NULL, 10);
    if (numAttrs > MAX_NUM_ATTR) {
      return showError("MAX EXCEEDED");
    }
    char **attrNames = newArray(char *, numAttrs);
    DataType *dataTypes = newArray(DataType, numAttrs);
    int *typeLength = newIntArr(numAttrs);
    int i;
    DataType dt;
    int dtlen;
    for (i = 0; i < numAttrs; i++) {
      printf("Building attribute#%d!\n", i);
      printf("\tEnter attribute#%d name! (MAX_LEN=%d) : ", i, MAX_NAME - 1);
      scanf("%s", token);
      if (strlen(token) > MAX_NAME - 1) {
        return showError("MAX EXCEEDED");
      }
      attrNames[i] = copyStr(token);
      printf("\tEnter attribute#%d type! [0 : INT, 1 : STRING, 2 : FLOAT, 3 : BOOL] : ", i);
      scanf("%s", token);
      dt = (DataType) strtol(token, NULL, 10);
      if (dt != 0 && dt != 1 && dt != 2 && dt != 3) {
        return showError("UNKNOWN DATA TYPE");
      }
      dataTypes[i] = dt;
      if (dt == 1) {
        printf("\tEnter attribute#%d length! (MAX=%d) : ", i, MAX_STRING);
        scanf("%s", token);
        dtlen = (int) strtol(token, NULL, 10);
        if (dtlen > MAX_STRING) {
          return showError("MAX EXCEEDED");
        }
        typeLength[i] = dtlen;
      }
      else {
        typeLength[i] = 0;
      }
    }
    int *keys = newArray(int, 1);
    keys[0] = 0;
    Schema *schema = createSchema(numAttrs, attrNames, dataTypes, typeLength, 1, keys);
    err = createTable (tableName, schema);
    if (err) {
      printf("%d\n", err);
      return showError("SOMETHING WENT WRONG");
    }
    printf("Table created!\n");
  }
  else if (strcmp(input, "IR") == 0) {
    // TODO
  }
  else {
    showError("UNKNOWN COMMAND");
  }
}


int main(int argc, char *argv[]) {
  char *input = newStr(PAGE_SIZE);
  while(1) {
    printf("Enter your command! [CT : createTable, IR : insertRecord, DR : deleteRecord, UR : updateRecord, ST : scanTable, DT : deleteTable] : ");
    scanf("%s", input);
    processCommand(input);
  }
}
