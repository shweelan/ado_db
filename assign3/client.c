#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "record_mgr.h"
#include "dberror.h"
#include "const.h"

#define MAX_NAME 256
#define MAX_NUM_ATTR 16
#define MAX_STRING 7000
#define MAX_NAME_ATTEMPTS 5
#define RID_DELIM "."
#define DISPLAY_LEN 1000

//These are error for client
#define STR_OK 0
#define STR_EMPTY 1
#define STR_TOO_LONG 2
#define STR_INVALID 3

char *copyStr(char *_string) {
  char *string = newStr(strlen(_string)); // TODO free it[count, multiple maybe]
  strcpy(string, _string);
  return string;
}


void showError(char *msg) {
  printf("ERROR! : %s!\n", msg);
}

int validateName(char *str) {
  str[0] = tolower(str[0]);
  if (str[0] < 97 || str[0] > 122) {
    return -1;
  }
  for(int i = 1; str[i]; i++) {
    if (i >= MAX_NAME - 1) {
      return -1;
    }
    str[i] = tolower(str[i]);
    if (str[i] == ',') {
      return -1;
    }
  }
  return 0;
}

void getName(char *token) {
  while (true) {
    scanf("%s", token);
    if (validateName(token) == -1) {
      showError("NAMES MUST NOT EXCEED THE MAX! NAMES MUST START WITH LETTER! NAMES MUST NOT CONTAIN `,`!");
      printf("Please try again : ");
      continue;
    }
    break;
  }
}

int getInput(char* displayString, char* token, int size, bool validate){
  printf("%s", displayString);
  fgets(token, size, stdin);
  size_t tokenLen = strlen(token);
  if(token[0] == '\n'){
    return STR_EMPTY;
  }
  if (token[tokenLen-1] == '\n'){
    if (tokenLen > 1){
      token[tokenLen-1] = 0;
    }
    if(validate){
      if (validateName(token) == -1) {
        return STR_INVALID;
      }
    }
  }else {
    return STR_TOO_LONG;
  }
  return STR_OK;
}


char* getTableName(char* lastTableName, bool validate){
  char*token = newStr(MAX_NAME);
  char* displayStr = newStr(DISPLAY_LEN);
  char* tableName ;
  if(strlen(lastTableName)>0){
    sprintf(displayStr, "Enter table name or press enter to use '%s'! (MAX_LEN=%d) : ", lastTableName, MAX_NAME - 1);
  }else {
    sprintf(displayStr, "Enter table name! (MAX_LEN=%d) : ", MAX_NAME - 1); // -1 for \0 terminator
  }
  
  int str = getInput(displayStr, token, MAX_NAME, validate);
  if (str == STR_TOO_LONG) {
    showError("MAX EXCEEDED");
  }else if(str==STR_EMPTY){
    tableName = copyStr(lastTableName);
  } else if(str==STR_INVALID){
    showError("NAMES MUST NOT EXCEED THE MAX! NAMES MUST START WITH LETTER! NAMES MUST NOT CONTAIN `,`! \nPlease try again : ");
  } else{
    tableName = copyStr(token);
  }
  free(displayStr);
  return tableName;
}

void populateRecord(RM_TableData *rel, char* tableName, Record *record, bool isUpdate){
  Value * val = new(Value);
  char *token;
  DataType * datatype = rel->schema->dataTypes;
  char ** attrNames = rel->schema->attrNames;
  int* typeLen = rel->schema->typeLength;
  
  if(isUpdate){
    printf("\n-----------------------------------");
    printf("\nCurrent record values are: \n");
    printf("\n-----------------------------------");
    printRecord(rel->schema, record);
    printf("-----------------------------------\n");
  }
  
  for (int i=0; i<rel->schema->numAttr;i++){
    int len = typeLen[i];
    if(len == 0){
      len = 4;
    }
    char * displayStr = malloc(1000);
    if(isUpdate){
      sprintf(displayStr, "Enter the value for %s, press enter to skip :", attrNames[i]);
    } else {
      sprintf(displayStr, "Enter the value for %s:", attrNames[i]);
    }
    token=newStr(PAGE_SIZE);
    int skip = getInput(displayStr, token, len, false);
    if(skip==STR_EMPTY){
      continue;
    }

    switch ((DataType)datatype[i]) {
      case DT_INT:
        val->dt = DT_INT;
        //val->v.intV = (int) strtol(token, NULL, 10);
        val->v.intV = atoi(token);
        setAttr(record, rel->schema, i, val);
        break;
        
      case DT_FLOAT:
        val->dt = DT_FLOAT;
        val->v.floatV = atof(token);
        setAttr(record, rel->schema, i, val);
        break;
      case DT_BOOL:
        val->dt = DT_BOOL;
        val->v.boolV = (strcmp(token,"1")==0)?true:false;
        setAttr(record, rel->schema, i, val);
        break;
      case DT_STRING:
        val->dt = DT_STRING;
        val->v.stringV = token;
        setAttr(record, rel->schema, i, val);
        break;
    }
    free(token);
    free(displayStr);
  }
  free(val);
}

void processCommand(char *input, char * lastTableName) {
  RC err;
  char *displayStr = malloc(DISPLAY_LEN);
  if (strcmp("CT", input) == 0) {
    int str;
    char* tableName= getTableName("", true);
    if(tableName==NULL || strlen(tableName)==0){
      return;
    }
    char *token = newStr(PAGE_SIZE);
    sprintf(displayStr,"Enter number of attributes! (MAX=%d) : ", MAX_NUM_ATTR);
    str = getInput(displayStr, token, 10, false);
    
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
      sprintf(displayStr, "\tEnter attribute#%d name! (MAX_LEN=%d) : ", i, MAX_NAME - 1);
      str = getInput(displayStr, token, MAX_NAME, true);
      if(str==STR_INVALID){
        showError("NAMES MUST NOT EXCEED THE MAX! NAMES MUST START WITH LETTER! NAMES MUST NOT CONTAIN `,`!");
        printf("Please try again : ");
        return;
      }
      attrNames[i] = copyStr(token);
      sprintf(displayStr,"\tEnter attribute#%d type! [0 : INT, 1 : STRING, 2 : FLOAT, 3 : BOOL] : ", i);
      str =getInput(displayStr, token, 10, false);
      dt = (DataType) strtol(token, NULL, 10);
      if (dt != 0 && dt != 1 && dt != 2 && dt != 3) {
        return showError("UNKNOWN DATA TYPE");
      }
      dataTypes[i] = dt;
      if (dt == 1) {
        sprintf(displayStr,"\tEnter attribute#%d length! (MAX=%d) : ", i, MAX_STRING);
        getInput(displayStr, token, 10, false);
        //scanf("%s", token);
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
    Schema * schema = createSchema(numAttrs, attrNames, dataTypes, typeLength, 1, keys);
    err = createTable (tableName, schema);
    if (err) {
      printf("%d\n", err);
      showError("SOMETHING WENT WRONG");
    } else {
      printf("Table `%s` created!\n", tableName);
      memcpy(lastTableName, tableName, MAX_NAME);
    }
    
    free(tableName);
    free(attrNames);
    free(token);
  }
  else if (strcmp(input, "IR") == 0) {
    char* tableName= getTableName(lastTableName, false);
    RM_TableData *rel = new(RM_TableData);
    Record *record;
    int rc = openTable(rel, tableName);
    if(rc != RC_OK) {
      return showError("UNKNOWN TABLE!");
    }
    createRecord(&record, rel->schema);
    populateRecord(rel, tableName, record, false);
    insertRecord(rel, record);
    printf("Record inserted! RID: %d.%d\n",record->id.page,record->id.slot);
    closeTable(rel);
    memcpy(lastTableName, tableName, MAX_NAME);
    free(tableName);
  }
  else if (strcmp(input, "DR") == 0) {
    char* tableName= getTableName(lastTableName, false);
    RM_TableData *rel = new(RM_TableData);
    
    int rc = openTable(rel, tableName);
    if(rc != RC_OK) {
      showError("UNKNOWN TABLE!");
    } else {
      char *token = newStr(PAGE_SIZE);
      
      sprintf(displayStr,"Enter RID for record deletion! : "); // -1 for \0 terminator
      getInput(displayStr, token, 10, false);
      //scanf("%s", token);
      
      RID *rid= new(RID);;
      char *temp1= malloc(30);
      char *temp2= malloc(30);
      temp1 = strtok(token, RID_DELIM);
      temp2 = strtok(NULL, RID_DELIM);
      if(temp1 == NULL || strlen(temp1) == 0 || temp2 == NULL || strlen(temp2) == 0){
        free(temp1);
        free(temp2);
        free(rid);
        free(displayStr);
        free(token);
        free(tableName);
        return showError("Incorrect Format! Try again!");
      }
      rid->page = atoi(temp1);
      rid->slot = atoi(temp2); //TODO: add null chk
      int err = deleteRecord(rel, *rid);
      if (RC_OK==err){
        printf("Record Deleted Successfully!\n");
      } else {
        printf("Record deletion failed with code : %d\n", err); // -1 for \0 terminator
      }
      free(token);
    }
    memcpy(lastTableName, tableName, MAX_NAME);
    free(tableName);
    closeTable(rel);
  }
  else if (strcmp(input, "UR") == 0) {
    char *token = newStr(PAGE_SIZE);
    char* tableName= getTableName(lastTableName, false);
    RM_TableData *rel = new(RM_TableData);
    int rc = openTable(rel, tableName);
    free(tableName);
    if(rc != RC_OK) {
      return showError("UNKNOWN Table!\n");
    }

    Record *record = new(Record);
    createRecord(&record, rel->schema);
    
    sprintf(displayStr,"Enter RID for record updation! : "); // -1 for \0 terminator
    getInput(displayStr, token, 10, false);
    //scanf("%s", token);
    
    RID *rid= new(RID);
    char *temp1= malloc(30);
    char *temp2= malloc(30);
    temp1 = strtok(token, RID_DELIM);
    temp2 = strtok(NULL, RID_DELIM);
    if(temp1 == NULL || strlen(temp1) == 0 || temp2 == NULL || strlen(temp2) == 0){
      free(temp1);
      free(temp2);
      free(rid);
      free(displayStr);
      free(record);
      free(token);
      free(tableName);
      return showError("Incorrect Format! Try again!");
    }
    rid->page = atoi(temp1);
    rid->slot = atoi(temp2); //TODO: add null chk
    record->id=*rid;
    getRecord(rel, *rid, record);
    if(rc != RC_OK) {
      return showError("Record doesnot exist!\n");
    }
    
    populateRecord(rel, tableName, record, true);
    
    rc= updateRecord(rel, record);
    
    if(rc != RC_OK) {
      showError("Record Updation Failed!\n");
    } else {
      printf("Record updated! \n");
    }
    closeTable(rel);
    memcpy(lastTableName, tableName, MAX_NAME);
    free(tableName);
    free(rid);
    free(record);
    free(rel);
    free(token);
    
  }
  else if (strcmp(input, "ST") == 0) {
    char *token = newStr(PAGE_SIZE);
    char* tableName= getTableName(lastTableName, false);
    RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
    RM_TableData *rel = (RM_TableData *) malloc(sizeof(RM_TableData));
      
    openTable(rel, tableName);
    //Expr cond = NULL;
    //printf("Enter the condition! (MAX_LEN=%d) : ", MAX_NAME - 1); // -1 for \0 terminator
    // Mix 2 scans with c=3 as condition
    Expr *se1, *left, *right;
    MAKE_CONS(left, stringToValue("id"));
    MAKE_ATTRREF(right, 2);
    MAKE_BINOP_EXPR(se1, left, right, OP_COMP_EQUAL);
      
    Record *record;
    createRecord(&record, rel->schema);
    startScan(rel, sc, se1);
    while(true) {
      err = next(sc, record);
      if (err != RC_OK) {
        if (err != RC_RM_NO_MORE_TUPLES) {
          return showError("SOMETHING WENT WRONG");
        }
        break;
      }
      printRecord(rel->schema, record);
    }
    printf("%s : Table scan complete! \n",tableName);
    closeTable(rel);
    free(sc);
    free(rel);
    memcpy(lastTableName, tableName, MAX_NAME);
    free(tableName);
    free(token);
  }
  else if (strcmp(input, "DT") == 0) {
    char *token = newStr(PAGE_SIZE);
    char* tableName= getTableName(lastTableName, false);
    int rc = deleteTable(tableName);
    if(rc != RC_OK) {
      showError("UNKNOWN TABLE!");
    } else {
      printf("%s : Table deleted! \n",tableName);
    }
    memcpy(lastTableName, "", MAX_NAME);
    free(tableName);
    free(token);
  }
  else if(strcmp(input, "GR") == 0){
    char *token = newStr(PAGE_SIZE);
    char* tableName= getTableName(lastTableName, false);
    RM_TableData *rel = new(RM_TableData);
    int rc = openTable(rel, tableName);
    if(rc != RC_OK) {
      return showError("UNKNOWN Table!\n");
    }
    Record *record = new(Record);
    createRecord(&record, rel->schema);
    sprintf(displayStr,"Enter RID for record retrieval! : "); // -1 for \0 terminator
    getInput(displayStr, token, 10, false);
    //scanf("%s", token);
    
    RID *rid= new(RID);
    char *temp1= malloc(30);
    char *temp2= malloc(30);
    temp1 = strtok(token, RID_DELIM);
    temp2 = strtok(NULL, RID_DELIM);
    if(temp1 == NULL || strlen(temp1) == 0 || temp2 == NULL || strlen(temp2) == 0){
      free(temp1);
      free(temp2);
      free(rid);
      free(displayStr);
      free(record);
      free(token);
      free(tableName);
      return showError("Incorrect Format! Try again!");
    }
    rid->page = atoi(temp1);
    rid->slot = atoi(temp2); //TODO: add null chk
    record->id = *rid;
    rc= getRecord(rel, *rid, record);

    if(rc != RC_OK) {
      return showError("UNKNOWN RECORD!\n");
    }
    
    printf("Record: \n");
    printRecord(rel->schema, record);
    memcpy(lastTableName, tableName, MAX_NAME);
    free(tableName);
    free(record);
    free(rid);
  }
  else {
    showError("UNKNOWN COMMAND");
  }
  free(displayStr);
}

int main(int argc, char *argv[]) {
  initRecordManager(NULL);
  char *input = newStr(PAGE_SIZE);
  char *str = newStr(PAGE_SIZE);
  char *lastTableName = newStr(MAX_NAME);
  while(1) {
    sprintf(str,"\nEnter your command! [CT : createTable, IR : insertRecord, GR : GetRecord, DR : deleteRecord, UR : updateRecord, ST : scanTable, DT : deleteTable] : ");
    getInput(str, input, PAGE_SIZE, false);
    //scanf("%s", input);
    processCommand(input, lastTableName);
  }
}
