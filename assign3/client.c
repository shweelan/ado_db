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
    if (str[i] == ',' || str[i] == ' ') {
      return -1;
    }
  }
  return 0;
}

void getName(char *token) {
  while (true) {
    scanf("%s", token);
    if (validateName(token) == -1) {
      showError("NAMES MUST NOT EXCEED THE MAX! NAMES MUST START WITH LETTER! NAMES MUST NOT CONTAIN `,` OR SPACES");
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
  }
  else {
    return STR_TOO_LONG;
  }
  return STR_OK;
}

char getType(char *str) {
  if (strcmp(str, "true") == 0) {
    return 't'; // bool true
  }
  if (strcmp(str, "false") == 0) {
    return 'f'; // bool false, f is used for float
  }
  char *ep = NULL;
  long i = strtol(str, &ep, 10);
  if (!*ep) {
    return 'i'; // int
  }
  ep = NULL;
  double f = strtod (str, &ep);
  if (!ep  ||  *ep) {
    return 's'; // string
  }
  return 'p'; // float point
}

int getAttrPostionByName(Schema *schema, char *name) {
  for (int i = 0; i < schema->numAttr; i++) {
    if (strcmp(schema->attrNames[i], name) == 0) {
      return i;
    }
  }
  return -1;
}


Expr *makeExpr(char *exprString, Schema *schema) {
  Expr *expr = NULL;
  char *stringValue = newCharArr(PAGE_SIZE);
  char type = getType(exprString);
  int len;
  switch (type) {
    case 'i':
      sprintf(stringValue, "i%d", (int) strtol(exprString, NULL, 10));
      MAKE_CONS(expr, stringToValue(stringValue));
      break;
    case 'p':
      sprintf(stringValue, "f%f", (float) strtod(exprString, NULL));
      MAKE_CONS(expr, stringToValue(stringValue));
      break;
    case 't':
    case 'f':
      sprintf(stringValue, "b%c", type);
      MAKE_CONS(expr, stringToValue(stringValue));
      break;
    case 's':
      len = strlen(exprString);
      if (exprString[0] == '\"' && exprString[len - 1] == '\"') {
        exprString[len - 1] = '\0';
        sprintf(stringValue, "b%s", (exprString + 1));
        MAKE_CONS(expr, stringToValue(stringValue));
      } else {
        int idx = getAttrPostionByName(schema, exprString);
        if (idx >= 0) {
          MAKE_ATTRREF(expr, idx);
        }
      }
      break;
  }
  free(stringValue);
  return expr;
}

Expr *makeOpExpr(Expr *left, Expr *right, char *op) {
  Expr *expr = NULL;
  Expr *tmp1 = NULL;
  Expr *tmp2 = NULL;
  if (strcmp(op, "=") == 0) {
    MAKE_BINOP_EXPR(expr, left, right, OP_COMP_EQUAL);
    return expr;
  }
  if (strcmp(op, "<") == 0) {
    MAKE_BINOP_EXPR(expr, left, right, OP_COMP_SMALLER);
    return expr;
  }
  if (strcmp(op, ">") == 0) {
    MAKE_BINOP_EXPR(expr, right, left, OP_COMP_SMALLER);
    return expr;
  }
  if (strcmp(op, ">=") == 0) {
    MAKE_BINOP_EXPR(tmp1, right, left, OP_COMP_SMALLER);
    MAKE_BINOP_EXPR(tmp2, left, right, OP_COMP_EQUAL);
    MAKE_BINOP_EXPR(expr, tmp1, tmp2, OP_BOOL_OR);
    return expr;
  }
  if (strcmp(op, "<=") == 0) {
    MAKE_BINOP_EXPR(tmp1, left, right, OP_COMP_SMALLER);
    MAKE_BINOP_EXPR(tmp2, left, right, OP_COMP_EQUAL);
    MAKE_BINOP_EXPR(expr, tmp1, tmp2, OP_BOOL_OR);
    return expr;
  }
  if (strcmp(op, "!=") == 0) {
    MAKE_BINOP_EXPR(tmp1, left, right, OP_COMP_EQUAL);
    MAKE_UNOP_EXPR(expr, tmp1, OP_BOOL_NOT);
    return expr;
  }
  return expr;
}


Expr *makeLOpExpr(Expr *left, Expr *right, char *lop) {
  Expr *expr = NULL;
  if (strcmp(lop, "&") == 0) {
    MAKE_BINOP_EXPR(expr, left, right, OP_BOOL_AND);
    return expr;
  }
  if (strcmp(lop, "|") == 0) {
    MAKE_BINOP_EXPR(expr, left, right, OP_BOOL_OR);
    return expr;
  }
  return expr;
}


Expr *parseExpressions(Schema *schema) {
  char *token = newCharArr(PAGE_SIZE);
  getInput("Input expression : ", token, PAGE_SIZE, false);
  int i = 0;
  int j = 0;
  bool isStringExpr = false;
  char c;
  char *expr = newCharArr(PAGE_SIZE);
  char *exprsTokens[7]; // max 2 boolean terms each. e.g a > 5 & s <= 6
  int numExprs = 0;
  for (i = 0; (c = token[i]); i++) {
    if (!isStringExpr) {
      if(c == ' ') {
        continue;
      }
      c = tolower(c);
    }
    if (!isStringExpr && (c == ' ' || c == '>' || c == '=' || c == '<' || c == '!' || c == '&' || c == '|')) {
      expr[j] = '\0';
      exprsTokens[numExprs++] = copyStr(expr);
      j = 0;
      if (c != ' ') {
        expr[j++] = c; // taking ops as expressions
        if (c == '>' || c == '<' || c == '!') {
          c = token[i + 1];
          if (c == '=') {
            expr[j++] = c;
            i++;
          }
        }
        expr[j] = '\0';
        j = 0;
        exprsTokens[numExprs++] = copyStr(expr);
      }
    }
    else {
      if (c == '\"') {
        isStringExpr = abs(isStringExpr - 1); // toogle
      }
      expr[j++] = c;
    }
  }
  Expr *l1, *r1, *term1, *final;
  if (numExprs == 0) {
    l1 = makeExpr("true", schema);
    r1 = makeExpr("true", schema);
    final = makeOpExpr(l1, r1, "=");
    return final;
  }
  expr[j] = '\0';
  exprsTokens[numExprs++] = copyStr(expr);
  free(token);
  free(expr);
  if (numExprs != 3 && numExprs != 7) {
    for (i = 0; i < numExprs; i++) {
      free(exprsTokens[i]);
    }
    return NULL;
  }

  if ((l1 = makeExpr(exprsTokens[0], schema)) == NULL) {
    for (i = 0; i < numExprs; i++) {
      free(exprsTokens[i]);
    }
    return NULL;
  }
  if ((r1 = makeExpr(exprsTokens[2], schema)) == NULL) {
    for (i = 0; i < numExprs; i++) {
      free(exprsTokens[i]);
    }
    return NULL;
  }

  if ((term1 = makeOpExpr(l1, r1, exprsTokens[1])) == NULL) {
    for (i = 0; i < numExprs; i++) {
      free(exprsTokens[i]);
    }
    return NULL;
  }

  final = term1;

  if (numExprs == 7) {
    Expr *l2, *r2, *term2 = NULL;
    if ((l2 = makeExpr(exprsTokens[4], schema)) == NULL) {
      for (i = 0; i < numExprs; i++) {
        free(exprsTokens[i]);
      }
      return NULL;
    }
    if ((r2 = makeExpr(exprsTokens[6], schema)) == NULL) {
      for (i = 0; i < numExprs; i++) {
        free(exprsTokens[i]);
      }
      return NULL;
    }

    if ((term2 = makeOpExpr(l2, r2, exprsTokens[5])) == NULL) {
      for (i = 0; i < numExprs; i++) {
        free(exprsTokens[i]);
      }
      return NULL;
    }
    if ((final = makeLOpExpr(term1, term2, exprsTokens[3])) == NULL) {
      for (i = 0; i < numExprs; i++) {
        free(exprsTokens[i]);
      }
      return NULL;
    }
  }


  for (i = 0; i < numExprs; i++) {
    free(exprsTokens[i]);
  }
  return final;
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
    showError("NAMES MUST NOT EXCEED THE MAX! NAMES MUST START WITH LETTER! NAMES MUST NOT CONTAIN `,` OR SPACES");
    printf("Please try again : ");
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
    int len = PAGE_SIZE;
    if(datatype[i] == DT_STRING){
      len = typeLen[i];
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
        char *ptr;
        while (ptr) {
          ptr[0] = tolower(ptr[0]);
        }
        val->v.boolV = (strcmp(token,"0") == 0 || strcmp(token, "false") == 0) ? false : true;
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
        showError("NAMES MUST NOT EXCEED THE MAX! NAMES MUST START WITH LETTER! NAMES MUST NOT CONTAIN `,` OR SPACES");
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

    RC err = openTable(rel, tableName);
    if (err != RC_OK) {
      free(sc);
      free(rel);
      return showError("UNKNOWN TABLE!");
    }
    Expr *expr = parseExpressions(rel->schema);
    if (expr == NULL) {
      free(sc);
      free(rel);
      return showError("INVALID EXPRESSSION");
    }
    Record *record;
    createRecord(&record, rel->schema);
    startScan(rel, sc, expr);
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
