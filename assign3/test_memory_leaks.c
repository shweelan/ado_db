#include "record_mgr.h"
#include <string.h>
#include <stdlib.h>

char *test_copyStr(char *_string) {
  char *string = newStr(strlen(_string)); // TODO free it[count, multiple maybe]
  strcpy(string, _string);
  return string;
}

int main(int argc, char *argv[]) {
  int a = 4;
  initRecordManager(NULL);
  char * tableName = test_copyStr("shweelan");
  char **b = newArray(char *, a);
  b[0] = "SH";
  b[1] = "WE";
  b[2] = "EL";
  b[3] = "AN";
  DataType *c = newArray(DataType, a);
  c[0] = DT_STRING;
  c[1] = DT_INT;
  c[2] = DT_BOOL;
  c[3] = DT_FLOAT;
  int *d = newIntArr(a);
  d[0] = 1000;
  d[1] = 0;
  d[2] = 0;
  d[3] = 0;
  int e = 2;
  int *f = newIntArr(e);
  f[0] = 1;
  f[1] = 3;
  Schema * s = createSchema(a, b, c, d, e, f);
  createTable(tableName, s);
  RM_TableData *rel = new(RM_TableData);
  openTable(rel, tableName);
  Record *record;
  createRecord(&record, s);
  Value *val = new(Value);
  val->dt = DT_STRING;
  val->v.stringV = test_copyStr("56fs.58274283748237483278947238947932874fkdjshfjhsdjh sejkhfksnkchsfclalcbaewfgaewkkcaew");
  setAttr(record, s, 0, val);
  free(val->v.stringV);
  val->dt = DT_INT;
  val->v.intV = 2;
  setAttr(record, s, 1, val);
  val->dt = DT_BOOL;
  val->v.boolV = true;
  setAttr(record, s, 2, val);
  val->dt = DT_FLOAT;
  val->v.floatV = 56.56;
  setAttr(record, s, 3, val);
  printRecord(s, record);
  insertRecord(rel, record);
  val->dt = DT_STRING;
  val->v.stringV = test_copyStr("DDDDDDDclalcbaewfgaewkkcaew");
  setAttr(record, s, 0, val);
  free(val->v.stringV);
  updateRecord(rel, record);
  getRecord(rel, record->id, record);
  printRecord(s, record);
  getNumTuples(rel);
  Expr *se1, *left, *right;
  MAKE_CONS(right, stringToValue("s3"));
  MAKE_ATTRREF(left, 0);
  MAKE_BINOP_EXPR(se1, left, right, OP_COMP_EQUAL);
  RM_ScanHandle *sc = new(RM_ScanHandle);
  startScan(rel, sc, se1);
  next(sc, record);
  closeScan(sc);
  
  free(sc);
  deleteRecord(rel, record->id);
  free(val);
  freeRecord(record);
  closeTable(rel);
  free(rel);
  deleteTable(tableName);
  freeSchema(s);
  free(b);
  free(c);
  free(d);
  free(f);
  free(tableName);
  shutdownRecordManager();
  free(right->expr.op);
  free(right);
  free(left);
  free(se1->expr.op->args);
  free(se1->expr.op);
  free(se1);
  return 0;
}
