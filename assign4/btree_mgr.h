#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"
#include "data_structures.h"

// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;


typedef struct BT_Node {
  int size; // values size
  int isLeaf;
  smartArray *vals;
  smartArray *childrenPages;
  smartArray *leafRIDPages;
  smartArray *leafRIDSlots;
} BT_Node;

// Node functions
BT_Node *createBTNode(int size, int isLeaf);

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);

#endif // BTREE_MGR_H
