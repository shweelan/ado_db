#include "btree_mgr.h"

// helpers

BT_Node *createBTNode(int size, int isLeaf) {
  BT_Node *node = new(BT_Node); // TODO free it, Done in destroyBTNode
  node->isLeaf = isLeaf;
  node->size = size;
  node->vals = saInit(size);
  if (isLeaf) {
    node->leafRIDPages = saInit(size);
    node->leafRIDSlots = saInit(size);
  }
  else {
    node->childrenPages = saInit(size + 1);
  }
  return node;
}

void destroyBTNode(BT_Node *node) {
  saDestroy(node->vals);
  if (node->isLeaf) {
    saDestroy(node->leafRIDPages);
    saDestroy(node->leafRIDSlots);
  }
  else {
    saDestroy(node->childrenPages);
  }
  free(node);
}

//functionality

int main () {
  smartArray *arr = saInit(20);
  saInsert(arr, 17);
  saDeleteOne(arr, 17);
  saInsert(arr, 17);
  saInsert(arr, 4);
  saInsert(arr, 5);
  saInsert(arr, 5);
  saInsert(arr, 15);
  saDeleteOne(arr, 5);
  saDeleteOne(arr, 17);
  saDeleteOne(arr, 4);
  saDeleteOne(arr, 5);
  saDeleteOne(arr, 15);
  saDeleteOne(arr, 15);
  saInsert(arr, 17);
  saInsert(arr, 4);
  saInsert(arr, 5);
  saInsert(arr, 5);
  saInsert(arr, 15);
  saInsert(arr, 7);
  saInsert(arr, 13);
  saInsert(arr, 10);
  saInsert(arr, 4);
  saInsert(arr, 10);
  saInsert(arr, 10);
  saInsert(arr, 10);
  saInsert(arr, 8);
  saInsert(arr, 10);
  saDeleteAll(arr, 5);
  saInsert(arr, 5);
  int fitOn;
  for (int i = 0; i < 20; i++) {
    int index = saBinarySearch(arr, i, &fitOn);
    printf("searching for %d, found?=%s, %son index=%d\n", i, (index < 0) ? "false" : "true", (index < 0) ? "it can fit " : "", fitOn);
  }
  saDestroy(arr);
  return 0;
}
