#include "btree_mgr.h"
#include "string.h"
#include "data_structures.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include <stdarg.h>

// helpers

void freeit(int num, ...) {
  va_list valist;
  va_start(valist, num);
  for (int i = 0; i < num; i++) {
    free(va_arg(valist, void *));
  }
  va_end(valist);
}

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
    node->children = newArray(BT_Node *, size + 1);
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


RC readNode(BT_Node **node, BTreeHandle *tree, int pageNum) {
  RC err;
  BM_PageHandle *page = new(BM_PageHandle);
  if (RC_OK!=(err = pinPage(tree->mgmtData, page, pageNum))) {
    free(page);
    return err;
  }

  int isLeaf;
  char *ptr = page->data;
  memcpy(&isLeaf, ptr, SIZE_INT);
  ptr += SIZE_INT;
  int filled;
  memcpy(&filled, ptr, SIZE_INT);
  ptr = page->data + 10 * SIZE_INT; // skip 40 bytes for header of the node
  BT_Node *_node = createBTNode(tree->size, isLeaf); // TODO destroyBTNode
  int value, i;
  if (!isLeaf) {
    int childPage;
    for (i = 0; i < filled; i++) {
      memcpy(&childPage, ptr, SIZE_INT);
      ptr += SIZE_INT;
      memcpy(&value, ptr, SIZE_INT);
      ptr += SIZE_INT;
      saInsertAt(_node->vals, value, i);
      saInsertAt(_node->childrenPages, childPage, i);
    }
    memcpy(&childPage, ptr, SIZE_INT);
    saInsertAt(_node->childrenPages, childPage, i);
  }
  else {
    int ridPage, ridSlot;
    for (i = 0; i < filled; i++) {
      memcpy(&ridPage, ptr, SIZE_INT);
      ptr += SIZE_INT;
      memcpy(&ridSlot, ptr, SIZE_INT);
      ptr += SIZE_INT;
      memcpy(&value, ptr, SIZE_INT);
      ptr += SIZE_INT;
      saInsertAt(_node->vals, value, i);
      saInsertAt(_node->leafRIDPages, ridPage, i);
      saInsertAt(_node->leafRIDSlots, ridSlot, i);
    }
  }

  err = unpinPage(tree->mgmtData, page);
  free(page);
  *node = _node;
  return err;
}


RC writeNode(BT_Node *node, BTreeHandle *tree, int pageNum) {
  RC err;
  BM_PageHandle *page = new(BM_PageHandle);
  if (RC_OK!=(err = pinPage(tree->mgmtData, page, pageNum))) {
    free(page);
    return err;
  }

  char *ptr = page->data;
  memcpy(ptr, &node->isLeaf, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &node->vals->fill, SIZE_INT);
  ptr = page->data + 10 * SIZE_INT; // skip 40 bytes for header of the node

  int i;
  if (!node->isLeaf) {
    for (i = 0; i < node->vals->fill; i++) {
      memcpy(ptr, &node->childrenPages->elems[i], SIZE_INT);
      ptr += SIZE_INT;
      memcpy(ptr, &node->vals->elems[i], SIZE_INT);
      ptr += SIZE_INT;
    }
    memcpy(ptr, &node->childrenPages->elems[i], SIZE_INT);
  }
  else {
    for (i = 0; i < node->vals->fill; i++) {
      memcpy(ptr, &node->leafRIDPages->elems[i], SIZE_INT);
      ptr += SIZE_INT;
      memcpy(ptr, &node->leafRIDSlots->elems[i], SIZE_INT);
      ptr += SIZE_INT;
      memcpy(ptr, &node->vals->elems[i], SIZE_INT);
      ptr += SIZE_INT;
    }
  }
  err = markDirty(tree->mgmtData, page);
  err = unpinPage(tree->mgmtData, page);
  forceFlushPool(tree->mgmtData);
  free(page);
  return err;
}


void printNode(BT_Node *nodeNew) {
  printf("\nNode details==>\n");
  printf("Is Leaf : %d\t Size: %d\t Filled: %d \nNodeData:\t [ ", nodeNew->isLeaf, nodeNew->size, nodeNew->vals->fill);
  int i;
  if(nodeNew->isLeaf){
    for (i = 0; i < nodeNew->vals->fill; i++) {
      printf("%d", nodeNew->leafRIDPages->elems[i]);
      printf(".%d , ", nodeNew->leafRIDSlots->elems[i]);
      printf("<%d> ", nodeNew->vals->elems[i]);
      if(i != nodeNew->vals->fill-1){
        printf(" ,");
      }
    }
    printf("]");

  } else {
    for (i = 0; i < nodeNew->vals->fill; i++) {
      printf("%d , ", nodeNew->childrenPages->elems[i]);
      printf("<%d> , ", nodeNew->vals->elems[i]);
    }
    printf("%d ]", nodeNew->childrenPages->elems[i]);
  }
  printf("\n------------\n");
}

RC gwn(BTreeHandle *tree, int pageNum, int size, int fill, int *vals, int *ptrs) {
  BT_Node *node = createBTNode(size, 0);
  int index, i;
  for (i = 0; i < fill; i++) {
    index = saInsert(node->vals, vals[i]);
    saInsertAt(node->childrenPages, ptrs[i], index);
  }
  saInsertAt(node->childrenPages, ptrs[i], node->childrenPages->fill);
  RC err = writeNode(node, tree, pageNum);
  destroyBTNode(node);
  return err;
}


RC gwl(BTreeHandle *tree, int pageNum, int size, int fill, int *vals, int *ridP, int *ridS) {
  BT_Node *node = createBTNode(size, 1);
  int index, i;
  for (i = 0; i < fill; i++) {
    index = saInsert(node->vals, vals[i]);
    saInsertAt(node->leafRIDPages, ridP[i], index);
    saInsertAt(node->leafRIDSlots, ridS[i], index);
  }
  RC err = writeNode(node, tree, pageNum);
  destroyBTNode(node);
  return err;
}


RC loadBtreeNodes(BTreeHandle *tree, BT_Node *root, BT_Node **leftOnLevel, int level) {
  BT_Node *child;
  BT_Node *left = leftOnLevel[level];
  RC err;
  if(!root->isLeaf) {
    for (int i = 0; i < root->childrenPages->fill; i++) {
      child = root->children[i];
      if ((err = readNode(&child, tree, root->childrenPages->elems[i]))) {
        return err;
      }
      if (left != NULL) {
        left->right = child;
      }
      child->left = left;
      left = child;
      child->parent = root;
      leftOnLevel[level] = left;
      if ((err = loadBtreeNodes(tree, child, leftOnLevel, i + 1))) {
        return err;
      }
    }
  }
  return RC_OK;
}


//functionality

RC initIndexManager (void *mgmtData) {
  return RC_OK;
}


RC shutdownIndexManager () {
  return RC_OK;
}


RC createBtree (char *idxId, DataType keyType, int n)
{
  RC rc;
  if(RC_OK!= (rc = createPageFile (idxId))){
    return rc;
  }

  SM_FileHandle *fHandle = new(SM_FileHandle);// TODO free it, Done below
  if (RC_OK != (rc = openPageFile(idxId, fHandle))) {
    free(fHandle);
    return rc;
  }

  char *header = newCleanArray(char, PAGE_SIZE);
  char *ptr = header;
  memcpy(ptr, &n, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &keyType, SIZE_INT);
  ptr += SIZE_INT;
  int whereIsRoot = 0;
  memcpy(ptr, &whereIsRoot, SIZE_INT);
  ptr += SIZE_INT;
  int numNodes = 0;
  memcpy(ptr, &numNodes, SIZE_INT);
  ptr += SIZE_INT;
  int numEntries = 0;
  memcpy(ptr, &numEntries, SIZE_INT);
  ptr += SIZE_INT;
  int depth = 0;
  memcpy(ptr, &depth, SIZE_INT);

  if (RC_OK != (rc = writeBlock(0, fHandle, header))) {
    free(fHandle);
    free(header);
    return rc;
  }
  free(header);
  rc = closePageFile(fHandle);
  free(fHandle);
  return rc;
}

RC openBtree (BTreeHandle **tree, char *idxId){
  BTreeHandle *_tree = new(BTreeHandle);
  RC err;
  BM_BufferPool *bm = new(BM_BufferPool); //TODO: free it in close
  if ((err = initBufferPool(bm, idxId, PER_IDX_BUF_SIZE, RS_LRU, NULL))) {
    freeit(2, bm, _tree);
    return err;
  }

  BM_PageHandle *pageHandle = new(BM_PageHandle); //TODO: free it, Done below
  if (RC_OK != (err = pinPage(bm, pageHandle, 0))) {
    freeit(3, bm, pageHandle, _tree);
    return err;
  }
  char *ptr = pageHandle->data;
  _tree->idxId = idxId;
  _tree->mgmtData = bm;

  memcpy(&_tree->size, ptr, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(&_tree->keyType, ptr, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(&_tree->whereIsRoot, ptr, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(&_tree->numNodes, ptr, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(&_tree->numEntries, ptr, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(&_tree->depth, ptr, SIZE_INT);

  if ((err = unpinPage(bm, pageHandle)) != RC_OK) {
    freeit(1, pageHandle);
    closeBtree(_tree);
    return err;
  }
  freeit(1, pageHandle);

  if (_tree->depth) {
    if ((err = readNode(&_tree->root, _tree, _tree->whereIsRoot))) {
      closeBtree(_tree);
      return err;
    }
    BT_Node **leftOnLevel = newArray(BT_Node *, _tree->depth);
    for (int i = 0; i < _tree->depth; i++) {
      leftOnLevel[i] = NULL;
    }
    if ((err = loadBtreeNodes(_tree, _tree->root, leftOnLevel, 0))) {
      closeBtree(_tree);
      return err;
    }
  }
  *tree = _tree;
  return RC_OK;
}


RC closeBtree (BTreeHandle *tree){
  shutdownBufferPool(tree->mgmtData);
  // TODO free all nodes
  free(tree);
  return RC_OK;
}


int main () {
  ///*
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
  //*/
  int n = 3;
  char *name = "myIndex";
  createBtree (name, DT_INT, n);
  BTreeHandle *tree = new(BTreeHandle);
  tree->size = n;
  openBtree(&tree, name);

  //----------------Non Leaf Node-------------------------------
  ///*
  BT_Node *node = createBTNode(4, 0);
  int index;
  index = saInsert(node->vals, 1214237423);
  saInsertAt(node->childrenPages, 2, index);
  index = saInsert(node->vals, 1514234234);
  saInsertAt(node->childrenPages, 3, index);
  index = saInsert(node->vals, 1314234234);
  saInsertAt(node->childrenPages, 4, index);
  saInsertAt(node->childrenPages, 5, node->childrenPages->fill);
  writeNode(node, tree, 1);
  destroyBTNode(node);
  //*/

  ///*
  BT_Node *nodeNew = NULL;
  readNode(&nodeNew, tree, 1);
  printNode(nodeNew);
  destroyBTNode(nodeNew);
  //*/
  //-----------------------------------------------

  //--------------Leaf Node---------------------------------
  ///*
  node = createBTNode(4, 1);
  index = saInsert(node->vals, 1244324234);
  saInsertAt(node->leafRIDPages, 2, index);
  saInsertAt(node->leafRIDSlots, 2, index);
  index = saInsert(node->vals, 1544234234);
  saInsertAt(node->leafRIDPages, 3, index);
  saInsertAt(node->leafRIDSlots, 3, index);
  index = saInsert(node->vals, 1344234344);
  saInsertAt(node->leafRIDPages, 4, index);
  saInsertAt(node->leafRIDSlots, 4, index);
  writeNode(node, tree, 2);
  destroyBTNode(node);
  //*/

  ///*
  readNode(&nodeNew, tree, 2);
  printNode(nodeNew);
  destroyBTNode(nodeNew);
  //*/
  //-----------------------------------------------

  // example node
  int dummy = 2912;
  n = 5;
  int pn = 1;
  int fill = 4;
  int vals[5] = {5, 7, 8, 10, dummy};
  int ptrs[6] = {2, 6, 10, 4, 11, dummy};

  gwn(tree, pn, n, fill, &vals[0], &ptrs[0]);

  // exmaple leaf
  fill = 3;
  pn = 2;
  int lvals[5] = {1, 2, 3, dummy, dummy};
  int ridP[5] = {5, 2, 6, dummy, dummy};
  int ridS[5] = {5, 3, 5, dummy, dummy};
  gwl(tree, pn, n, fill, &vals[0], &ridP[0], &ridS[0]);

  // TODO it is not tested
  // TODO must write all nodes and leafes for the tree I mentioned on whats app, like the examples above, u will be cnahging the pn for every one
  // TODO must know exactly(when u draw it on papers) where is the root and write its pageNum when u create the tree, change it manually for now

  closeBtree(tree);
  return 0;
}
