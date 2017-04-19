#include "btree_mgr.h"
#include "string.h"
#include "data_structures.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"

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

RC initIndexManager (void *mgmtData) {
  return RC_OK;
}


RC shutdownIndexManager () {
  return RC_OK;
}


RC createBtree (char *idxId, DataType keyType, int n)
{
  RC rc;
  if(RC_OK!= (rc=createPageFile (idxId))){
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

  if (RC_OK != (rc= writeBlock(0, fHandle, header))) {
    free(fHandle);
    free(header);
    return rc;
  }
  free(header);
  rc= closePageFile(fHandle);
  free(fHandle);
  return rc;
}

RC openBtree (BTreeHandle **tree, char *idxId){
  BTreeHandle *_tree = *tree;
  RC err;
  BM_BufferPool *bm = new(BM_BufferPool); //TODO: free it in close
  BM_PageHandle *pageHandle = new(BM_PageHandle);//TODO: free it, Done below
  if ((err = initBufferPool(bm, idxId, PER_IDX_BUF_SIZE, RS_LRU, NULL))) {
    free(bm);
    return err;
  }
  if (RC_OK!=(err = pinPage(bm, pageHandle, 0))) {
    free(bm);
    return err;
  }
  char *ptr = pageHandle->data;
  _tree->idxId = idxId;
  _tree->mgmtData = bm;

  int size;
  int whereIsRoot;
  DataType keyType;

  memcpy(&size, ptr, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(&keyType, ptr, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(&whereIsRoot, ptr, SIZE_INT);
  ptr += SIZE_INT;

  _tree->keyType = keyType;
  _tree->size = size;
  _tree->whereIsRoot = whereIsRoot;
  *tree = _tree;
  int rc= unpinPage(bm, pageHandle);
  free(pageHandle);
  return rc;
}

RC closeBtree (BTreeHandle *tree){
  shutdownBufferPool(tree->mgmtData);
  free(tree);
  return RC_OK;
}

RC readNode(BT_Node **node, BTreeHandle *tree, int size, int pageNum) {
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
  ptr += 9 * SIZE_INT; // skip 40 bytes for header of the node
  BT_Node *_node = createBTNode(size, isLeaf); // TODO destroyBTNode
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

RC writeNode(BT_Node *node, BTreeHandle *tree, int size, int pageNum) {
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
  ptr += 9 * SIZE_INT; // skip 40 bytes for header of the node

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

void printNode(BT_Node *nodeNew){

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
  writeNode(node, tree, n, 1);
  destroyBTNode(node);
  //*/

  ///*
  BT_Node *nodeNew = NULL;
  readNode(&nodeNew, tree, n, 1);
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
  writeNode(node, tree, n, 2);
  destroyBTNode(node);
  //*/

  ///*
  readNode(&nodeNew, tree, n, 2);
  printNode(nodeNew);
  destroyBTNode(nodeNew);
  //*/
  //-----------------------------------------------
  closeBtree(tree);
  return 0;
}
