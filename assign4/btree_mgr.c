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

  int n = 3;
  char *name = "myIndex";
  createBtree (name, DT_INT, n);
  BTreeHandle *tree = new(BTreeHandle);
  openBtree(&tree, name);
  //BT_Node *node = createBTNode(4, 0);
  //writeNode(node, BM_BufferPool *bm, int size, int pageNum);
  return 0;
}
