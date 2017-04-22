#include "btree_mgr.h"
#include "string.h"
#include "data_structures.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include <stdarg.h>
#include <unistd.h>

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
  node->right = NULL;
  node->left = NULL;
  node->parent = NULL;
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


RC deleteBtree (char *idxId) {
  if(access(idxId, F_OK) == -1) {
    return RC_FILE_NOT_FOUND;
  }
  int ret = unlink(idxId);
  if (ret == -1) {
    return RC_FS_ERROR;
  }
  return RC_OK;
}


void printKeyType(DataType dataType){
  switch(dataType)
  {
    case DT_INT:
      printf("DT_INT\n");
      break;
    case DT_FLOAT:
      printf("DT_FLOAT\n");
      break;
    case DT_STRING:
      printf("DT_STRING\n");
      break;
    case DT_BOOL:
      printf("DT_BOOL\n");
      break;
  }
}


RC getNumNodes (BTreeHandle *tree, int *result) {
  *result = tree->numNodes;
  return RC_OK;
}


RC getNumEntries (BTreeHandle *tree, int *result) {
  *result = tree->numEntries;
  return RC_OK;
}


RC getKeyType (BTreeHandle *tree, DataType *result) {
  *result = tree->keyType;
  return RC_OK;
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


void printNode(BT_Node *node) {
  if (node == NULL) {
    printf("NULL Node !!\n");
    return;
  }
  printf("\nNode details==>\n");
  printf("Is Leaf : %d\t Size: %d\t Filled: %d \nNodeData:\t [ ", node->isLeaf, node->size, node->vals->fill);
  int i;
  if(node->isLeaf){
    for (i = 0; i < node->vals->fill; i++) {
      printf("%d", node->leafRIDPages->elems[i]);
      printf(".%d , ", node->leafRIDSlots->elems[i]);
      printf("<%d> ", node->vals->elems[i]);
      if(i != node->vals->fill-1){
        printf(" ,");
      }
    }
    printf("]");

  } else {
    for (i = 0; i < node->vals->fill; i++) {
      printf("%d , ", node->childrenPages->elems[i]);
      printf("<%d> , ", node->vals->elems[i]);
    }
    printf("%d ]", node->childrenPages->elems[i]);
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
  BT_Node *left = leftOnLevel[level];
  RC err;
//  printf("######################### LEVEL = %d\n", level);
//  printNode(root);
  if(!root->isLeaf) {
    for (int i = 0; i < root->childrenPages->fill; i++) {
      if ((err = readNode(&root->children[i], tree, root->childrenPages->elems[i]))) {
        return err;
      }
      if (left != NULL) {
        left->right = root->children[i];
      }
      root->children[i]->left = left;
      left = root->children[i];
      root->children[i]->parent = root;
      leftOnLevel[level] = left;
      if ((err = loadBtreeNodes(tree, root->children[i], leftOnLevel, level + 1))) {
        return err;
      }
    }
  }
  return RC_OK;
}


RC loadBtree(BTreeHandle *tree) {
  RC err;
  tree->root = NULL;
  if (tree->depth) {
    if ((err = readNode(&tree->root, tree, tree->whereIsRoot))) {
      return err;
    }
    BT_Node **leftOnLevel = newArray(BT_Node *, tree->depth);
    for (int i = 0; i < tree->depth; i++) {
      leftOnLevel[i] = NULL;
    }
    if ((err = loadBtreeNodes(tree, tree->root, leftOnLevel, 0))) {
      return err;
    }
  }
  printf("depth = %d\n", tree->depth);
  return RC_OK;
}


RC writeBtreeHeader(BTreeHandle *tree) {
  RC err;
  BM_BufferPool *bm = tree->mgmtData;
  BM_PageHandle *page = new(BM_PageHandle); // TODO free it
  if (RC_OK != (err = pinPage(bm, page, 0))) {
    freeit(1, page);
    return err;
  }
  char *ptr = page->data;
  memcpy(ptr, &tree->size, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &tree->keyType, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &tree->whereIsRoot, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &tree->numNodes, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &tree->numEntries, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &tree->depth, SIZE_INT);
  err = markDirty(bm, page);
  err = unpinPage(bm, page);
  forceFlushPool(bm);
  freeit(1, page);
  return err;
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
  ///*  Coment me
  if ((err = loadBtree(_tree))) {
    closeBtree(_tree);
    return err;
  }
  //*/
  *tree = _tree;
  return RC_OK;
}


RC closeBtree (BTreeHandle *tree){
  shutdownBufferPool(tree->mgmtData);
  // TODO free all nodes
  free(tree);
  return RC_OK;
}

void saveTestLeaf2(BTreeHandle *tree,int pn, int val, int i, int fill){
  int dummy = 2912;
  int lvals[2] = {val, dummy};
  int ridP[2] = {i, dummy};
  int ridS[2] = {i, dummy};
  gwl(tree, pn, 2, fill, &lvals[0], &ridP[0], &ridS[0]);
  i++;
}

void saveTestNode2(BTreeHandle *tree,int pn, int val, int i, int j, int fill){
  int dummy = 2912;
  int lvals[2] = {val,dummy};
  int ptr[3] = {i, j, dummy};
  gwn(tree, pn, 2, fill, &lvals[0], &ptr[0]);
  i++;
}

RC createTestBTree2(BTreeHandle *tree){
  int fill = 2;
  int pn = 1;
  int lvals1[2] = {1, 20};
  int ridP1[2] = {0,1};
  int ridS1[2] = {0,1};
  gwl(tree, pn, 2, fill, &lvals1[0], &ridP1[0], &ridS1[0]);

  int i=2;
  fill = 1;
  pn = 27;
  int val = 25;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 26;
  val = 28;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 24;
  val = 29;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 23;
  val = 30;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 20;
  val = 33;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 19;
  val = 35;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 17;
  val = 39;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 16;
  val = 40;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 12;
  val = 43;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 11;
  val = 45;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 9;
  val = 49;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 8;
  val = 50;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 5;
  val = 52;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 4;
  val = 60;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  pn = 2;
  val = 70;
  saveTestLeaf2(tree, pn,val, i, fill);
  i++;

  //-----------------------------------------------
  pn = 3;
  val = 25;
  int prevPage=1;
  int nextPage=27;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 28;
  val = 29;
  prevPage=26;
  nextPage=24;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 25;
  val = 33;
  prevPage=23;
  nextPage=20;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 21;
  val = 39;
  prevPage=19;
  nextPage=17;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 18;
  val = 43;
  prevPage=16;
  nextPage=12;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 13;
  val = 49;
  prevPage=11;
  nextPage=9;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 10;
  val = 52;
  prevPage=8;
  nextPage=5;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 6;
  val = 70;
  prevPage=4;
  nextPage=2;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 7;
  val = 28;
  prevPage=3;
  nextPage=28;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 29;
  val = 35;
  prevPage=25;
  nextPage=21;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 22;
  val = 45;
  prevPage=18;
  nextPage=13;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 14;
  val = 60;
  prevPage=10;
  nextPage=6;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 15;
  val = 30;
  prevPage=7;
  nextPage=29;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 30;
  val = 50;
  prevPage=22;
  nextPage=14;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  pn = 31;
  val = 40;
  prevPage=15;
  nextPage=30;
  saveTestNode2(tree, pn, val, prevPage, nextPage, fill);

  return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
  BT_ScanHandle *_handle = new(BT_ScanHandle);
  ScanMgmtInfo *scanInfo = new(ScanMgmtInfo);
  _handle->tree = tree;
  scanInfo->currentNode = tree->root;
  while(!scanInfo->currentNode->isLeaf) { // finding the left most leaf
    scanInfo->currentNode = scanInfo->currentNode->children[0];
  }
  scanInfo->elementIndex = 0;
  _handle->mgmtData =scanInfo;
  *handle = _handle;
  return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle){
  free(handle);
  return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result){
  ScanMgmtInfo *scanInfo = handle->mgmtData;
  if(scanInfo->elementIndex >= scanInfo->currentNode->leafRIDPages->fill) { // finding the left most leaf
    //searchNextNode
    if(scanInfo->currentNode->right==NULL){
      return RC_IM_NO_MORE_ENTRIES;
    } else {
      scanInfo->currentNode = scanInfo->currentNode->right;
      scanInfo->elementIndex = 0;
    }
  }
  result->page =scanInfo->currentNode->leafRIDPages->elems[scanInfo->elementIndex];
  result->slot =scanInfo->currentNode->leafRIDPages->elems[scanInfo->elementIndex];
  scanInfo->elementIndex++;
  return RC_OK;
}

int main () {
  /*
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
  int n = 2;
  char *name = "myIndex";
  createBtree (name, DT_INT, n);
  BTreeHandle *tree;
  openBtree(&tree, name);

  //----------------Non Leaf Node-------------------------------
  /*
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

  /*
  BT_Node *nodeNew = NULL;
  readNode(&nodeNew, tree, 1);
  printNode(nodeNew);
  destroyBTNode(nodeNew);
  //*/
  //-----------------------------------------------

  //--------------Leaf Node---------------------------------
  /*
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

  /*
  readNode(&nodeNew, tree, 2);
  printNode(nodeNew);
  destroyBTNode(nodeNew);
  //*/


  //-----------------------------------------------

  createTestBTree2(tree);
  tree->depth = 5;
  tree->numNodes = 31;
  tree->whereIsRoot = 31;
  writeBtreeHeader(tree);
  //-----------------------------------------------

  /*
  int result;
  getNumNodes(tree, &result);
  printf("\nNum Nodes : %d",result);

  getNumEntries(tree, &result);
  printf("\nNum Entries : %d",result);

  DataType dataType;
  getKeyType(tree, &dataType);
  printf("\nKey Type: ");
  printKeyType(dataType);
  //*/
  //-----------------------------------------------
  closeBtree(tree);
  BTreeHandle *fullTree;
  openBtree(&fullTree, name);
  printNode(fullTree->root->children[0]->children[1]->right);

  BT_ScanHandle *handle;
  openTreeScan(fullTree, &handle);
  RID *result = new(RID);
  while (nextEntry(handle, result) != RC_IM_NO_MORE_ENTRIES) {
    printf("\nRID: %d.%d\n", result->page,result->slot);
  }
  closeTreeScan(handle);
  closeBtree(fullTree);
  free(result);
  //deleteBtree(name);

  return 0;
}
