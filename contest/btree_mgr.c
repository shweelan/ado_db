#include "btree_mgr.h"
#include "string.h"
#include "data_structures.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include <stdarg.h>
#include <unistd.h>
#include <math.h>

// helpers

void freeit(int num, ...) {
  va_list valist;
  va_start(valist, num);
  for (int i = 0; i < num; i++) {
    free(va_arg(valist, void *));
  }
  va_end(valist);
}

BT_Node *createBTNode(int size, int isLeaf, int pageNum) {
  BT_Node *node = new(BT_Node); // TODO free it, Done in destroyBTNode
  node->isLeaf = isLeaf;
  node->size = size;
  node->pageNum = pageNum;
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
    free(node->children);
  }
  free(node);
}

RC printNodeHelper(BT_Node *node, char *result) {
  if (node == NULL) {
    sprintf(result+ strlen(result),"NULL Node !!\n");
    return RC_GENERAL_ERROR;
  }
  sprintf(result + strlen(result), "(%d)[", node->pageNum);

  int i;
  if(node->isLeaf){
    for (i = 0; i < node->vals->fill; i++) {
      sprintf(result + strlen(result),"%d", node->leafRIDPages->elems[i]);
      sprintf(result + strlen(result),".%d,", node->leafRIDSlots->elems[i]);
      sprintf(result + strlen(result),"%d", node->vals->elems[i]);
      if(i < node->vals->fill-1){
        sprintf(result + strlen(result),",");
      }
    }
  } else {
    for (i = 0; i < node->vals->fill; i++) {
      sprintf(result + strlen(result),"%d,", node->childrenPages->elems[i]);
      sprintf(result + strlen(result),"%d,", node->vals->elems[i]);
    }
    sprintf(result + strlen(result),"%d", node->childrenPages->elems[i]);
  }
  sprintf(result+strlen(result), "]\n");
  //printf("%s\n", result);
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
  ptr = page->data + BYTES_BT_HEADER_LEN; // skip header bytes of the node
  BT_Node *_node = createBTNode(tree->size, isLeaf, pageNum); // TODO destroyBTNode
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

RC writeNode(BT_Node *node, BTreeHandle *tree) {
  RC err;
  BM_PageHandle *page = new(BM_PageHandle);
  if (RC_OK!=(err = pinPage(tree->mgmtData, page, node->pageNum))) {
    free(page);
    return err;
  }

  char *ptr = page->data;
  memcpy(ptr, &node->isLeaf, SIZE_INT);
  ptr += SIZE_INT;
  memcpy(ptr, &node->vals->fill, SIZE_INT);
  ptr = page->data + BYTES_BT_HEADER_LEN; // skip header bytes of the node

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
  printf("Is Leaf : %d\t Size: %d\t Filled: %d\t pageNum: %d\nNodeData:\t [ ", node->isLeaf, node->size, node->vals->fill, node->pageNum);
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

RC loadBtreeNodes(BTreeHandle *tree, BT_Node *root, BT_Node **leftOnLevel, int level) {
  BT_Node *left = leftOnLevel[level];
  RC err;
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

BT_Node *findNodeByKey(BTreeHandle *tree, int key) {
  BT_Node *current = tree->root;
  int index, fitOn;
  while(current != NULL && !current->isLeaf) {
    index = saBinarySearch(current->vals, key, &fitOn);
    if (index >= 0) {
      fitOn += 1;
    }
    current = current->children[fitOn];
  }
  return current;
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
    err = loadBtreeNodes(tree, tree->root, leftOnLevel, 0);
    free(leftOnLevel);
    return err;
  }
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
  ptr += SIZE_INT;
  memcpy(ptr, &tree->nextPage, SIZE_INT);
  err = markDirty(bm, page);
  err = unpinPage(bm, page);
  forceFlushPool(bm);
  freeit(1, page);
  return err;
}

RC insPropagateParent(BTreeHandle *tree, BT_Node *left, BT_Node *right, int key) {
  BT_Node *parent = left->parent;
  int index, i;
  if (parent == NULL) {
    parent = createBTNode(tree->size, 0, tree->nextPage);
    saInsertAt(parent->childrenPages, left->pageNum, 0);
    parent->children[0] = left;
    tree->nextPage++;
    tree->whereIsRoot = parent->pageNum;
    tree->numNodes++;
    tree->depth++;
    tree->root = parent;
    writeBtreeHeader(tree);
  }
  right->parent = parent;
  left->parent = parent;
  index = saInsert(parent->vals, key);
  if (index >= 0) {
    index += 1; // next position is the pointer
    saInsertAt(parent->childrenPages, right->pageNum, index);
    for (int i = parent->vals->fill; i > index; i--) {
      parent->children[i] = parent->children[i - 1];
    }
    parent->children[index] = right;
    return writeNode(parent, tree);
  }
  else {
    // parent is full
    // Overflowed = parent + 1 new item
    BT_Node * overflowed = createBTNode(tree->size + 1, 0, -1);
    overflowed->vals->fill = parent->vals->fill;
    overflowed->childrenPages->fill = parent->childrenPages->fill;
    memcpy(overflowed->vals->elems, parent->vals->elems, SIZE_INT * parent->vals->fill);
    memcpy(overflowed->childrenPages->elems, parent->childrenPages->elems, SIZE_INT * parent->childrenPages->fill);
    memcpy(overflowed->children, parent->children, sizeof(BT_Node *) * parent->childrenPages->fill);
    index = saInsert(overflowed->vals, key);
    saInsertAt(overflowed->childrenPages, right->pageNum, index + 1);
    for (i = parent->childrenPages->fill; i > index + 1; i--) {
      overflowed->children[i] = overflowed->children[i - 1];
    }
    overflowed->children[index + 1] = right;

    int leftFill = overflowed->vals->fill / 2;
    int rightFill = overflowed->vals->fill - leftFill;
    BT_Node *rightParent = createBTNode(tree->size, 0, tree->nextPage);
    tree->nextPage++;
    tree->numNodes++;
    // Since overflowed is sorted then it is safe to just copy the content
    // copy left
    parent->vals->fill = leftFill;
    parent->childrenPages->fill = leftFill + 1;
    int lptrsSize = parent->childrenPages->fill;
    memcpy(parent->vals->elems, overflowed->vals->elems, SIZE_INT * leftFill);
    memcpy(parent->childrenPages->elems, overflowed->childrenPages->elems, SIZE_INT * lptrsSize);
    memcpy(parent->children, overflowed->children, sizeof(BT_Node *) * lptrsSize);

    // copy right
    rightParent->vals->fill = rightFill;
    rightParent->childrenPages->fill = overflowed->childrenPages->fill - lptrsSize;
    int rptrsSize = rightParent->childrenPages->fill;
    memcpy(rightParent->vals->elems, overflowed->vals->elems + leftFill, SIZE_INT * rightFill);
    memcpy(rightParent->childrenPages->elems, overflowed->childrenPages->elems + lptrsSize, SIZE_INT * rptrsSize);
    memcpy(rightParent->children, overflowed->children + lptrsSize, sizeof(BT_Node *) * rptrsSize);

    destroyBTNode(overflowed);

    // always take median to parent
    key = rightParent->vals->elems[0];
    saDeleteAt(rightParent->vals, 0, 1);

    // introduce to each other
    rightParent->right = parent->right;
    if (rightParent->right != NULL) {
      rightParent->right->left = rightParent;
    }
    parent->right = rightParent;
    rightParent->left = parent;

    writeNode(parent, tree);
    writeNode(rightParent, tree);
    writeBtreeHeader(tree);
    return insPropagateParent(tree, parent, rightParent, key);
  }
}

void freeNodes(BT_Node *root) {
  if (root == NULL) {
    return;
  }
  BT_Node *leaf = root;
  while(!leaf->isLeaf) { // Finding the first leaf
    leaf = leaf->children[0];
  }
  BT_Node *parent = leaf->parent;
  BT_Node *next;
  while (true) {
    while(leaf != NULL) {
      next = leaf->right;
      destroyBTNode(leaf);
      leaf = next;
    }
    if (parent == NULL) {
      break;
    }
    leaf = parent;
    parent = leaf->parent;
  }
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
  if (n > (PAGE_SIZE - BYTES_BT_HEADER_LEN) / (3 * SIZE_INT)) { // 3 for the leaf case where u need 1 for key, 1 for page, and 1 for slot
    return RC_IM_N_TO_LAGE;
  }
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
  int nextPage = 1;
  ptr += SIZE_INT;
  memcpy(ptr, &nextPage, SIZE_INT);

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
  ptr += SIZE_INT;
  memcpy(&_tree->nextPage, ptr, SIZE_INT);

  if ((err = unpinPage(bm, pageHandle)) != RC_OK) {
    freeit(1, pageHandle);
    closeBtree(_tree);
    return err;
  }
  freeit(1, pageHandle);
  if ((err = loadBtree(_tree))) {
    closeBtree(_tree);
    return err;
  }
  *tree = _tree;
  return RC_OK;
}

RC closeBtree (BTreeHandle *tree){
  shutdownBufferPool(tree->mgmtData);
  freeNodes(tree->root);
  freeit(2, tree->mgmtData, tree);
  return RC_OK;
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

RC findKey (BTreeHandle *tree, Value *key, RID *result) {
  int index, fitOn;
  BT_Node *leaf = findNodeByKey(tree, key->v.intV);
  index = saBinarySearch(leaf->vals, key->v.intV, &fitOn);
  if (index < 0) {
    return RC_IM_KEY_NOT_FOUND;
  }
  result->page = leaf->leafRIDPages->elems[index];
  result->slot = leaf->leafRIDSlots->elems[index];
  return RC_OK;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid) {
  BT_Node *leaf = findNodeByKey(tree, key->v.intV);
  if (leaf == NULL) {
    leaf = createBTNode(tree->size, 1, tree->nextPage);
    tree->nextPage++;
    tree->whereIsRoot = leaf->pageNum;
    tree->numNodes++;
    tree->depth++;
    tree->root = leaf;
    writeBtreeHeader(tree);
  }
  int index, fitOn;
  index = saBinarySearch(leaf->vals, key->v.intV, &fitOn);
  if (index >= 0) {
    return RC_IM_KEY_ALREADY_EXISTS;
  }
  index = saInsert(leaf->vals, key->v.intV);
  if (index >= 0) {
    saInsertAt(leaf->leafRIDPages, rid.page, index);
    saInsertAt(leaf->leafRIDSlots, rid.slot, index);
  }
  else {
    // leaf is full
    // Overflowed = leaf + 1 new item
    BT_Node * overflowed = createBTNode(tree->size + 1, 1, -1);
    memcpy(overflowed->vals->elems, leaf->vals->elems, SIZE_INT * leaf->vals->fill);
    overflowed->vals->fill = leaf->vals->fill;
    memcpy(overflowed->leafRIDPages->elems, leaf->leafRIDPages->elems, SIZE_INT * leaf->leafRIDPages->fill);
    overflowed->leafRIDPages->fill = leaf->leafRIDPages->fill;
    memcpy(overflowed->leafRIDSlots->elems, leaf->leafRIDSlots->elems, SIZE_INT * leaf->leafRIDSlots->fill);
    overflowed->leafRIDSlots->fill = leaf->leafRIDSlots->fill;
    index = saInsert(overflowed->vals, key->v.intV);
    saInsertAt(overflowed->leafRIDPages, rid.page, index);
    saInsertAt(overflowed->leafRIDSlots, rid.slot, index);

    int leftFill = ceil((float) overflowed->vals->fill / 2);
    int rightFill = overflowed->vals->fill - leftFill;
    BT_Node *rightLeaf = createBTNode(tree->size, 1, tree->nextPage);
    tree->nextPage++;
    tree->numNodes++;
    // Since overflowed is sorted then it is safe to just copy the content
    // copy left
    leaf->vals->fill = leaf->leafRIDPages->fill = leaf->leafRIDSlots->fill = leftFill;
    memcpy(leaf->vals->elems, overflowed->vals->elems, SIZE_INT * leftFill);
    memcpy(leaf->leafRIDPages->elems, overflowed->leafRIDPages->elems, SIZE_INT * leftFill);
    memcpy(leaf->leafRIDSlots->elems, overflowed->leafRIDSlots->elems, SIZE_INT * leftFill);

    // copy right
    rightLeaf->vals->fill = rightLeaf->leafRIDPages->fill = rightLeaf->leafRIDSlots->fill = rightFill;
    memcpy(rightLeaf->vals->elems, overflowed->vals->elems + leftFill, SIZE_INT * rightFill);
    memcpy(rightLeaf->leafRIDPages->elems, overflowed->leafRIDPages->elems + leftFill, SIZE_INT * rightFill);
    memcpy(rightLeaf->leafRIDSlots->elems, overflowed->leafRIDSlots->elems + leftFill, SIZE_INT * rightFill);

    destroyBTNode(overflowed);

    // introduce to each other
    rightLeaf->right = leaf->right;
    if (rightLeaf->right != NULL) {
      rightLeaf->right->left = rightLeaf;
    }
    leaf->right = rightLeaf;
    rightLeaf->left = leaf;
    writeNode(rightLeaf, tree);
    writeNode(leaf, tree);
    insPropagateParent(tree, leaf, rightLeaf, rightLeaf->vals->elems[0]);
  }
  tree->numEntries++;
  writeBtreeHeader(tree);
  return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key) {
  BT_Node *leaf = findNodeByKey(tree, key->v.intV);
  if (leaf != NULL) {
    int index, _unused;
    index = saBinarySearch(leaf->vals, key->v.intV, &_unused);
    if (index >= 0) {
      saDeleteAt(leaf->vals, index, 1);
      saDeleteAt(leaf->leafRIDPages, index, 1);
      saDeleteAt(leaf->leafRIDSlots, index, 1);
      tree->numEntries--;
      writeNode(leaf, tree);
      writeBtreeHeader(tree);
    }
  }
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
  _handle->mgmtData = scanInfo;
  *handle = _handle;
  return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle){
  freeit(2, handle->mgmtData, handle);
  return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result){
  ScanMgmtInfo *scanInfo = handle->mgmtData;
  if(scanInfo->elementIndex >= scanInfo->currentNode->leafRIDPages->fill) { // finding the left most leaf
    //searchNextNode
    if(scanInfo->elementIndex == scanInfo->currentNode->vals->fill && scanInfo->currentNode->right==NULL){
      return RC_IM_NO_MORE_ENTRIES;
    } else {
      scanInfo->currentNode = scanInfo->currentNode->right;
      scanInfo->elementIndex = 0;
    }
  }
  result->page = scanInfo->currentNode->leafRIDPages->elems[scanInfo->elementIndex];
  result->slot = scanInfo->currentNode->leafRIDSlots->elems[scanInfo->elementIndex];
  scanInfo->elementIndex++;
  return RC_OK;
}

char *printTree (BTreeHandle *tree){
  int size = tree->numNodes * tree->size * 11 + tree->size + 14 + tree->numNodes;
  char *result = newCharArr(size);
  BT_Node *node = tree->root;
  int level=0;
  while(node!=NULL){
    printNodeHelper(node, result);
    if(node->isLeaf){
      node = node->right;
    } else {
      if(NULL == node->right){
        BT_Node *temp = tree->root;
        for(int j=0; j<=level;j++){
          temp=temp->children[0];
        }
        node = temp;
        level++;
      } else {
        node = node->right;
      }
    }
  }
  return result;
}
