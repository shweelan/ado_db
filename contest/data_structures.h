#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include "dt.h"
#include "dberror.h"

// 1250 * 8KB(PAGE_SIZE is 8196) = 10MB, assuming we will need 10MB for every pool, 1259 is the closest prime number to 1250
#define HASH_LEN 1259

// linked list Node
typedef struct Node {
  void *data; // general pointer to be used with different data types
  struct Node *next;
  struct Node *previous;
} Node;

// key, value combination for hashmap
typedef struct HM_Comb {
  int key;
  void *val;
} HM_Comb;

// hashmap
typedef struct HM {
  Node *tbl[HASH_LEN]; // table of linked list to solve hashmap collision
} HM;

// smart sorted array
typedef struct smartArray {
  int size;
  int fill;
  int *elems; // extra extenstion void * with DataType keyType
} smartArray;


// hashmap management functions
HM *hmInit();
int hash(int key);
int hmInsert(HM *hm, int key, void *val);
void *hmGet(HM *hm, int key);
int hmDelete(HM *hm, int key);
void hmDestroy(HM *hm);

// smartArray management functions
smartArray *saInit(int size);
int saBinarySearch(smartArray *arr, int elem, int *fitOn);
void saDestroy(smartArray *arr);
int saInsertAt(smartArray *arr, int elem, int index);
int saInsert(smartArray *arr, int elem);
void saDeleteAt(smartArray *arr, int index, int count);
int saDeleteOne(smartArray *arr, int elem);
int saDeleteAll(smartArray *arr, int elem);
void saEmpty(smartArray *arr);
void saPrint(smartArray *arr);

#endif
