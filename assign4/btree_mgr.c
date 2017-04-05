//#include "btree_mgr.h"
#include "dberror.h"

// helpers
typedef struct smartArray {
  int size;
  int fill;
  int *elems; // extra extenstion void * with DataType keyType
} smartArray;

void printArr(smartArray *arr) {
  printf("------------------------\n");
  printf("[ ");
  for(int i = 0; i < arr->fill; i++) {
    printf("%d ", arr->elems[i]);
  }
  printf("]\n");
  printf("------------------------\n");
}

int binarySearch(int elem, smartArray *arr, int *fitOn) {
  int first = 0;
  int last = arr->fill - 1;
  if (last < 0) { // empty array
    (*fitOn) = first;
    return -1;
  }
  int pos;
  //printf("\n\n");
  while(1) {
    pos = (first + last) / 2;
    //printf("first=%d last=%d pos=%d val=%d\n", first, last, pos, arr->elems[pos]);
    if (elem == arr->elems[pos]) {
      while(elem == arr->elems[pos - 1]) { // always get the first occurance
        pos--;
      }
      (*fitOn) = pos;
      return pos;
    }
    if (first >= last) {
      if (elem > arr->elems[first]) {
        first++;
      }
      (*fitOn) = first; // must fit here
      return -1; // not found
    }
    if (elem < arr->elems[pos]) {
      last = pos - 1;
    }
    else {
      first = pos + 1;
    }
  }
}

smartArray *saInit(int size) {
  smartArray *arr = new(smartArray); // TODO free, Done in saDestroy
  arr->elems = newIntArr(size); // TODO free, Done in saDestroy
  arr->size = size;
  arr->fill = 0;
  return arr;
}

void saDestroy(smartArray *arr) {
  free(arr->elems);
  free(arr);
}

int saInsertSorted(int elem, smartArray *arr) {
  int fitOn = -1; // not more place
  if (arr->size > arr->fill) {
    int index = binarySearch(elem, arr, &fitOn);
    printf("inserting %d, found?=%s, will fit on index=%d\n", elem, (index < 0) ? "false" : "true", fitOn);
    if (fitOn != arr->fill) { // insert at the end
      for (int i = arr->fill; i > fitOn; i--) {
        arr->elems[i] = arr->elems[i - 1];
      }      
    }
    arr->elems[fitOn] = elem;
    arr->fill++;
  }
  printArr(arr);
  return fitOn;
}

//functionality

int main () {
  smartArray *arr = saInit(20);
  saInsertSorted(17, arr);
  saInsertSorted(4, arr);
  saInsertSorted(5, arr);
  saInsertSorted(5, arr);
  saInsertSorted(15, arr);
  saInsertSorted(7, arr);
  saInsertSorted(13, arr);
  saInsertSorted(10, arr);
  saInsertSorted(4, arr);
  saInsertSorted(10, arr);
  saInsertSorted(10, arr);
  saInsertSorted(10, arr);
  saInsertSorted(8, arr);
  saInsertSorted(10, arr);
  int fitOn;
  for (int i = 0; i < 20; i++) {
    int index = binarySearch(i, arr, &fitOn);
    printf("searching for %d, found?=%s, %son index=%d\n", i, (index < 0) ? "false" : "true", (index < 0) ? "it can fit " : "", fitOn);
  }
  saDestroy(arr);
  return 0;
}
