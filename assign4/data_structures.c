#include "data_structures.h"
#include <stdlib.h>
#include <stdio.h>

HM *hmInit() {
  HM *hm = NULL;
  hm = malloc(sizeof(HM));
  int i;
  for(i = 0; i < HASH_LEN; i++) {
    hm->tbl[i] = NULL;
  }
  return hm;
}


// hash function, inspired by djb2 algorithm
int hash(int key) {
  unsigned h = 31;
  h = (h * HASH_LEN) ^ (key * HASH_LEN);
  return h % HASH_LEN;
}


// making key value pairs
HM_Comb *getComb(HM *hm, int key) {
  int indx = hash(key);
  Node *head = hm->tbl[indx];
  HM_Comb *comb = NULL;
  while(true) {
    if (head == NULL) {
      return NULL;
    }
    comb = (HM_Comb *)(head->data);
    if (comb->key == key) {
      return comb;
    }
    head = head->next;
  }
}


// insert a pointer to any type of value to the hash map
int hmInsert(HM *hm, int key, void *val) {
  HM_Comb *comb = getComb(hm, key);
  if (comb != NULL) {
    comb->val = val;
    return 0; // means updated
  }

  comb = malloc(sizeof(HM_Comb));
  comb->key = key;
  comb->val = val;
  Node *newNode = NULL;
  newNode = malloc(sizeof(Node));
  newNode->data = comb;
  newNode->next = NULL;
  newNode->previous = NULL;

  int indx = hash(key);
  Node *head = hm->tbl[indx];

  if (head != NULL) {
    newNode->next = head;
    head->previous = newNode;
  }
  hm->tbl[indx] = newNode;

  return 1; // means inserted
}


// get value by key from hashmap
void *hmGet(HM *hm, int key) {
  HM_Comb *comb = getComb(hm, key);
  if (comb == NULL) {
    return NULL;
  }
  return comb->val;
}


// delete a key, value pair from the hashmap
int hmDelete(HM *hm, int key) {
  int indx = hash(key);
  Node *head = hm->tbl[indx];
  HM_Comb *comb = NULL;
  while(head != NULL) {
    comb = (HM_Comb *)(head->data);
    if (comb->key == key) {
      if (head->previous == NULL) { // is it found on the first element of linked list?
        hm->tbl[indx] = head->next;
      }
      else {
        head->previous->next = head->next;
      }
      if (head->next != NULL) {
        head->next->previous = head->previous;
      }
      free(comb);
      free(head);
      return 1; // means deleted
    }
    head = head->next;
  }
  return 0; // means not found
}


// delete all nodes of hashmap and release resources
void hmDestroy(HM *hm) {
  Node *deletable = NULL;
  int i;
  for(i = 0; i < HASH_LEN; i++) {
    Node *head = hm->tbl[i];
    while (head != NULL) {
      deletable = head;
      head = head->next;
      free(deletable->data);
      free(deletable);
    }
  }
  free(hm);
}


void saPrint(smartArray *arr) {
  return;
  printf("------------------------\n");
  printf("[ ");
  for(int i = 0; i < arr->fill; i++) {
    printf("%d ", arr->elems[i]);
  }
  printf("]\n");
  printf("------------------------\n");
}


int saBinarySearch(smartArray *arr, int elem, int *fitOn) {
  int first = 0;
  int last = arr->fill - 1;
  if (last < 0) { // empty array
    (*fitOn) = first;
    return -1;
  }
  int pos;
  while(1) {
    pos = (first + last) / 2;
    //printf("first=%d last=%d pos=%d val=%d\n", first, last, pos, arr->elems[pos]);
    if (elem == arr->elems[pos]) {
      while(pos && elem == arr->elems[pos - 1]) { // always get the first occurance
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


int saInsertAt(smartArray *arr, int elem, int index) {
  if (arr->size > arr->fill && index <= arr->fill) {
    if (index != arr->fill) { // insert at the end
      for (int i = arr->fill; i > index; i--) {
        arr->elems[i] = arr->elems[i - 1];
      }
    }
    arr->elems[index] = elem;
    arr->fill++;
    return index;
  }
  return -1; // not more place, or invalid index
}


int saInsert(smartArray *arr, int elem) {
  int fitOn = -1; // not more place
  if (arr->size > arr->fill) {
    int index = saBinarySearch(arr, elem, &fitOn);
    //printf("inserting %d, found?=%s, will fit on index=%d\n", elem, (index < 0) ? "false" : "true", fitOn);
    fitOn = saInsertAt(arr, elem, fitOn);
  }
  saPrint(arr);
  return fitOn;
}


void saDeleteAt(smartArray *arr, int index, int count) {
  arr->fill -= count;
  for (int i = index; i < arr->fill; i++) {
    arr->elems[i] = arr->elems[i + count];
  }
}


int saDeleteOne(smartArray *arr, int elem) {
  int _unused;
  int index = saBinarySearch(arr, elem, &_unused);
  //printf("deleteing %d, found?=%s, will delete from index=%d\n", elem, (index < 0) ? "false" : "true", index);
  if (index >= 0) {
    saDeleteAt(arr, index, 1);
  }
  saPrint(arr);
  return index;
}


int saDeleteAll(smartArray *arr, int elem) {
  int _unused;
  int index = saBinarySearch(arr, elem, &_unused);
  //printf("deleteing all %d, found?=%s, will delete from index=%d\n", elem, (index < 0) ? "false" : "true", index);
  if (index >= 0) {
    int count = 1;
    while(index + count < arr->fill && elem == arr->elems[index + count]) {
      count++;
    }
    saDeleteAt(arr, index, count);
  }
  saPrint(arr);
  return index;
}


void saEmpty(smartArray *arr) {
  arr->fill = 0;
}
