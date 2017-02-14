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
