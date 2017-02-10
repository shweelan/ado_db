#include "data_structures.h"
#include <stdlib.h>
#include <stdio.h>

HM *hmInit() {
  HM *hm = NULL;
  hm = malloc(sizeof(HM));
  for(int i = 0; i < HASH_LEN; i++) {
    hm->tbl[i] = NULL;
  }
  return hm;
}


int hash(int key) {
  unsigned h = 31;
  h = (h * HASH_LEN) ^ (key * HASH_LEN);
  return h % HASH_LEN;
}


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


void *hmGet(HM *hm, int key) {
  HM_Comb *comb = getComb(hm, key);
  if (comb == NULL) {
    return NULL;
  }
  return comb->val;
}


int hmDelete(HM *hm, int key) {
  int indx = hash(key);
  Node *head = hm->tbl[indx];
  HM_Comb *comb = NULL;
  while(true) {
    if (head == NULL) {
      return 0; // means not found
    }
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
}

// test, will remove later

typedef struct X {
  int d;
} X;

int main(int argc, char *argv[]) {
  HM *hm = hmInit();
  X *x = malloc(sizeof(X));
  x->d = 44;
  printf("insert 5 = %d\n", hmInsert(hm, 5, x));
  printf("insert 55 = %d\n", hmInsert(hm, 55, x));
  printf("insert 5 = %d\n", hmInsert(hm, 5, x));
  X *new_x = (X *)(hmGet(hm, 5));
  printf("get 5 = %d\n", new_x->d);
  printf("delete 5 = %d\n", hmDelete(hm, 5));
  printf("delete 5 = %d\n", hmDelete(hm, 5));
  printf("get 5 = %s\n", hmGet(hm, 5));
}
