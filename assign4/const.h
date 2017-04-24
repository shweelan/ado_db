#ifndef CONST_H
#define CONST_H

/* module wide constants */
/*
  choosing 8192 as block size, because most of the operating systems are implementing blocks of size of power of 2, mostly 4096 or 8192
  reading smaller or larger that are not power of 2 blocks will waste bytes already feched by the file system
*/
#define PAGE_SIZE 8192

/* Schema Stringify delimiter */
#define DELIMITER ((char *) ",")

/* Per table buffer size */
#define PER_TBL_BUF_SIZE 10

/* Per table index size */
#define PER_IDX_BUF_SIZE 10

/* Page header length */
#define PAGE_HEADER_LEN 11

/* bytes to represent number of slots in page */
#define BYTES_SLOTS_COUNT 2

/* Table header length */
#define TABLE_HEADER_PAGES_LEN 2

/* Number of bits in byte */
#define NUM_BITS 8

/* Number of bytes for each btree node header */
#define BYTES_BT_HEADER_LEN 40

/* DB path configuration */
#define PATH_DIR "/tmp/database_ado/"
#define DEFAULT_MODE 0777

#define SIZE_INT sizeof(int)
#endif
