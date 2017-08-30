all: run1 run2 run3 run4 run5 run6 run7

run1: test_assign1_1.o dberror.o storage_mgr.o
	gcc -o bin/test_storage/run bin/test_assign1_1.o bin/storage_mgr.o bin/dberror.o

run2: test_assign2_1.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o data_structures.o
	gcc -o bin/test_buffer1/run bin/test_assign2_1.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/buffer_mgr_stat.o bin/data_structures.o

run3: test_assign2_2.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o data_structures.o
	gcc -o bin/test_buffer2/run bin/test_assign2_2.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/buffer_mgr_stat.o bin/data_structures.o

run4: test_assign3_1.o rm_serializer.o record_mgr.o dberror.o storage_mgr.o buffer_mgr.o data_structures.o expr.o
	gcc -o bin/test_record/run bin/test_assign3_1.o bin/rm_serializer.o bin/record_mgr.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/data_structures.o bin/expr.o -lm

run5: test_assign4_1.o btree_mgr.o rm_serializer.o record_mgr.o dberror.o storage_mgr.o buffer_mgr.o data_structures.o expr.o
	gcc -o bin/test_index/run bin/test_assign4_1.o bin/btree_mgr.o bin/rm_serializer.o bin/record_mgr.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/data_structures.o bin/expr.o -lm

run6: test_expr.o btree_mgr.o rm_serializer.o record_mgr.o dberror.o storage_mgr.o buffer_mgr.o data_structures.o expr.o
	gcc -o bin/test_expr/run bin/test_expr.o bin/btree_mgr.o bin/rm_serializer.o bin/record_mgr.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/data_structures.o bin/expr.o -lm

run7: client.o record_mgr.o dberror.o storage_mgr.o buffer_mgr.o data_structures.o expr.o rm_serializer.o
	gcc -o bin/test_client/run bin/client.o bin/rm_serializer.o bin/record_mgr.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/data_structures.o bin/expr.o -lm

test_assign1_1.o: test_assign1_1.c test_helper.h dberror.h storage_mgr.h
	gcc -c test_assign1_1.c -o bin/test_assign1_1.o

test_assign2_1.o: test_assign2_1.c test_helper.h dberror.h storage_mgr.h buffer_mgr.h data_structures.h buffer_mgr_stat.h dt.h
	gcc -c test_assign2_1.c -o bin/test_assign2_1.o

test_assign2_2.o: test_assign2_2.c test_helper.h dberror.h storage_mgr.h buffer_mgr.h data_structures.h buffer_mgr_stat.h dt.h
	gcc -c test_assign2_2.c -o bin/test_assign2_2.o

test_assign3_1.o: test_assign3_1.c dberror.h expr.h record_mgr.h tables.h test_helper.h
	gcc -c test_assign3_1.c -o bin/test_assign3_1.o

test_expr.o: test_expr.c dberror.h expr.h record_mgr.h tables.h test_helper.h btree_mgr.h
	gcc -c test_expr.c -o bin/test_expr.o

test_assign4_1.o: test_assign4_1.c btree_mgr.h dberror.h expr.h record_mgr.h tables.h test_helper.h btree_mgr.h
	gcc -c test_assign4_1.c -o bin/test_assign4_1.o

client.o: client.c record_mgr.h dberror.h const.h expr.h
	gcc -c client.c -o bin/client.o

rm_serializer.o: dberror.h record_mgr.h tables.h
	gcc -c rm_serializer.c -o bin/rm_serializer.o

record_mgr.o: record_mgr.c dberror.h storage_mgr.h buffer_mgr.h record_mgr.h tables.h data_structures.h expr.h
	gcc -c record_mgr.c -o bin/record_mgr.o

expr.o: expr.c dberror.h expr.h tables.h record_mgr.h
	gcc -c expr.c -o bin/expr.o

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	gcc -c buffer_mgr_stat.c -o bin/buffer_mgr_stat.o

buffer_mgr.o: buffer_mgr.c buffer_mgr.h storage_mgr.h dberror.h data_structures.h dt.h
	gcc -c buffer_mgr.c -o bin/buffer_mgr.o

btree_mgr.o: btree_mgr.c btree_mgr.h buffer_mgr.h storage_mgr.h dberror.h data_structures.h dt.h
	gcc -c btree_mgr.c -o bin/btree_mgr.o

storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
	gcc -c storage_mgr.c -o bin/storage_mgr.o

data_structures.o: data_structures.c data_structures.h dt.h
	gcc -c data_structures.c -o bin/data_structures.o

dberror.o: dberror.c dberror.h
	gcc -c dberror.c -o bin/dberror.o

clean:
	-rm bin/*.o bin/*/run
