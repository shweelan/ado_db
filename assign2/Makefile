all: run1 run2
run1: test_assign2_1.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o data_structures.o
	gcc -o bin/test1/run bin/test_assign2_1.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/buffer_mgr_stat.o bin/data_structures.o

run2: test_assign2_2.o dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o data_structures.o
	gcc -o bin/test2/run bin/test_assign2_2.o bin/dberror.o bin/storage_mgr.o bin/buffer_mgr.o bin/buffer_mgr_stat.o bin/data_structures.o

test_assign2_1.o: test_assign2_1.c test_helper.h dberror.h storage_mgr.h buffer_mgr.h data_structures.h buffer_mgr_stat.h dt.h
	gcc -c test_assign2_1.c -o bin/test_assign2_1.o

test_assign2_2.o: test_assign2_2.c test_helper.h dberror.h storage_mgr.h buffer_mgr.h data_structures.h buffer_mgr_stat.h dt.h
	gcc -c test_assign2_2.c -o bin/test_assign2_2.o

data_structures.o: data_structures.c data_structures.h dt.h
	gcc -c data_structures.c -o bin/data_structures.o

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	gcc -c buffer_mgr_stat.c -o bin/buffer_mgr_stat.o

buffer_mgr.o: buffer_mgr.c buffer_mgr.h storage_mgr.h dberror.h data_structures.h dt.h
	gcc -c buffer_mgr.c -o bin/buffer_mgr.o

storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
	gcc -c storage_mgr.c -o bin/storage_mgr.o

dberror.o: dberror.c dberror.h
	gcc -c dberror.c -o bin/dberror.o

clean:
	-rm bin/*.o bin/test1/run bin/test2/run
