run: test_assign1_1.o dberror.o storage_mgr.o
	gcc -o bin/run bin/*.o

test_assign1_1.o: test_assign1_1.c test_helper.h dberror.h storage_mgr.h
	gcc -c test_assign1_1.c -o bin/test_assign1_1.o

storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
	gcc -c storage_mgr.c -o bin/storage_mgr.o

dberror.o: dberror.c dberror.h
	gcc -c dberror.c -o bin/dberror.o

clean:
	-rm bin/test_assign1_1.o bin/storage_mgr.o bin/dberror.o bin/run
