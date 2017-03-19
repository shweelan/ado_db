run: test_assign3_1.o rm_serializer.o record_mgr.o dberror.o storage_mgr.o buffer_mgr.o data_structures.o expr.o
	gcc -o bin/run bin/*.o

test_assign3_1.o: test_assign3_1.c dberror.h expr.h record_mgr.h tables.h test_helper.h
	gcc -c test_assign3_1.c -o bin/test_assign3_1.o

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


storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
	gcc -c storage_mgr.c -o bin/storage_mgr.o

data_structures.o: data_structures.c data_structures.h dt.h
	gcc -c data_structures.c -o bin/data_structures.o

dberror.o: dberror.c dberror.h
	gcc -c dberror.c -o bin/dberror.o
