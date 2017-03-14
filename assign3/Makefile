run: record_mgr.o dberror.o storage_mgr.o buffer_mgr.o data_structures.o
	gcc -o bin/run bin/*.o

record_mgr.o: record_mgr.c dberror.h storage_mgr.h buffer_mgr.h record_mgr.h tables.h data_structures.h
	gcc -c record_mgr.c -o bin/record_mgr.o


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
