OBJ=expr.o dberror.o rm_serializer.o record_mgr.o buffer_mgr.o buffer_mgr_stat.o btree_mgr.o storage_mgr.o contest_setup.o data_structures.o
HEADERS=buffer_mgr.h dberror.h expr.h record_mgr.h storage_mgr.h tables.h test_helper.h data_structures.h const.h
TEST_BIN=test_expr.bin test_assign1_1.bin test_assign2_1.bin test_assign3_1.bin test_assign4_1.bin contest.bin test_contest.bin
TEST_OBJ=$(TEST_BIN:.bin=.o)
CFLAGS:=$(CFLAGS) -I. -g -Wall -Werror

all: $(TEST_BIN) $(OBJ) $(TEST_OBJ) $(HEADERS)

$(OBJ): $(HEADERS)

%.bin: %.o $(OBJ) $(TEST_OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(@:.bin=.o) -lm

clean:
	rm -f $(TEST_BIN) $(OBJ) $(TEST_OBJ)

.PHONY: clean
