# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

CFLAGS ?= -g -fPIC -O3 -std=gnu99 -Wall -Wno-unused-function
CFLAGS += -I$(RM_INCLUDE_DIR)
CC=gcc

OBJS=util.o strings.o sds.o vector.o alloc.o cJSON.o thread_pool.o string_pool.o

all: librmutil.a

clean:
	rm -rf *.o *.a

librmutil.a: $(OBJS)
	ar rcs $@ $^

test_vector: test_vector.o vector.o
	$(CC) -Wall -o $@ $^ -lc -lpthread -O0
	@(sh -c ./$@)
.PHONY: test_vector
	
test: test_vector
.PHONY: test
