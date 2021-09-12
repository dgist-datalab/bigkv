SRC_DIR = ./src
OBJ_DIR = ./obj
LIB_DIR = ./lib
BIN_DIR = ./bin
INC_DIR = ./include

CC = g++

CFLAGS += \
	-g \
	-Wall \
	-Wno-unused-function \
	-std=c++11 \

DBG_CFLAGS += \
	-O0 \
	-fsanitize=address \
	-static-libasan \
#	-O2 \
#	-fsanitize=undefined \
#	-fsanitize=address \
#	-fsanitize=thread \

LIBS += \
	-lpthread \
	-laio \


#	-DHOPSCOTCH_FULL \ # hopscotch or hopscotch-swap
#	-DHOPSCOTCH \ # hopscotch hash
#	-DHOPSCOTCH_PART \ # hopscotch-cache
#	-DPART_MEM \ # hopscotch-cache: store full parts in memory
#	-DTEST_GC \ # bigkv test gc
#	-DREDIS \ # YCSB
#	-DTEST_GC \ # bigkv test gc

DEFS += \
	-DLINUX_AIO \
	-DUNIFORM \
	-DCDF \
	-DREDIS \
	-DBIGKV \
#	-DHOPSCOTCH \
	-DHOPSCOTCH_PART \
	-DPART_MEM \
#	-DHOPSCOTCH_FULL \
#	-DDEBUG_GC \
#	-DTEST_GC \
#	-DUSE_HUGEPAGE\
#	-DYCSB \
#	-DHOTSPOT \

OBJ_SRC += \
	$(SRC_DIR)/index/hopscotch.c \
	$(SRC_DIR)/index/bigkv_index.c \
	$(SRC_DIR)/platform/util.c \
	$(SRC_DIR)/platform/keygen.c \
	$(SRC_DIR)/platform/master.c \
	$(SRC_DIR)/platform/client.c \
	$(SRC_DIR)/platform/request.c \
	$(SRC_DIR)/platform/handler.c \
	$(SRC_DIR)/platform/request.c \
	$(SRC_DIR)/platform/device.c \
	$(SRC_DIR)/platform/poller.c \
	$(SRC_DIR)/platform/aio.c \
	$(SRC_DIR)/platform/redis.c \
	$(SRC_DIR)/platform/redis_exec.c \
	$(SRC_DIR)/utility/queue.c \
	$(SRC_DIR)/utility/lfqueue.c \
	$(SRC_DIR)/utility/cond_lock.c \
	$(SRC_DIR)/utility/lru_cache.c \
	$(SRC_DIR)/utility/art.c \
	$(SRC_DIR)/utility/list.c \

OPT_SRC += \
	$(SRC_DIR)/platform/xxhash.c \

TARGET_OBJ =\
		$(patsubst %.c,%.o,$(OBJ_SRC))\

OPT_OBJ =\
		$(patsubst %.c,%.o,$(OPT_SRC))\

all: client server

client: $(SRC_DIR)/client.cc $(LIB_DIR)/libbigkv.a
	@mkdir -p $(BIN_DIR)
	$(CC) -o $(BIN_DIR)/$@ $^ $(CFLAGS) $(DBG_CFLAGS) $(LIBS) $(DEFS) -I$(INC_DIR) 

server: $(SRC_DIR)/server.cc $(LIB_DIR)/libbigkv.a
	@mkdir -p $(BIN_DIR)
	$(CC) -o $(BIN_DIR)/$@ $^ $(CFLAGS) $(DBG_CFLAGS) $(LIBS) $(DEFS) -I$(INC_DIR)

$(LIB_DIR)/libbigkv.a: $(TARGET_OBJ) $(OPT_OBJ)
	@mkdir -p $(LIB_DIR)
	@mv $(SRC_DIR)/platform/*.o $(OBJ_DIR)
	@mv $(SRC_DIR)/index/*.o $(OBJ_DIR)
	@mv $(SRC_DIR)/utility/*.o $(OBJ_DIR)
	$(AR) r $@ $(OBJ_DIR)/*

$(TARGET_OBJ): EXTRA_FLAGS := $(DBG_CFLAGS)
$(OPT_OBJ): EXTRA_FLAGS := -O3 -mavx2

.c.o:
	$(CC) $(CFLAGS) $(EXTRA_FLAGS) $(LIBS) $(DEFS) -c $< -o $@ -I$(INC_DIR)

clean:
	@rm -vf $(BIN_DIR)/*
	@rm -vf $(OBJ_DIR)/*.o
	@rm -vf $(LIB_DIR)/*.o
	@rm -vf $(LIB_DIR)/libbigkv.a
