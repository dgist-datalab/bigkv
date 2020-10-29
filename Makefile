SRC_DIR = ./src
OBJ_DIR = ./obj
LIB_DIR = ./lib
BIN_DIR = ./bin
INC_DIR = ./include

CC = g++

CFLAGS += \
	-g \
	-Wall \
	-std=c++11 \
#	-O2 \
#	-fsanitize=address \

LIBS += \
	-lcityhash \
	-lpthread \
	-laio \

DEFS += \
	-DCITYHASH \
	-DLINUX_AIO \
	-DUNIFORM \
	-DUSE_HUGEPAGE\
	-DBIGKV \
#	-DYCSB \
	-DHOPSCOTCH \
#	-DHOTSPOT \

OBJ_SRC += \
	$(SRC_DIR)/index/hopscotch.c \
	$(SRC_DIR)/index/bigkv_index.c \
	$(SRC_DIR)/platform/util.c \
	$(SRC_DIR)/platform/keygen.c \
	$(SRC_DIR)/platform/master.c \
	$(SRC_DIR)/platform/request.c \
	$(SRC_DIR)/platform/handler.c \
	$(SRC_DIR)/platform/request.c \
	$(SRC_DIR)/platform/device.c \
	$(SRC_DIR)/platform/poller.c \
	$(SRC_DIR)/platform/aio.c \
	$(SRC_DIR)/utility/queue.c \
	$(SRC_DIR)/utility/lfqueue.c \
	$(SRC_DIR)/utility/cond_lock.c \

TARGET_OBJ =\
		$(patsubst %.c,%.o,$(OBJ_SRC))\

all: client server

client: $(SRC_DIR)/client.cc $(LIB_DIR)/libbigkv.a
	@mkdir -p $(BIN_DIR)
	$(CC) -o $(BIN_DIR)/$@ $^ $(CFLAGS) $(LIBS) $(DEFS) -I$(INC_DIR) 

server: $(SRC_DIR)/server.cc $(LIB_DIR)/libbigkv.a
	@mkdir -p $(BIN_DIR)
	$(CC) -o $(BIN_DIR)/$@ $^ $(CFLAGS) $(LIBS) $(DEFS) -I$(INC_DIR)

$(LIB_DIR)/libbigkv.a: $(TARGET_OBJ)
	@mkdir -p $(LIB_DIR)
	@mv $(SRC_DIR)/platform/*.o $(OBJ_DIR)
	@mv $(SRC_DIR)/index/*.o $(OBJ_DIR)
	@mv $(SRC_DIR)/utility/*.o $(OBJ_DIR)
	$(AR) r $@ $(OBJ_DIR)/*

.c.o:
	$(CC) $(CFLAGS) $(LIBS) $(DEFS) -c $< -o $@ -I$(INC_DIR)

clean:
	@rm -vf $(BIN_DIR)/*
	@rm -vf $(OBJ_DIR)/*.o
	@rm -vf $(LIB_DIR)/*.o
	@rm -vf $(LIB_DIR)/libbigkv.a
