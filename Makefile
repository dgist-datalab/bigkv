SRC_DIR = ./src
OBJ_DIR = ./obj
LIB_DIR = ./lib
BIN_DIR = ./bin

all: bigkv udepot-opt udepot-cache slickcache
bigkv:
	make clean; make -j 4 -f Makefile.bigkv

udepot-opt:
	make clean; make -j 4 -f Makefile.udepot-opt

udepot-cache:
	make clean; make -j 4 -f Makefile.udepot-cache

slickcache:
	make clean; make -j 4 -f Makefile.slickcache

clean:
	@rm -vf $(OBJ_DIR)/*.o
	@rm -vf $(LIB_DIR)/*.o
	@rm -vf $(LIB_DIR)/libbigkv.a

clean-all:
	@rm -vf $(BIN_DIR)/*
	@rm -vf $(OBJ_DIR)/*.o
	@rm -vf $(LIB_DIR)/*.o
	@rm -vf $(LIB_DIR)/libbigkv.a
