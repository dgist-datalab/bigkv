# BigKV
BigKV is a key-value cache specifically designed for caching large objects in an all-flash array (AFA).

The original paper that introduced KEVIN is currently in the revision stage of [ACM/SIGOPS EuroSys 2023](https://2023.eurosys.org/).

## Prerequisites
* The hardware/software requirements for executing BigKV are as followed.

### Hardware
  * `DRAM`: Larger than 4GB for running the server and YCSB benchmark.
  * `SSD`: At least one SSD, on which the file system is not mounted, is required for running the server.
  * `CPU`: At least 8 cores are recommended to run a server (3 threads) and YCSB (100 clients).

### Software
* BigKV uses several third parties. The following libraries are essential for executing BigKV with a default setup.
  * These are mandatory libraries to compile BigKV. We summarize commands for installation below. We recommend to check the README file of each repository to use it.
  
  * [CityHash](https://github.com/google/cityhash)
   
	```
	git clone https://github.com/google/cityhash
	cd cityhash
	./configure
	make all check CXXFLAGS="-g -O3"
	sudo make install
	```
  * [liburing](https://github.com/axboe/liburing)
   
	```
	git clone https://github.com/axboe/liburing
	cd liburing
	./configure
	make
	sudo make install
	```
  * [libnuma](https://github.com/numactl/numactl)
  
	```
	sudo apt install libnuma-dev
	```
	
  * [libaio](https://pagure.io/libaio)
  
  	```
	sudo apt install libaio1 libaio-dev
  	```
  
  * After installing, put the command for shared library links.
  
  ```
  sudo ldconfig
  ```

  > ldconfig creates the necessary links and cache to the most recent shared libraries found in the directories specified on the command line, in the file /etc/ld.so.conf, and in the trusted directories (/lib and /usr/lib).

## Installation

* Clone required repository (BigKV).

```
git clone https://github.com/dgist-datalab/bigkv
cd bigkv
```

### Complilation

* There are four caches in this repository. The binaries are in the `./bin` directory.
  * BigKV: BigKV is our caching system. To complie this, just put the following command.

  ```
  make bigkv
  ```

  * uDepot-OPT: [uDepot-OPT](https://www.usenix.org/conference/fast19/presentation/kourtis) stores the entire index in DRAM. It uses hopscotch hasing for the indexing algorithm.

  ```
  make udepot-opt
  ```

  * uDepot-Cache: [uDepot-Cache](https://www.usenix.org/conference/fast19/presentation/kourtis) caches hot parts of the index table in DRAM. It also uses hopscotch hasing for the indexing algorithm.

  ```
  make udepot-cache
  ```

  * SlickCache: SlickCache: [SlickCache](http://bit.csc.lsu.edu/~fchen/publications/papers/socc18.pdf) cache hot index entries in DRAM. An entry stores the address of a KV object in a storage device.

  ```
  make slickcache
  ```
	
  * To complie the four caches, just put the command.
  ```
  make
  ```
  
## Exucution

### Prepare YCSB

* The default setup of our caches support redis protocols. We can test the cache server with [YCSB](https://github.com/brianfrankcooper/YCSB) benchmark with redis interface.

* Installing [YCSB](https://github.com/brianfrankcooper/YCSB/tree/master/redis) benchmark with redis interface.

  * First, if you don't have Maven and Java, install them.

  * Maven

  ```
  sudo apt install maven
  ```

  * Java: Please refer these guides: 
  
  - [How To Install Java with Apt on Ubuntu 18.04](https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-on-ubuntu-18-04)
	
  * The YCSB repository provides detailed guide for installation. Following commands decribe the summary of the installation.

  ```
  git clone https://github.com/brianfrankcooper/YCSB
  cd YCSB
  mvn -pl redis -am clean package
  ```

### Test

* After installing YCSB, you can test caches. We provide example YCSB script for the test.

  * Open the `./exp/ycsb/ycsb.sh` and modify the directory path to your directory path.

  - `ycsb_dir`: Change it to where YCSB installed. For example:

  ```
  ycsb_dir="/home/your_name/YCSB"
  ```

  - `bigkv_dir`: Change it to where bigkv installed. For example:

  ```
  bigkv_dir="/home/your_name/bigkv"
  ```

  - `dev_path`: The script uses single block device for testing. So you must prepare a separate block device. The block device is used as a raw block device. You must not have any filesystem on the block device. Change `dev_path` to the block device path. For example:

  ```
  dev_path="/dev/nvme2n1"
  ```

  * After setting, You can test by running the script. This script run YCSB benchmark to the four caches (bigkv, udepot-opt, udepot-cache, and slickcache). The results are stored `./exp/ycsb/log` directory. This script may be finished in 5~20 minutes by your server environment.

  ```
  ./ycsb.sh test -f
  ```

### Results

  * You can see several results in the `./exp/ycsb/log/ycsb-test-{time}` directory. The directory path names mean configurations of the benchmakrs. The directory paths are formatted as follows: `./ycsb-test-{time}/{cache type}/{total amount of memory for index}/{locality of YCSB workloads}/{results}`.
	
  - `cache type`: Currently, we provide four caches as mentioned above.

  - `total amount of memory for index`: The default amount of DRAM for index is 128MB.

  - `locality of YCSB workloads`: We use the hotspot distribution for test.

  - `results`: You can check various results in the directories. For example, the YCSB throughput is stored in the `ycsb` directory.

## Some statements for code structure

First, platform-specific source codes are tied in a `src/platform/` directory.
This includes device handler, poller, IO backend, and so on.

(some important files in src/platform/)
- `aio.c`     : Linux aio backend
- `device.c`  : Device abstraction layer (managing devices)
- `handler.c` : Request handler thread
- `master.c`  : Master thread
- `poller.c`  : Poller thread
- `request.c` : Generate/reap reqeust structures


Second, source codes implementing index structures are in `src/index`.
You can find two index structures here, `bigkv_index.c` (for bigkv), `hopscotch.c` (for udepot-opt and -cache), and `cascade.c` (for slickcache).

Finally, main files for `server` and `client` are out of there, and data structures resides in `src/utility`.

And for `include/`, vice versa.
