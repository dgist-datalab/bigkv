# BigKV
A key-value cache system designed for petabyte-scale all-flash-array (AFA).

## How to run

### Run a cache server
```bash
$ make -j
$ sudo ./bin/server -d [num of devices] [device paths]

(for example)
$ sudo ./bin/server -d 2 /dev/nvme1n1 /dev/nvme2n1
```

### Run a client to benchmark (Optional)
```bash
$ ./bin/client
```

(Caution! To run it over network, you should set a correct `IP` at `include/config.h`.
For local test, use `127.0.0.1`)

### Change index structure
```bash
$ vim Makefile
...
DEF += -DBIGKV \ # <= change it!
...
```
We also implemented an index structure presented in uDepot at FAST'18 which is named here `hopscotch`.
To switch the index structure, replace `-DBIGKV` as `-DHOPSCOTCH` in `Makefile` like above.

### Some statements for code structure

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
You can find two index structures here, `bigkv_index` and `hopscotch`.

Finally, main files for `server` and `client` are out of there, and data structures resides in `src/utility`.

And for `include/`, vice versa.
