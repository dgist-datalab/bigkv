#include "config.h"
#include "platform/util.h"
#include "utility/stopwatch.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>

#ifdef TEST_GC
//#define NR_KEY (10 * 1000 * 1000)
//#define NR_QUERY (10 * 1000 * 1000)

#define NR_KEY (10 * 1000 * 1000)
#define NR_QUERY (10 * 1000 * 1000)

#else
#define NR_KEY   500000
#define NR_QUERY 100000
#endif
//#define NR_KEY   50000000
//#define NR_QUERY 50000000
//#define NR_KEY   2000000
//#define NR_QUERY 2000000

//#define NR_KEY   1000000
//#define NR_QUERY 1000000
//#define NR_KEY   1000000
//#define NR_QUERY 1000000

#define CLIENT_QDEPTH 1024

bool stopflag;

int sock;
pthread_t tid;
uint32_t seq_num_global;

uint64_t cdf_table[CDF_TABLE_MAX];

stopwatch *sw;
sem_t sem;

static void close_trace(void);

static int bench_free() {
	sleep(5);
	stopflag=true;
	int *temp;
	while (pthread_tryjoin_np(tid, (void **)&temp)) {}
	close(sock);
	close_trace();
	return 0;
}

static void client_exit(int sig) {
	puts("");
	bench_free();
	exit(1);
}

static int sig_add() {
	struct sigaction sa;
	sa.sa_handler = client_exit;
	sigaction(SIGINT, &sa, NULL);
	return 0;
}

void *ack_poller(void *arg) {
	struct netack na_arr[128];
	int len = 0;

	puts("Bench :: ack_poller() created");

	while (1) {
		if (stopflag) break;

		len = read_sock_bulk(sock, na_arr, 128, sizeof(struct netack));
		if (len == -1) continue;
		else if (len == 0) {
			close(sock);
			printf("ack_poller:: Disconnected!\n");
			break;
		}

		int n_obj = len / sizeof(struct netack);
		for (int i = 0; i < n_obj; i++) {
			struct netack *na = &na_arr[i];
			if (na->type == REQ_TYPE_GET) {
				collect_latency(cdf_table, na->elapsed_time);
			}
			req_out(&sem);
		}
	}
	return NULL;
}

static int bench_init() {
	sem_init(&sem, 0, CLIENT_QDEPTH);

	pthread_create(&tid, NULL, &ack_poller, NULL);

	sw = sw_create();

	return 0;
}

static int connect_server() {
	struct sockaddr_in addr;

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock == -1) abort();

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = inet_addr(IP);
	addr.sin_port = htons(PORT);

	puts("- Wait for server connection...");

	if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
		abort();
	}
	puts("- Server is connected!");

	return 0;
}

#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define XXH_INLINE_ALL
#include "platform/xxhash.h"

struct titem {
	int timestamp;
	char key[128];
	int key_size;
	int value_size;
	int client_id;
	int op;
	int ttl;
};

// We can't use enum here as it's 64-bit
#define H_SET		14527620346409587884UL
#define H_GET		15044932803361617835UL
#define H_DELETE	 4817649594423435005UL
#define H_GETS		 5205206981515762381UL
#define H_ADD		18425161627894713984UL
#define H_REPLACE	 7630174865538825181UL
#define H_CAS		15353879888721228789UL
#define H_APPEND	 1418217033154176949UL
#define H_PREPEND	 3181235489723104214UL
#define H_INCR		 4353100097886328392UL
#define H_DECR		17022530745781332774UL

enum op {
	OP_SET = 0,
	OP_GET = 1,
	OP_DELETE = 2
};

static char *mmap_addr;
static size_t mmap_length;
static int total_items;

static void open_trace(const char *trace_path)
{
	int fd;
	struct stat st;

	/* get trace file info */
	if ((fd = open(trace_path, O_RDONLY)) < 0) {
		fprintf(stderr,"Unable to open '%s', %s\n", trace_path, strerror(errno));
		exit(1);
	}

	if ((fstat(fd, &st)) < 0) {
		close(fd);
		fprintf(stderr, "Unable to fstat '%s', %s\n", trace_path, strerror(errno));
		exit(1);
	}

	/* set up mmap region */
	mmap_addr = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if (mmap_addr == MAP_FAILED) {
		close(fd);
		fprintf(stderr, "Unable to allocate %zu bytes of memory, %s\n", st.st_size,
				strerror(errno));
		exit(1);
	}
	mmap_length = st.st_size;

	/* USE_HUGEPAGE */
	madvise(mmap_addr, st.st_size, MADV_HUGEPAGE | MADV_SEQUENTIAL);

	close(fd);
}

static void close_trace(void)
{
	munmap(mmap_addr, mmap_length);
}

static inline int fast_atoi(const char *str, const char delim)
{
	int val = 0;
	while (*str != delim)
		val = val * 10 + (*str++ - '0');
	return val;
}

static inline void dump_item(const struct titem *item)
{
	putchar('\n');
	printf("timestamp:  %d\n", item->timestamp);
	printf("key:       \"%s\"\n", item->key);
	printf("key_size:   %d\n", item->key_size);
	printf("value_size: %d\n", item->value_size);
	printf("client_id:  %d\n", item->client_id);
	printf("op:         %d\n", item->op);
	printf("ttl:        %d\n", item->ttl);
	putchar('\n');
}

static int read_next(struct netreq *netreq)
{
	static size_t offset;
	if (offset >= mmap_length)
		return 1;

	int oplen = 0;
	char *addr;
	XXH64_hash_t ophash;

	addr = mmap_addr + offset;

	// Initialize netreq
	memset(netreq, 0, sizeof(struct netreq));

	// Skip timestamp
	while (*addr++ != ',');

	// Read anonymized key
	while (*addr != ',') {
		netreq->keylen++;
		addr++;
	}
	memcpy(netreq->key, addr - netreq->keylen, netreq->keylen);
	*(netreq->key + netreq->keylen) = '\0';
	addr++;

	// Skip key size
	while (*addr++ != ',');

	// Read value size
	while (*addr != ',')
		netreq->kv_size = netreq->kv_size * 10 + (*addr++ - '0');
	addr++;

	// Skip client id
	while (*addr++ != ',');

	// Read operation string and compare via xxHash
	while (*addr != ',') {
		oplen++;
		addr++;
	}
	ophash = XXH3_64bits(addr - oplen, oplen);

	switch (ophash) {
	case H_SET:
		netreq->type = REQ_TYPE_SET;
		break;
	case H_GET:
		netreq->type = REQ_TYPE_GET;
		break;
	case H_DELETE:
		netreq->type = REQ_TYPE_DELETE;
		break;
	case H_GETS:
	case H_ADD:
	case H_REPLACE:
	case H_CAS:
	case H_APPEND:
	case H_PREPEND:
	case H_INCR:
	case H_DECR:
		//printf("Ignoring \"");
		goto skip_silent;
	default:
		printf("Unrecognized operation: \"");
		goto skip;
	}

	addr++;

	// Skip TTL
	while (*addr++ != '\n');

	// Update offset
	offset = addr - mmap_addr;

	total_items++;

	// printf("\rOffset: %lu", offset);

	// Dump item
	//dump_item(&item);

	return 0;

skip:
	fwrite(addr - oplen, oplen, 1, stdout);
	puts("\"");

skip_silent:
	while (*addr++ != '\n');

	// Update offset
	offset = addr - mmap_addr;

	return 0;
}

struct netreq nr_arr[128];

static int run_bench(void) {
	struct netreq netreq;

	sw_start(sw);
	for (size_t i = 0; i < NR_QUERY; i++) {
		if (i%(NR_QUERY/100)==0) {
			printf("\rProgress [%3.0f%%] (%lu/%d)",
				(float)i/NR_QUERY*100,i,NR_QUERY);
			fflush(stdout);
		}

		if (read_next(&netreq) != 0)
			break;

		req_in(&sem);
		netreq.seq_num = ++seq_num_global;
		send_request(sock, &netreq);
	}
	wait_until_finish(&sem, CLIENT_QDEPTH);
	sw_end(sw);

	puts("\nBenchmark finished!");
	print_cdf(cdf_table, NR_QUERY);
	printf("%.4f seconds elapsed...\n", sw_get_sec(sw));
	printf("Throughput(IOPS): %.2f\n\n", (double)NR_QUERY/sw_get_sec(sw));

	return 0;
}

int main(int argc, char *argv[]) {
	sig_add();

	bench_init();

	/* Connect to server */
	connect_server();

	/* Load trace file */
	open_trace(argv[1]);

	/* Benchmark phase */
	run_bench();

	bench_free();
	return 0;
}
