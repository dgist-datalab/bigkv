#include "config.h"
#include "platform/keygen.h"
#include "platform/util.h"
#include "utility/stopwatch.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>

#ifdef TEST_GC
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

struct keygen *kg;
int sock;
pthread_t tid;
uint32_t seq_num_global;

uint64_t cdf_table[CDF_TABLE_MAX];

stopwatch *sw;
sem_t sem;


static int bench_free() {
	sleep(5);
	stopflag=true;
	int *temp;
	while (pthread_tryjoin_np(tid, (void **)&temp)) {}
	keygen_free(kg);
	close(sock);
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
	kg = keygen_init(NR_KEY, KEY_LEN);

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

struct netreq nr_arr[128];

static int load_kvpairs() {
	for (int i = 0; i < 128; i++) {
		nr_arr[i].keylen  = KEY_LEN;
		nr_arr[i].type    = REQ_TYPE_SET;
		nr_arr[i].kv_size = VALUE_LEN;
	}

	sw_start(sw);
	for (size_t i = 0; i < NR_KEY;) {
		if (i%(NR_KEY/100)==0) {
			printf("\rProgress [%2.0f%%] (%lu/%d)",
				(float)i/NR_KEY*100, i, NR_KEY);
			fflush(stdout);
		}

		int j;
		for (j = 0; j < 128; i++, j++) {
			kg_key_t next_key = get_next_key_for_load(kg);
			if (next_key == NULL) {
				break;
			}
			req_in(&sem);
			memcpy(nr_arr[j].key, next_key, KEY_LEN);
			nr_arr[j].seq_num = ++seq_num_global;
		}
		send_request_bulk(sock, nr_arr,j);
	}
	wait_until_finish(&sem, CLIENT_QDEPTH);
	sw_end(sw);

	puts("\nLoad finished!");
	printf("%.4f seconds elapsed...\n", sw_get_sec(sw));
	printf("Throughput(IOPS): %.2f\n\n", (double)NR_KEY/sw_get_sec(sw));

	return 0;
}

static int run_bench(key_dist_t dist, int query_ratio, int hotset_ratio) {
	struct netreq netreq;

	netreq.keylen = KEY_LEN;
	netreq.type = REQ_TYPE_GET;
	netreq.kv_size = VALUE_LEN;

	set_key_dist(kg, dist, query_ratio, hotset_ratio);

	sw_start(sw);
	for (size_t i = 0; i < NR_QUERY; i++) {
		if (i%(NR_QUERY/100)==0) {
			printf("\rProgress [%3.0f%%] (%lu/%d)",
				(float)i/NR_QUERY*100,i,NR_QUERY);
			fflush(stdout);
		}
		req_in(&sem);
		//memcpy(netreq.key, get_next_key(kg), KEY_LEN);
		memcpy(netreq.key, get_next_key_for_load(kg), KEY_LEN);
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

	/* Load phase */
#ifdef TEST_GC
	load_kvpairs();
	//load_kvpairs();
	//load_kvpairs();
	//load_kvpairs();
	//load_kvpairs();
#endif
	load_kvpairs();

	/* Benchmark phase */
	run_bench(KEY_DIST_UNIFORM, 50, 50);
//	run_bench(KEY_DIST_UNIFORM, 50, 50);
//	run_bench(KEY_DIST_LOCALITY, 60, 40);
//	run_bench(KEY_DIST_LOCALITY, 70, 30);
//	run_bench(KEY_DIST_LOCALITY, 80, 20);
//	run_bench(KEY_DIST_LOCALITY, 90, 10); 
//	run_bench(KEY_DIST_LOCALITY, 99, 1);

	bench_free();
	return 0;
}
