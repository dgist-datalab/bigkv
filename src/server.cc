#include "platform/util.h"
#include "platform/handler.h"
#include "platform/request.h"
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>

extern int first;
extern bool fault_occurs;
extern bool repaired;
extern uint64_t G_count;

struct server {
	int num_dev;
	char *device[32];
	char core_mask[128];
	int num_hlr;
	struct master *mas;
} server;

static void server_exit(int sig) {
	struct master *mas = server.mas;
	for (int i = 0; i < mas->num_hlr; i++) {
		printf("hlr[%d]\n", i);
		for (int j = 0; j < 10; j++) {
			mas->sw_total[j] += mas->hlr[i]->sw_total[j];
		}
		print_hlr_stat(mas->hlr[i]);
		//print_hlr_sw(mas->hlr[i]);
		fflush(stdout);
	}
	print_mas_sw(mas);
	printf("asdasdasd\n");
	fflush(stdout);
	uint64_t nr_query = 0;
#ifdef CDF
	for (int i = 0; i < mas->num_dev; i++) {
		for (int j = 0; j < CDF_TABLE_MAX; j++) {
			nr_query += mas->hlr[i]->cdf_table[j];
			mas->cdf_table[j] += mas->hlr[i]->cdf_table[j];
		}
	}	
	print_cdf(mas->cdf_table, nr_query);
	fflush(stdout);
#endif
#ifdef AMF_CDF
	uint64_t nr_query_cnt = 0, nr_query_bytes = 0;
	for (int i = 0; i < mas->num_dev; i++) {
		for (int j = 0; j < CDF_TABLE_MAX; j++) {
			mas->amf_cdf_table[j] += mas->hlr[i]->amf_cdf_table[j];
			nr_query_cnt += mas->hlr[i]->amf_cdf_table[j];
			mas->amf_bytes_cdf_table[j] += mas->hlr[i]->amf_bytes_cdf_table[j];
			nr_query_bytes += mas->hlr[i]->amf_bytes_cdf_table[j];
		}
	}	
	print_io_cdf(mas->amf_cdf_table, nr_query_cnt);
	print_io_bytes_cdf(mas->amf_bytes_cdf_table, nr_query_bytes);
	fflush(stdout);
#endif

	exit(1);
}


static int signal_fault_add();
static int signal_repaired_add();
static void server_fault (int sig) {
	printf("FAULT!!! %lu\n", G_count);
	fault_occurs = true;
	signal_fault_add();
	return;
}

static void server_repaired (int sig) {
	printf("Repaired!!! %lu\n", G_count);
	repaired = true;
	signal_repaired_add();
	return;
}

static void server_reset (int sig) {
	static int number = 0;
	printf("Reset %d\n", ++number);
	return;
}

static void server_print (int sig) {
	struct master *mas = server.mas;
	static int number = 0;
	printf("Print %d\n", ++number);
	for (int i = 0; i < mas->num_hlr; i++) {
		printf("hlr[%d]\n", i);
		for (int j = 0; j < 10; j++) {
			mas->sw_total[j] += mas->hlr[i]->sw_total[j];
		}
		print_hlr_stat(mas->hlr[i]);
		//print_hlr_sw(mas->hlr[i]);
		fflush(stdout);
	}
	print_mas_sw(mas);
	fflush(stdout);
	uint64_t nr_query = 0;
#ifdef CDF
	for (int i = 0; i < mas->num_dev; i++) {
		for (int j = 0; j < CDF_TABLE_MAX; j++) {
			nr_query += mas->hlr[i]->cdf_table[j];
			mas->cdf_table[j] += mas->hlr[i]->cdf_table[j];
		}
	}	
	print_cdf(mas->cdf_table, nr_query);
	fflush(stdout);
#endif
#ifdef AMF_CDF
	uint64_t nr_query_cnt = 0, nr_query_bytes = 0;
	for (int i = 0; i < mas->num_dev; i++) {
		for (int j = 0; j < CDF_TABLE_MAX; j++) {
			mas->amf_cdf_table[j] += mas->hlr[i]->amf_cdf_table[j];
			nr_query_cnt += mas->hlr[i]->amf_cdf_table[j];
			mas->amf_bytes_cdf_table[j] += mas->hlr[i]->amf_bytes_cdf_table[j];
			nr_query_bytes += mas->hlr[i]->amf_bytes_cdf_table[j];
		}
	}	
	print_io_cdf(mas->amf_cdf_table, nr_query_cnt);
	print_io_bytes_cdf(mas->amf_bytes_cdf_table, nr_query_bytes);
	fflush(stdout);
#endif

	return;
}

static int signal_reset_print_add();

static void server_reset_print (int sig) {
	printf("GOT SIGCONT\n");
	server_print(sig);
	server_reset(sig);
	signal_reset_print_add();
}

static int signal_add() {
	struct sigaction sa;
	sigset_t newmask;
	sa.sa_handler = server_exit;
	sigemptyset(&sa.sa_mask);
	sigemptyset(&newmask);
	sigaddset(&newmask, SIGUSR1);
	sigaddset(&newmask, SIGUSR2);
	sigaddset(&newmask, SIGCONT);
	sigaction(SIGSEGV, &sa, NULL);
	sigaction(SIGINT, &sa, NULL);
	pthread_sigmask(SIG_BLOCK, &newmask, NULL);
	return 0;
}

static int signal_reset_add() {
	struct sigaction sa;
	sigset_t newmask;
	sa.sa_handler = server_reset;
	sigemptyset(&newmask);
	sigaddset(&newmask, SIGUSR1);
	sigaction(SIGUSR1, &sa, NULL);
	pthread_sigmask(SIG_UNBLOCK, &newmask, NULL);
	return 0;
}

static int signal_print_add() {
	struct sigaction sa;
	sigset_t newmask;
	sa.sa_handler = server_print;
	sigemptyset(&sa.sa_mask);
	sigaddset(&newmask, SIGUSR2);
	sigaction(SIGUSR2, &sa, NULL);
	pthread_sigmask(SIG_UNBLOCK, &newmask, NULL);
	return 0;
}

static int signal_fault_add() {
	struct sigaction sa;
	sigset_t newmask;
	sa.sa_handler = server_fault;
	sigemptyset(&newmask);
	sigaddset(&newmask, SIGUSR1);
	sigaction(SIGUSR1, &sa, NULL);
	pthread_sigmask(SIG_UNBLOCK, &newmask, NULL);
	return 0;
}

static int signal_repaired_add() {
	struct sigaction sa;
	sigset_t newmask;
	sa.sa_handler = server_repaired;
	sigemptyset(&sa.sa_mask);
	sigaddset(&newmask, SIGCONT);
	sigaction(SIGCONT, &sa, NULL);
	pthread_sigmask(SIG_UNBLOCK, &newmask, NULL);
	return 0;
}


static int signal_reset_print_add() {
	struct sigaction sa = {0};
	sigset_t newmask;
	sa.sa_handler = server_reset_print;
	sigemptyset(&sa.sa_mask);
	sigaddset(&newmask, SIGCONT);
	sigaction(SIGCONT, &sa, NULL);
	pthread_sigmask(SIG_UNBLOCK, &newmask, NULL);
	return 0;
}

static int server_init(struct server *srv) {
	srv->mas = master_init(srv->num_hlr, srv->num_dev, srv->device, srv->core_mask, 0, NULL);
	return 0;
}

static int 
server_getopt(int argc, char *argv[], struct server *const srv) {
	int c;
	bool dev_flag = false;

	static struct option long_options[] = {
		{"devices", required_argument, 0, 'd'},
		{"handler", required_argument, 0, 'h'},
		{"core_mask", required_argument, 0, 'c'},
		{"first", required_argument, 0, 'f'},
		{0, 0, 0, 0}
	};

	int option_index = 0;

	while ((c = getopt_long(argc, argv, "d:h:c:f:", long_options, &option_index)) != -1) {
		switch (c) {
		case 'd':
			dev_flag = true;
			srv->num_dev = atoi(optarg);
			for (int i = 0; i < srv->num_dev; i++) {
				srv->device[i] = (char *)calloc(32, sizeof(char));
				strncpy(srv->device[i], argv[optind+i],
					strlen(argv[optind+i]));
			}
			break;
		case 'h':
			srv->num_hlr = atoi(optarg);
			break;
		case 'f':
			first = atoi(optarg);
			break;
		case 'c':
			strncpy(srv->core_mask, optarg, strlen(optarg));
		case '?':
			// Error
			break;
		default:
			abort();
		}
	}

	if (!dev_flag) {
		fprintf(stderr, "(%s:%d) No devices specified!\n", 
			__FILE__, __LINE__);
		exit(1);
	}
	return 0;
}

int main(int argc, char *argv[]) {
	signal_add();

	server_getopt(argc, argv, &server);

	server_init(&server);

	//signal_reset_add();
	//signal_print_add();
	//signal_reset_print_add();
	
	signal_fault_add();
	signal_repaired_add();
	printf("add signal\n");

	pthread_join(server.mas->tid, NULL);

	return 0;
}
