#include "platform/util.h"
#include "platform/handler.h"
#include "platform/request.h"
#include "config.h"
#include "type.h"

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

extern int dev_cap;
extern int first;

struct tracer {
	int num_dev;
	char *device[32];
	char core_mask[128];
	int num_hlr;
	int num_traces;
	struct master *mas;
	struct trace traces[64];
} tracer;

void trace_exit(int sig) {
	struct master *mas = tracer.mas;
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

	exit(1);
}

static int signal_add() {
	struct sigaction sa;
	sa.sa_handler = trace_exit;
	sigemptyset(&sa.sa_mask);
	sigaction(SIGSEGV, &sa, NULL);
	sigaction(SIGINT, &sa, NULL);
	return 0;
}

static int trace_init(struct tracer *trc) {
	trc->mas = master_init(trc->num_hlr, trc->num_dev, trc->device, trc->core_mask, trc->num_traces, trc->traces);
	return 0;
}

static int 
trace_getopt(int argc, char *argv[], struct tracer *const trc) {
	int c;
	bool dev_flag = false;

	static struct option long_options[] = {
		{"devices", required_argument, 0, 'd'},
		{"handler", required_argument, 0, 'h'},
		{"core_mask", required_argument, 0, 'c'},
		{"traces", required_argument, 0, 't'},
		{"dev_cap", required_argument, 0, 'g'},
		{"first", required_argument, 0, 'f'},
		{0, 0, 0, 0}
	};

	int option_index = 0;

	first = -1;
	dev_cap = 0;

	while ((c = getopt_long(argc, argv, "d:h:c:t:g:f:", long_options, &option_index)) != -1) {
		switch (c) {
		case 'd':
			dev_flag = true;
			trc->num_dev = atoi(optarg);
			for (int i = 0; i < trc->num_dev; i++) {
				trc->device[i] = (char *)calloc(32, sizeof(char));
				strncpy(trc->device[i], argv[optind+i],
					strlen(argv[optind+i]));
			}
			break;
		case 'h':
			trc->num_hlr = atoi(optarg);
			break;
		case 'g':
			dev_cap = atoi(optarg);
			break;
		case 'f':
			first = atoi(optarg);
			break;
		case 'c':
			strncpy(trc->core_mask, optarg, strlen(optarg));
			break;
		case 't':
			trc->num_traces = atoi(optarg);
			//trc->traces = (struct trace *)calloc(trc->num_traces, sizeof(struct trace));
			for (int i = 0; i < trc->num_traces; i++) {
				//strncpy(trc->trace[i].cluster_num, argv[optind+(i*2)], argv[optind+(i*2)]);
				trc->traces[i].cluster_num = atoi(argv[optind+(i*2)]);
				trc->traces[i].num_files = atoi(argv[optind+(i*2+1)]);
			}
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

	trace_getopt(argc, argv, &tracer);

	trace_init(&tracer);

	pthread_join(tracer.mas->tid, NULL);


	return 0;
}
