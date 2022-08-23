/*
 * Master Thread Header
 *
 * Description: A master thread works for request delivery. A master thread
 * should be located to a NIC. Its main role is to accept network requests and
 * convert it to internal key-value requests.
 *
 * Current network back-end is implemented as epoll(), but this would be
 * overheads due to the OS network stack. Since almost high-performance NICs
 * support DPDK, it could be improved by changing it to DPDK.
 *
 */

#ifndef __BIGKV_MASTER_H__
#define __BIGKV_MASTER_H__

#include "type.h"
#include "config.h"
#include "platform/handler.h"
#include "platform/client.h"
#ifdef DEV_SPDK
#include "platform/dev_spdk.h"
#endif
#include "platform/keygen.h"

#include <time.h>
#include <pthread.h>

#define MAX_NETREQ_BUF 1024
#define SOCK_BUF_SIZE (1024 * 1024 * 2L)
#define MASTER_SOCK_MOVE (1024 * 16L)
#define CLIENT_MAX_NUM 128

struct master {
	pthread_t tid;

	int num_hlr;
	int num_dev;

	int num_traces;
	struct trace *traces;
	int current_trace_num;
	int current_trace_idx;
	char *trace_buf;
	FILE *trace_fd;

	pthread_barrier_t barrier;

	char **device;
	char core_mask[128];

	struct handler *hlr[32];
	struct spdk_ctx *sctx;

	struct keygen *kg;

	int fd;
	int epfd;
	int log_fd;

	struct stats stat;

	struct netreq netreq_buf[MAX_NETREQ_BUF];
	struct client *clients[2048];
	/*
	char *buf;
	ssize_t buf_start;
	ssize_t buf_end;
	ssize_t buf_len;
	ssize_t buf_size;

	char *tmp_err;
	const char **args;
	int* args_size;
	int args_len;
	int args_cap;
	*/

	int processed_req;
#ifdef CDF
	uint64_t *cdf_table;
#endif
#ifdef AMF_CDF
	uint64_t *amf_cdf_table;
	uint64_t *amf_bytes_cdf_table;
#endif
	uint64_t sw_total[10];

	int (*parse_and_make_request)(struct master *mas, int fd);
};


struct master *master_init(int num_hlr, int num_dev, char **device, char *core_mask, int num_traces, struct trace *traces);
void master_free(struct master *mas);
void *master_thread(void *input);
int master_parse_and_make_request(struct master *mas, int fd);
void master_reset(struct master *mas);

static void print_mas_sw(struct master *mas) {
	printf("\
		\rsw_total: %lu %.2f\n \
		\rsw_queue: %lu %.2f\n \
		\rsw_table: %lu %.2f\n \
		\rsw_submit: %lu %.2f\n \
		\rsw_device: %lu %.2f\n \
		\rsw_end_req: %lu %.2f\n \
		\rsw_forward: %lu %.2f\n",
		mas->sw_total[0], mas->sw_total[0]/(double)mas->sw_total[0], mas->sw_total[1], mas->sw_total[1]/(double)mas->sw_total[0], mas->sw_total[2], mas->sw_total[2]/(double)mas->sw_total[0], mas->sw_total[3], mas->sw_total[3]/(double)mas->sw_total[0], mas->sw_total[4], mas->sw_total[4]/(double)mas->sw_total[0], mas->sw_total[5], mas->sw_total[5]/(double)mas->sw_total[0], mas->sw_total[6], mas->sw_total[6]/(double)mas->sw_total[0]);
}

static void print_mas_stat(struct master *hlr) {
	printf("\
		\rnr_read: %lu\n \
		\rnr_read_cache_miss: %lu\n \
		\rnr_read_miss: %lu\n \
		\rnr_read_key_mismatch: %lu\n \
		\rnr_read_find_miss: %lu\n \
		\rnr_read_expired: %lu\n \
		\rnr_read_evicted: %lu\n \
		\rnr_write: %lu\n \
		\rnr_write_cache_miss: %lu\n \
		\rnr_write_key_mismatch: %lu\n \
		\rnr_write_find_miss: %lu\n \
		\rnr_write_expired: %lu\n \
		\rnr_write_evicted: %lu\n",
		hlr->stat.nr_read, hlr->stat.nr_read_cache_miss, hlr->stat.nr_read_miss, hlr->stat.nr_read_key_mismatch, hlr->stat.nr_read_find_miss, hlr->stat.nr_read_expired, hlr->stat.nr_read_evicted, hlr->stat.nr_write, hlr->stat.nr_write_cache_miss, hlr->stat.nr_write_key_mismatch, hlr->stat.nr_write_find_miss, hlr->stat.nr_write_expired, hlr->stat.nr_write_evicted);
}


#endif
