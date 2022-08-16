/*
 * Request Handler Header.
 *
 * Description: A request handler runs for a dedicated device at a dedicated
 * core. Each handler has its own poller, and the poller will call registered
 * callback function.
 *
 * Handlers do reads/writes to/from device abstraction layer (dev_abs).
 * Further, each handler must maintain its index structure, and this should be
 * mapped at init time (kv_ops).
 *
 */

#ifndef __HANDLER_H__
#define __HANDLER_H__

#include "config.h" 
#include "type.h"
#include "utility/queue.h"
#include "utility/lfqueue.h"
#include "utility/cond_lock.h"
#include "platform/device.h"
#include "platform/uring.h"

#include <pthread.h>
#include <libaio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/mman.h>

#define GC_DONE 0
#define GC_FLYING 1

struct gc {
	uint32_t valid_cnt; 
	uint16_t current_idx;
	void *buf;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	bool is_idx;
	void *_private;
	uint8_t state;
};

struct handler;

struct callback {
	void *(*func)(void *);
	void *arg;
	struct iocb *iocb;
	struct handler *hlr;
};

struct stats {
	uint64_t nr_read;
	uint64_t nr_read_cache_miss;
	uint64_t nr_read_miss;
	uint64_t nr_read_key_mismatch;
	uint64_t nr_read_find_miss;
	uint64_t nr_read_expired;
	uint64_t nr_read_evicted;
	uint64_t nr_write;
	uint64_t nr_write_cache_miss;
	uint64_t nr_write_key_mismatch;
	uint64_t nr_write_find_miss;
	uint64_t nr_write_expired;
	uint64_t nr_write_evicted;
};

struct handler {
	int number;
	int num_hlr;
	pthread_t hlr_tid, plr_tid;

	struct kv_ops **ops;
	struct dev_abs **dev;

#ifdef DEV_SPDK
	struct spdk_ctx *sctx;
#endif

	cl_lock *flying;

	char **dev_name;

	queue *req_q;
	//lfqueue *req_q;
	queue *retry_q;
	queue *done_q;
	queue *submit_q;

	queue *req_pool;
	queue *value_pool;
	//lfqueue *req_pool;
	struct request *req_arr;
	queue *iocb_pool;
	struct iocb *iocb_arr;
	queue *cb_pool;
	struct callback *cb_arr;
	queue *io_data_pool;
	struct io_data *io_data_arr;

	pthread_barrier_t *barrier;

	struct gc *gc;

	int num_dev;
	int num_dev_per_hlr;
	unsigned core_mask;

	uint64_t sw_total[10];

#ifdef CDF
	uint64_t *cdf_table;
#endif
#ifdef AMF_CDF
	uint64_t *amf_cdf_table;
	uint64_t *amf_bytes_cdf_table;
#endif
	uint64_t nr_query;
	struct stats stat;

	int (*read)(struct handler *, uint64_t, uint32_t, char *,
		    struct callback *, int, int);
	int (*poller_read)(struct handler *, uint64_t, uint32_t, char *,
		    struct callback *, int);
	int (*sync_read)(struct handler *, uint64_t, uint32_t, char *, int);
#ifdef TTL_GROUP
	int (*write)(struct handler *, uint64_t, uint64_t, uint32_t, char *,
		     struct callback *, int, int, uint32_t);
#else
	int (*write)(struct handler *, uint64_t, uint64_t, uint32_t, char *,
		     struct callback *, int, int);
#endif
	int (*idx_write)(struct handler *, uint64_t, uint64_t, uint32_t, char *,
		     struct callback *, uint64_t, int);
	int (*idx_overwrite)(struct handler *, uint64_t, uint32_t, char *,
		     struct callback *, int);
	int (*sync_write)(struct handler *, uint64_t, uint32_t, char *, int);

};



static inline struct gc *
init_gc(void) {
	struct gc *gc = (struct gc *)malloc(sizeof(struct gc));
#ifdef USE_HUGEPAGE
	gc->buf = mmap(NULL, SEGMENT_SIZE, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, 0, 0);
#else
	gc->buf = aligned_alloc(MEM_ALIGN_UNIT, SEGMENT_SIZE);
#endif
	if (!gc->buf) {
		abort();
	}
	memset(gc->buf, 0, SEGMENT_SIZE);

	pthread_mutex_init(&gc->mutex, NULL);
	pthread_cond_init(&gc->cond, NULL);
	gc->valid_cnt = 0;
	gc->current_idx = 0;
	gc->is_idx = false;
	gc->_private = NULL;

	return gc;
}

static inline void
free_gc(struct gc *gc) {
#ifdef USE_HUGEPAGE
	munmap(gc->buf, SEGMENT_SIZE);
#else
	free(gc->buf);
#endif
	free(gc);
	return;
}


static inline struct callback *
make_callback(struct handler *hlr, void *(*func)(void*), void *arg) {
	//struct callback *cb = (struct callback *)malloc(sizeof(struct callback));
	struct callback *cb = (struct callback *)q_dequeue(hlr->cb_pool);
	cb->func = func;
	cb->arg  = arg;
	cb->iocb = NULL;
	cb->hlr = hlr;
	return cb;
}

/* handler */
struct handler *handler_init(struct master *mas, struct handler *hlr, char **dev_name, int num_dev, int num_dev_per_hlr, int num_hlr);
void handler_free(struct handler *hlr);

int forward_req_to_hlr(struct handler *hlr, struct request *req);
int retry_req_to_hlr(struct handler *hlr, struct request *req);
int move_req_to_other(struct handler *hlr, struct request* org_req);

struct request *get_next_request(struct handler *hlr);

#ifdef DEV_SPDK
static int request_handler(void *input);
#else
static void *request_handler(void *input);
#endif


static void print_hlr_stat(struct handler *hlr) {
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

static void print_hlr_sw(struct handler *hlr) {
	printf("\
		\rsw_total: %lu %.2f\n \
		\rsw_queue: %lu %.2f\n \
		\rsw_table: %lu %.2f\n \
		\rsw_device: %lu %.2f\n \
		\rsw_retry: %lu %.2f\n \
		\rsw_end: %lu %.2f\n",
		hlr->sw_total[0], hlr->sw_total[0]/(double)hlr->sw_total[0], hlr->sw_total[1], hlr->sw_total[1]/(double)hlr->sw_total[0], hlr->sw_total[2], hlr->sw_total[2]/(double)hlr->sw_total[0], hlr->sw_total[3], hlr->sw_total[3]/(double)hlr->sw_total[0], hlr->sw_total[4], hlr->sw_total[4]/(double)hlr->sw_total[0], hlr->sw_total[5], hlr->sw_total[5]/(double)hlr->sw_total[0]);
}

/* poller */
void *device_poller(void *input);
void *device_polling(void *input);

#endif
