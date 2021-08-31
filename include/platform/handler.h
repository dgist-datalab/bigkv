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
#include "utility/cond_lock.h"
#include "platform/device.h"

#include <pthread.h>
#include <libaio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#define GC_DONE 0
#define GC_FYLING 1

struct gc {
	uint32_t valid_cnt; 
	void *buf;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	bool is_idx;
	void *_private;
	uint8_t state;
};

struct callback {
	void *(*func)(void *);
	void *arg;
#ifdef LINUX_AIO
	struct iocb *iocb;
#endif
};

struct stats {
	uint64_t nr_read;
	uint64_t nr_read_cache_miss;
	uint64_t nr_read_miss;
	uint64_t nr_read_key_mismatch;
	uint64_t nr_read_find_miss;
	uint64_t nr_write;
	uint64_t nr_write_cache_miss;
	uint64_t nr_write_key_mismatch;
	uint64_t nr_write_find_miss;
};

struct handler {
	int number;
	pthread_t hlr_tid, plr_tid;

	struct kv_ops *ops;
	struct dev_abs *dev;

	cl_lock *flying;

	queue *req_q;
	queue *retry_q;
	queue *done_q;

	queue *req_pool;
	struct request *req_arr;
	queue *iocb_pool;
	struct iocb *iocb_arr;
	queue *cb_pool;
	struct callback *cb_arr;

	struct gc *gc;

#ifdef CDF
	uint64_t *cdf_table;
	uint64_t nr_query;
#endif
	struct stats stat;

#ifdef LINUX_AIO
	io_context_t aio_ctx;
#elif SPDK
	// TODO: include SPDK variables
#endif

	int (*read)(struct handler *, uint64_t, uint32_t, char *,
		    struct callback *);
	int (*write)(struct handler *, uint64_t, uint64_t, uint32_t, char *,
		     struct callback *);
	int (*idx_write)(struct handler *, uint64_t, uint64_t, uint32_t, char *,
		     struct callback *, uint64_t);
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
	gc->is_idx = false;
	gc->_private = NULL;

	return gc;
}

static inline void
free_gc(struct gc *gc) {
#ifdef USE_HUGEPAGE
	unmap(gc->buf, SEGMENT_SIZE);
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
#ifdef LINUX_AIO
	cb->iocb = NULL;
#endif
	return cb;
}

/* handler */
struct handler *handler_init(char dev_name[]);
void handler_free(struct handler *hlr);

int forward_req_to_hlr(struct handler *hlr, struct request *req);
int retry_req_to_hlr(struct handler *hlr, struct request *req);

struct request *get_next_request(struct handler *hlr);

void *request_handler(void *input);

static void print_hlr_stat(struct handler *hlr) {
	printf("\
		\rnr_read: %lu\n \
		\rnr_read_cache_miss: %lu\n \
		\rnr_read_miss: %lu\n \
		\rnr_read_key_mismatch: %lu\n \
		\rnr_read_find_miss: %lu\n \
		\rnr_write: %lu\n \
		\rnr_write_cache_miss: %lu\n \
		\rnr_write_key_mismatch: %lu\n \
		\rnr_write_find_miss: %lu\n",
		hlr->stat.nr_read, hlr->stat.nr_read_cache_miss, hlr->stat.nr_read_miss, hlr->stat.nr_read_key_mismatch, hlr->stat.nr_read_find_miss, hlr->stat.nr_write, hlr->stat.nr_write_cache_miss, hlr->stat.nr_write_key_mismatch, hlr->stat.nr_write_find_miss);
}


/* poller */
void *device_poller(void *input);

#endif
