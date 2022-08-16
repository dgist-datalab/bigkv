#include "platform/handler.h"
#include "platform/kv_ops.h"
#include "platform/aio.h"
#include "platform/device.h"
#include "utility/ttl.h"
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <numa.h>

#ifdef HOPSCOTCH
#include "index/hopscotch.h"
#elif BIGKV
#include "index/bigkv_index.h"
#elif CASCADE
#include "index/cascade.h"
#endif

int global_hlr_number = 0;
bool stopflag_hlr;

extern int first;

struct handler *handler_init(struct master *mas, struct handler *hlr, char **dev_name, int num_dev, int num_dev_per_hlr, int num_hlr) {
	//struct handler *hlr = (struct handler *)calloc(1, sizeof(struct handler));
	memset(hlr, 0, sizeof(struct handler));

	hlr->num_hlr = num_hlr;
	hlr->number = global_hlr_number++;
	hlr->num_dev = num_dev;
	hlr->num_dev_per_hlr = num_dev_per_hlr;
	hlr->barrier = &mas->barrier;

#ifdef DEV_SPDK
	hlr->sctx = &mas->sctx[hlr->number];
#endif

	if (hlr->number < 8) {
		numa_set_preferred(0);
	} else {
		numa_set_preferred(1);
	}

	hlr->ops = (struct kv_ops **)malloc(sizeof(struct kv_ops *) * num_dev_per_hlr);
	for (int i = 0; i < num_dev_per_hlr; i++) {
		hlr->ops[i] = (struct kv_ops *)calloc(1, sizeof(struct kv_ops));
		hlr->ops[i]->hlr = (void *)hlr;
		hlr->ops[i]->ops_number = i;
#ifdef HOPSCOTCH
		hlr->ops[i]->init = hopscotch_init;
		hlr->ops[i]->free = hopscotch_free;
		hlr->ops[i]->get_kv = hopscotch_get;
		hlr->ops[i]->set_kv = hopscotch_set;
		hlr->ops[i]->delete_kv = hopscotch_delete;
		hlr->ops[i]->need_gc = hopscotch_need_gc;
		hlr->ops[i]->trigger_gc = hopscotch_trigger_gc;
		hlr->ops[i]->wait_gc = hopscotch_wait_gc;
#elif BIGKV
		hlr->ops[i]->init = bigkv_index_init;
		hlr->ops[i]->free = bigkv_index_free;
		hlr->ops[i]->get_kv = bigkv_index_get;
		hlr->ops[i]->set_kv = bigkv_index_set;
		hlr->ops[i]->delete_kv = bigkv_index_delete;
		hlr->ops[i]->need_gc = bigkv_index_need_gc;
		hlr->ops[i]->trigger_gc = bigkv_index_trigger_gc;
		hlr->ops[i]->wait_gc = bigkv_index_wait_gc;
#elif CASCADE
		hlr->ops[i]->init = cascade_init;
		hlr->ops[i]->free = cascade_free;
		hlr->ops[i]->get_kv = cascade_get;
		hlr->ops[i]->set_kv = cascade_set;
		hlr->ops[i]->delete_kv = cascade_delete;
		hlr->ops[i]->need_gc = cascade_need_gc;
		hlr->ops[i]->trigger_gc = cascade_trigger_gc;
		hlr->ops[i]->wait_gc = cascade_wait_gc;
#endif

	}

	hlr->dev_name = (char **)malloc(sizeof(char *) * num_dev_per_hlr);	
	for (int i = 0; i < num_dev_per_hlr; i++) {
		hlr->dev_name[i] = (char *)malloc(sizeof(char) * 128);
		strcpy(hlr->dev_name[i], (const char *)dev_name[i]);
	}

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);

#ifdef CPU_PIN
	CPU_SET(2+hlr->number*2, &cpuset);
	hlr->core_mask = 2 + (hlr->number)*2;
#elif CPU_PIN_NUMA
	if (hlr->number < 8) {
		hlr->core_mask = 2 + (hlr->number)*2;
		CPU_SET(first + 2+(hlr->number)*2, &cpuset);
	} else {
		hlr->core_mask = 6 + (hlr->number)*2;
		CPU_SET(6+(hlr->number)*2, &cpuset);
	}
#endif

#ifdef DEV_SPDK
	spdk_env_thread_launch_pinned(hlr->core_mask, request_handler, hlr);
#else
	pthread_create(&hlr->hlr_tid, NULL, &request_handler, hlr);
	pthread_setaffinity_np(hlr->hlr_tid, sizeof(cpu_set_t), &cpuset);
#endif


	return hlr;
}

void handler_free(struct handler *hlr) {
	int *temp;
	stopflag_hlr=true;
	while (pthread_tryjoin_np(hlr->hlr_tid, (void **)&temp)) {
		//cl_release(hlr->cond);
	}
	while (pthread_tryjoin_np(hlr->plr_tid, (void **)&temp)) {
		//cl_release(hlr->cond);
	}

#ifdef PER_CORE
	for (int i = 0; i < hlr->num_dev_per_hlr; i++) {
		print_kv_ops_stat(&hlr->ops[i]->stat);
		hlr->ops[i]->free(hlr->ops[i]);
		free(hlr->ops[i]);
	}
	free(hlr->ops);
#endif

	cl_free(hlr->flying);

	q_free(hlr->req_q);
	//lfq_free(hlr->req_q);
	q_free(hlr->retry_q);
	q_free(hlr->done_q);
	q_free(hlr->submit_q);

	q_free(hlr->req_pool);
	free(hlr->req_arr);
	void *value_ptr;
	while ((value_ptr = q_dequeue(hlr->value_pool)) != NULL) {
#ifdef USE_HUGEPAGE
		munmap(value_ptr, VALUE_LEN_MAX);
#elif DEV_SPDK
		spdk_dma_free(value_ptr);
#else
		free(value_ptr);
#endif
	}
	q_free(hlr->value_pool);
	q_free(hlr->iocb_pool);
	free(hlr->iocb_arr);
	q_free(hlr->io_data_pool);
	free(hlr->io_data_arr);

	q_free(hlr->cb_pool);
	free(hlr->cb_arr);

	for (int i = 0; i < hlr->num_dev_per_hlr; i++) {
		dev_abs_free(hlr->dev[i]);
		free(hlr->dev[i]);
	}
	free(hlr->dev);

#ifdef CDF
	free(hlr->cdf_table);
#endif
#ifdef AMF_CDF
	free(hlr->amf_cdf_table);
	free(hlr->amf_bytes_cdf_table);
#endif

	//free(hlr);
}

int forward_req_to_hlr(struct handler *hlr, struct request *req) {
	int rc = 0;
	static int cnt = 0;
	cl_grap(hlr->flying);
	req->hlr = hlr;
	if (!q_enqueue((void *)req, hlr->req_q)) {
		rc = -1;
	}
	/*
	if (hlr->req_q->size > QSIZE/2) {
		printf("put request[%p] %d %d %d\n", hlr, ++cnt, hlr->req_q->size, hlr->req_q->m_size);
		usleep(10);
	}
	*/
	return rc;
}

int move_req_to_other(struct handler *hlr, struct request *req) {
	int rc = 0;
	static int cnt = 0;
	struct request *new_req;
	if (hlr == NULL) {
		printf("ASDASD\n");
		abort();
	}
	cl_grap(hlr->flying);
	while ((new_req = (struct request *)q_dequeue(hlr->req_pool)) == NULL);
	memset(new_req, 0, sizeof(*new_req));
	new_req->type = REQ_TYPE_SET;
	new_req->seq_num = 0;
	new_req->key.len = req->key.len;
	memcpy(new_req->key.key, req->key.key, req->key.len);
	new_req->key.hash_high = req->key.hash_high;
	new_req->key.hash_low = req->key.hash_low;
	new_req->value.len = VALUE_LEN;
	new_req->value.value = NULL;
	new_req->hlr = hlr;
	new_req->cl_sock = req->cl_sock;
	new_req->end_req = req->end_req;
	new_req->params = NULL;
	new_req->temp_buf = NULL;
	new_req->fault = req->fault;
	new_req->repaired = 0;
	new_req->moved = 1;

	if (!q_enqueue((void *)new_req, hlr->req_q)) {
		rc = -1;
	}
	/*
	if (hlr->req_q->size > QSIZE/2) {
		printf("put request[%p] %d %d %d\n", hlr, ++cnt, hlr->req_q->size, hlr->req_q->m_size);
		usleep(10);
	}
	*/
	return rc;
}


int retry_req_to_hlr(struct handler *hlr, struct request *req) {
	int rc = 0;
	if (!q_enqueue((void *)req, hlr->retry_q)) {
		rc = 1;
	}
	return rc;
}

struct request *get_next_request(struct handler *hlr) {
	struct request *req = NULL;
	if ((req = (struct request *)q_dequeue(hlr->retry_q))) goto exit;
	else if ((req = (struct request *)q_dequeue(hlr->req_q))) {
		if (req->value.value == NULL) {
			add_request_info(req);
		}
		sw_start(&req->sw);
		goto exit;
	}
exit:
	return req;
}

struct request *get_next_retry_request(struct handler *hlr) {
	struct request *req = NULL;
	req = (struct request *)q_dequeue(hlr->retry_q);
	return req;
}


#ifdef DEV_SPDK
static int request_handler(void *input) {
#else
static void *request_handler(void *input) {
#endif
	int rc = 0;

	struct request *req = NULL;
	struct handler *hlr = (struct handler *)input;
	struct kv_ops *ops = NULL;

	struct callback *cb = NULL;
	struct gc *gc;

	char thread_name[128] = {0};
	sprintf(thread_name, "%s[%d]", "request_handler", hlr->number);
	pthread_setname_np(pthread_self(), thread_name);


	printf("numa_preferred: %d\n", numa_preferred());
	numa_set_localalloc();
	numa_set_strict(numa_preferred());


	hlr->flying = cl_init(QSIZE, false);

	q_init(&hlr->req_q, QSIZE);
	//lfq_init(&hlr->req_q, QSIZE);
	q_init(&hlr->retry_q, QSIZE);
	q_init(&hlr->done_q, QSIZE);
	q_init(&hlr->submit_q, QSIZE * 2);
	q_init(&hlr->req_pool, QSIZE);
	q_init(&hlr->value_pool, QSIZE);
	q_init(&hlr->iocb_pool, QSIZE * 2);
	q_init(&hlr->cb_pool, QSIZE * 2);
	q_init(&hlr->io_data_pool, QSIZE * 2);

	hlr->dev = (struct dev_abs **)malloc(sizeof(struct dev_abs *) * hlr->num_dev_per_hlr);
	for (int i = 0; i < hlr->num_dev_per_hlr; i++) {
		hlr->dev[i] = dev_abs_init(hlr, hlr->dev_name[i], hlr->core_mask, i);
	}

	hlr->req_arr = (struct request *)calloc(QSIZE, sizeof(struct request));
	for (int i = 0; i < QSIZE; i++) {
		q_enqueue((void *)&hlr->req_arr[i], hlr->req_pool);
		//lfq_enqueue((void *)&hlr->req_arr[i], hlr->req_pool);
	}


	hlr->iocb_arr = (struct iocb *)calloc(QSIZE * 2, sizeof(struct iocb));
	for (int i = 0; i < QSIZE * 2; i++) {
		q_enqueue((void *)&hlr->iocb_arr[i], hlr->iocb_pool);
	}

	hlr->io_data_arr = (struct io_data *)calloc(QSIZE * 2, sizeof(struct io_data));
	for (int i = 0; i < QSIZE * 2; i++) {
		q_enqueue((void *)&hlr->io_data_arr[i], hlr->io_data_pool);
	}


	hlr->cb_arr = (struct callback *)calloc(QSIZE * 2, sizeof(struct callback));
	for (int i = 0; i < QSIZE * 2; i++) {
		q_enqueue((void *)&hlr->cb_arr[i], hlr->cb_pool);
	}

	hlr->gc = NULL;

	hlr->read = dev_abs_read;
	hlr->poller_read = dev_abs_poller_read;
	hlr->sync_read = dev_abs_sync_read;
	hlr->write = dev_abs_write;
	hlr->idx_write = dev_abs_idx_write;
	hlr->idx_overwrite = dev_abs_idx_overwrite;
	hlr->sync_write = dev_abs_sync_write;

#ifdef CDF
	hlr->cdf_table = (uint64_t *)malloc(CDF_TABLE_MAX * sizeof(uint64_t));
	memset(hlr->cdf_table, 0, CDF_TABLE_MAX * sizeof (uint64_t));
#endif
#ifdef AMF_CDF
	hlr->amf_cdf_table = (uint64_t *)malloc(CDF_TABLE_MAX * sizeof(uint64_t));
	memset(hlr->amf_cdf_table, 0, CDF_TABLE_MAX * sizeof (uint64_t));
	hlr->amf_bytes_cdf_table = (uint64_t *)malloc(CDF_TABLE_MAX * sizeof(uint64_t));
	memset(hlr->amf_bytes_cdf_table, 0, CDF_TABLE_MAX * sizeof (uint64_t));
#endif
	hlr->nr_query = 0;

	memset(&hlr->stat, 0, sizeof(struct stats));

#ifndef PER_CORE
	if (hlr->number == 0) {	
		hlr->ops[0]->init(hlr->ops);
		for (int i = 1; i < hlr->num_dev_per_hlr; i++) {
			hlr->ops[i] = hlr->ops[0];
		}
	} else {
		for (int i = 1; i < hlr->num_dev_per_hlr; i++) {
			hlr->ops[i] = (hlr - hlr->number)->ops[0];
		}
	}
	//printf("number: %d, hlr: %p, hlr->ops: %p cal_hlr: %p dev: %p\n", hlr->number,hlr, hlr->ops, hlr-hlr->number, hlr->dev);
#else
	for (int i = 0; i < hlr->num_dev_per_hlr; i++) {
		hlr->ops[i]->init(hlr->ops[i]);
	}
#endif

#ifndef HLR_POLLING
	cpu_set_t cpuset;
	pthread_create(&hlr->plr_tid, NULL, &device_poller, hlr);
	CPU_ZERO(&cpuset);
#ifdef CPU_PIN
	CPU_SET(2+hlr->number*2+1, &cpuset);
	pthread_setaffinity_np(hlr->plr_tid, sizeof(cpu_set_t), &cpuset);
#elif CPU_PIN_NUMA
	if (hlr->number < 8) {
		CPU_SET(first+2+(hlr->number)*2+1, &cpuset);
	} else {
		CPU_SET(6+(hlr->number)*2+1, &cpuset);
	}
	pthread_setaffinity_np(hlr->plr_tid, sizeof(cpu_set_t), &cpuset);
#endif
#endif

	char *value_ptr;
	for (int i = 0; i < QSIZE; i++) {
#ifdef USE_HUGEPAGE
		value_ptr = (char*)mmap(NULL, VALUE_LEN_MAX, PROT_READ | PROT_WRITE,
						MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, 0, 0);
#elif DEV_SPDK
		value_ptr = (char *)spdk_dma_zmalloc(8192, 4096, NULL);
#else
		value_ptr = (char *)aligned_alloc(VALUE_ALIGN_UNIT, VALUE_LEN_MAX);
#endif
		memset(value_ptr, 0, 8192);
		q_enqueue((void*)value_ptr, hlr->value_pool);
	}

	printf("HANDLER[%d] INIT FINISHIED\n", hlr->number);
	fflush(stdout);

	pthread_barrier_wait(hlr->barrier);

	stopwatch *sw = sw_create();
	sw_start(sw);

	uint64_t ops_idx;
	while (1) {
		if (stopflag_hlr && (hlr->flying->now==0)) { 
			return NULL;
		}
		sw_end(sw);
		if (sw_get_sec(sw) >= 1) {
#ifdef PRINT_QD
			printf("[hlr %d] QD: %d\n", hlr->number, hlr->flying->now);
#endif
			memset(sw, 0, sizeof(*sw));
			sw_start(sw);

		}


#ifdef HLR_POLLING
completion_req:
		device_polling((void *)hlr);
#endif

		if (hlr->flying->now == QSIZE) {
			//printf("FLYING FULL\n");
			//continue;
		}

		if (!(req=get_next_request(hlr))) {
			//usleep(1000);
			continue;
		}


		ops_idx = (req->key.hash_high >> 32) % hlr->num_dev_per_hlr;
		ops = hlr->ops[ops_idx];


		if ((hlr->gc && hlr->gc->state == GC_FLYING) || ops->need_gc(ops, hlr)) {
			static uint64_t gc_cnt = 0;
			if ((++gc_cnt % 1000) == 0) {
				printf("gc_cnt: %lu\n", gc_cnt);
				dev_print_gc_info(hlr, hlr->dev[ops->ops_number]);
			}
			if (!hlr->gc)
				hlr->gc = init_gc();
			gc = hlr->gc;
			rc = ops->trigger_gc(ops, hlr);
			if (gc->state == GC_DONE) {
				free_gc(gc);
				hlr->gc = NULL;
				//continue;
			} else {
				retry_req_to_hlr(hlr, req);
				if ((req = get_next_retry_request(hlr)))
					goto process_req;
				else
					abort();
				ops->wait_gc(ops, hlr);
				free_gc(gc);
			}
			
		}


process_req:
		while ((cb = (struct callback *)q_dequeue(hlr->done_q))) {
			printf("cb call??\n");
			cb->func(cb->arg);
			q_enqueue((void *)cb, hlr->cb_pool);
		}

		switch (req->type) {
		case REQ_TYPE_SET:
			set_cur_sec(req->req_time);
			rc = ops->set_kv(ops, req);
			break;	
		case REQ_TYPE_GET:
			set_value(&req->value, VALUE_LEN_MAX, NULL, 1, hlr);
			rc = ops->get_kv(ops, req);
			if (rc) {
				//puts("Not existing key!");
				//printf("%lu\n", req->key.hash_low);
				//printf("%s\n", req->key.key);
#ifdef CASCADE
				//printf("%d\n", ((struct cas_params *)req->params)->read_step);
#endif
				//static int not_exist = 0;
				//if ((++not_exist % 10) == 0)
				//	printf("not: %d\n", not_exist);
				req->end_req(req);
			}
			break;
		case REQ_TYPE_DELETE:
		case REQ_TYPE_RANGE:
		case REQ_TYPE_ITERATOR:
		default:
			fprintf(stderr, "Wrong req type!\n");
			return NULL;
		}

	}
	return NULL;
}
