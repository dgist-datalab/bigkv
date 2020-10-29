#include "platform/handler.h"
#include "platform/kv_ops.h"
#include "platform/aio.h"
#include "platform/device.h"
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#ifdef HOPSCOTCH
#include "index/hopscotch.h"
#elif BIGKV
#include "index/bigkv_index.h"
#endif

int global_hlr_number;
bool stopflag_hlr;

struct handler *handler_init(char dev_name[]) {
	struct handler *hlr = (struct handler *)calloc(1, sizeof(struct handler));

	hlr->number = global_hlr_number++;

	hlr->ops = (struct kv_ops *)calloc(1, sizeof(struct kv_ops));
#ifdef HOPSCOTCH
	hlr->ops->init = hopscotch_init;
	hlr->ops->free = hopscotch_free;
	hlr->ops->get_kv = hopscotch_get;
	hlr->ops->set_kv = hopscotch_set;
	hlr->ops->delete_kv = hopscotch_delete;
#elif BIGKV
	hlr->ops->init = bigkv_index_init;
	hlr->ops->free = bigkv_index_free;
	hlr->ops->get_kv = bigkv_index_get;
	hlr->ops->set_kv = bigkv_index_set;
	hlr->ops->delete_kv = bigkv_index_delete;
#endif
	hlr->ops->init(hlr->ops);

	hlr->flying = cl_init(QDEPTH, false);

	q_init(&hlr->req_q, QSIZE);
	q_init(&hlr->retry_q, QSIZE);
	q_init(&hlr->done_q, QSIZE);
	q_init(&hlr->req_pool, QSIZE);
	q_init(&hlr->iocb_pool, QSIZE);
	q_init(&hlr->cb_pool, QSIZE);

	hlr->dev = dev_abs_init(dev_name);

	hlr->req_arr = (struct request *)calloc(QSIZE, sizeof(struct request));
	for (int i = 0; i < QSIZE; i++) {
		q_enqueue((void *)&hlr->req_arr[i], hlr->req_pool);
	}

	hlr->iocb_arr = (struct iocb *)calloc(QSIZE, sizeof(struct iocb));
	for (int i = 0; i < QSIZE; i++) {
		q_enqueue((void *)&hlr->iocb_arr[i], hlr->iocb_pool);
	}

	hlr->cb_arr = (struct callback *)calloc(QSIZE, sizeof(struct callback));
	for (int i = 0; i < QSIZE; i++) {
		q_enqueue((void *)&hlr->cb_arr[i], hlr->cb_pool);
	}

	hlr->read = dev_abs_read;
	hlr->write = dev_abs_write;

#ifdef LINUX_AIO
	memset(&hlr->aio_ctx, 0, sizeof(io_context_t));
	if (io_setup(QDEPTH*2, &hlr->aio_ctx) < 0) {
		perror("io_setup");
		abort();
	}
#elif SPDK
	// TODO
#endif

	pthread_create(&hlr->hlr_tid, NULL, &request_handler, hlr);
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(2+hlr->number*2, &cpuset);
	pthread_setaffinity_np(hlr->hlr_tid, sizeof(cpu_set_t), &cpuset);

	pthread_create(&hlr->plr_tid, NULL, &device_poller, hlr);
	CPU_ZERO(&cpuset);
	CPU_SET(2+hlr->number*2+1, &cpuset);
	pthread_setaffinity_np(hlr->plr_tid, sizeof(cpu_set_t), &cpuset);

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

	print_kv_ops_stat(&hlr->ops->stat);
	hlr->ops->free(hlr->ops);
	free(hlr->ops);

	cl_free(hlr->flying);

	q_free(hlr->req_q);
	q_free(hlr->retry_q);
	q_free(hlr->done_q);

	q_free(hlr->req_pool);
	free(hlr->req_arr);
	q_free(hlr->iocb_pool);
	free(hlr->iocb_arr);

	q_free(hlr->cb_pool);
	free(hlr->cb_arr);

	dev_abs_free(hlr->dev);
	free(hlr->dev);

	io_destroy(hlr->aio_ctx);

	free(hlr);
}

int forward_req_to_hlr(struct handler *hlr, struct request *req) {
	int rc = 0;
	cl_grap(hlr->flying);
	req->hlr = hlr;
	if (!q_enqueue((void *)req, hlr->req_q)) {
		rc = -1;
	}
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
		goto exit;
	}
exit:
	return req;
}


void *request_handler(void *input) {
	int rc = 0;

	struct request *req = NULL;
	struct handler *hlr = (struct handler *)input;
	struct kv_ops *ops = hlr->ops;

	struct callback *cb = NULL;

	char thread_name[128] = {0};
	sprintf(thread_name, "%s[%d]", "request_handler", hlr->number);
	pthread_setname_np(pthread_self(), thread_name);

	printf("handler: %s for %s is launched\n\n", thread_name,
	       hlr->dev->dev_name);

	while (1) {
		if (stopflag_hlr && (hlr->flying->now==0)) { 
			return NULL;
		}

		if (!(req=get_next_request(hlr))) {
			continue;
		}

		while ((cb = (struct callback *)q_dequeue(hlr->done_q))) {
			cb->func(cb->arg);
			q_enqueue((void *)cb, hlr->cb_pool);
		}

		switch (req->type) {
		case REQ_TYPE_SET:
			rc = ops->set_kv(ops, req);
			break;	
		case REQ_TYPE_GET:
			rc = ops->get_kv(ops, req);
			if (rc) {
				puts("Not existing key!");
				printf("%lu\n", req->key.hash_low);
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
