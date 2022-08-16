#include "platform/handler.h"
#include "platform/request.h"
#include "platform/dev_spdk.h"
#include <numa.h>
#include <errno.h>
#include <stdio.h>

#ifdef LINUX_AIO
#include <libaio.h>
#endif

//#define NR_EVENTS QDEPTH*2
#define NR_EVENTS QDEPTH*4

extern bool stopflag_hlr;

#ifdef LINUX_AIO
static void *aio_poller(void *input) {
	int ret;
	struct io_event *ev;
	struct iocb *iocb, *submit_iocb;
	struct callback *cb;
	struct request *req;

	struct handler *hlr = (struct handler *)input;

	io_context_t ctx;
	struct io_event events[NR_EVENTS];
	struct timespec timeout = { 0, 0 };

	int dev_idx;


	numa_set_localalloc();
	numa_set_strict(numa_preferred());

	while (1) {
		if (stopflag_hlr) return NULL;

		for (dev_idx = 0; dev_idx < hlr->num_dev_per_hlr; dev_idx++) {
			ctx = hlr->dev[dev_idx]->aio_ctx;

			if ((ret = io_getevents(ctx, 0, NR_EVENTS, events, &timeout))) {
				for (int i = 0; i < ret; i++) {
					ev = &events[i];
					iocb = ev->obj;
					cb = (struct callback *)ev->data;
	
					if (ev->res == EINVAL) {
						fprintf(stderr, "aio: I/O failed\n");
					} else if (ev->res != iocb->u.c.nbytes) {
						fprintf(stderr, "aio: Data size error %lu!=%lu\n", ev->res, iocb->u.c.nbytes);
						printf("%s\n",strerror(-ev->res));
						perror("sss\n");
						abort();
					}
#ifdef BREAKDOWN
					if (iocb->aio_lio_opcode == 0) {
						req = (struct request *)(cb->arg);
						if (req->type == REQ_TYPE_GET) {
							sw_end(req->sw_bd[4]);
							sw_start(req->sw_bd[5]);
						}
					}
#endif

					//q_enqueue((void *)cb, hlr->done_q);
					cb->func(cb->arg);
					q_enqueue((void *)cb, cb->hlr->cb_pool);
					//free(ev->data);
					q_enqueue((void *)iocb, cb->hlr->iocb_pool);
				}
			}	
		}
	}
	return NULL;
}
#elif DEV_SPDK
static void *spdk_poller(void *input) {
	// TODO: implement SPDK poller
	return NULL;
}
#elif URING
static void *uring_poller(void *input) {
	int ret;
	struct io_uring_cqe *cqe;
	struct io_data *io_data;
	struct callback *cb;
	struct request *req;
	volatile int got_comp = 1;

	struct handler *hlr = (struct handler *)input;
	struct dev_abs *dev;
	int dev_idx;


	numa_set_localalloc();
	numa_set_strict(numa_preferred());

	while (1) {
		if (stopflag_hlr) return NULL;

		for (dev_idx = 0; dev_idx < hlr->num_dev_per_hlr, dev_idx++) {
			dev = hlr->dev[dev_idx];
			if (!got_comp) {
				ret = io_uring_wait_cqe(&dev->ring, &cqe);
				got_comp = 0;
			} else {
				if ((ret = io_uring_peek_cqe(&dev->ring, &cqe)) == -EAGAIN) {
					got_comp = 1;
					continue;
				} else if (ret < 0) {
					fprintf(stderr, "uring: I/O failed\n");
					printf("%s\n",strerror(-ret));
					perror("sss1\n");
					abort();
				}
			}
	
			if (ret < 0 || cqe == NULL) {
				printf("%s\n",strerror(-ret));
				perror("sss0\n");
				abort();
			}
			
			io_data = (struct io_data *)io_uring_cqe_get_data(cqe);
			if (io_data == NULL)
				abort();
			if (cqe->res < 0) {
				if (cqe->res == -EAGAIN) {
					io_uring_cqe_seen(&dev->ring, cqe);
					uring_retry(dev, &dev->ring, io_data);
					continue;
				}
				else {
					printf("%s\n",strerror(-cqe->res));
					perror("sss2\n");
					abort();
				}
			} else if (cqe->res != io_data->iovec.iov_len) {
				io_uring_cqe_seen(&dev->ring, cqe);
				//io_data->iovec.iov_base = (char *)(io_data->iovec.iov_base) + cqe->res;
				//io_data->iovec.iov_len -= cqe->res;
				uring_retry(dev, &dev->ring, io_data);
				continue;
			}


			cb = (struct callback *)io_data->data;
			io_uring_cqe_seen(&dev->ring, cqe);

#ifdef BREAKDOWN
			if (io_data->read == 1) {
				req = (struct request *)(cb->arg);
				if (req->type == REQ_TYPE_GET) {
					sw_end(req->sw_bd[4]);
					sw_start(req->sw_bd[5]);
				}
			}
#endif

			cb->func(cb->arg);
			q_enqueue((void *)cb, cb->hlr->cb_pool);
			q_enqueue((void *)io_data, cb->hlr->io_data_pool);
		}
	}
	return NULL;
}
#endif

void *device_poller(void *input) {
#ifdef LINUX_AIO
	aio_poller(input);
#elif DEV_SPDK
	spdk_poller(input);
#elif URING
	uring_poller(input);
#endif
	return NULL;
}



#ifdef LINUX_AIO
static void *aio_polling(void *input) {
	int ret;
	struct io_event *ev;
	struct iocb *iocb, *submit_iocb;
	struct callback *cb;
	struct request *req;

	struct handler *hlr = (struct handler *)input;

	io_context_t ctx; 
	struct io_event events[NR_EVENTS];
	struct timespec timeout = { 0, 0 };

	int dev_idx;
	volatile bool found = false;


	while (1) {
		if (stopflag_hlr) return NULL;

		found = false;

		for (dev_idx = 0; dev_idx < hlr->num_dev_per_hlr; dev_idx++) {
			ctx = hlr->dev[dev_idx]->aio_ctx;

			if ((ret = io_getevents(ctx, 0, NR_EVENTS, events, &timeout))) {
				for (int i = 0; i < ret; i++) {
					ev = &events[i];
					iocb = ev->obj;
					cb = (struct callback *)ev->data;
	
					if (ev->res == EINVAL) {
						fprintf(stderr, "aio: I/O failed\n");
					} else if (ev->res != iocb->u.c.nbytes) {
						fprintf(stderr, "aio: Data size error %lu!=%lu\n", ev->res, iocb->u.c.nbytes);
						printf("%s\n",strerror(-ev->res));
						perror("sss\n");
						abort();
					}
#ifdef BREAKDOWN
					if (iocb->aio_lio_opcode == 0) {
						req = (struct request *)(cb->arg);
						if (req->type == REQ_TYPE_GET) {
							sw_end(req->sw_bd[4]);
							sw_start(req->sw_bd[5]);
						}
					}
#endif
	
					//q_enqueue((void *)cb, hlr->done_q);
					cb->func(cb->arg);
					q_enqueue((void *)cb, cb->hlr->cb_pool);
					//free(ev->data);
					q_enqueue((void *)iocb, cb->hlr->iocb_pool);
				}
				found = true;
			} else {
				found = false;
			}
		}
		if (!found)
			return NULL;
	}
	return NULL;
}
#elif DEV_SPDK
static void
perf_disconnect_cb(struct spdk_nvme_qpair *qpair, void *ctx)
{
	printf("??????????????????\n");

}

static int64_t
nvme_check_io(struct ns_worker_ctx *ns_ctx)
{
	int64_t rc;

	rc = spdk_nvme_poll_group_process_completions(ns_ctx->u.nvme.group, 256,
			perf_disconnect_cb);
	if (rc < 0) {
		fprintf(stderr, "NVMe io qpair process completion error\n");
		exit(1);
	}
	return rc;
}

static void *spdk_polling(void *input) {
	int ret = 0;
	struct io_uring_cqe *cqe;
	struct io_data *io_data;
	struct callback *cb;
	struct request *req;

	struct handler *hlr = (struct handler *)input;
	struct dev_abs *dev;
	struct spdk_ctx *sctx;
	struct ns_worker_ctx *ns_ctx;
	int dev_idx;
	volatile bool found = false;

	while (1) {
		if (stopflag_hlr) return NULL;
		found = false;

		for (dev_idx = 0; dev_idx < hlr->num_dev_per_hlr; dev_idx++) {
			dev = hlr->dev[dev_idx];
			sctx = dev->sctx;

			TAILQ_FOREACH(ns_ctx, &sctx->ns_ctx, link) {
				ret = nvme_check_io(ns_ctx);
			}

			if (ret)
				found = true;
			else
				return NULL;
			
		}
		if (!found)
			return NULL;
	}
	return NULL;
}
#elif URING
static void *uring_polling(void *input) {
	int ret;
	struct io_uring_cqe *cqe;
	struct io_data *io_data;
	struct callback *cb;
	struct request *req;
	volatile int got_comp = 1;

	struct handler *hlr = (struct handler *)input;
	struct dev_abs *dev;
	int dev_idx;
	volatile bool found = false;

	while (1) {
		if (stopflag_hlr) return NULL;
		found = false;

		for (dev_idx = 0; dev_idx < hlr->num_dev_per_hlr; dev_idx++) {
			dev = hlr->dev[dev_idx];

			if (!got_comp) {
				ret = io_uring_wait_cqe(&dev->ring, &cqe);
				got_comp = 0;
			} else {
				if ((ret = io_uring_peek_cqe(&dev->ring, &cqe)) == -EAGAIN) {
					got_comp = 1;
					dev_idx--;
					continue;
				} else if (ret < 0) {
					fprintf(stderr, "uring: I/O failed\n");
					printf("%s\n",strerror(-ret));
					perror("sss1\n");
					abort();
				}
			}
	
			if (ret < 0 || cqe == NULL) {
				printf("%s\n",strerror(-ret));
				perror("sss0\n");
				abort();
			}
			
			io_data = (struct io_data *)io_uring_cqe_get_data(cqe);
			if (io_data == NULL)
				abort();
			if (cqe->res < 0) {
				if (cqe->res == -EAGAIN) {
					io_uring_cqe_seen(&dev->ring, cqe);
					uring_retry(dev, &dev->ring, io_data);
					dev_idx--;
					continue;
				}
				else {
					printf("%s\n",strerror(-cqe->res));
					perror("sss2\n");
					abort();
				}
			} else if (cqe->res != io_data->iovec.iov_len) {
				io_uring_cqe_seen(&dev->ring, cqe);
				//io_data->iovec.iov_base = (char *)(io_data->iovec.iov_base) + cqe->res;
				//io_data->iovec.iov_len -= cqe->res;
				uring_retry(dev, &dev->ring, io_data);
				dev_idx--;
				continue;
			}
	
			cb = (struct callback *)io_data->data;
			io_uring_cqe_seen(&dev->ring, cqe);
	
#ifdef BREAKDOWN
			if (io_data->read == 1) {
				req = (struct request *)(cb->arg);
				if (req->type == REQ_TYPE_GET) {
					sw_end(req->sw_bd[4]);
					sw_start(req->sw_bd[5]);
				}
			}
#endif

			cb->func(cb->arg);
			q_enqueue((void *)cb, cb->hlr->cb_pool);
			q_enqueue((void *)io_data, cb->hlr->io_data_pool);

			found = true;
		}
		if (!found)
			return NULL;
	}
	return NULL;
}
#endif

void *device_polling(void *input) {
#ifdef LINUX_AIO
	aio_polling(input);
#elif DEV_SPDK
	spdk_polling(input);
#elif URING
	uring_polling(input);
#endif
	return NULL;
}
