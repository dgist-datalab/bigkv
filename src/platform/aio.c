#include "platform/aio.h"
#include "platform/device.h"

#include <stdlib.h>
#include <stdio.h>

int
aio_read(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	 char *buf, struct callback *cb) {

	int rc = 0;

	//struct iocb *iocb = (struct iocb *)malloc(sizeof(struct iocb));
	//printf("aio_read %llu, %u\n", addr_in_byte, size);
	struct iocb *iocb = (struct iocb *)q_dequeue(cb->hlr->iocb_pool);
	io_prep_pread(iocb, dev->dev_fd, buf, size, addr_in_byte);

	cb->iocb = iocb;
	iocb->data = cb;

#ifdef ASYNC_SUBMIT
	q_enqueue((void*)iocb, cb->hlr->submit_q);
	return 0;
#endif

	if (io_submit(dev->aio_ctx, 1, &iocb) < 0) {
		perror("io_sumbit");
		rc = -1;
	}

	return rc;
}

int
aio_write(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	  char *buf, struct callback *cb) {

	int rc = 0;

	//struct iocb *iocb = (struct iocb *)malloc(sizeof(struct iocb));
	//printf("aio_write %lld, %d\n", addr_in_byte, size);
	struct iocb *iocb = (struct iocb *)q_dequeue(cb->hlr->iocb_pool);
	io_prep_pwrite(iocb, dev->dev_fd, buf, size, addr_in_byte);

	cb->iocb = iocb;
	iocb->data = cb;

#ifdef ASYNC_SUBMIT
	q_enqueue((void*)iocb, cb->hlr->submit_q);
	return 0;
#endif

	if (io_submit(dev->aio_ctx, 1, &iocb) < 0) {
		perror("io_sumbit");
		rc = -1;
	}

	return rc;
}
