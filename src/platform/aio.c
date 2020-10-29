#include "platform/aio.h"
#include "platform/device.h"

#include <stdlib.h>
#include <stdio.h>

int
aio_read(struct handler *hlr, uint64_t addr_in_byte, uint32_t size,
	 char *buf, struct callback *cb) {

	int rc = 0;

	//struct iocb *iocb = (struct iocb *)malloc(sizeof(struct iocb));
	struct iocb *iocb = (struct iocb *)q_dequeue(hlr->iocb_pool);
	io_prep_pread(iocb, hlr->dev->dev_fd, buf, size, addr_in_byte);

	cb->iocb = iocb;
	iocb->data = cb;

	if (io_submit(hlr->aio_ctx, 1, &iocb) < 0) {
		perror("io_sumbit");
		rc = -1;
	}

	return rc;
}

int
aio_write(struct handler *hlr, uint64_t addr_in_byte, uint32_t size,
	  char *buf, struct callback *cb) {

	int rc = 0;

	//struct iocb *iocb = (struct iocb *)malloc(sizeof(struct iocb));
	struct iocb *iocb = (struct iocb *)q_dequeue(hlr->iocb_pool);
	io_prep_pwrite(iocb, hlr->dev->dev_fd, buf, size, addr_in_byte);

	cb->iocb = iocb;
	iocb->data = cb;

	if (io_submit(hlr->aio_ctx, 1, &iocb) < 0) {
		perror("io_sumbit");
		rc = -1;
	}

	return rc;
}
