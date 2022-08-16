#include "platform/uring.h"
#include "platform/device.h"

#include <stdlib.h>
#include <stdio.h>


int
uring_read(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	 char *buf, struct callback *cb) {

	int rc = 0;

	struct io_uring_sqe *sqe;

	struct io_data *io_data;
	while ((io_data = (struct io_data *)q_dequeue(cb->hlr->io_data_pool)) == NULL);
	//if ((io_data = (struct io_data *)q_dequeue(cb->hlr->io_data_pool)) == NULL) {
	//	return -1;
	//}
	if (io_data == NULL)
		abort();
	io_data->iovec.iov_base = buf;
	io_data->iovec.iov_len = size;
	io_data->data = cb;
	io_data->read = 1;
	io_data->offset = addr_in_byte;

	sqe = io_uring_get_sqe(&dev->ring);
	if (!sqe) {
		abort();
	}

	io_uring_prep_readv(sqe, dev->dev_fd, &io_data->iovec, 1, addr_in_byte);
	io_uring_sqe_set_data(sqe, io_data);

	
	if (io_uring_submit(&dev->ring) < 0) {
		perror("io_uring_sumbit");
		rc = -1;
		abort();
	}

	return rc;
}

int
uring_write(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	  char *buf, struct callback *cb) {

	int rc = 0;

	struct io_uring_sqe *sqe;

	struct io_data *io_data;
	while ((io_data = (struct io_data *)q_dequeue(cb->hlr->io_data_pool)) == NULL);
	//if ((io_data = (struct io_data *)q_dequeue(cb->hlr->io_data_pool)) == NULL) {
	//	return -1
	//}
	if (io_data == NULL)
		abort();
	io_data->iovec.iov_base = buf;
	io_data->iovec.iov_len = size;
	io_data->data = cb;
	io_data->read = 0;
	io_data->offset = addr_in_byte;

	sqe = io_uring_get_sqe(&dev->ring);
	if (!sqe) {
		abort();
	}

	io_uring_prep_writev(sqe, dev->dev_fd, &io_data->iovec, 1, addr_in_byte);
	io_uring_sqe_set_data(sqe, io_data);

	
	if (io_uring_submit(&dev->ring) < 0) {
		perror("io_uring_sumbit");
		rc = -1;
		abort();
	}

	return rc;
}


int uring_retry (struct dev_abs *dev, struct io_uring *ring, struct io_data *io_data) {
	struct io_uring_sqe *sqe;
	struct callback *cb = (struct callback *)io_data->data;
	while ((sqe = io_uring_get_sqe(ring)) == NULL);

	if (io_data->read)
		io_uring_prep_readv(sqe, dev->dev_fd, &io_data->iovec, 1, io_data->offset);
	else
		io_uring_prep_writev(sqe, dev->dev_fd, &io_data->iovec, 1, io_data->offset);

	io_uring_sqe_set_data(sqe, io_data);

	if (io_uring_submit(ring) < 0) {
		perror("io_uring_retry");
		abort();
		return -1;
	}
	return 1;
}
