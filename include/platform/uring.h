#ifndef __URING_H__
#define __URING_H__

#include "platform/handler.h"

#include <liburing.h>
#include <stdint.h>

#define URING_QD 2048

struct io_data {
	int read;
	off_t offset;
	struct iovec iovec;
	void *data;
};

int uring_read(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	     char *buf, struct callback *cb);

int uring_write(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	      char *buf, struct callback *cb);

int uring_retry (struct dev_abs *dev, struct io_uring *ring, struct io_data *io_data);

#endif
