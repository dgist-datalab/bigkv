/*
 * Linux AIO I/O Back-end Header.
 *
 * Description: Linux AIO library must be installed (libaio.h)
 *
 * ex) $ sudo apt install libaio-dev
 *
 */

#ifndef __AIO_H__
#define __AIO_H__

#include "platform/handler.h"

#include <libaio.h>
#include <stdint.h>

int aio_read(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	     char *buf, struct callback *cb);

int aio_write(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	      char *buf, struct callback *cb);

#endif
