/*
 * Device Abstraction Layer Header
 *
 * Description: Each device is managed by a 'dev_abs' struct, including read,
 * write, and addressing. Address space is divided into fixed-size segments.
 * Segment size is 2 MB in default.
 *
 * Segments has 3 statuses; Used, Staged, and Free.
 *
 * We put all incoming writes into staged segment buffer which is allocated
 * in hugepage memory. If the buffer becomes full, we explicitly call I/O
 * back-end. I/O back-end could be Linux AIO, io_uring, or SPDK.
 */

#ifndef __SEGMENT_H__
#define __SEGMENT_H__

#include "platform/handler.h"
#include <stdint.h>

/* Each device is divided into fixed-size segments */
struct segment {
	uint32_t idx;
	seg_state_t state;

	uint64_t start_addr;
	uint64_t end_addr;
	uint64_t offset;
};

/* Device abstraction */
struct dev_abs {
	char dev_name[128];
	int dev_fd;

	uint32_t nr_logical_block;
	uint32_t logical_block_size;
	uint64_t size_in_byte;

	uint64_t segment_size;
	uint32_t nr_segment;

	struct segment *seg_array;

	struct segment *staged_seg;
	uint32_t staged_seg_idx;
	void *staged_seg_buf;

	uint32_t grain_unit;
};

struct dev_abs *dev_abs_init(const char dev_name[]);
int dev_abs_free(struct dev_abs *dev);

int dev_abs_read(struct handler *hlr, uint64_t pba, uint32_t size, char *buf, struct callback *cb);
int dev_abs_write(struct handler *hlr, uint64_t pba, uint32_t size, char *buf, struct callback *cb);

uint64_t get_next_pba(struct handler *hlr, uint32_t size);

#endif
