#include "config.h"
#include "platform/device.h"
#include "platform/aio.h"
#include "utility/stopwatch.h"
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

extern int errno;

static void
print_device_init(struct dev_abs *dev) {
	printf("dev_abs: %s is initialized\n", dev->dev_name);
	printf("|-- Logical Block Size: %u B\n", dev->logical_block_size);
	printf("|-- Logical Blocks: %u\n", dev->nr_logical_block);
	printf("|-- Total Size: %lu (%.2f GB, %.2fGiB)\n", dev->size_in_byte,
		(double)dev->size_in_byte/1000/1000/1000,
		(double)dev->size_in_byte/1024/1024/1024);
	printf("|-- Segment Size: %lu B\n", dev->segment_size);
	printf("`-- Segments: %u\n", dev->nr_segment);
	puts("");
}

static void *
alloc_seg_buffer(uint32_t size) {
#ifdef USE_HUGEPAGE
	void *seg_buffer = mmap(NULL, size, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, 0, 0);
#else
	void *seg_buffer = aligned_alloc(MEM_ALIGN_UNIT, size);
#endif
	if (!seg_buffer) {
		perror("Allocating seg_buffer");
		abort();
	}
	return seg_buffer;
}

struct dev_abs *
dev_abs_init(const char dev_name[]) {
	struct dev_abs *dev = (struct dev_abs *)malloc(sizeof(struct dev_abs));

	strcpy(dev->dev_name, dev_name);

	dev->dev_fd = open(dev->dev_name, O_RDWR|O_CREAT|O_DIRECT, 0666);
	if (dev->dev_fd < 0) {
		perror("Device open()");
		abort();
	}
	if (ioctl(dev->dev_fd, BLKGETSIZE, &dev->nr_logical_block) < 0) {
		perror("Device ioctl(): BLKGETSIZE");
		abort();
	}
	if (ioctl(dev->dev_fd, BLKSSZGET, &dev->logical_block_size) < 0) {
		perror("Device ioctl(): BLKSSZGET");
		abort();
	}

	dev->size_in_byte = (uint64_t)dev->nr_logical_block * dev->logical_block_size;

	dev->segment_size = SEGMENT_SIZE;
	dev->nr_segment = dev->size_in_byte / dev->segment_size;

	dev->seg_array =
		(struct segment *)calloc(dev->nr_segment, sizeof(struct segment));

	for (size_t i = 0; i < dev->nr_segment; i++) {
		struct segment *seg = &dev->seg_array[i];
		seg->idx = i;
		seg->state = SEG_STATE_FREE;

		seg->start_addr = i * dev->segment_size;
		seg->end_addr = (i+1) * dev->segment_size;
		seg->offset = seg->start_addr;
	}
	dev->staged_seg = &dev->seg_array[0];
	dev->staged_seg->state = SEG_STATE_STAGED;
	dev->staged_seg_idx = 0;
	dev->staged_seg_buf = alloc_seg_buffer(dev->segment_size);

	dev->grain_unit = GRAIN_UNIT;

	print_device_init(dev);

	return dev;
}

int
dev_abs_free(struct dev_abs *dev) {
	close(dev->dev_fd);
	free(dev->seg_array);
#ifdef USE_HUGEPAGE
	munmap(dev->staged_seg_buf, dev->segment_size);
#else
	free(dev->staged_seg_buf);
#endif
	return 0;
}

static int
is_staged(struct dev_abs *dev, uint64_t pba) {
	uint64_t addr_in_byte = pba * dev->grain_unit;
	struct segment *ss = dev->staged_seg;

	if (addr_in_byte < ss->start_addr) {
		return -1;
	} else if (addr_in_byte >= ss->offset) {
		return 1;
	} else {
		return 0;
	}
}

int
dev_abs_read(struct handler *hlr, uint64_t pba, uint32_t size_in_grain,
	     char *buf, struct callback *cb) {

	struct dev_abs *dev = hlr->dev;
	struct segment *ss = dev->staged_seg;

	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t addr_in_byte = pba * dev->grain_unit;

	if (is_staged(dev, pba) == 0) {
		uint64_t offset = addr_in_byte - ss->start_addr;
		memcpy(buf, (char *)dev->staged_seg_buf + offset, size);

		cb->func(cb->arg);
		q_enqueue((void *)cb, hlr->cb_pool);
		return 0;

	} else {
		return aio_read(hlr, addr_in_byte, size, buf, cb);
	}
}

int
dev_abs_write(struct handler *hlr, uint64_t pba, uint32_t size_in_grain,
	      char *buf, struct callback *cb) {

	struct dev_abs *dev = hlr->dev;
	struct segment *ss = dev->staged_seg;

	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t offset = (pba * dev->grain_unit) - ss->start_addr;

	memcpy((char *)dev->staged_seg_buf + offset, buf, size);

	cb->func(cb->arg);
	q_enqueue((void *)cb, hlr->cb_pool);
	return 0;
}

static void *
reap_seg_buf(void *arg) {
	void *seg_buf = arg;
#ifdef USE_HUGEPAGE
	munmap(seg_buf, SEGMENT_SIZE);
#else
	free(seg_buf);
#endif
	return NULL;
}

static struct segment *
stage_next_segment(struct dev_abs *dev) {
	dev->staged_seg_idx = (dev->staged_seg_idx+1) % dev->nr_segment;
	dev->staged_seg = &dev->seg_array[dev->staged_seg_idx];
	dev->staged_seg->state = SEG_STATE_STAGED;
	dev->staged_seg->offset = dev->staged_seg->start_addr;
	dev->staged_seg_buf = alloc_seg_buffer(dev->segment_size);
	return dev->staged_seg;
}

uint64_t get_next_pba(struct handler *hlr, uint32_t size) {
	uint64_t pba;
	struct dev_abs *dev = hlr->dev;
	struct segment *ss = dev->staged_seg;

	struct callback *new_cb;

	// if there is no space to alloc kv-pair, need to reclaim old segment
	if (ss->offset + size > ss->end_addr) {
		ss->state = SEG_STATE_USED;

		new_cb = make_callback(hlr, reap_seg_buf, dev->staged_seg_buf);
		aio_write(hlr, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_seg_buf, new_cb);

		ss = stage_next_segment(dev);
	}

	pba = ss->offset / dev->grain_unit;

	if (size % dev->grain_unit != 0) {
		size += (dev->grain_unit-(size%dev->grain_unit));
		if (size % dev->grain_unit != 0) abort(); // FIXME: remove later
	}
	ss->offset += size;

	return pba;
}

