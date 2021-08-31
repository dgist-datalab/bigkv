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
#include "utility/queue.h"
#include "utility/list.h"
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

#define FLYING_QSIZE 1024
#define GC_VICTIM_THRESHOLD 0.1
#define GC_TRIGGER_THRESHOLD 2 // number of free segments remained
#define DEV_OP_RATE 0.1
#define SEG_MAX_AGE 32


#define PART_IDX_SIZE 8
#define SEG_IDX_HEADER_SIZE 4096 // SEGMENT_SIZE/PART_TABLE_SIZE * PART_IDX_SIZE

#define	SEG_STATE_IDX 1
#define	SEG_STATE_DATA 2
#define	SEG_STATE_COMMITTED 4
#define	SEG_STATE_FREE 8
#define	SEG_STATE_STAGED 16
#define	SEG_STATE_FLYING 32


/* Each device is divided into fixed-size segments */
struct segment {
	uint32_t idx;
	uint8_t state;
	uint64_t start_addr;
	uint64_t end_addr;
	uint64_t offset;
	void *buf;
	void *_private;
	li_node *lnode;
	uint32_t entry_cnt;
	uint32_t invalid_cnt;
	uint64_t age;
	uint8_t *seg_bitmap;
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
	uint32_t nr_free_segment;

	uint64_t age;

	struct segment *seg_array;
	
	queue *free_seg_q;
	list *flying_seg_list;
	list *committed_seg_list;


	pthread_mutex_t flying_seg_lock;
	pthread_mutex_t committed_seg_lock;

	struct segment *staged_seg;
	struct segment *staged_idx_seg;

	uint32_t grain_unit;

	// statistics
	// about GC
	uint32_t gc_trigger_cnt;
	uint32_t invalid_seg_cnt;
	uint32_t victim_larger_valid_seg_cnt;
	uint32_t victim_larger_valid_idx_seg_cnt;
	uint32_t victim_larger_valid_data_seg_cnt;

	uint32_t victim_idx_seg_cnt;
	uint64_t victim_invalid_idx_cnt;
	uint64_t victim_entry_idx_cnt;
	uint64_t victim_trim_idx_seg_cnt;

	uint32_t victim_data_seg_cnt;
	uint64_t victim_invalid_data_cnt;
	uint64_t victim_entry_data_cnt;
	uint64_t fail_victim_entry_cnt;
	uint64_t fail_victim_invalid_cnt;
	uint64_t victim_trim_data_seg_cnt;
};

struct dev_abs *dev_abs_init(const char dev_name[]);
int dev_abs_free(struct dev_abs *dev);

int dev_abs_read(struct handler *hlr, uint64_t pba, uint32_t size, char *buf, struct callback *cb);
int dev_abs_write(struct handler *hlr, uint64_t pba, uint64_t old_pba, uint32_t size, char *buf, struct callback *cb);
int dev_abs_idx_write(struct handler *hlr, uint64_t pba, uint64_t old_pba, uint32_t size, char *buf, struct callback *cb, uint64_t part_idx);

void *alloc_seg_buffer(uint32_t size);
bool dev_need_gc(struct handler *hlr);
int dev_read_victim_segment(struct handler *hlr, struct gc *gc);
bool is_idx_seg(struct handler *hlr);
void reap_gc_segment(struct handler *hlr, struct gc *gc);

uint64_t get_next_pba(struct handler *hlr, uint32_t size);
uint64_t get_next_idx_pba(struct handler *hlr, uint32_t size);

static void print_segment (struct segment *seg, const char *str) {
	printf("PRINT SEGMENT [%s] idx: %d, state: %d, start_addr: %lu, end_addr: %lu, offset: %lu, entry_cnt: %u, invalid_cnt: %u, age: %lu\n", str, seg->idx, seg->state, seg->start_addr, seg->end_addr, seg->offset, seg->entry_cnt, seg->invalid_cnt, seg->age);
}

static void print_segment_by_addr (struct dev_abs *dev, uint64_t addr, const char *str) {
	struct segment *seg = &dev->seg_array[addr/dev->segment_size];
	printf("PRINT SEGMENT BY ADDR addr: %lu, idx: %lu [%s] idx: %d, state: %d, start_addr: %lu, end_addr: %lu, offset: %lu, entry_cnt: %u, invalid_cnt: %u, age: %lu\n", addr, addr/dev->segment_size, str, seg->idx, seg->state, seg->start_addr, seg->end_addr, seg->offset, seg->entry_cnt, seg->invalid_cnt, seg->age);
}

void dev_print_gc_info(struct handler *hlr);


#endif
