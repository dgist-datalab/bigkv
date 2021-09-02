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

void *
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
	memset(seg_buffer, 0, size);
	return seg_buffer;
}

struct dev_abs *
dev_abs_init(const char dev_name[]) {
	struct dev_abs *dev = (struct dev_abs *)calloc(1, sizeof(struct dev_abs));

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

#ifdef TEST_GC
	dev->size_in_byte = TEST_GC_CAPACITY;
#else
	dev->size_in_byte = (uint64_t)dev->nr_logical_block * dev->logical_block_size;
#endif

	dev->segment_size = SEGMENT_SIZE;
	dev->nr_segment = dev->size_in_byte / dev->segment_size;

	dev->seg_array =
		(struct segment *)calloc(dev->nr_segment, sizeof(struct segment));

	q_init(&dev->free_seg_q, dev->nr_segment);
	dev->flying_seg_list = list_init();
	dev->committed_seg_list = list_init();

	pthread_mutex_init(&dev->flying_seg_lock, NULL);
	pthread_mutex_init(&dev->committed_seg_lock, NULL);

	for (size_t i = 0; i < dev->nr_segment; i++) {
		struct segment *seg = &dev->seg_array[i];
		//struct segment *seg = (struct segment *)malloc(sizeof(struct segment));
		seg->idx = i;
		seg->state = SEG_STATE_FREE;

		seg->start_addr = i * dev->segment_size;
		seg->end_addr = (i+1) * dev->segment_size;
		seg->offset = seg->start_addr;
		seg->entry_cnt = 0;
		seg->invalid_cnt = 0;
		seg->age = 0;
#ifdef DEBUG_GC
		seg->seg_bitmap = (uint8_t *)calloc(1, 2048);
#endif
		q_enqueue((void *)seg, dev->free_seg_q);

	}
	dev->staged_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_seg->state = SEG_STATE_DATA;
	dev->staged_seg->buf = alloc_seg_buffer(dev->segment_size);

	dev->staged_idx_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_idx_seg->state = SEG_STATE_IDX;
	dev->staged_idx_seg->buf = alloc_seg_buffer(dev->segment_size);
	dev->staged_idx_seg->offset = dev->staged_idx_seg->start_addr + SEG_IDX_HEADER_SIZE;

	dev->nr_free_segment = dev->nr_segment - 2;
	dev->grain_unit = GRAIN_UNIT;
	dev->age = 0;

	print_device_init(dev);

	return dev;
}

int
dev_abs_free(struct dev_abs *dev) {
	struct segment *seg;
	close(dev->dev_fd);
	li_node *cur;
#ifdef USE_HUGEPAGE
	munmap(dev->staged_seg->buf, dev->segment_size);
	munmap(dev->staged_idx_seg->buf, dev->segment_size);
#else
	free(dev->staged_seg->buf);
	free(dev->staged_idx_seg->buf);
#endif
	list_for_each_node(dev->flying_seg_list, cur) {
		seg = (struct segment *)cur->data;
		while (!(seg->state & SEG_STATE_COMMITTED));
		free(seg->buf);
	}
	list_free(dev->flying_seg_list);
	list_free(dev->committed_seg_list);
	
	while((seg = (struct segment *)q_dequeue(dev->free_seg_q))) {
		//free(seg);
	}
	free(dev->seg_array);
	q_free(dev->free_seg_q);
	return 0;
}

static struct segment *
is_staged(struct dev_abs *dev, uint64_t pba) {
	uint64_t addr_in_byte = pba * dev->grain_unit;
	struct segment *ss = dev->staged_seg;

	if (addr_in_byte >= ss->start_addr && addr_in_byte < ss->offset) {
		return ss;
	}

	ss = dev->staged_idx_seg;
	if (addr_in_byte >= ss->start_addr && addr_in_byte < ss->offset) {
		return ss;
	}
	return NULL;
}

static struct segment *
is_flying(struct dev_abs *dev, uint64_t pba) {
	struct segment *seg, *ret;
	uint64_t addr_in_byte = pba * dev->grain_unit;
	li_node *cur;

	if (!dev->flying_seg_list->size)
		return NULL;

	list_for_each_node(dev->flying_seg_list, cur) {
		seg = (struct segment *)cur->data;
		if (addr_in_byte < seg->start_addr) {
			ret = NULL;
		} else if (addr_in_byte >= seg->offset) {
			ret = NULL;
		} else {
			ret = seg;
		}
	}
	return ret;
}

static int
reap_committed_seg(struct dev_abs *dev) {
	struct segment *seg;
	li_node *cur, *next;
	if (!dev->flying_seg_list->size)
		return 0;
	list_for_each_node_safe(dev->flying_seg_list, cur, next) {
		seg = (struct segment *)cur->data;
		if (seg->state & SEG_STATE_COMMITTED) {
			list_delete_node(dev->flying_seg_list, seg->lnode);
			seg->lnode = list_insert(dev->committed_seg_list, seg);
			if (seg->buf)
				free(seg->buf);
			seg->buf = NULL;
		}
	}
	return 0;
}

static int invalidate_seg_entry(struct handler *hlr, uint64_t pba, bool is_idx_write) {
	struct dev_abs *dev = hlr->dev;
	int seg_idx = pba * GRAIN_UNIT/ dev->segment_size;
	struct segment *seg = dev->seg_array + seg_idx;

	if (!is_idx_write)
		return SEG_MAX_AGE;

	if (pba > dev->size_in_byte / GRAIN_UNIT)
		return (dev->age / dev->nr_segment) % MAX_SEG_AGE;

	// the data is removed by data GC
	if (seg->state == SEG_STATE_FREE) 
		return SEG_MAX_AGE;

	//if (!is_idx_write && seg->state & SEG_STATE_IDX)
	//	return SEG_MAX_AGE;

	if (is_idx_write && seg->state & SEG_STATE_DATA)
		abort();


	//else
	
#ifdef DEBUG_GC
	int pba_idx = pba % (dev->segment_size / GRAIN_UNIT);
	pba_idx = (seg->state & SEG_STATE_IDX) ? pba_idx / 32 : pba_idx / 8;
	if(seg->seg_bitmap[pba_idx % (dev->segment_size / GRAIN_UNIT)] >= 1) {
		int cnt = 0, sum = 0;;
		for (int i = 0; i < 2048/16; i++) {
			for (int j = 0; j < 16; j++) {
				printf("%d ", seg->seg_bitmap[i*16+j]);
				cnt = cnt + !!(seg->seg_bitmap[i*16+j]);
				sum = sum + seg->seg_bitmap[i*16+j];
			}
			printf("\n");
		}
		print_segment(seg, "Too many invalid");
		printf("cnt: %d, sum: %d\n", cnt, sum);
		fflush(stdout);


		abort();
	}
	seg->seg_bitmap[pba_idx]++;
#endif

	seg->invalid_cnt++;
	if ((seg->state & SEG_STATE_COMMITTED) && (seg->invalid_cnt == seg->entry_cnt)) {
		//print_segment(seg, "Should trim");
		dev->invalid_seg_cnt++;
		list_move_to_head(dev->committed_seg_list, seg->lnode);
	} else if (seg->invalid_cnt > seg->entry_cnt) {
#ifdef DEBUG_GC
		int cnt = 0, sum = 0;;
		for (int i = 0; i < 2048/16; i++) {
			for (int j = 0; j < 16; j++) {
				printf("%d ", seg->seg_bitmap[i*16+j]);
				cnt = cnt + !!(seg->seg_bitmap[i*16+j]);
				sum = sum + seg->seg_bitmap[i*16+j];
			}
			printf("\n");
		}
		print_segment(seg, "Too many invalid");
		printf("cnt: %d, sum: %d\n", cnt, sum);
#endif
		abort();
	}
	return 0;
}

int
dev_abs_read(struct handler *hlr, uint64_t pba, uint32_t size_in_grain,
	     char *buf, struct callback *cb) {

	struct dev_abs *dev = hlr->dev;
	struct segment *seg;

	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t addr_in_byte = pba * dev->grain_unit;

	reap_committed_seg(dev);

	//if (addr_in_byte >= 2097152 && addr_in_byte <= 3097152)
	//	printf("!!!!!!!!!!!!!!!!!!! %d %d\n", addr_in_byte, size);

	if ((seg = is_staged(dev, pba)) != NULL) {
		uint64_t offset = addr_in_byte - seg->start_addr;
		memcpy(buf, (char *)seg->buf + offset, size);

		if (cb->func)
			cb->func(cb->arg);
		q_enqueue((void *)cb, hlr->cb_pool);
		return 1;

	} else if ((seg = is_flying(dev, pba))) {
		uint64_t offset = addr_in_byte - seg->start_addr;
		memcpy(buf, (char *)seg->buf + offset, size);
		//printf("FLYING!!!! seg->start: %llu, seg->offset: %llu, addr: %llu, size: %llu, offset: %llu\n", seg->start_addr, seg->offset, addr_in_byte, size, offset);
		if (offset > SEGMENT_SIZE) abort();

		//return aio_read(hlr, addr_in_byte, size, buf, cb);

		if (cb->func)
			cb->func(cb->arg);
		q_enqueue((void *)cb, hlr->cb_pool);
		return 2;
	} else {
		//seg = dev->staged_idx_seg;
		//if (addr_in_byte > seg->offset) abort();

		return aio_read(hlr, addr_in_byte, size, buf, cb);
	}
}

int
dev_abs_write(struct handler *hlr, uint64_t pba, uint64_t old_pba, uint32_t size_in_grain,
	      char *buf, struct callback *cb) {

	struct dev_abs *dev = hlr->dev;
	struct segment *ss = dev->staged_seg;

	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t offset = (pba * dev->grain_unit) - ss->start_addr;

	reap_committed_seg(dev);

	if (old_pba == 33575920) {
		printf("pba: %lu, old_pba: %lu, size_in_grain: %u\n", pba, old_pba, size_in_grain);
		print_segment(ss, "write");
	}

	if (offset > SEGMENT_SIZE) abort();

	memcpy((char *)ss->buf + offset, buf, size);

	ss->entry_cnt++;

	cb->func(cb->arg);
	q_enqueue((void *)cb, hlr->cb_pool);
	
	return invalidate_seg_entry(hlr, old_pba, false);
}

int
dev_abs_idx_write(struct handler *hlr, uint64_t pba, uint64_t old_pba, uint32_t size_in_grain,
	      char *buf, struct callback *cb, uint64_t part_idx) {

	struct dev_abs *dev = hlr->dev;
	struct segment *ss = dev->staged_idx_seg;

	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t offset = (pba * dev->grain_unit) - ss->start_addr;

	reap_committed_seg(dev);

	if (offset > SEGMENT_SIZE) abort();

	/*
	if (old_pba == 33575920) {
		printf("pba: %lu, old_pba: %lu, size_in_grain: %u\n", pba, old_pba, size_in_grain);
		print_segment(ss, "idx_write");
	}
	*/


	memcpy((char *)ss->buf + offset, buf, size);

	memcpy((char*)ss->buf + (ss->entry_cnt * PART_IDX_SIZE), &part_idx, PART_IDX_SIZE);
	/*
	if (ss->entry_cnt == 0) {
		//uint64_t tmp = *ss->buf;
		printf("idx: %d, part_idx: %lu\n", ss->idx, part_idx);
	}
	*/

	ss->entry_cnt++;

	cb->func(cb->arg);
	q_enqueue((void *)cb, hlr->cb_pool);

	return invalidate_seg_entry(hlr, old_pba, true);
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

static void *cb_commit_seg (void *arg) {
	struct segment *seg = (struct segment *)arg;
	//struct dev_abs *dev = (struct dev_abs *)seg->_private;

	//pthread_mutex_lock(&dev->flying_seg_lock);
	//list_delete_node(dev->flying_seg_lock, lnode);
	seg->state |= SEG_STATE_COMMITTED;
	//pthread_mutex_unlock(&dev->flying_seg_lock);

	//pthread_mutex_lock(&dev->committed_seg_lock);
	//seg->lnode = list_insert(dev->committed_seg_list, seg);
	//pthread_mutex_unlock(&dev->committed_seg_lock);
	return NULL;
}


static struct segment *
stage_next_segment(struct dev_abs *dev) {
	dev->staged_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_seg->state = SEG_STATE_DATA;
	dev->staged_seg->offset = dev->staged_seg->start_addr;
	dev->staged_seg->buf = alloc_seg_buffer(dev->segment_size);
	dev->nr_free_segment--;
	return dev->staged_seg;
}

uint64_t get_next_pba(struct handler *hlr, uint32_t size) {
	uint64_t pba;
	struct dev_abs *dev = hlr->dev;
	struct segment *ss = dev->staged_seg;

	struct callback *new_cb;

	// if there is no space to alloc kv-pair, need to reclaim old segment
	if (ss->offset + size > ss->end_addr) {
		//if (ss->offset % dev->segment_size)
		//	printf("[next_pba] ss->start: %llu, ss->offset: %llu\n", ss->start_addr, ss->offset);
		//ss->state = SEG_STATE_FLYING;
		ss->_private = (void *)dev;

		//new_cb = make_callback(hlr, reap_seg_buf, dev->staged_seg_buf);
		ss->lnode = list_insert(dev->flying_seg_list, ss);
		new_cb = make_callback(hlr, cb_commit_seg, ss);
		aio_write(hlr, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_seg->buf, new_cb);

		ss = stage_next_segment(dev);
	}

	pba = ss->offset / dev->grain_unit;

	if (size % dev->grain_unit != 0) {
		abort();
		size += (dev->grain_unit-(size%dev->grain_unit));
		if (size % dev->grain_unit != 0) abort(); // FIXME: remove later
	}
	ss->offset += size;

	return pba;
}


static struct segment *
stage_next_idx_segment(struct dev_abs *dev) {
	dev->staged_idx_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_idx_seg->state = SEG_STATE_IDX;
	dev->staged_idx_seg->offset = dev->staged_idx_seg->start_addr + SEG_IDX_HEADER_SIZE;
	dev->staged_idx_seg->buf = alloc_seg_buffer(dev->segment_size);
	dev->nr_free_segment--;
	return dev->staged_idx_seg;
}

uint64_t get_next_idx_pba(struct handler *hlr, uint32_t size) {
	uint64_t pba;
	struct dev_abs *dev = hlr->dev;
	struct segment *ss = dev->staged_idx_seg;

	struct callback *new_cb;

	if (size % 4096) abort();
	// if there is no space to alloc kv-pair, need to reclaim old segment
	if (ss->offset + size > ss->end_addr) {
		//if (ss->offset % dev->segment_size)
		//	printf("[next_idx_pba] ss->start: %llu, ss->offset: %llu\n", ss->start_addr, ss->offset);
		//ss->state = SEG_STATE_FLYING;
		ss->_private = (void *)dev;

		//new_cb = make_callback(hlr, reap_seg_buf, dev->staged_seg_buf);
		ss->lnode = list_insert(dev->flying_seg_list, ss);
		new_cb = make_callback(hlr, cb_commit_seg, ss);
		aio_write(hlr, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_idx_seg->buf, new_cb);

		ss = stage_next_idx_segment(dev);
	}

	pba = ss->offset / dev->grain_unit;

	if (size % dev->grain_unit != 0) {
		abort();
		size += (dev->grain_unit-(size%dev->grain_unit));
		if (size % dev->grain_unit != 0) abort(); // FIXME: remove later
	}
	ss->offset += size;

	return pba;
}

bool dev_need_gc(struct handler *hlr) {
	struct dev_abs *dev = hlr->dev;
	if (dev->nr_free_segment <= GC_TRIGGER_THRESHOLD) {
		dev->gc_trigger_cnt++;
		return true;
	}
	return false;
	
}

static bool is_victim(struct dev_abs *dev, struct segment *seg) {
	uint32_t valid_cnt = seg->entry_cnt - seg->invalid_cnt;
	if (valid_cnt > seg->entry_cnt * GC_VICTIM_THRESHOLD) {
		dev->victim_larger_valid_seg_cnt++;
		if (seg->state & SEG_STATE_IDX) dev->victim_larger_valid_idx_seg_cnt++;
		if (seg->state & SEG_STATE_DATA) dev->victim_larger_valid_data_seg_cnt++;
		dev->fail_victim_entry_cnt += seg->entry_cnt;
		dev->fail_victim_invalid_cnt += seg->invalid_cnt;
		printf("entry: %d, invalid: %d, valid: %f\n",seg->entry_cnt, seg->invalid_cnt, (double)valid_cnt/seg->entry_cnt);
		//return false;
		return true;
	}

	if (seg->state & SEG_STATE_IDX) {
		dev->victim_idx_seg_cnt++;
		dev->victim_invalid_idx_cnt += seg->invalid_cnt;
		dev->victim_entry_idx_cnt += seg->entry_cnt;
	} else if (seg->state & SEG_STATE_DATA) {
		dev->victim_data_seg_cnt++;
		dev->victim_invalid_data_cnt += seg->invalid_cnt;
		dev->victim_entry_data_cnt += seg->entry_cnt;
	} else {
		printf("invalid seg state\n");
		abort();
	}

	return true;
}

static struct segment *select_gc_victim_seg(struct handler *hlr) {
	struct dev_abs *dev = hlr->dev;
	list *committed_seg_list = dev->committed_seg_list;
	li_node *cur;
	struct segment *seg;
	list_for_each_node(committed_seg_list, cur) {
		seg = (struct segment *)cur->data;
		if (is_victim(dev, seg)) {
			goto success;
		}
	}

	dev_print_gc_info(hlr);
	printf("fail to select a victim segment, valid: %f\n", \
			1 - ((double)dev->fail_victim_invalid_cnt/(double)dev->fail_victim_entry_cnt));

	return NULL;

success:
	return seg;
}


static void *cb_read_victim(void *arg) {
	struct gc *gc = (struct gc *)arg;
	pthread_mutex_lock(&gc->mutex);
	pthread_cond_signal(&gc->cond);
	pthread_mutex_unlock(&gc->mutex);
	return NULL;
}


int dev_read_victim_segment(struct handler *hlr, struct gc *gc) {
	struct dev_abs *dev = hlr->dev;	
	struct segment *victim_seg = select_gc_victim_seg(hlr);

	if (!victim_seg)
		abort();

	if (victim_seg->entry_cnt == victim_seg->invalid_cnt) {
		if (victim_seg->state & SEG_STATE_IDX) {
			dev->victim_trim_idx_seg_cnt++;
			gc->is_idx = 1;
		} else {
			dev->victim_trim_data_seg_cnt++;
			gc->is_idx = 0;
		}
		//printf("무야호 IDX: %d, DATA: %d\n",dev->victim_trim_idx_seg_cnt, dev->victim_trim_data_seg_cnt);
		gc->_private = victim_seg;
		gc->valid_cnt = 0;

		return 0;
	}

	uint32_t size = victim_seg->offset - victim_seg->start_addr;
	uint64_t addr_in_byte = victim_seg->start_addr;

	struct callback *cb = make_callback(hlr, cb_read_victim, gc);
	pthread_mutex_lock(&gc->mutex);
	aio_read(hlr, addr_in_byte, size, (char *)gc->buf, cb);
	pthread_cond_wait(&gc->cond, &gc->mutex);
	pthread_mutex_unlock(&gc->mutex);

	gc->_private = victim_seg;
	gc->valid_cnt = victim_seg->entry_cnt - victim_seg->invalid_cnt;
	gc->is_idx = victim_seg->state & SEG_STATE_IDX;


	print_segment(victim_seg, "victim");

	return 1;

}

bool is_idx_seg(struct gc *gc) {
	// if a victim segment is a index segment, it returns 1. Otherwise, returns 0.
	struct segment *seg = (struct segment *)gc->_private;

	return seg->state & SEG_STATE_IDX;
}

void reap_gc_segment(struct handler *hlr, struct gc *gc) {
	struct segment *victim = (struct segment *)gc->_private;
	struct dev_abs *dev = hlr->dev;	

	list_delete_node(dev->committed_seg_list, victim->lnode);
	victim->state = SEG_STATE_FREE;
	victim->entry_cnt = 0;
	victim->invalid_cnt = 0;
	victim->lnode = NULL;
	victim->offset = victim->start_addr;
	victim->age = ((++dev->age) / dev->nr_segment) % MAX_SEG_AGE;

#ifdef DEBUG_GC
	memset(victim->seg_bitmap, 0, 2048);
#endif
	
	dev->nr_free_segment++;

	q_enqueue((void *)victim, dev->free_seg_q);
}

void dev_print_gc_info (struct handler *hlr) {
	struct dev_abs *dev = hlr->dev;

	printf("gc_trigger_cnt: %u\n", dev->gc_trigger_cnt);
	printf("victim_larger_valid_seg_cnt: %u\n", dev->victim_larger_valid_seg_cnt);
	printf("victim_larger_valid_idx_seg_cnt: %u\n", dev->victim_larger_valid_idx_seg_cnt);
	printf("victim_larger_valid_data_seg_cnt: %u\n", dev->victim_larger_valid_data_seg_cnt);
	printf("\n");


	printf("invalid_seg_cnt: %u\n", dev->invalid_seg_cnt);
	printf("victim_trim_idx_seg_cnt: %lu\n", dev->victim_trim_idx_seg_cnt);
	printf("victim_trim_data_seg_cnt: %lu\n", dev->victim_trim_data_seg_cnt);
	printf("\n");


	printf("victim_idx_seg_cnt: %u\n", dev->victim_idx_seg_cnt);
	printf("victim_invalid_data_cnt: %lu\n", dev->victim_invalid_data_cnt);
	printf("victim_entry_idx_cnt: %lu\n", dev->victim_entry_idx_cnt);
	printf("\n");


	printf("victim_data_seg_cnt: %u\n", dev->victim_data_seg_cnt);
	printf("victim_invalid_data_cnt: %lu\n", dev->victim_invalid_data_cnt);
	printf("victim_entry_data_cnt: %lu\n", dev->victim_entry_data_cnt);
	printf("\n");


	printf("fail_victim_entry_cnt: %lu\n", dev->fail_victim_entry_cnt);
	printf("fail_victim_invalid_cnt: %lu\n", dev->fail_victim_invalid_cnt);
	printf("\n");
}
