#include "config.h"
#include "platform/device.h"
#include "platform/aio.h"
#include "platform/uring.h"
#include "platform/request.h"
#include "platform/dev_spdk.h"
#include "utility/stopwatch.h"
#include "utility/ttl.h"
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
extern uint64_t debug_pba;
extern int dev_cap;

#ifdef TTL_GROUP
static uint64_t ttl_group_std[NR_TTL_GROUPS] = {
	60,
	120,
	180,
	240,
	300,
	360,
	420,
	480,
	540,
	600, // 10 mins
	1200, // 20 mins
	1800, // 30 mins
	2400, // 40 mins
	3000, // 50 mins
	3600, // 1 hour
	7200, // 2 hours
	10800, // 3 hours
	14400, // 4
	18000, // 5
	21600, // 6
	43200, // 12 hours
	86400, // 1 day
	172800, // 2 day
	259200, // 3 day
	345600, // 4 day
	432000, // 5 day
	518400, // 6 day
	604800, // 7 day
	1209600, // 14 day
	2592000, // 30 day
	5184000, // 60 day
	8640000 // 100 day
};

uint32_t get_ttl_idx (uint32_t ttl) {
	int l = 0, r = NR_TTL_GROUPS, m;
	while (l < r) {
		m = (l + r) / 2;
		if (ttl_group_std[m] >= ttl)
			r = m;
		else
			l = m + 1;
	}
	return r;
}
#endif

static void
print_device_init(struct dev_abs *dev) {
	printf("dev_abs: %s is initialized\n", dev->dev_name);
	printf("|-- Logical Block Size: %lu B\n", dev->logical_block_size);
	printf("|-- Logical Blocks: %lu\n", dev->nr_logical_block);
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
#elif DEV_SPDK
	//void *seg_buffer = aligned_alloc(MEM_ALIGN_UNIT, size);
	void *seg_buffer = spdk_dma_zmalloc(size, MEM_ALIGN_UNIT, NULL);
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

#ifndef TTL_GROUP
struct dev_abs *
dev_abs_init(struct handler *hlr, const char dev_name[], int core_mask, int dev_number) {
	struct dev_abs *dev = (struct dev_abs *)calloc(1, sizeof(struct dev_abs));

	strcpy(dev->dev_name, dev_name);
	dev->dev_number = dev_number;

#ifndef DEV_SPDK
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
#elif DEV_SPDK
	//dev_spdk_open(&dev->sctx, dev->dev_name, core_mask);
	dev->sctx = hlr->sctx+dev_number;
	//struct ns_entry *entry = TAILQ_FIRST(&dev->sctx->namespaces);
	struct ns_entry *entry;
	TAILQ_FOREACH(entry, &dev->sctx->namespaces, link) {
		dev->nr_logical_block = spdk_nvme_ns_get_num_sectors(entry->u.nvme.ns);
		dev->logical_block_size = spdk_nvme_ns_get_sector_size(entry->u.nvme.ns);
	}
#endif

	printf("asdasd %lu\n", dev->size_in_byte);

#ifdef TEST_GC
	dev->size_in_byte = TEST_GC_CAPACITY;
#elif TTL
	if (dev_cap)
		dev->size_in_byte = 1024ULL * 1024 * 1024 * dev_cap;
	else
		dev->size_in_byte = (uint64_t)dev->nr_logical_block * dev->logical_block_size;
#else
	//dev->size_in_byte = 1024ULL * 1024 * 1024 * 96;
	dev->size_in_byte = (uint64_t)dev->nr_logical_block * dev->logical_block_size;
#endif


	printf("asdasd %lu\n", dev->size_in_byte);

	dev->segment_size = SEGMENT_SIZE;
	dev->nr_segment = dev->size_in_byte / dev->segment_size;

	printf("%d\n", dev->nr_segment);
	dev->seg_array =
		(struct segment *)calloc(dev->nr_segment, sizeof(struct segment));

	q_init(&dev->free_seg_q, dev->nr_segment);
	dev->flying_seg_list = list_init();
	dev->committed_seg_list = list_init();
	dev->dummy_committed_seg_list = list_init();

	pthread_mutex_init(&dev->flying_seg_lock, NULL);
	pthread_mutex_init(&dev->committed_seg_lock, NULL);
	pthread_mutex_init(&dev->dev_lock, NULL);

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

#ifdef RAMDISK
	dev->ram_disk = (char **)malloc(sizeof(char*) * (RAM_SIZE/4096));
	if (dev->ram_disk == NULL)
		abort();
	for (int i = 0; i < RAM_SIZE/4096; i++) {
		dev->ram_disk[i] = (char *)malloc(4096);
		if (dev->ram_disk[i] == NULL)
			abort();
	}
#endif
#ifdef URING
	int ret_val;
	if (ret_val = io_uring_queue_init(URING_QD, &dev->ring, 0)) {
		if(ret_val < 0) {
			fprintf(stderr, "queue_init: %s\n", strerror(-ret_val));
		}	
	}
#endif
#ifdef LINUX_AIO
	memset(&dev->aio_ctx, 0, sizeof(io_context_t));
	if (io_setup(QDEPTH*2, &dev->aio_ctx) < 0) {
		perror("io_setup");
		abort();
	}
#endif

	print_device_init(dev);

	return dev;
}

int
dev_abs_free(struct dev_abs *dev) {
	struct segment *seg;
	li_node *cur;
#ifndef DEV_SPDK
	close(dev->dev_fd);
#endif
#ifdef USE_HUGEPAGE
	munmap(dev->staged_seg->buf, dev->segment_size);
	munmap(dev->staged_idx_seg->buf, dev->segment_size);
#elif DEV_SPDK
	spdk_dma_free(dev->staged_seg->buf);
	spdk_dma_free(dev->staged_idx_seg->buf);
#else
	free(dev->staged_seg->buf);
	free(dev->staged_idx_seg->buf);
#endif
	list_for_each_node(dev->flying_seg_list, cur) {
		seg = (struct segment *)cur->data;
		while (!(seg->state & SEG_STATE_COMMITTED));
#ifdef USE_HUGEPAGE
		munmap(seg->buf, dev->segment_size);
#elif DEV_SPDK
		spdk_dma_free(seg->buf);
#else
		free(seg->buf);
#endif
	}
	list_free(dev->flying_seg_list);
	list_free(dev->committed_seg_list);
	list_free(dev->dummy_committed_seg_list);
	
	while((seg = (struct segment *)q_dequeue(dev->free_seg_q))) {
		//free(seg);
	}
	free(dev->seg_array);
	
#ifdef RAMDISK
	for (int i = 0; i < RAM_SIZE/4096; i++) {
		free(dev->ram_disk[i]);
		//printf("dev->ramdisk[%d]: %p\n", i, dev->ram_disk[i]);
		//if (dev->ram_disk[i] == NULL)
		//	abort();
	}

	free(dev->ram_disk);
#endif
#ifdef URING
	io_uring_queue_exit(&dev->ring);
#endif
#ifdef LINUX_AIO
	io_destroy(dev->aio_ctx);
#endif
#ifdef DEV_SPDK
	dev_spdk_close(dev->sctx);
	free(dev->sctx);
#endif
	q_free(dev->free_seg_q);
	return 0;
}
#else // TTL_GROUP
struct dev_abs *
dev_abs_init(struct handler *hlr, const char dev_name[], int core_mask, int dev_number) {
	struct dev_abs *dev = (struct dev_abs *)calloc(1, sizeof(struct dev_abs));

	strcpy(dev->dev_name, dev_name);
	dev->dev_number = dev_number;

#ifndef DEV_SPDK
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
#elif DEV_SPDK
	//dev_spdk_open(&dev->sctx, dev->dev_name, core_mask);
	dev->sctx = hlr->sctx+dev_number;
	//struct ns_entry *entry = TAILQ_FIRST(&dev->sctx->namespaces);
	struct ns_entry *entry;
	TAILQ_FOREACH(entry, &dev->sctx->namespaces, link) {
		dev->nr_logical_block = spdk_nvme_ns_get_num_sectors(entry->u.nvme.ns);
		dev->logical_block_size = spdk_nvme_ns_get_sector_size(entry->u.nvme.ns);
	}
#endif

	if (dev_cap)
		dev->size_in_byte = 1024ULL * 1024 * 1024 * dev_cap;
	else
		dev->size_in_byte = (uint64_t)dev->nr_logical_block * dev->logical_block_size;

	dev->segment_size = SEGMENT_SIZE;
	dev->nr_segment = dev->size_in_byte / dev->segment_size;

	printf("%d\n", dev->nr_segment);
	dev->seg_array =
		(struct segment *)calloc(dev->nr_segment, sizeof(struct segment));

	q_init(&dev->free_seg_q, dev->nr_segment);
	dev->flying_seg_list = list_init();
	dev->committed_seg_list = list_init();
	dev->dummy_committed_seg_list = list_init();

	pthread_mutex_init(&dev->flying_seg_lock, NULL);
	pthread_mutex_init(&dev->committed_seg_lock, NULL);
	pthread_mutex_init(&dev->dev_lock, NULL);

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
		seg->creation_time = 0;
#ifdef DEBUG_GC
		seg->seg_bitmap = (uint8_t *)calloc(1, 2048);
#endif
		q_enqueue((void *)seg, dev->free_seg_q);

	}

	for (int i = 0; i < NR_TTL_GROUPS; i++) {
		dev->staged_segs[i] = (struct segment *)q_dequeue(dev->free_seg_q);
		dev->staged_segs[i]->state = SEG_STATE_DATA;
		dev->staged_segs[i]->buf = alloc_seg_buffer(dev->segment_size);
		dev->ttl_fifo[i] = list_init();
	}
	//dev->staged_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	//dev->staged_seg->state = SEG_STATE_DATA;
	//dev->staged_seg->buf = alloc_seg_buffer(dev->segment_size);

	dev->staged_idx_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_idx_seg->state = SEG_STATE_IDX;
	dev->staged_idx_seg->buf = alloc_seg_buffer(dev->segment_size);
	dev->staged_idx_seg->offset = dev->staged_idx_seg->start_addr + SEG_IDX_HEADER_SIZE;

	dev->nr_free_segment = dev->nr_segment - (NR_TTL_GROUPS + 1);
	dev->grain_unit = GRAIN_UNIT;
	dev->age = 0;

#ifdef RAMDISK
	dev->ram_disk = (char **)malloc(sizeof(char*) * (RAM_SIZE/4096));
	if (dev->ram_disk == NULL)
		abort();
	for (int i = 0; i < RAM_SIZE/4096; i++) {
		dev->ram_disk[i] = (char *)malloc(4096);
		if (dev->ram_disk[i] == NULL)
			abort();
	}
#endif
#ifdef URING
	int ret_val;
	if (ret_val = io_uring_queue_init(URING_QD, &dev->ring, 0)) {
		if(ret_val < 0) {
			fprintf(stderr, "queue_init: %s\n", strerror(-ret_val));
		}	
	}
#endif
#ifdef LINUX_AIO
	memset(&dev->aio_ctx, 0, sizeof(io_context_t));
	if (io_setup(QDEPTH*2, &dev->aio_ctx) < 0) {
		perror("io_setup");
		abort();
	}
#endif

	print_device_init(dev);

	return dev;
}

int
dev_abs_free(struct dev_abs *dev) {
	struct segment *seg;
	li_node *cur;
#ifndef DEV_SPDK
	close(dev->dev_fd);
#endif
#ifdef USE_HUGEPAGE
	munmap(dev->staged_idx_seg->buf, dev->segment_size);
#elif DEV_SPDK
	spdk_dma_free(dev->staged_idx_seg->buf);
#else
	free(dev->staged_idx_seg->buf);
#endif
	list_for_each_node(dev->flying_seg_list, cur) {
		seg = (struct segment *)cur->data;
		while (!(seg->state & SEG_STATE_COMMITTED));
#ifdef USE_HUGEPAGE
		munmap(seg->buf, dev->segment_size);
#elif DEV_SPDK
		spdk_dma_free(seg->buf);
#else
		free(seg->buf);
#endif
	}
	list_free(dev->flying_seg_list);
	list_free(dev->committed_seg_list);
	list_free(dev->dummy_committed_seg_list);
	for (int i = 0; i < NR_TTL_GROUPS; i++) {
#ifdef USE_HUGEPAGE
		munmap(dev->staged_segs[i]->buf, dev->segment_size);
#elif DEV_SPDK
		spdk_dma_free(dev->staged_segs[i]->buf);
#else
		free(dev->staged_segs[i]->buf);
#endif
		list_free(dev->ttl_fifo[i]);
	}
	
	while((seg = (struct segment *)q_dequeue(dev->free_seg_q))) {
		//free(seg);
	}
	free(dev->seg_array);
	
#ifdef RAMDISK
	for (int i = 0; i < RAM_SIZE/4096; i++) {
		free(dev->ram_disk[i]);
		//printf("dev->ramdisk[%d]: %p\n", i, dev->ram_disk[i]);
		//if (dev->ram_disk[i] == NULL)
		//	abort();
	}

	free(dev->ram_disk);
#endif
#ifdef URING
	io_uring_queue_exit(&dev->ring);
#endif
#ifdef LINUX_AIO
	io_destroy(dev->aio_ctx);
#endif
#ifdef DEV_SPDK
	dev_spdk_close(dev->sctx);
	free(dev->sctx);
#endif
	q_free(dev->free_seg_q);
	return 0;
}
#endif // TTL_GROUP

#ifndef TTL_GROUP
static struct segment *
is_staged(struct dev_abs *dev, uint64_t pba) {
	uint64_t addr_in_byte = pba * dev->grain_unit;
	struct segment *ss;

	ss = dev->staged_seg;

	if (addr_in_byte >= ss->start_addr && addr_in_byte < ss->offset) {
		return ss;
	}

	ss = dev->staged_idx_seg;
	if (addr_in_byte >= ss->start_addr && addr_in_byte < ss->offset) {
		return ss;
	}
	return NULL;
}
#else // TTL_GROUP
static struct segment *
is_staged(struct dev_abs *dev, uint64_t pba) {
	uint64_t addr_in_byte = pba * dev->grain_unit;
	struct segment *ss;

	ss = dev->staged_idx_seg;
	if (addr_in_byte >= ss->start_addr && addr_in_byte < ss->offset) {
		return ss;
	}

	for (int i = 0; i < NR_TTL_GROUPS; i++) {
		ss = dev->staged_segs[i];
		if (addr_in_byte >= ss->start_addr && addr_in_byte < ss->offset) {
			return ss;
		}
	}
	return NULL;
}
#endif

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

#ifndef TTL_GROUP
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
#ifdef USE_HUGEPAGE
				munmap(seg->buf, SEGMENT_SIZE);
#elif DEV_SPDK
				spdk_dma_free(seg->buf);
#else
				free(seg->buf);
#endif
			seg->buf = NULL;
		}
	}
	return 0;
}
#else // TTL_GROUP
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
			if (seg->buf)
#ifdef USE_HUGEPAGE
				munmap(seg->buf, SEGMENT_SIZE);
#elif DEV_SPDK
				spdk_dma_free(seg->buf);
#else
				free(seg->buf);
#endif
			seg->buf = NULL;

			if (seg->state & SEG_STATE_IDX)
				seg->lnode = list_insert(dev->committed_seg_list, seg);
			else
				seg->ttl_lnode = list_insert(dev->ttl_fifo[seg->ttl], seg);
		}
	}
	return 0;
}
#endif


void reap_segment(struct dev_abs *dev, struct segment *seg) {
	struct segment *victim = seg;

#ifdef TTL_GROUP
	if (victim->state & SEG_STATE_IDX) {
		list_delete_node(dev->committed_seg_list, victim->lnode);
	} else {
		list_delete_node(dev->ttl_fifo[victim->ttl], victim->ttl_lnode);
	}
#else
	list_delete_node(dev->committed_seg_list, victim->lnode);
#endif
	victim->state = SEG_STATE_FREE;
	victim->entry_cnt = 0;
	victim->invalid_cnt = 0;
	victim->lnode = NULL;
	victim->ttl_lnode = NULL;
	victim->offset = victim->start_addr;
	victim->age = ((++dev->age) / dev->nr_segment) % MAX_SEG_AGE;
	victim->creation_time = 0;

#ifdef DEBUG_GC
	memset(victim->seg_bitmap, 0, 2048);
#endif
	
	dev->nr_free_segment++;

	q_enqueue((void *)victim, dev->free_seg_q);
}


static int invalidate_seg_entry(struct handler *hlr, struct dev_abs *dev, uint64_t pba, bool is_idx_write) {
	uint64_t seg_idx = pba * GRAIN_UNIT/ dev->segment_size;
	struct segment *seg = dev->seg_array + seg_idx;

	if (!is_idx_write) {
		return -1;
	}

	if (pba > dev->size_in_byte / GRAIN_UNIT)
		return -1;

	// the data is removed by data GC
	if (seg->state == SEG_STATE_FREE) 
		return -1;

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
		//reap_segment(dev, seg);
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
		print_segment(seg, "Too many invalid\n");
		printf("cnt: %d, sum: %d\n", cnt, sum);
#endif
		abort();
	}
	return 0;
}

int
dev_abs_read(struct handler *hlr, uint64_t pba, uint32_t size_in_grain,
	     char *buf, struct callback *cb, int hlr_idx, int dev_idx) {
	struct dev_abs *dev;
	int ret = 0;
	struct request *req = (struct request *)cb->arg;

	struct segment *seg = NULL;

	uint32_t size;
	uint64_t addr_in_byte;

#ifndef PER_CORE
	hlr = hlr - hlr->number + hlr_idx;
#endif
	dev = hlr->dev[dev_idx];

	size = size_in_grain * dev->grain_unit;
	addr_in_byte = pba * dev->grain_unit;

	reap_committed_seg(dev);


	//if (addr_in_byte >= 2097152 && addr_in_byte <= 3097152)
	//	printf("!!!!!!!!!!!!!!!!!!! %d %d\n", addr_in_byte, size);
#ifndef PER_CORE
	pthread_mutex_lock(&dev->dev_lock);
#endif

	/*
	if (pba == debug_pba) {
			printf("[READ@@@@@] %ld %ld %lu %d %p\n", size, addr_in_byte, pba, dev_idx, dev);
	}
	*/


	if ((seg = is_staged(dev, pba)) != NULL) {
		uint64_t offset = addr_in_byte - seg->start_addr;
		memcpy(buf, (char *)seg->buf + offset, size);

#ifdef BREAKDOWN
		if (req->type == REQ_TYPE_GET) {
			sw_end(req->sw_bd[3]);
			sw_start(req->sw_bd[4]);
			sw_end(req->sw_bd[4]);
			sw_start(req->sw_bd[5]);
		}
#endif

		if (cb->func)
			cb->func(cb->arg);
		q_enqueue((void *)cb, cb->hlr->cb_pool);
#ifndef PER_CORE
		pthread_mutex_unlock(&dev->dev_lock);
#endif
		ret = 1;

	} else if ((seg = is_flying(dev, pba)) != NULL) {
		uint64_t offset = addr_in_byte - seg->start_addr;
		memcpy(buf, (char *)seg->buf + offset, size);
		//printf("FLYING!!!! seg->start: %llu, seg->offset: %llu, addr: %llu, size: %llu, offset: %llu\n", seg->start_addr, seg->offset, addr_in_byte, size, offset);
		if (offset > SEGMENT_SIZE) abort();

		//return aio_read(hlr, addr_in_byte, size, buf, cb);


#ifdef BREAKDOWN
		if (req->type == REQ_TYPE_GET) {
			sw_end(req->sw_bd[3]);
			sw_start(req->sw_bd[4]);
			sw_end(req->sw_bd[4]);
			sw_start(req->sw_bd[5]);
		}
#endif

		if (cb->func)
			cb->func(cb->arg);
		q_enqueue((void *)cb, cb->hlr->cb_pool);
#ifndef PER_CORE
		pthread_mutex_unlock(&dev->dev_lock);
#endif
		ret = 2;
	} else {
		uint64_t seg_idx = pba * dev->grain_unit / dev->segment_size;

		seg = dev->seg_array + seg_idx;

		if (!(seg->state | SEG_STATE_COMMITTED))
			abort();
		if (seg->state == SEG_STATE_FREE) {
			q_enqueue((void *)cb, cb->hlr->cb_pool);	
#ifndef PER_CORE
		pthread_mutex_unlock(&dev->dev_lock);
#endif
			//abort();
			printf("FREE SEG!!!\n");
			return -1;
		}
		//seg = dev->staged_idx_seg;
		//if (addr_in_byte > seg->offset) abort();
		//printf("[READ] %ld %ld %d\n", size, addr_in_byte, dev_idx);
		/*
		if (pba == debug_pba) {
			printf("[READ] %ld %ld %lu %d %p\n", size, addr_in_byte, pba, dev_idx, dev);
		}
		fflush(stdout);
		*/
		//
#ifndef PER_CORE
		pthread_mutex_unlock(&dev->dev_lock);
#endif
#ifdef RAMDISK
		uint32_t ram_addr = addr_in_byte/4096;
		uint32_t ram_off = addr_in_byte%4096;
		uint32_t cpy_size;
		char *cpy_buf = buf;
		while (size > 0) {
			cpy_size = size >= 4096 ? 4096 : size%4096;

			//printf("[%p]dev->ram_disk[%d]: %p, %lu\n", dev->ram_disk, ram_addr, dev->ram_disk[ram_addr], addr_in_byte);
			//fflush(stdout);
			memcpy(cpy_buf, (dev->ram_disk[ram_addr]) + ram_off, cpy_size);
			
			size -= cpy_size;
			ram_addr++;
			ram_off = 0;
			cpy_buf += cpy_size;
		}
		if (cb->func)
			cb->func(cb->arg);
		q_enqueue((void *)cb, cb->hlr->cb_pool);
		return 0;
#else

#ifdef BREAKDOWN
		if (req->type == REQ_TYPE_GET) {
			sw_end(req->sw_bd[3]);
			sw_start(req->sw_bd[4]);
		}
#endif
#ifdef LINUX_AIO
		ret = aio_read(hlr, dev, addr_in_byte, size, buf, cb);
#elif URING
		ret = uring_read(hlr, dev, addr_in_byte, size, buf, cb);
#elif DEV_SPDK
		ret = dev_spdk_read(hlr, dev, addr_in_byte, size, buf ,cb);
#endif
#endif
	}
	return ret;
}

int
dev_abs_poller_read(struct handler *hlr, uint64_t pba, uint32_t size_in_grain,
	     char *buf, struct callback *cb, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	struct segment *seg = NULL;

	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t addr_in_byte = pba * dev->grain_unit;

	//if (addr_in_byte >= 2097152 && addr_in_byte <= 3097152)
	//	printf("!!!!!!!!!!!!!!!!!!! %d %d\n", addr_in_byte, size);

	if ((seg = is_staged(dev, pba)) != NULL) {
		uint64_t offset = addr_in_byte - seg->start_addr;
		memcpy(buf, (char *)seg->buf + offset, size);

		if (cb->func)
			cb->func(cb->arg);
		q_enqueue((void *)cb, hlr->cb_pool);
		return 1;

	} else if ((seg = is_flying(dev, pba)) != NULL) {
		uint64_t offset = addr_in_byte - seg->start_addr;
		memcpy(buf, (char *)seg->buf + offset, size);
		//printf("FLYING!!!! seg->start: %llu, seg->offset: %llu, addr: %llu, size: %llu, offset: %llu\n", seg->start_addr, seg->offset, addr_in_byte, size, offset);
		if (offset > SEGMENT_SIZE) abort();

		//return aio_read(hlr, addr_in_byte, size, buf, cb);

		if (cb->func)
			cb->func(cb->arg);
		q_enqueue((void *)cb, cb->hlr->cb_pool);
		return 2;
	} else {
		uint64_t seg_idx = pba * dev->grain_unit / dev->segment_size;
		seg = dev->seg_array + seg_idx;
		if (seg->state == SEG_STATE_FREE) {
			q_enqueue((void *)cb, cb->hlr->cb_pool);	
			return -1;
		}
		//seg = dev->staged_idx_seg;
		//if (addr_in_byte > seg->offset) abort();
		//printf("[poller READ] %ld %ld\n", size, addr_in_byte);

#ifdef LINUX_AIO
		return aio_read(cb->hlr, dev, addr_in_byte, size, buf, cb);
#elif URING
		return uring_read(cb->hlr, dev, addr_in_byte, size, buf, cb);
#elif DEV_SPDK
		return dev_spdk_read(cb->hlr, dev, addr_in_byte, size, buf ,cb);
#endif
	}
}

#ifndef TTL_GROUP
int
dev_abs_write(struct handler *hlr, uint64_t pba, uint64_t old_pba, uint32_t size_in_grain,
	      char *buf, struct callback *cb, int hlr_idx, int dev_idx) {
	struct dev_abs *dev;
	struct segment *ss;

	uint32_t size;
	uint64_t offset;

#ifndef PER_CORE
	hlr = hlr - hlr->number + hlr_idx;
#endif
	dev = hlr->dev[dev_idx];
	ss = dev->staged_seg;
	size = size_in_grain * dev->grain_unit;
	offset = (pba * dev->grain_unit) - ss->start_addr;

#ifndef PER_CORE
	pthread_mutex_lock(&dev->dev_lock);
#endif

	reap_committed_seg(dev);

	if (offset > SEGMENT_SIZE) abort();

	/*
	if (pba == debug_pba) {
		printf("[WRITE] %ld %ld %lu %d %p\n", size, offset, pba, dev_idx, dev);
	}
	*/

	memcpy((char *)ss->buf + offset, buf, size);

	ss->entry_cnt++;

	cb->func(cb->arg);
	q_enqueue((void *)cb, cb->hlr->cb_pool);
	
	int ret = invalidate_seg_entry(cb->hlr, dev, old_pba, false);
#ifndef PER_CORE
	pthread_mutex_unlock(&dev->dev_lock);
#endif
	return ret;
}
#else // TTL_GROUP
int
dev_abs_write(struct handler *hlr, uint64_t pba, uint64_t old_pba, uint32_t size_in_grain,
	      char *buf, struct callback *cb, int hlr_idx, int dev_idx, uint32_t ttl) {
	struct dev_abs *dev;
	struct segment *ss;

	uint32_t size;
	uint64_t offset;

	dev = hlr->dev[dev_idx];
	ss = dev->staged_segs[get_ttl_idx(ttl)];
	size = size_in_grain * dev->grain_unit;
	offset = (pba * dev->grain_unit) - ss->start_addr;

	reap_committed_seg(dev);

	if (offset > SEGMENT_SIZE) abort();

	memcpy((char *)ss->buf + offset, buf, size);

	ss->entry_cnt++;

	cb->func(cb->arg);
	q_enqueue((void *)cb, cb->hlr->cb_pool);
	
	int ret = invalidate_seg_entry(cb->hlr, dev, old_pba, false);
	return ret;
}
#endif

int
dev_abs_idx_write(struct handler *hlr, uint64_t pba, uint64_t old_pba, uint32_t size_in_grain,
	      char *buf, struct callback *cb, uint64_t part_idx, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	struct segment *ss = dev->staged_idx_seg;

	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t offset = (pba * dev->grain_unit) - ss->start_addr;

	reap_committed_seg(dev);

	if (offset >= SEGMENT_SIZE) abort();

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
	//printf("IDX WRITE pba: %lu, old_pba: %lu, part_idx: %lu, offset: %lu\n", pba,old_pba, part_idx, offset);

	ss->entry_cnt++;

	cb->func(cb->arg);
	q_enqueue((void *)cb, cb->hlr->cb_pool);

	return invalidate_seg_entry(cb->hlr, dev, old_pba, true);
}

int
dev_abs_idx_overwrite(struct handler *hlr, uint64_t pba, uint32_t size_in_grain,
	      char *buf, struct callback *cb, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t offset = pba * dev->grain_unit;


	/*
	if (old_pba == 33575920) {
		printf("pba: %lu, old_pba: %lu, size_in_grain: %u\n", pba, old_pba, size_in_grain);
		print_segment(ss, "idx_write");
	}
	*/


#ifdef LINUX_AIO
	return aio_write(cb->hlr, dev, offset, size, buf, cb);
#elif URING
	return uring_write(cb->hlr, dev, offset, size, buf, cb);
#elif DEV_SPDK
	return dev_spdk_write(cb->hlr, dev, offset, size, buf, cb);
#endif


	/*
	if (ss->entry_cnt == 0) {
		//uint64_t tmp = *ss->buf;
		printf("idx: %d, part_idx: %lu\n", ss->idx, part_idx);
	}
	*/

	return 0;
}

int
dev_abs_sync_write(struct handler *hlr, uint64_t pba, uint32_t size_in_grain, char *buf, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t offset = pba * dev->grain_unit;

	//printf("[SYNC WRITE] %ld %ld\n", size, offset);
	
	//if (((pba < 4224 * GRAIN_UNIT + 16384) && (pba + size > 4224 * GRAIN_UNIT)) || ((pba >= 4224 * GRAIN_UNIT) && pba < (4224 * GRAIN_UNIT + 16384)))
	//	printf("SIBAL %lu %u\n", pba, size);

	return pwrite(dev->dev_fd, buf, size, offset);


	/*
	if (ss->entry_cnt == 0) {
		//uint64_t tmp = *ss->buf;
		printf("idx: %d, part_idx: %lu\n", ss->idx, part_idx);
	}
	*/
}

int
dev_abs_sync_read(struct handler *hlr, uint64_t pba, uint32_t size_in_grain, char *buf, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	uint32_t size = size_in_grain * dev->grain_unit;
	uint64_t offset = pba * dev->grain_unit;

	//printf("[SYNC READ] %ld %ld\n", size, offset);

	return pread(dev->dev_fd, buf, size, offset);

	/*
	if (ss->entry_cnt == 0) {
		//uint64_t tmp = *ss->buf;
		printf("idx: %d, part_idx: %lu\n", ss->idx, part_idx);
	}
	*/
}



static void *
reap_seg_buf(void *arg) {
	void *seg_buf = arg;
#ifdef USE_HUGEPAGE
	munmap(seg_buf, SEGMENT_SIZE);
#elif DEV_SPDK
	spdk_dma_free(seg_buf);
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

#ifndef TTL_GROUP
static struct segment *
stage_next_segment(struct dev_abs *dev) {
	dev->staged_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_seg->state = SEG_STATE_DATA;
	dev->staged_seg->offset = dev->staged_seg->start_addr;
	dev->staged_seg->buf = alloc_seg_buffer(dev->segment_size);
	dev->nr_free_segment--;
	return dev->staged_seg;
}

uint64_t get_next_pba(struct handler *hlr, uint32_t size, int hlr_idx, int dev_idx) {
	struct dev_abs *dev;
	uint64_t pba;
	struct segment *ss;

	struct callback *new_cb;

#ifndef PER_CORE
	hlr = hlr - hlr->number + hlr_idx;
#endif
	dev = hlr->dev[dev_idx];
	ss = dev->staged_seg;

#ifndef PER_CORE
	pthread_mutex_lock(&dev->dev_lock);
#endif
	// if there is no space to alloc kv-pair, need to reclaim old segment
	if (ss->offset + size > ss->end_addr) {
		//if (ss->offset % dev->segment_size)
		//printf("[next_pba] ss->start: %llu, ss->offset: %llu\n", ss->start_addr, ss->offset);
		//ss->state = SEG_STATE_FLYING;
		ss->_private = (void *)dev;

		//new_cb = make_callback(hlr, reap_seg_buf, dev->staged_seg_buf);
		ss->lnode = list_insert(dev->flying_seg_list, ss);
#ifdef RAMDISK
		//memcpy(dev->ram_disk + ss->start_addr, (char *)dev->staged_seg->buf, dev->segment_size);
		for (int i = 0; i < dev->segment_size/4096; i++) {
			memcpy(dev->ram_disk[ss->start_addr/4096 + i], (char *)(ss->buf) + i * 4096, 4096);
		}
		cb_commit_seg((void *)ss);
#else
		new_cb = make_callback(hlr, cb_commit_seg, ss);
#ifdef LINUX_AIO
		aio_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_seg->buf, new_cb);
#elif URING
		uring_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_seg->buf, new_cb);
#elif DEV_SPDK 
		dev_spdk_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_seg->buf, new_cb);
#endif
#endif

		ss = stage_next_segment(dev);
	}

	pba = ss->offset / dev->grain_unit;

	if (size % dev->grain_unit != 0) {
		//abort();
		size += (dev->grain_unit-(size%dev->grain_unit));
		if (size % dev->grain_unit != 0) abort(); // FIXME: remove later
	}
	ss->offset += size;

#ifndef PER_CORE
	pthread_mutex_unlock(&dev->dev_lock);
#endif

	//printf("get next pba size: %u, pba: %lu, dev_idx: %d\n", size, pba, dev_idx);

	return pba;
}
#else // TTL_GROUP
static struct segment *
stage_next_segment(struct handler *hlr, struct dev_abs *dev, uint32_t ttl) {
	dev->staged_segs[ttl] = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_segs[ttl]->state = SEG_STATE_DATA;
	dev->staged_segs[ttl]->offset = dev->staged_segs[ttl]->start_addr;
	dev->staged_segs[ttl]->buf = alloc_seg_buffer(dev->segment_size);
	dev->staged_segs[ttl]->creation_time = get_cur_sec();
	dev->staged_segs[ttl]->ttl = ttl;
	dev->nr_free_segment--;
	return dev->staged_segs[ttl];
}

uint64_t get_next_pba(struct handler *hlr, uint32_t size, int hlr_idx, int dev_idx, uint32_t ttl) {
	struct dev_abs *dev;
	uint64_t pba;
	struct segment *ss;

	uint32_t ttl_idx = get_ttl_idx(ttl);
	struct callback *new_cb;

	dev = hlr->dev[dev_idx];
	ss = dev->staged_segs[ttl_idx];

	if (ttl > 8640000)
		abort();

	// if there is no space to alloc kv-pair, need to reclaim old segment
	if (ss->offset + size > ss->end_addr) {
		ss->_private = (void *)dev;

		ss->lnode = list_insert(dev->flying_seg_list, ss);
#ifdef RAMDISK
		for (int i = 0; i < dev->segment_size/4096; i++) {
			memcpy(dev->ram_disk[ss->start_addr/4096 + i], (char *)(ss->buf) + i * 4096, 4096);
		}
		cb_commit_seg((void *)ss);
#else
		new_cb = make_callback(hlr, cb_commit_seg, ss);
#ifdef LINUX_AIO
		aio_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)ss->buf, new_cb);
#elif URING
		uring_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)ss->buf, new_cb);
#elif DEV_SPDK 
		dev_spdk_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)ss->buf, new_cb);
#endif
#endif
		ss = stage_next_segment(hlr, dev, ttl_idx);
	}

	pba = ss->offset / dev->grain_unit;

	if (size % dev->grain_unit != 0) {
		//abort();
		size += (dev->grain_unit-(size%dev->grain_unit));
		if (size % dev->grain_unit != 0) abort(); // FIXME: remove later
	}
	ss->offset += size;

	//printf("get next pba size: %u, pba: %lu, dev_idx: %d\n", size, pba, dev_idx);

	return pba;
}

#endif

uint64_t get_next_pba_dummy(struct handler *hlr, uint32_t size, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	uint64_t pba;
	struct segment *ss = dev->staged_seg;

	struct callback *new_cb;

	// if there is no space to alloc kv-pair, need to reclaim old segment
	if (ss->offset + size > ss->end_addr) {
		//if (ss->offset % dev->segment_size)
		//	printf("[next_pba] ss->start: %llu, ss->offset: %llu\n", ss->start_addr, ss->offset);
		//ss->state = SEG_STATE_FLYING;
		ss->_private = (void *)dev;

		//new_cb = make_callback(hlr, reap_seg_buf, dev->staged_seg_buf);
		ss->state |= SEG_STATE_COMMITTED;
		//ss->lnode = list_insert(dev->committed_seg_list, ss);
		ss->lnode = list_insert(dev->dummy_committed_seg_list, ss);
		if (ss->buf)
			free(ss->buf);
		ss->buf = NULL;
#ifdef TTL_GROUP
		ss = stage_next_segment(hlr, dev, -1);
#else
		ss = stage_next_segment(dev);
#endif
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


#ifndef TTL_GROUP
static struct segment *
stage_next_idx_segment(struct dev_abs *dev) {
	dev->staged_idx_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_idx_seg->state = SEG_STATE_IDX;
	dev->staged_idx_seg->offset = dev->staged_idx_seg->start_addr + SEG_IDX_HEADER_SIZE;
	dev->staged_idx_seg->buf = alloc_seg_buffer(dev->segment_size);
	dev->nr_free_segment--;
	return dev->staged_idx_seg;
}

uint64_t get_next_idx_pba(struct handler *hlr, uint32_t size, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	uint64_t pba;
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
#ifdef LINUX_AIO
		aio_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_idx_seg->buf, new_cb);
#elif URING
		uring_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_idx_seg->buf, new_cb);
#elif DEV_SPDK
		dev_spdk_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_idx_seg->buf, new_cb);
#endif

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
#else // TTL_GROUP
static struct segment *
stage_next_idx_segment(struct dev_abs *dev) {
	dev->staged_idx_seg = (struct segment *)q_dequeue(dev->free_seg_q);
	dev->staged_idx_seg->state = SEG_STATE_IDX;
	dev->staged_idx_seg->offset = dev->staged_idx_seg->start_addr + SEG_IDX_HEADER_SIZE;
	dev->staged_idx_seg->buf = alloc_seg_buffer(dev->segment_size);
	dev->staged_idx_seg->creation_time = get_cur_sec();
	dev->nr_free_segment--;
	return dev->staged_idx_seg;
}

uint64_t get_next_idx_pba(struct handler *hlr, uint32_t size, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	uint64_t pba;
	struct segment *ss = dev->staged_idx_seg;

	struct callback *new_cb;

	if (size % 4096) abort();
	// if there is no space to alloc kv-pair, need to reclaim old segment
	if (ss->offset + size > ss->end_addr) {
		ss->_private = (void *)dev;

		ss->lnode = list_insert(dev->flying_seg_list, ss);
		new_cb = make_callback(hlr, cb_commit_seg, ss);
#ifdef LINUX_AIO
		aio_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_idx_seg->buf, new_cb);
#elif URING
		uring_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_idx_seg->buf, new_cb);
#elif DEV_SPDK
		dev_spdk_write(hlr, dev, ss->start_addr, dev->segment_size, 
			  (char *)dev->staged_idx_seg->buf, new_cb);
#endif
		ss = stage_next_idx_segment(dev);
	}

	pba = ss->offset / dev->grain_unit;
	//printf("start_addr: %lu, offset: %lu, idx pba: %lu\n", ss->start_addr, ss->offset, pba);

	if (size % dev->grain_unit != 0) {
		abort();
		size += (dev->grain_unit-(size%dev->grain_unit));
		if (size % dev->grain_unit != 0) abort(); // FIXME: remove later
	}
	ss->offset += size;

	return pba;
}

#endif

#ifndef TTL_GROUP
bool dev_need_gc(struct handler *hlr, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
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
		//printf("entry: %d, invalid: %d, valid: %f\n",seg->entry_cnt, seg->invalid_cnt, (double)valid_cnt/seg->entry_cnt);
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

static struct segment *select_gc_victim_seg(struct handler *hlr, struct dev_abs *dev) {
	list *committed_seg_list = dev->committed_seg_list;
	li_node *cur;
	struct segment *seg;
	list_for_each_node(committed_seg_list, cur) {
		seg = (struct segment *)cur->data;
		if (is_victim(dev, seg)) {
			goto success;
		}
	}

	dev_print_gc_info(hlr, dev);
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


bool do_expiration(struct handler *hlr, int dev_idx) {
	return false;
}

int dev_read_victim_segment(struct handler *hlr, int dev_idx, struct gc *gc) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	struct segment *victim_seg = select_gc_victim_seg(hlr, dev);

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
#ifdef LINUX_AIO
	aio_read(hlr, dev, addr_in_byte, size, (char *)gc->buf, cb);
#elif URING
	uring_read(hlr, dev, addr_in_byte, size, (char *)gc->buf, cb);
#elif DEV_SPDK
	dev_spdk_read(hlr, dev, addr_in_byte, size, (char *)gc->buf, cb);
#endif
	pthread_cond_wait(&gc->cond, &gc->mutex);
	pthread_mutex_unlock(&gc->mutex);

	gc->_private = victim_seg;
	gc->valid_cnt = victim_seg->entry_cnt - victim_seg->invalid_cnt;
	gc->is_idx = victim_seg->state & SEG_STATE_IDX;


	//print_segment(victim_seg, "victim");

	return 1;

}

bool is_idx_seg(struct gc *gc) {
	// if a victim segment is a index segment, it returns 1. Otherwise, returns 0.
	struct segment *seg = (struct segment *)gc->_private;

	return seg->state & SEG_STATE_IDX;
}

void reap_gc_segment(struct handler *hlr, int dev_idx, struct gc *gc) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	struct segment *victim = (struct segment *)gc->_private;

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
#else // TTL_GROUP
bool dev_need_gc(struct handler *hlr, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	if (dev->nr_free_segment <= GC_TRIGGER_THRESHOLD) {
		dev->gc_trigger_cnt++;
		return true;
	}
	return false;
	
}

static bool is_victim(struct dev_abs *dev, struct segment *seg) {
	/*
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
	*/

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

bool do_expiration(struct handler *hlr, int dev_idx) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	list *fifo_list;
	struct segment *victim;

	static int exp_cnt = 0;
	static int gc_cnt = 0;

	gc_cnt++;
	for (int i = 0; i < NR_TTL_GROUPS; i++) {
		fifo_list = dev->ttl_fifo[i];
		if (fifo_list->size == 0)
			continue;
		victim = (struct segment *)(fifo_list->head->data);
		if (victim && (victim->creation_time + ttl_group_std[i] < get_cur_sec())) {
			exp_cnt++;
			printf("gc_cnt: %d\n, exp_cnt: %d\n", gc_cnt, exp_cnt);
			//printf("SEG: %d, CUR SEC: %lu, SEG_EXP: %lu + %lu = %lu\n", victim->idx, get_cur_sec(), victim->creation_time, ttl_group_std[i], victim->creation_time + ttl_group_std[i]);
			list_delete_node(dev->ttl_fifo[victim->ttl], victim->ttl_lnode);
			victim->state = SEG_STATE_FREE;
			victim->entry_cnt = 0;
			victim->invalid_cnt = 0;
			victim->lnode = NULL;
			victim->offset = victim->start_addr;
			victim->age = ((++dev->age) / dev->nr_segment) % MAX_SEG_AGE;
			victim->creation_time = 0;

			dev->nr_free_segment++;
	
			q_enqueue((void *)victim, dev->free_seg_q);

			return true;
		}
	}
	return false;
}

static struct segment *select_gc_victim_seg(struct handler *hlr, struct dev_abs *dev) {
	list *committed_seg_list = dev->committed_seg_list;
	li_node *cur;
	struct segment *seg, *idx_seg;
	int fifo_idx = -1;
	uint64_t min_creation_time = get_cur_sec() + 1;
	list *fifo_list;

	for (int i = 0; i < NR_TTL_GROUPS; i++) {
		fifo_list = dev->ttl_fifo[i];
		if (fifo_list->size == 0)
			continue;
		seg = (struct segment *)(fifo_list->head->data);
		if (seg->creation_time < min_creation_time) {
			min_creation_time = seg->creation_time;
			fifo_idx = i;
		}
	}

	if (fifo_idx == -1)
		abort();

	if (committed_seg_list->size != 0) {
		idx_seg = (struct segment *)committed_seg_list->head->data;
	} else {
		seg = (struct segment *)dev->ttl_fifo[fifo_idx]->head->data;
		return seg;
	}

	if (idx_seg->creation_time < min_creation_time) {
		seg = idx_seg;
	} else {
		seg = (struct segment *)dev->ttl_fifo[fifo_idx]->head->data;
	}

	/*
	list_for_each_node(committed_seg_list, cur) {
		seg = (struct segment *)cur->data;
		if (is_victim(dev, seg)) {
			goto success;
		}
	}

	dev_print_gc_info(hlr, dev);
	printf("fail to select a victim segment, valid: %f\n", \
			1 - ((double)dev->fail_victim_invalid_cnt/(double)dev->fail_victim_entry_cnt));
	*/

	is_victim(dev,seg);

	return seg;
}


static void *cb_read_victim(void *arg) {
	struct gc *gc = (struct gc *)arg;
	pthread_mutex_lock(&gc->mutex);
	pthread_cond_signal(&gc->cond);
	pthread_mutex_unlock(&gc->mutex);
	return NULL;
}


int dev_read_victim_segment(struct handler *hlr, int dev_idx, struct gc *gc) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	struct segment *victim_seg = select_gc_victim_seg(hlr, dev);

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
		printf("무야호 IDX: %d, DATA: %d\n",dev->victim_trim_idx_seg_cnt, dev->victim_trim_data_seg_cnt);
		gc->_private = victim_seg;
		gc->valid_cnt = 0;

		return 0;
	}

	uint32_t size = victim_seg->offset - victim_seg->start_addr;
	uint64_t addr_in_byte = victim_seg->start_addr;

	struct callback *cb = make_callback(hlr, cb_read_victim, gc);
	pthread_mutex_lock(&gc->mutex);
#ifdef LINUX_AIO
	aio_read(hlr, dev, addr_in_byte, size, (char *)gc->buf, cb);
#elif URING
	uring_read(hlr, dev, addr_in_byte, size, (char *)gc->buf, cb);
#elif DEV_SPDK
	dev_spdk_read(hlr, dev, addr_in_byte, size, (char *)gc->buf, cb);
#endif
	pthread_cond_wait(&gc->cond, &gc->mutex);
	pthread_mutex_unlock(&gc->mutex);

	gc->_private = victim_seg;
	gc->valid_cnt = victim_seg->entry_cnt - victim_seg->invalid_cnt;
	gc->is_idx = victim_seg->state & SEG_STATE_IDX;



	static int count = 0;
	if (gc->is_idx) {
		print_segment(victim_seg, "victim");
		printf("READ GC %d\n", ++count);
	}

	return 1;

}

bool is_idx_seg(struct gc *gc) {
	// if a victim segment is a index segment, it returns 1. Otherwise, returns 0.
	struct segment *seg = (struct segment *)gc->_private;

	return seg->state & SEG_STATE_IDX;
}

void reap_gc_segment(struct handler *hlr, int dev_idx, struct gc *gc) {
	struct dev_abs *dev = hlr->dev[dev_idx];
	struct segment *victim = (struct segment *)gc->_private;

	if (victim->state & SEG_STATE_IDX)
		list_delete_node(dev->committed_seg_list, victim->lnode);
	else
		list_delete_node(dev->ttl_fifo[victim->ttl], victim->ttl_lnode);
	victim->state = SEG_STATE_FREE;
	victim->entry_cnt = 0;
	victim->invalid_cnt = 0;
	victim->lnode = NULL;
	victim->ttl_lnode = NULL;
	victim->offset = victim->start_addr;
	victim->age = ((++dev->age) / dev->nr_segment) % MAX_SEG_AGE;
	victim->creation_time = 0;

#ifdef DEBUG_GC
	memset(victim->seg_bitmap, 0, 2048);
#endif
	
	dev->nr_free_segment++;

	q_enqueue((void *)victim, dev->free_seg_q);
}
#endif

void dev_print_gc_info (struct handler *hlr, struct dev_abs *dev) {

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
	printf("victim_invalid_idx_cnt: %lu\n", dev->victim_invalid_idx_cnt);
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
