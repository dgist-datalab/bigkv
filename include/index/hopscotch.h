/*
 * Hopscotch Hash Index Structure Header
 *
 * Description: It is an implementation of uDepot's hopscotch hash table which
 * was published at FAST '19.
 *
 * The implementation is totally based on the paper, but some minor updates
 * were done here. In fact, since they supposed that the system has sufficient
 * memory which is conflicted in our case, we modified some parts appropriately
 * (e.g., no resizing, demand paging).
 *
 */

#ifndef __HOPSCOTCH_H__
#define __HOPSCOTCH_H__

#include "config.h"
#include "platform/handler.h"
#include "utility/lru_cache.h"

#ifdef __cplusplus
#include <atomic>
using namespace std;
#else
#include <stdatomic.h>
#endif
#include <stdint.h>

#define NUM_CPU 1

#ifdef TTL
#define HASH_ENTRY_BYTES 16
#else
#define HASH_ENTRY_BYTES 8
#endif

//#define IDX_BIT 27 // can index a 128 GB stroage device
//#define IDX_BIT 27 // can idx a 512 GB storage device with 4 KB KV
#define IDX_BIT 25ULL // can idx a 1024 GB storage device with 4 KB KV
#define DIR_BIT 0 // 1 ==> 2GB
#define NR_TABLE (1 << DIR_BIT)
#define NR_ENTRY (1ULL << IDX_BIT)
#define NR_PART (NR_ENTRY/PAGESIZE*HASH_ENTRY_BYTES) // for 1TB, 18승개 part, 1part에 10승개 entry, 즉 256M 개 entry
#define MAX_HOP 32
#define PBA_INVALID 0xffffffffff
#define NR_CACHED_PART (AVAIL_MEMORY/PAGESIZE) // 512 MB memory
//#define NR_CACHED_PART NR_PART
#define FLASH_READ_MAX 10
#define NR_ENTRY_PER_PART (PAGESIZE/HASH_ENTRY_BYTES)

#define IDX_START_IN_DEV 0
#define IDX_SIZE_PER_TABLE (NR_ENTRY*HASH_ENTRY_BYTES)
#define IDX_SIZE_IN_DEV (NR_TABLE*IDX_SIZE_PER_TABLE)
#define KV_START_IN_DEV (IDX_START_IN_DEV+IDX_SIZE_IN_DEV)
#define KV_SIZE_IN_DEV 0
#define PART_SIZE PAGESIZE
#define GRAINS_PER_PART (PART_SIZE/GRAIN_UNIT)

struct hash_entry {
	uint64_t neigh_off:5;
	uint64_t key_fp_tag:8;
	uint64_t kv_size:11;
	uint64_t pba:40;
#ifdef TTL
	uint64_t ttl;
#endif
};

struct hash_table {
	struct hash_entry *entry;
};

struct hash_part {
	uint32_t page_num;
	lru_node *lnode;
	void *_private;
	uint64_t pba;
	uint8_t dir;
	//struct hash_entry entry[NR_ENTRY_PER_PART];
	struct hash_entry *entry;
};

struct part_info {
	uint64_t pba;
	uint64_t cnt;
	uint64_t latest;
	uint64_t latest_pba;
	uint8_t debug;
	//uint8_t state;
	atomic_int state;
	struct hash_part *part_ptr;
};

struct hopscotch {
	uint8_t type;
	struct hash_table *table;
	LRU **part_lru;
	uint64_t idx_start;
	uint64_t kv_start;
	struct part_info part_info[NR_TABLE][NR_PART];
	queue *part_q[NR_TABLE];
	struct handler *hlr;

	int ops_number;

	uint64_t fill_cnt;
	uint64_t dis_cnt;
	uint64_t off_cnt[MAX_HOP];

	void (*idx_init)(struct hopscotch *);
	void (*idx_free)(struct hopscotch *);
	struct hash_entry* (*find_matching_tag)(struct hopscotch *, uint8_t, uint32_t, int*, uint8_t);
	int (*find_free_entry)(struct hopscotch *, uint8_t, uint32_t, struct request *);
#ifdef TTL
	struct hash_entry* (*fill_entry)(struct hopscotch *, uint8_t, uint32_t, uint8_t, uint8_t, uint16_t, uint64_t, uint64_t);
#else
	struct hash_entry* (*fill_entry)(struct hopscotch *, uint8_t, uint32_t, uint8_t, uint8_t, uint16_t, uint64_t);
#endif
	uint64_t (*get_idx_pba)(struct hopscotch *, uint8_t, uint32_t, uint8_t offset); 
	void (*print_info)(struct hopscotch *);
	uint64_t lookup_cost[FLASH_READ_MAX];

	pthread_mutex_t lock;
};

enum {
	HOP_STEP_INIT = 0,
	HOP_STEP_KEY_MATCH,
	HOP_STEP_KEY_MISMATCH,
	HOP_STEP_LRU_MISS,
	HOP_STEP_FLYING,
	HOP_STEP_FIND_RETRY,
	HOP_STEP_EXPIRED,
	HOP_STEP_EXPIRED_CONTINUE,
	HOP_STEP_EVICTED,
};

enum {
	HOP_FULL,
	HOP_PART,
};

enum {
	LRU_INVALID,
	LRU_FLYING,
	LRU_DONE,
	LRU_DIRTY,
	LRU_CLEAN,
};

struct hop_params {
	uint32_t idx;
	int offset;
	struct hash_part *part;
	int lookup_step;
	int insert_step;
	uint8_t page_read_cnt;
};

static inline struct hop_params *make_hop_params() {
	struct hop_params *hp = (struct hop_params *)malloc(sizeof(struct hop_params));
	hp->idx = 0;
	hp->offset = 0;
	hp->part = NULL;
	hp->lookup_step = HOP_STEP_INIT;
	hp->insert_step = HOP_STEP_INIT;
	hp->page_read_cnt = 0;
	return hp;
}

static inline struct hash_entry *get_entry (struct hopscotch *hs, uint8_t dir, uint32_t idx, int offset) {
	struct hash_entry *entry = NULL;
#ifdef HOPSCOTCH_FULL
	struct hash_table *ht = &hs->table[dir];
	entry = &ht->entry[(idx+offset)%NR_ENTRY];
#elif HOPSCOTCH_PART
	entry = NULL;
#endif
	return entry;
}

static inline uint32_t get_lru_key (uint32_t idx) {
	return idx/NR_ENTRY_PER_PART;
}

static inline uint32_t get_idx_page_num (uint32_t idx, uint8_t offset) {
	return ((idx + offset)%NR_ENTRY)/NR_ENTRY_PER_PART;
}

static inline struct hash_entry *get_entry_from_part (struct hash_part *part, uint32_t idx) {
	return &part->entry[idx%NR_ENTRY_PER_PART];
}

static inline void hash_part_init (struct hash_part **part, struct hopscotch *hs, uint8_t dir, uint32_t idx) {
	*part = (struct hash_part *)malloc(sizeof(struct hash_part));
	(*part)->_private = (void *)hs;
	(*part)->dir = dir;
	(*part)->page_num = idx/NR_ENTRY_PER_PART;
	(*part)->pba = 0;
	(*part)->lnode = NULL;
	//(*part)->entry = (struct hash_entry *)aligned_alloc(MEM_ALIGN_UNIT, PART_SIZE); 
	(*part)->entry = (struct hash_entry *)q_dequeue(hs->part_q[dir]); 
	if (((*part)->entry) == NULL) abort();
	memset((*part)->entry, 0, PART_SIZE);

	if ((*part)->page_num >= NR_PART) abort();

	//intf("init[%p] - page_num: %d\n",*part, (*part)->page_num);
	//if ((*part)->page_num == 93677) { // 157091 93677
	//if ((*part)->page_num == 157091) { // 157091 93677
	//	abort();
	//}
	return;
}

static inline void print_hash_entry (struct hash_entry *entry, const char *str) {
	printf("[%s] hentry[%p] - offset: %lu, tag:%lu, size: %ld, pba: %lu\n", str, entry, entry->neigh_off, entry->key_fp_tag, entry->kv_size, entry->pba);
}


static inline void print_part (struct hash_part *part, const char *str) {
	struct hopscotch *hs = (struct hopscotch *)part->_private;
	printf("[%s] print_part[%p] - dir: %hd, page_num: %u, lnode: %p, pba: %lu, addr: %lu, part_info.pba: %lu, part_info.debug: %d latest: %lu latest_pba: %lu, entry: %p\n", str, part, part->dir, part->page_num, part->lnode, part->pba, part->pba * GRAIN_UNIT, hs->part_info[part->dir][part->page_num].pba, hs->part_info[part->dir][part->page_num].debug, hs->part_info[part->dir][part->page_num].latest, hs->part_info[part->dir][part->page_num].latest_pba, part->entry);
}

static inline void print_hop_entry (struct hash_part *part, uint32_t idx, uint32_t offset, const char *str) {
	struct hash_entry *tmp;
	print_part(part, "HOP");
	for (int i = offset; i < MAX_HOP; i++) {
		printf("current_idx: %d, offset: %d\n", idx, i);
		tmp = get_entry_from_part(part, ((idx + i)%NR_ENTRY)%NR_ENTRY_PER_PART);
		print_hash_entry(tmp, str);
	}

}


int hopscotch_init(struct kv_ops *ops);
int hopscotch_free(struct kv_ops *ops);
int hopscotch_get(struct kv_ops *ops, struct request *req);
int hopscotch_set(struct kv_ops *ops, struct request *req);
int hopscotch_delete(struct kv_ops *ops, struct request *req);
int hopscotch_need_gc(struct kv_ops *ops, struct handler *hlr);
int hopscotch_trigger_gc(struct kv_ops *ops, struct handler *hlr);
int hopscotch_wait_gc(struct kv_ops *ops, struct handler *hlr);



#endif
