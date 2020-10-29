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

#include <stdint.h>

#define IDX_BIT 27
#define DIR_BIT 0
#define NR_TABLE (1 << DIR_BIT)
#define NR_ENTRY (1 << IDX_BIT)
#define NR_PART (NR_ENTRY/PAGESIZE*8)
#define MAX_HOP 32
#define PBA_INVALID 0xffffffffff
#define NR_CACHED_PART (AVAIL_MEMORY/PAGESIZE)
#define FLASH_READ_MAX 10

struct hash_entry {
	uint64_t neigh_off:5;
	uint64_t key_fp_tag:8;
	uint64_t kv_size:11;
	uint64_t pba:40;
};

struct hash_table {
	struct hash_entry *entry;
};

struct hopscotch {
	struct hash_table *table;

	uint64_t lookup_cost[FLASH_READ_MAX];
};

enum {
	HOP_STEP_INIT,
	HOP_STEP_KEY_MATCH,
	HOP_STEP_KEY_MISMATCH,
};

struct hop_params {
	int offset;
	int lookup_step;
	int insert_step;
};

static inline struct hop_params *make_hop_params() {
	struct hop_params *hp = (struct hop_params *)malloc(sizeof(struct hop_params));
	hp->offset = 0;
	hp->lookup_step = HOP_STEP_INIT;
	hp->insert_step = HOP_STEP_INIT;
	return hp;
}

int hopscotch_init(struct kv_ops *ops);
int hopscotch_free(struct kv_ops *ops);
int hopscotch_get(struct kv_ops *ops, struct request *req);
int hopscotch_set(struct kv_ops *ops, struct request *req);
int hopscotch_delete(struct kv_ops *ops, struct request *req);

#endif
