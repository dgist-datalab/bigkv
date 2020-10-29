#ifndef __BIGKV_INDEX_H__
#define __BIGKV_INDEX_H__

#include "platform/kv_ops.h"

#include <stdint.h>

#define PBA_INVALID 0x3fffffffff

#define ENTRY_SIZE 16
#define NR_SET 4
#define BUCKET_SIZE 64

#define BUCKETS_IN_PART 4
//#define PART_TABLE_SIZE (16*Ki)
#define PART_TABLE_SIZE (4*Ki)
#define PART_TABLE_GRAINS (PART_TABLE_SIZE/GRAIN_UNIT)
#define PART_TABLE_ENTRYS (PART_TABLE_SIZE/ENTRY_SIZE)

#define NR_BUCKET (1 << 22) // 24 for 1GiB
#define NR_PART (NR_BUCKET/BUCKETS_IN_PART)

#define MAX_HOP 256

struct hash_entry {
	uint64_t fingerprint;
	uint64_t reserve:5;
	uint64_t dirty_bit:1;
	uint64_t lru_bit:2;
	uint64_t kv_size:14;
	uint64_t pba:42;
};

struct hash_bucket {
	struct hash_entry entry[NR_SET];
};

struct hash_entry_f {
	uint64_t fingerprint;
	uint64_t offset:8;
	uint64_t kv_size:14;
	uint64_t pba:42;
};

struct hash_part_table {
	struct hash_entry_f entry[PART_TABLE_ENTRYS];
};

struct hash_partition {
	uint64_t flying:1;
	uint64_t pba:42;
};

struct hash_table {
	struct hash_bucket *bucket;
	struct hash_partition *part;
};

struct bigkv_index {
	struct hash_table *table;
};

struct bi_params {
	int read_step;
	int write_step;
};

enum {
	BI_STEP_INIT,
	BI_STEP_KEY_MISMATCH,
	BI_STEP_UPDATE_TABLE,
	BI_STEP_WRITE_PTABLE,
	BI_STEP_WRITE_KV,
};

static inline struct bi_params * make_bi_params() {
	struct bi_params *bp = (struct bi_params *)malloc(sizeof(struct bi_params));
	bp->read_step = BI_STEP_INIT;
	bp->write_step = BI_STEP_INIT;
	return bp;
}

int bigkv_index_init(struct kv_ops *ops);
int bigkv_index_free(struct kv_ops *ops);
int bigkv_index_get(struct kv_ops *ops, struct request *req);
int bigkv_index_set(struct kv_ops *ops, struct request *req);
int bigkv_index_delete(struct kv_ops *ops, struct request *req);

#endif
