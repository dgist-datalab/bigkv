#ifndef __BIGKV_INDEX_H__
#define __BIGKV_INDEX_H__

#include "platform/kv_ops.h"

#include <stdint.h>

#define PBA_INVALID 0x3fffffffff

#define ENTRY_SIZE 16
#define NR_SET 4
#define BUCKET_SIZE 64

//#define BUCKETS_IN_PART 8 // 이렇게 하면 30승개 가능 1G 개
#define PART_TABLE_SIZE (16*Ki)
//#define PART_TABLE_SIZE (4*Ki)
#define PART_TABLE_GRAINS (PART_TABLE_SIZE/GRAIN_UNIT)
#define PART_TABLE_ENTRYS (PART_TABLE_SIZE/ENTRY_SIZE)

#ifdef TEST_GC
#define NR_BUCKET (1 << 22) // 24 for 1GiB, In-memory, 256MB, 1 ssd space
#define NR_PART (1<<20) // flash mapping pages, 1 index 1 part, part * 256 = all indices ==> 256GB storage, bucket_in_part = 8 ==> 128GB
#define BUCKETS_IN_PART 32 // 이렇게 하면 28승개 가능 256M 개
#else
//#define NR_BUCKET (1 << 23) // 64B bucket이 8M 개 => 512MB => 32M개의 index, 1TB는 4KB를 256M개 가능, 즉 1/8개의 entry 캐싱.
//#define NR_BUCKET (1 << 22) // 64B bucket이 4M 개 => 256MB => 16M개의 index, 1TB는 4KB를 256M개 가능, 즉 1/16개의 entry 캐싱.
#define NR_BUCKET (1 << 21) // 64B bucket이 2M 개 => 128MB => 8M개의 index, 1TB는 4KB를 256M개 가능, 즉 1/32개의 entry 캐싱.
//#define NR_PART (NR_BUCKET/BUCKETS_IN_PART) // flash mapping pages, 1 index 1 part, part * 256 = all indices ==> 256GB storage, bucket_in_part = 8 ==> 128GB
#define NR_PART (1 << 18)
#define BUCKETS_IN_PART (NR_BUCKET/NR_PART)

#endif

#define MAX_HOP (PART_TABLE_SIZE/ENTRY_SIZE)

#define KEY_LEN_OFF 0
#define KEY_OFF KEY_LEN_OFF + sizeof(uint8_t)
#define VAL_LEN_OFF KEY_OFF + sizeof(uint32_t)

struct hash_entry {
	uint64_t fingerprint;
	//uint64_t reserve:5;
	uint64_t ttl:5;
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
	uint64_t reserve:3;
	uint64_t ttl:5;
	uint64_t kv_size:14;
	uint64_t pba:42;
};

struct hash_part_table {
	struct hash_entry_f entry[PART_TABLE_ENTRYS];
};

struct hash_partition {
	uint64_t flying:1;
	uint64_t pba:42;
	uint64_t base_min;
	uint16_t base_idx;
};

struct hash_table {
	struct hash_bucket *bucket;
	struct hash_partition *part;
};

struct bigkv_index {
	struct hash_table *table;
	int ops_number;
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
	BI_STEP_EXPIRED,
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
int bigkv_index_need_gc(struct kv_ops *ops, struct handler *hlr);
int bigkv_index_trigger_gc(struct kv_ops *ops, struct handler *hlr);
int bigkv_index_wait_gc(struct kv_ops *ops, struct handler *hlr);

static void print_ptable (struct hash_part_table *ptable, const char *str, int part_idx) {
	printf("[PTABLE][%s] ptable idx = %d\n", str, part_idx);
	for (int i = 0; i < PART_TABLE_ENTRYS; i++)
		printf("ptable[%d], fp = %lu, ttl = %lu, kv_size: %lu, pba: %lu\n", i, ptable->entry[i].fingerprint, ptable->entry[i].ttl, ptable->entry[i].kv_size, ptable->entry[i].pba);

}

static void print_entry (struct hash_entry *entry, const char *str) {
	printf("[ENTRY][%s] fp: %lu, dirty: %lu, lru: %lu, kv_size: %lu, pba: %lu\n", str, entry->fingerprint, entry->dirty_bit, entry->lru_bit, entry->kv_size, entry->pba);
}

static bool ptable_sanity_check (struct hash_part_table *ptable, const char *str, int part_idx) {

	if (part_idx == 288345) print_ptable(ptable, str, part_idx);

	bool ret = false, flag = true, zero_flag = true;
	for (int i = 0; i < PART_TABLE_ENTRYS; i++) {
		if (ptable->entry[i].pba == 0) {
			if (flag) {
				printf("[WARNING][%s] ptable idx = %d\n", str, part_idx);
				flag = false;
			}
			printf("ptable[%d], fp = %lu, ttl = %lu, kv_size: %lu, pba: %lu\n", i, ptable->entry[i].fingerprint, ptable->entry[i].ttl, ptable->entry[i].kv_size, ptable->entry[i].pba);
			if (!zero_flag) ret = true;
			if (zero_flag) zero_flag = false;
		}
	}
	return ret;

}


#endif
