#ifndef __CASCADE_H__
#define __CASCADE_H__

#include "platform/kv_ops.h"
#include "utility/bloomfilter.h"

#include <stdint.h>

#define PBA_INVALID 0x3fffffffff

#define ENTRY_SIZE 16
//#define ENTRY_SIZE 24
#define MAX_KEY_LEN 1024
#define MAX_KV_LEN (16 * 1024)

#ifdef CASCADE_DEBUG
#define NR_T1_PART (1 << 2) // 4 parts
#define NR_T1_ENTRY (1 << 15) // 16 million * 16 B = 256 MB DRAM
#define NR_T2_BLOCK 4
#define NR_T3_NARROW 4
#define NR_T3_NARROW_BLOCK 4
#define T3_RATIO 16
#define NR_T3_WIDE_BLOCK 2
#else
#define NR_T1_PART (1 << 5) // 128 parts
#define NR_T1_ENTRY (1 << 25) // 16 million * 16 B =  512MB DRAM
//#define NR_T1_ENTRY (1 << 24) // 16 million * 16 B =  256MB DRAM
//#define NR_T1_ENTRY (1 << 23) // 16 million * 16 B =  128MB DRAM
//#define NR_T1_ENTRY (22369622) // 512 MB
//#define NR_T1_ENTRY (2369622) // 512 MB
#define NR_T2_BLOCK 32
//#define NR_T3_NARROW 1024
#define NR_T3_NARROW 256
#define NR_T3_NARROW_BLOCK 64
#define NR_T3_WIDE_BLOCK 2
#define T3_RATIO 1024
#endif


#define NR_T1_BUCKET (1 << 14) // 16 * 1024 buckets, On average, 1024 entries per bucket
#define NR_T1_BUCKET_PER_PART (NR_T1_BUCKET/NR_T1_PART) // 512 buckets per part
#define CACHE_THRESHOLD NR_T1_ENTRY

//#ifdef TTL
//#define T2_BLOCK_SIZE (48*Ki)
//#define T3_BLOCK_SIZE (192*Ki)
//#define T3_WIDE_MIN_UNIT (12*Ki)
//#else
#define T2_BLOCK_SIZE (16*Ki)
#define T3_BLOCK_SIZE (128*Ki)
#define T3_WIDE_MIN_UNIT (4*Ki)
//#endif
#define T2_BLOCK_ENTRY (T2_BLOCK_SIZE/ENTRY_SIZE)
#define T2_PAR_UNIT 8
#define T2_BLOCK_GRAINS (T2_BLOCK_SIZE/GRAIN_UNIT)

#define T3_BLOCK_GRAINS (T3_BLOCK_SIZE/GRAIN_UNIT)
#define T3_BLOCK_ENTRY (T3_BLOCK_SIZE/ENTRY_SIZE) // 8192 entries
#define NR_T3_WIDE (NR_T3_NARROW * T3_RATIO)
#define T3_WIDE_MIN_UNIT_GRAINS (T3_WIDE_MIN_UNIT/GRAIN_UNIT)


struct hash_entry {
	uint64_t fingerprint;
	uint64_t reserve:4;
	uint64_t flying_bit:1;
	uint64_t dirty_bit:1;
	uint64_t clock_bit:1;
	uint64_t evict_bit:1;
	uint64_t kv_size:14;
	uint64_t pba:42;
	uint64_t ttl;
	li_node *lnode;
};

struct hash_entry_f {
	uint64_t fingerprint;
	uint64_t reserve:7;
	uint64_t evict_bit:1;
	uint64_t kv_size:14;
	uint64_t pba:42;
	uint64_t ttl;
};

struct t1_hash_bucket {
	list *hash_list;
	li_node *clock_hand;
};

struct t1_virtual_buffer {
	struct hash_entry *entry_ptr[T2_BLOCK_ENTRY];
};

struct t2_hash_block {
	struct hash_entry_f entry[T2_BLOCK_ENTRY];
};

struct t2_fifo {
	struct t2_hash_block fifo[NR_T2_BLOCK];
};

struct t3_hash_block {
	struct hash_entry_f entry[T3_BLOCK_ENTRY];
};

#define BF_M (16*Ki) // 8192 items, 0.5%
#define BF_K 3

struct t3_list_item {
	struct bloomfilter *bf;
	uint64_t pba;
	li_node *lnode;
};

struct t3_hash_bucket {
	struct t3_hash_block *buf;
	int buf_idx;
	int last_idx;
	int new_cnt;
	list *hash_list;
	li_node *last;
	int size;
};

struct t2_fifo_buf {
	struct t2_hash_block *buf;
	int buf_idx;
};

struct t1_hash_partition {
	uint64_t t2_fifo_size:7;
	uint64_t flying_req:4;
	uint64_t t2_fifo_head:6;
	uint64_t flying:1;
	uint64_t reserved:4;
	uint64_t pba:42; // start pba of t2 fifo
	struct t2_fifo_buf *t2_fifo_buf;
	int is_set;
	int flying_type;
	uint64_t fp;
	uint64_t nr_read;
};

struct t1_hash_table {
	struct t1_hash_bucket *t1_bucket;
	struct t1_hash_partition *t1_part;
	uint64_t cached_entry;
};

struct t3_narrow_hash_table {
	struct t3_hash_bucket *t3_bucket;
};

struct t3_wide_hash_table {
	struct t3_hash_bucket *t3_bucket;
};


struct cascade {
	int ops_number;
	struct t1_hash_table *t1_table;
	struct t3_narrow_hash_table *t3_narrow;
	struct t3_wide_hash_table *t3_wide;
};

struct t2_read_params {
	int batch_idx;
	uint64_t target_fp;
	struct request *req;
	uint16_t t2_read_cnt;
	struct hash_entry_f entry_f;
	void *buf;
	void *kv_buf;
	int start_idx;
	bool is_read;
};

struct t3_read_params {
	struct t3_hash_bucket *t3_bucket;
	struct t3_list_item *item;
	int nr_entry;
	struct request *req;
	struct hash_entry_f entry_f;
	void *buf;
	void *kv_buf;
	int max_read_cnt;
	int read_cnt;
};

struct cas_params {
	int read_step;
	int write_step;
	struct hash_entry *entry;
	struct hash_entry_f entry_f;
	struct hash_entry_f *tmp_entry_f;
	int t2_target_idx;
	uint16_t read_done;
	struct cascade *cas;
	pthread_mutex_t mutex;
	bool is_read;
	bool is_expired;
};

enum {
	CAS_STEP_INIT,
	CAS_STEP_READ_T3_NARROW,
	CAS_STEP_READ_T3_WIDE,
	CAS_STEP_READ_KV_SUCC,
	CAS_STEP_KEY_MISMATCH,
	CAS_STEP_T1_MATCH,
	CAS_STEP_T1_MISMATCH,
	CAS_STEP_READ_T2_SUCC,
	CAS_STEP_READ_T2_FAIL,
	CAS_STEP_READ_T3_BUFFER,
	CAS_STEP_READ_T3_BUFFER_FAIL,
	CAS_STEP_READ_T3_NARROW_SUCC,
	CAS_STEP_READ_T3_NARROW_FAIL,
	CAS_STEP_READ_T3_WIDE_SUCC,
	CAS_STEP_READ_T3_WIDE_FAIL,
	CAS_STEP_T1_EXPIRED,
	CAS_STEP_EXPIRED,
	CSA_STEP_EVICTED,
};

static inline struct cas_params * make_cas_params() {
	struct cas_params *casp = (struct cas_params *)malloc(sizeof(struct cas_params));
	casp->read_step = CAS_STEP_INIT;
	casp->write_step = CAS_STEP_INIT;
	casp->t2_target_idx = 8;
	casp->read_done = 0;
	casp->tmp_entry_f = NULL;
	casp->cas = NULL;
	casp->entry = NULL;
	casp->is_expired = false;
	pthread_mutex_init(&casp->mutex, NULL);
	return casp;
}

int cascade_init(struct kv_ops *ops);
int cascade_free(struct kv_ops *ops);
int cascade_get(struct kv_ops *ops, struct request *req);
int cascade_set(struct kv_ops *ops, struct request *req);
int cascade_delete(struct kv_ops *ops, struct request *req);
int cascade_need_gc(struct kv_ops *ops, struct handler *hlr);
int cascade_trigger_gc(struct kv_ops *ops, struct handler *hlr);
int cascade_wait_gc(struct kv_ops *ops, struct handler *hlr);

static void print_entry (struct hash_entry *entry, const char *str) {
	printf("[ENTRY][%s] fp: %lu, dirty: %lu, kv_size: %lu, pba: %lu\n", str, entry->fingerprint, entry->dirty_bit, entry->kv_size, entry->pba);
}

static void print_entry_f (struct hash_entry_f *entry_f, const char *str) {
	printf("[ENTRY][%s] fp: %lu, kv_size: %lu, pba: %lu\n", str, entry_f->fingerprint, entry_f->kv_size, entry_f->pba);
}


/*
static void print_ptable (struct hash_part_table *ptable, const char *str, int part_idx) {
	printf("[PTABLE][%s] ptable idx = %d\n", str, part_idx);
	for (int i = 0; i < PART_TABLE_ENTRYS; i++)
		printf("ptable[%d], fp = %lu, ttl = %lu, kv_size: %lu, pba: %lu\n", i, ptable->entry[i].fingerprint, ptable->entry[i].ttl, ptable->entry[i].kv_size, ptable->entry[i].pba);

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
*/


#endif
