#include "index/bigkv_index.h"
#include "platform/device.h"
#include "utility/stopwatch.h"

#include <stdlib.h>
#include <string.h>

stopwatch *sw_set, *sw_lru, *sw_pba, *sw_cpy;
time_t set_total, set_lru, set_pba, set_cpy;

stopwatch *sw_get;
time_t t_get;

static void copy_key_to_value(struct key_struct *key, struct val_struct *value) {
	memcpy(value->value, &key->len, sizeof(key->len));
	memcpy(value->value+sizeof(key->len), key->key, key->len);
}

int bigkv_index_init(struct kv_ops *ops) {
	int rc = 0;
	struct bigkv_index *bi = 
		(struct bigkv_index *)calloc(1, sizeof(struct bigkv_index));

	bi->table = (struct hash_table *)calloc(1, sizeof(struct hash_table));
	bi->table->bucket = (struct hash_bucket *)aligned_alloc(64,
		NR_BUCKET * sizeof(struct hash_bucket));

	memset(bi->table->bucket, 0, NR_BUCKET * sizeof(struct hash_bucket));

	for (int i = 0; i < NR_BUCKET; i++) {
		struct hash_bucket *bucket = &bi->table->bucket[i];
		for (int j = 0; j < NR_SET; j++) {
			struct hash_entry *entry = &bucket->entry[j];
			entry->lru_bit = 0x3;
			entry->pba = PBA_INVALID;
		}
	}

	bi->table->part = (struct hash_partition *)aligned_alloc(64,
		NR_PART * sizeof(struct hash_partition));
	memset(bi->table->part, 0, NR_PART * sizeof(struct hash_partition));
	for (int i = 0; i < NR_PART; i++) {
		struct hash_partition *part = &bi->table->part[i];
		part->pba = PBA_INVALID;
	}

	ops->_private = (void *)bi;

	sw_set = sw_create();
/*	sw_lru = sw_create();
	sw_pba = sw_create();
	sw_cpy = sw_create();
	sw_get = sw_create(); */
	return rc;
}

int bigkv_index_free(struct kv_ops *ops) {
	int rc = 0;
	struct bigkv_index *bi = (struct bigkv_index *)ops->_private;
	free(bi->table->bucket);
	free(bi->table->part);
	free(bi->table);
	free(bi);
	ops->_private = NULL;
	printf("t_set: %lu\n", set_total);
	sw_destroy(sw_set);
/*	sw_destroy(sw_lru);
	sw_destroy(sw_cpy);
	sw_destroy(sw_pba);
	printf("set total: %lu\n", set_total);
	printf("set lru: %lu\n", set_lru);
	printf("set pba: %lu\n", set_pba);
	printf("set cpy: %lu\n", set_cpy);
	printf("set get: %lu\n", t_get); */
	return rc;
}

static struct hash_entry *
get_match_entry(struct hash_bucket *bucket, hash_t fp) {
	struct hash_entry *entry = NULL;
	for (int i = 0; i < NR_SET; i++) {
		entry = &bucket->entry[i];
		if (entry->pba != PBA_INVALID && entry->fingerprint == fp) {
			return entry;
		}
	}
	return NULL;
}

static void *cb_part_table_read(void *arg) {
	struct request *req = (struct request *)arg;
	struct bi_params *params = (struct bi_params *)req->params;
	params->write_step = BI_STEP_WRITE_PTABLE;
	params->read_step = BI_STEP_UPDATE_TABLE;
	retry_req_to_hlr(req->hlr, req);
	return NULL;
}

static int keycmp_k_v(struct key_struct *key, struct val_struct *value) {
	if (key->len != ((uint8_t *)value->value)[0]) {
		return (key->len > ((uint8_t *)value->value)[0]) ? 1:-1;
	}
	return strncmp(key->key, value->value+(sizeof(uint8_t)), key->len);
}

static void *cb_keycmp(void *arg) {
	struct request *req = (struct request *)arg;
	struct bi_params *params = (struct bi_params *)req->params;

	struct key_struct *key = &req->key;
	struct val_struct *value = &req->value;

	if (keycmp_k_v(key, value) == 0) {
		req->end_req(req);
		return NULL;
	} else {
		params->read_step = BI_STEP_KEY_MISMATCH;
	}

	retry_req_to_hlr(req->hlr, req);
	return NULL;
}

static struct hash_entry_f *
get_match_entry_f(struct hash_part_table *ptable, hash_t fp) {
	struct hash_entry_f *entry_f = NULL;
	uint32_t idx = fp % PART_TABLE_ENTRYS;
	for (int i = 0; i < MAX_HOP; i++) {
		entry_f = &ptable->entry[idx];
		if (entry_f->fingerprint == fp) {
			return entry_f;
		}
		idx = (idx+1) % PART_TABLE_ENTRYS;
	}
	return NULL;
}

static struct hash_entry *
get_target_entry(struct hash_bucket *bucket, hash_t fp) {
	struct hash_entry *entry = NULL;
	for (int i = 0; i < NR_SET; i++) {
		entry = &bucket->entry[i];
		if (entry->fingerprint == fp) return entry;
	}
	for (int i = 0; i < NR_SET; i++) {
		entry = &bucket->entry[i];
		if (entry->pba == PBA_INVALID) return entry;
	}
	for (int i = 0; i < NR_SET; i++) {
		entry = &bucket->entry[i];
		if (entry->lru_bit == 0x3) {
			if (entry->dirty_bit == 0) return entry;
			else return NULL;
		}
	}
	return NULL;
}

static int 
insert_to_ptable(struct hash_part_table *ptable, struct hash_entry *entry) {	
	int rc = 0, i;
	uint32_t idx = entry->fingerprint % PART_TABLE_ENTRYS;
	struct hash_entry_f *entry_f = NULL;

	for (i = 0; i < MAX_HOP; i++) {
		entry_f = &ptable->entry[idx];
		if (entry_f->pba == PBA_INVALID) {
			break;
		} else if (entry_f->fingerprint == entry->fingerprint) {
			break;
		}
		idx = (idx+1) % PART_TABLE_ENTRYS;
		entry_f = NULL;
	}

	if (entry_f) {
		entry_f->fingerprint = entry->fingerprint;
		entry_f->offset = i;
		entry_f->kv_size = entry->kv_size;
		entry_f->pba = entry->pba;
	} else {
		//print_ptable(ptable, "FULL", 0);
		rc = -1;
	}
	return rc;
}

static char *
update_part_table(struct hash_part_table *ptable, struct hash_bucket *buckets) {
	int rc = 0;
	struct hash_bucket *bucket;
	struct hash_entry *entry;

	int cnt = 0;

	for (int i = 0; i < BUCKETS_IN_PART; i++) {
		bucket = &buckets[i];
		for (int j = 0; j < NR_SET; j++) {
			entry = &bucket->entry[j];
			if (entry->dirty_bit) {
				rc = insert_to_ptable(ptable, entry);
				if (rc) {
					fprintf(stderr, "part_table: insert error");
					abort();
				}
				entry->dirty_bit = 0;
				cnt++;
			}
		}
	}
	//printf("%d entries persisted\n", cnt);
	return (char *)ptable;
}

static void *cb_part_table_write_for_get(void *arg) {
	free(arg);
	return NULL;
}

static void *cb_part_table_write(void *arg) {
	struct request *req = (struct request *)arg;
	struct bi_params *params = (struct bi_params *)req->params;
	params->write_step = BI_STEP_WRITE_KV;
	free(req->temp_buf);
	retry_req_to_hlr(req->hlr, req);
	return NULL;
}

int bigkv_index_get(struct kv_ops *ops, struct request *req) {
	//sw_start(sw_get);
	volatile int rc = 0;
	int lru_origin;
	struct handler *hlr = req->hlr;
	struct bigkv_index *bi = (struct bigkv_index *)ops->_private;
	struct hash_table *table = bi->table;

	uint64_t bucket_idx = req->key.hash_high % NR_BUCKET;
	uint64_t part_idx = bucket_idx / BUCKETS_IN_PART;

	struct hash_partition *part = &table->part[part_idx];
	struct hash_bucket *bucket = &table->bucket[bucket_idx];
	struct hash_bucket *part_buckets =
		&table->bucket[part_idx*BUCKETS_IN_PART];

	struct hash_entry *entry = NULL;
	struct hash_entry_f *entry_f = NULL, tmp_entry_f;
	struct hash_part_table *ptable = NULL;
	struct callback *cb = NULL;

	hash_t tmp_fp;

	if (!req->params) req->params = make_bi_params();
	struct bi_params *params = (struct bi_params *)req->params;

	uint64_t old_pba;

	//if (strncmp(req->key.key, "user4931155956028412375513337700", 32) == 0) {
	//	puts("get here");

	//}


	switch (params->read_step) {
	case BI_STEP_UPDATE_TABLE:
		hlr->stat.nr_read_cache_miss++;
		goto bi_update_table;
	case BI_STEP_KEY_MISMATCH:
		hlr->stat.nr_read_key_mismatch++;
		printf("wtf?\n");
		break;
	default:
		hlr->stat.nr_read++;
		break;
	}

	entry = get_match_entry(bucket, req->key.hash_low);
	if (entry) {
		for (int i = 0; i < NR_SET; i++) {
			struct hash_entry *tmp = &bucket->entry[i];
			if (entry != tmp && tmp->lru_bit < entry->lru_bit) {
				tmp->lru_bit++;
			}
		}
		entry->lru_bit = 0x0;
		goto read_kvpair;
	}

	req->temp_buf = (char *)aligned_alloc(MEM_ALIGN_UNIT, PART_TABLE_SIZE);
	cb = make_callback(hlr, cb_part_table_read, req);
	hlr->read(hlr, part->pba, PART_TABLE_GRAINS, req->temp_buf, cb);
	goto exit;

bi_update_table:
	ptable = (struct hash_part_table *)req->temp_buf;
	entry_f = get_match_entry_f(ptable, req->key.hash_low);
	if (!entry_f) {
		printf("req->key.key: %s\n", req->key.key);
		rc = -1;
		goto exit;
	}
	tmp_entry_f = *entry_f;
	tmp_fp = req->key.hash_low;
	req->temp_buf = NULL;
	cb = make_callback(hlr, cb_keycmp, req);
	hlr->read(hlr, entry_f->pba, entry_f->kv_size, req->value.value, cb);

get_target_retry:
	entry = get_target_entry(bucket, tmp_fp);
	if (entry) {
		lru_origin = entry->lru_bit;
		entry->fingerprint = tmp_entry_f.fingerprint;
		entry->reserve = 0;
		entry->dirty_bit = 0;
		entry->lru_bit = 0;
		entry->kv_size = tmp_entry_f.kv_size;
		entry->pba = tmp_entry_f.pba;
		for (int i = 0; i < NR_SET; i++) {
			struct hash_entry *tmp = &bucket->entry[i];
			if (entry != tmp && tmp->lru_bit < lru_origin) {
				tmp->lru_bit++;
			}
		}
		if (ptable) free(ptable);
		goto exit;
	} else {
		char *ptable_buf = update_part_table(ptable, part_buckets);
		ptable = NULL;
		cb = make_callback(hlr, cb_part_table_write_for_get, ptable_buf);
		old_pba = part->pba;
		part->pba = get_next_idx_pba(hlr, PART_TABLE_SIZE);
		hlr->idx_write(hlr, part->pba, old_pba, PART_TABLE_GRAINS, ptable_buf, cb, part_idx);
		//part->pba = get_next_pba(hlr, PART_TABLE_SIZE);
		//hlr->write(hlr, part->pba, old_pba, PART_TABLE_GRAINS, ptable_buf, cb);
		goto get_target_retry;
	}

read_kvpair:
	cb = make_callback(hlr, cb_keycmp, req);
	hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb);

exit:
	//sw_end(sw_get);
	//t_get += sw_get_usec(sw_get);
	if (rc == -1)
		hlr->stat.nr_read_miss++;
	return rc;
}


int bigkv_index_set(struct kv_ops *ops, struct request *req) {
	sw_start(sw_set);
	int rc = 0;
	struct handler *hlr = req->hlr;
	struct bigkv_index *bi = (struct bigkv_index *)ops->_private;
	struct hash_table *table = bi->table;

	uint64_t bucket_idx = req->key.hash_high % NR_BUCKET;
	uint64_t part_idx = bucket_idx / BUCKETS_IN_PART;

	struct hash_partition *part = &table->part[part_idx];
	struct hash_bucket *bucket = &table->bucket[bucket_idx];
	struct hash_bucket *part_buckets =
		&table->bucket[part_idx*BUCKETS_IN_PART];

	struct hash_entry *entry = NULL;
	struct callback *cb = NULL;

	if (!req->params) req->params = make_bi_params();
	struct bi_params *params = (struct bi_params *)req->params;

	uint64_t old_pba;

	//if (strncmp(req->key.key, "user4931155956028412375513337700", 32) == 0) {
	//	puts("set here");

	//}


	switch (params->write_step) {
	case BI_STEP_WRITE_PTABLE:
		hlr->stat.nr_write_cache_miss++;
		goto bi_write_ptable;
	case BI_STEP_WRITE_KV:
		goto bi_get_entry;
	default:
		hlr->stat.nr_write++;
		break;
	}

	if (part->flying) {
		retry_req_to_hlr(hlr, req);
		goto exit;
	}

bi_get_entry:
	entry = get_target_entry(bucket, req->key.hash_low);
	if (entry) {
		uint8_t lru_origin = entry->lru_bit;
		entry->fingerprint = req->key.hash_low;
		entry->dirty_bit = 1;
		entry->lru_bit = 0;
		entry->kv_size = req->value.len / GRAIN_UNIT;
		//sw_start(sw_pba);
		old_pba = entry->pba;
		entry->pba = get_next_pba(hlr, req->value.len);
		//sw_end(sw_pba);
		//set_pba += sw_get_usec(sw_pba);

		//sw_start(sw_lru);
		for (int i = 0; i < NR_SET; i++) {
			struct hash_entry *tmp = &bucket->entry[i];
			if (entry != tmp && tmp->lru_bit < lru_origin) {
				tmp->lru_bit++;
			}
		}
		//sw_end(sw_lru);
		//set_lru += sw_get_usec(sw_lru);
		goto write_kvpair;
	}

	req->temp_buf = (char *)aligned_alloc(MEM_ALIGN_UNIT, PART_TABLE_SIZE);
	if (part->pba != PBA_INVALID) {
		part->flying = 1;
		cb = make_callback(hlr, cb_part_table_read, req);
		hlr->read(hlr, part->pba, PART_TABLE_GRAINS, req->temp_buf, cb);
		goto exit;
	} else {
		struct hash_part_table *ptable =
			(struct hash_part_table *)req->temp_buf;
		memset(ptable, 0, PART_TABLE_SIZE);
		for (int i = 0; i < PART_TABLE_ENTRYS; i++) {
			ptable->entry[i].pba = PBA_INVALID;
		}
	}

bi_write_ptable:
	part->flying = 0;
	req->temp_buf = update_part_table(
		(struct hash_part_table *)req->temp_buf, part_buckets);
	cb = make_callback(hlr, cb_part_table_write, req);
	old_pba = part->pba;
	part->pba = get_next_idx_pba(hlr, PART_TABLE_SIZE);
	hlr->idx_write(hlr, part->pba, old_pba, PART_TABLE_GRAINS, req->temp_buf, cb, part_idx);
	//part->pba = get_next_pba(hlr, PART_TABLE_SIZE);
	//hlr->write(hlr, part->pba, old_pba, PART_TABLE_GRAINS, req->temp_buf, cb);
	goto exit;

write_kvpair:
	cb = make_callback(hlr, req->end_req, req);
	copy_key_to_value(&req->key, &req->value);
	//sw_start(sw_cpy);
	hlr->write(hlr, entry->pba, old_pba, entry->kv_size, req->value.value, cb);
	//sw_end(sw_cpy);
	//set_cpy += sw_get_usec(sw_cpy);

exit:
	sw_end(sw_set);
	set_total += sw_get_usec(sw_set);
	return rc;
}

int bigkv_index_delete(struct kv_ops *ops, struct request *req) {
	return 0;
}

int bigkv_index_need_gc(struct kv_ops *ops, struct handler *hlr) {

	return dev_need_gc(hlr);
}

void *cb_idx_gc (void *arg) {
	return NULL;
}

static int trigger_idx_gc(struct handler* hlr, struct gc *gc) {

	struct bigkv_index *bi = (struct bigkv_index *)hlr->ops->_private;
	struct hash_table *table = bi->table;

	struct segment *victim_seg = (struct segment *)gc->_private;
	uint32_t valid_cnt = gc->valid_cnt;
	uint32_t entry_cnt = victim_seg->entry_cnt;
	char *seg_buf = (char*)gc->buf;

	uint64_t start_pba = victim_seg->start_addr / GRAIN_UNIT;
	uint64_t victim_part_pba;
	uint64_t part_idx;
	struct hash_partition *part;
	char *ptable_buf;
	struct callback *cb = NULL;
	
	for (int i = 0; i < entry_cnt; i++) {
		part_idx = *(seg_buf + i * PART_IDX_SIZE);
		part = &table->part[part_idx];
		victim_part_pba = start_pba + i * PART_TABLE_GRAINS;
		if (part->pba == victim_part_pba) { // valid part
			ptable_buf = seg_buf + SEG_IDX_HEADER_SIZE + i * PART_TABLE_SIZE;
			cb = make_callback(hlr, cb_idx_gc, gc);
			part->pba = get_next_idx_pba(hlr, PART_TABLE_SIZE);
			hlr->idx_write(hlr, part->pba, victim_part_pba, PART_TABLE_GRAINS, ptable_buf, cb, part_idx);

			//part->pba = get_next_pba(hlr, PART_TABLE_SIZE);
			//hlr->write(hlr, part->pba, victim_part_pba, PART_TABLE_GRAINS, ptable_buf, cb);
		}
	}

	if (victim_seg->invalid_cnt != entry_cnt) {
		abort();
	}

	gc->state = GC_DONE;

	return 0;
}

static int trigger_data_gc(struct handler* hlr, struct gc *gc) {
	struct bigkv_index *bi = (struct bigkv_index *)hlr->ops->_private;
	struct hash_table *table = bi->table;

	struct segment *victim_seg = (struct segment *)gc->_private;
	uint32_t valid_cnt = gc->valid_cnt;
	uint32_t entry_cnt = victim_seg->entry_cnt;
	char *seg_buf = (char*)gc->buf;

	uint64_t start_pba = victim_seg->start_addr / GRAIN_UNIT;
	uint64_t victim_part_pba;
	uint64_t part_idx;
	struct hash_partition *part;
	char *ptable_buf;
	struct callback *cb = NULL;

	/*
	
	for (int i = 0; i < entry_cnt; i++) {
		part_idx = *(seg_buf + i * PART_IDX_SIZE);
		part = &table->part[part_idx];
		victim_part_pba = start_pba + i * PART_TABLE_GRAINS;
		if (part->pba == victim_part_pba) { // valid part
			ptable_buf = seg_buf + SEG_IDX_HEADER_SIZE + i * PART_TABLE_SIZE;
			cb = make_callback(hlr, cb_idx_gc, gc);
			part->pba = get_next_idx_pba(hlr, PART_TABLE_SIZE);
			hlr->idx_write(hlr, part->pba, victim_part_pba, PART_TABLE_GRAINS, ptable_buf, cb, part_idx);

			//part->pba = get_next_pba(hlr, PART_TABLE_SIZE);
			//hlr->write(hlr, part->pba, victim_part_pba, PART_TABLE_GRAINS, ptable_buf, cb);
		}
	}

	if (victim_seg->invalid_cnt != entry_cnt) {
		abort();
	}
	*/

	return 0;
}

int bigkv_index_trigger_gc(struct kv_ops *ops, struct handler *hlr) {
	struct gc *gc = hlr->gc;

	if (!dev_read_victim_segment(hlr, gc)) {
		goto exit;
	}

	if (gc->is_idx)
		trigger_idx_gc(hlr, gc);
	else
		trigger_data_gc(hlr, gc);


exit:
	reap_gc_segment(hlr, gc);
	gc->state = GC_DONE;

	return gc->valid_cnt;
}

int bigkv_index_wait_gc(struct kv_ops *ops, struct handler *hlr) {
	return 0;
}
