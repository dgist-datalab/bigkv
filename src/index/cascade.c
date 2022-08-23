#include "index/cascade.h"
#include "platform/device.h"
#include "platform/util.h"
#include "utility/stopwatch.h"
#include "utility/ttl.h"

#include <stdlib.h>
#include <string.h>

stopwatch *sw_set, *sw_lru, *sw_pba, *sw_cpy;
time_t set_total, set_lru, set_pba, set_cpy;

stopwatch *sw_get;
time_t t_get;

static uint64_t not_found_count[20] = {0,};
uint64_t notnot = 0;

static void print_notnot() {
	printf("notnot ");
	for (int i = 0; i < 20; i++) {
		printf("%lu ", not_found_count[i]);
	}
	printf("\n");
}

static void copy_key_to_value(struct key_struct *key, struct val_struct *value, uint32_t ttl) {
	memcpy(value->value, &key->len, sizeof(key->len));
	memcpy(value->value+sizeof(key->len), key->key, key->len);
	memcpy(value->value+sizeof(key->len)+key->len, &value->len, sizeof(value->len));
	memcpy(value->value+sizeof(key->len)+key->len+sizeof(value->len), &ttl, sizeof(ttl));
}

static bool is_expired_entry (struct request *req, struct key_struct *key, struct val_struct *value) {
#ifndef TTL
	return false;
#endif
	if (req->req_time > ((uint32_t *)(value->value + sizeof(key->len) + key->len + sizeof(value->len)))[0])
		return true;
	return false;
}


static void init_t2_fifo(struct handler *, struct cascade *, struct t1_hash_partition *);
static void init_t3_bucket(struct handler *, struct cascade *, struct t3_hash_bucket *, int, bool);
static void *cb_free(void *arg);

static int gdb_flags = 0;
static t3_hash_bucket *gdb_t3_bucket;


int cascade_init(struct kv_ops *ops) {
	int rc = 0;
	struct cascade *cas = 
		(struct cascade *)calloc(1, sizeof(struct cascade));
	struct handler *hlr = (struct handler *)(ops->hlr);

	cas->ops_number = ops->ops_number;

	cas->t1_table = (struct t1_hash_table *)calloc(1, sizeof(struct t1_hash_table));
	cas->t1_table->t1_bucket = (struct t1_hash_bucket *)malloc(NR_T1_BUCKET * sizeof(struct t1_hash_bucket));
	memset(cas->t1_table->t1_bucket, 0, NR_T1_BUCKET * sizeof(struct t1_hash_bucket));
	for (int i = 0; i < NR_T1_BUCKET; i++) {
		struct t1_hash_bucket *t1_bucket = &cas->t1_table->t1_bucket[i];
		t1_bucket->hash_list = list_init();
	}

	cas->t1_table->t1_part = (struct t1_hash_partition *)malloc(NR_T1_PART * sizeof(struct t1_hash_partition));
	memset(cas->t1_table->t1_part, 0, NR_T1_PART * sizeof(struct t1_hash_partition));
	for (int i = 0; i < NR_T1_PART; i++) {
		struct t1_hash_partition *t1_part = &cas->t1_table->t1_part[i];
		t1_part->pba = PBA_INVALID;
		init_t2_fifo(hlr, cas, t1_part); 
	}

	struct t3_hash_bucket *t3_bucket;

	cas->t3_narrow = (struct t3_narrow_hash_table *)malloc(sizeof(struct t3_narrow_hash_table));
	cas->t3_narrow->t3_bucket = (struct t3_hash_bucket *)malloc(NR_T3_NARROW * sizeof(struct t3_hash_bucket));
	for (int i = 0; i < NR_T3_NARROW; i++) {
		t3_bucket = &cas->t3_narrow->t3_bucket[i];
		t3_bucket->buf = (struct t3_hash_block *)aligned_alloc(MEM_ALIGN_UNIT,sizeof(struct t3_hash_block));
		t3_bucket->buf_idx = 0;
		t3_bucket->last_idx = 0;
		t3_bucket->hash_list = list_init();
		t3_bucket->size = 0;
		init_t3_bucket(hlr, cas, t3_bucket, NR_T3_NARROW_BLOCK, 1);
		t3_bucket->last = NULL;
	}

	cas->t3_wide = (struct t3_wide_hash_table *)malloc(sizeof(struct t3_wide_hash_table));
	cas->t3_wide->t3_bucket = (struct t3_hash_bucket *)malloc(NR_T3_WIDE * sizeof(struct t3_hash_bucket));
	for (int i = 0; i < NR_T3_WIDE; i++) {
		//if ((i % 1024) == 0) printf("%d/%d done\n", i, NR_T3_WIDE);
		t3_bucket = &cas->t3_wide->t3_bucket[i];
		t3_bucket->buf = NULL;
		t3_bucket->buf_idx = 0;
		t3_bucket->last_idx = 0;
		t3_bucket->hash_list = list_init();
		t3_bucket->size = 0;
		init_t3_bucket(hlr, cas, t3_bucket, NR_T3_WIDE_BLOCK, 0);
		t3_bucket->last = NULL;
		t3_bucket->hash_list->head->prv = t3_bucket->hash_list->tail;
		t3_bucket->hash_list->tail->nxt = t3_bucket->hash_list->head;
	}
	//char *dummy_buf = (char *)malloc(2 * 1024 * 1024);
	uint64_t dummy_pba = get_next_pba_dummy(hlr, 2 * 1024 * 1024, cas->ops_number);
	//struct callback *dummy_cb = make_callback(hlr, cb_free, dummy_buf); 
	//hlr->write(hlr, dummy_pba, PBA_INVALID, 2 * 1024 * 1024 / GRAIN_UNIT, dummy_buf, dummy_cb);
	
	printf("CASCADE READY %lu GB idx\n", dummy_pba * GRAIN_UNIT / 1024 / 1024 / 1024);


	ops->_private = (void *)cas;

	sw_set = sw_create();
/*	sw_lru = sw_create();
	sw_pba = sw_create();
	sw_cpy = sw_create();
	sw_get = sw_create(); */
	return rc;
}

int cascade_free(struct kv_ops *ops) {
	int rc = 0;
	struct cascade *cas = (struct cascade *)ops->_private;
	for (int i = 0; i < NR_T1_BUCKET; i++) {
		list_free(cas->t1_table->t1_bucket->hash_list);
	}
	free(cas->t1_table->t1_bucket);
	free(cas->t1_table->t1_part);
	free(cas->t1_table);
	li_node *cur;
	struct t3_list_item *item;
	for (int i = 0; i < NR_T3_NARROW; i++) {
		list_for_each_node(cas->t3_narrow->t3_bucket[i].hash_list, cur) {
			item = (struct t3_list_item *)cur->data;
			free(item->bf);
		}
		list_free(cas->t3_narrow->t3_bucket->hash_list);
		free(cas->t3_narrow->t3_bucket->buf);
	}
	free(cas->t3_narrow->t3_bucket);
	free(cas->t3_narrow);
	for (int i = 0; i < NR_T3_WIDE; i++) {
		list_free(cas->t3_wide->t3_bucket->hash_list);
	}
	free(cas->t3_wide->t3_bucket);
	free(cas->t3_wide);
	free(cas);
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
get_match_entry(li_node *cur, hash_t fp) {
	struct hash_entry *entry;
	get_next_for_each_node(cur) {
		entry = (struct hash_entry *)cur->data;
		if (entry->fingerprint == fp)
			return entry;
		
	}
	return NULL;
}

static int keycmp_k_v(struct key_struct *key, struct val_struct *value) {
	if (key->len != ((uint8_t *)value->value)[0]) {
		return (key->len > ((uint8_t *)value->value)[0]) ? 1:-1;
	}
	return strncmp(key->key, value->value+(sizeof(uint8_t)), key->len);
}

void *cb_keycmp(void *arg) {
	struct request *req = (struct request *)arg;
	struct cas_params *params = (struct cas_params *)req->params;

	struct key_struct *key = &req->key;
	struct val_struct *value = &req->value;
	bool is_expired;

	switch (req->type) {
		case REQ_TYPE_GET:
			if (keycmp_k_v(key, value) == 0) {
				is_expired = is_expired_entry(req, key, value);
				if (params->read_step == CAS_STEP_INIT) {
					if (is_expired) {
						params->read_step = CAS_STEP_T1_EXPIRED;
					} else {
						params->read_step = CAS_STEP_T1_MATCH;
					}
				} else if (params->read_step != CAS_STEP_READ_T3_BUFFER) {
					printf("CORRECT PARAMS->READ_STEP: %d\n", params->read_step);
					if (is_expired) {
						params->read_step = CAS_STEP_EXPIRED;
					} else {
						params->read_step = CAS_STEP_READ_KV_SUCC;
					}
				} else {
					if (is_expired)
						params->read_step = CAS_STEP_EXPIRED;
				}
				//req->end_req(req);
				//return NULL;
			} else {
				if (params->read_step == CAS_STEP_INIT) {
					params->read_step = CAS_STEP_T1_MISMATCH;
				} else if (params->read_step == CAS_STEP_READ_T3_BUFFER) {
					params->read_step = CAS_STEP_READ_T3_BUFFER_FAIL;
				} else {
					printf("PARAMS->READ_STEP: %d\n", params->read_step);
					params->read_step = CAS_STEP_KEY_MISMATCH;
				}
			}
			break;
		case REQ_TYPE_SET:
			if (keycmp_k_v(key, value) == 0) {
				is_expired = is_expired_entry(req, key, value);
				if (params->write_step == CAS_STEP_INIT) {
					if (is_expired) {
						params->write_step = CAS_STEP_T1_EXPIRED;
					} else {
						params->write_step = CAS_STEP_T1_MATCH;
					}
				} else {
					if (is_expired)
						params->write_step = CAS_STEP_EXPIRED;
					else
						params->write_step = CAS_STEP_READ_KV_SUCC;
				}
			} else {
				if (params->write_step == CAS_STEP_INIT) {
					params->write_step = CAS_STEP_T1_MISMATCH;
				} else if (params->write_step == CAS_STEP_READ_T3_BUFFER) {
					params->write_step = CAS_STEP_READ_T3_BUFFER_FAIL;
				} else {
					printf("PARAMS->WRITE_STEP: %d\n", params->write_step);
					params->write_step = CAS_STEP_KEY_MISMATCH;
				}
			}
			break;
		default:
			fprintf(stderr, "Wrong req type on cb_keycmp");
			break;
	}

	retry_req_to_hlr(req->hlr, req);
	return NULL;
}

#if 0
static void *cb_keycmp(void *arg) {
	struct request *req = (struct request *)arg;
	struct cas_params *params = (struct cas_params *)req->params;

	struct key_struct *key = &req->key;
	struct val_struct *value = &req->value;

	if (keycmp_k_v(key, value) == 0) {
		req->end_req(req);
		return NULL;
	} else {
		params->read_step = CAS_STEP_KEY_MISMATCH;
	}

	retry_req_to_hlr(req->hlr, req);
	return NULL;
}
#endif

static void *cb_free(void *arg) {
	free(arg);
	return NULL;
}

static void *cb_nan(void *arg) {
	return NULL;
}


static void
init_t2_fifo(struct handler *hlr, struct cascade *cas, struct t1_hash_partition *t1_part) {
	//char *dummy = (char *)malloc(NR_T2_BLOCK * T2_BLOCK_SIZE);
	//struct callback *cb = NULL;

	t1_part->t2_fifo_size = 0;
	t1_part->flying_req = 0;
	t1_part->t2_fifo_buf = (struct t2_fifo_buf *)malloc(sizeof(struct t2_fifo_buf));
	t1_part->t2_fifo_buf->buf_idx = 0;
	//cb = make_callback(hlr, cb_free, dummy);
	t1_part->t2_fifo_buf->buf = (struct t2_hash_block *)aligned_alloc(MEM_ALIGN_UNIT,sizeof(struct t2_hash_block));	
	t1_part->pba = get_next_pba_dummy(hlr, NR_T2_BLOCK * T2_BLOCK_SIZE, cas->ops_number);
	//hlr->write(hlr, t1_part->pba, PBA_INVALID, NR_T2_BLOCK * T2_BLOCK_GRAINS, dummy, cb);
	return;
}

static void
init_t3_bucket(struct handler *hlr, struct cascade *cas, struct t3_hash_bucket *t3_bucket, int size, bool is_narrow) {
	//char *dummy = (char *)malloc(T3_BLOCK_SIZE);
	//struct callback *cb = NULL;

	for (int i = 0; i < size; i++) {
		struct t3_list_item *t3_item = (struct t3_list_item *)malloc(sizeof(struct t3_list_item));
		//cb = make_callback(hlr, cb_nan, dummy);
		t3_item->lnode = list_insert(t3_bucket->hash_list, t3_item);
		t3_item->pba = get_next_pba_dummy(hlr, T3_BLOCK_SIZE, cas->ops_number);
		if (is_narrow) {
			t3_item->bf = (struct bloomfilter *)malloc(sizeof(struct bloomfilter) + BF_M);
			bloomfilter_init(t3_item->bf, BF_M, BF_K);
		} else {
			t3_item->bf = NULL;
		}
		//hlr->write(hlr, t3_item->pba, PBA_INVALID, T3_BLOCK_GRAINS, dummy, cb);
	}
	//free(dummy);
	return;
}

static void
init_t3_hash_block(struct t3_hash_block *t3_block) {
	struct hash_entry_f *entry_f = NULL;
	memset(t3_block, 0, T3_BLOCK_SIZE);
	for (int i = 0; i < T3_BLOCK_ENTRY; i++) {
		entry_f = t3_block->entry + i;
		entry_f->pba = PBA_INVALID;
	}
	return;
}

static struct hash_entry *
get_target_entry(struct cascade *cas, li_node *cur, hash_t fp) {
	struct hash_entry *entry;
	//list_for_each_node(hash_list, cur) {
	if (cur == NULL)
		return NULL;
	get_next_for_each_node(cur) {
		entry = (struct hash_entry *)cur->data;
		if (entry->fingerprint == fp)
			return entry;
		
	}
	return NULL;
}

static struct hash_entry_f *
get_match_entry_f(struct t3_hash_block *t3_block, hash_t fp, int start_idx, bool is_read) {
	struct hash_entry_f *entry_f = NULL;
	uint64_t t3_ttl_cnt = 0;
	if (start_idx == 0)
		abort();
	for (int i = start_idx - 1; i >= 0; i--) {
		entry_f = t3_block->entry + i;
		if ((entry_f->pba != PBA_INVALID) && (entry_f->fingerprint == fp)) {
#ifdef TTL
			/*
			if (is_read && entry_f->ttl < get_cur_sec()) {
				printf("TTL!!! T3 %lu\n", ++t3_ttl_cnt);
				//entry_f->pba = PBA_INVALID;
				entry_f->evict_bit = 1;
				entry_f = NULL;
				continue;
			}
			*/
#endif

			return entry_f;
		}
	}
	return NULL;
}

static bool need_cache_eviction(struct cascade *cas) {
	return cas->t1_table->cached_entry >= CACHE_THRESHOLD;
}

static void advance_clock_hand(struct t1_hash_bucket *t1_bucket) {
	li_node *clock_hand = t1_bucket->clock_hand;
	li_node *new_node;
	struct hash_entry *entry = (struct hash_entry *)clock_hand->data;
	if (entry->clock_bit)
		abort();

	if (clock_hand->nxt == NULL) {
		new_node = t1_bucket->hash_list->head;
	} else {
		new_node = clock_hand->nxt;
	}

	if (!new_node)
		abort();

	t1_bucket->clock_hand = new_node;
	return;
}

#ifdef TTL
static struct hash_entry *
insert_t1_entry(struct t1_hash_table *t1_table, struct t1_hash_bucket *t1_bucket, uint64_t fp, uint64_t pba, uint32_t kv_size, uint8_t dirty, uint64_t ttl, bool is_evicted) {
#else
static struct hash_entry *
insert_t1_entry(struct t1_hash_table *t1_table, struct t1_hash_bucket *t1_bucket, uint64_t fp, uint64_t pba, uint32_t kv_size, uint8_t dirty) {
#endif
	struct hash_entry *entry = (struct hash_entry *)malloc(sizeof(struct hash_entry));
	entry->fingerprint = fp;
	entry->reserve = 0;
	entry->clock_bit = 0;
	entry->flying_bit = 0;
	entry->kv_size = kv_size;
	entry->pba = pba;
#ifdef TTL
	entry->ttl = ttl;
	entry->evict_bit = is_evicted;
	entry->dirty_bit = dirty || is_evicted;
#else
	entry->dirty_bit = dirty;
	entry->evict_bit = 0;
	//entry->ttl = 0;
#endif

	entry->lnode = list_insert(t1_bucket->hash_list, entry);
	if (t1_bucket->hash_list->size == 1)
		t1_bucket->clock_hand = entry->lnode;

	t1_table->cached_entry++;
	return entry;
}

static struct t2_hash_block *
collect_t2_block(struct t1_hash_table *t1_table, struct t1_hash_partition *t1_part, int part_idx) {
	//struct t2_hash_block *t2_block = (struct t2_hash_block *)aligned_alloc(MEM_ALIGN_UNIT,sizeof(struct t2_hash_block));	
	struct t2_hash_block *t2_block = t1_part->t2_fifo_buf->buf;
	struct t1_hash_bucket *t1_bucket = &t1_table->t1_bucket[part_idx*NR_T1_BUCKET_PER_PART];
	list *hash_list;
	struct hash_entry *entry;
	struct hash_entry_f *entry_f;
	li_node *cur, *tmp, *nxt;
	int collected_entry_cnt, total_collected_cnt = 0;
	struct t1_hash_bucket *cur_bucket;
	int target_cnt = T2_BLOCK_ENTRY;
retry:
	target_cnt -= total_collected_cnt;
	for (int i = 0; i < NR_T1_BUCKET_PER_PART; i++) {
		cur_bucket = t1_bucket + i;
		hash_list = cur_bucket->hash_list;
		collected_entry_cnt = 0;
		for (cur = cur_bucket->clock_hand, nxt=cur?cur->nxt:NULL; cur != NULL; cur = nxt, nxt=cur?cur->nxt:NULL) {
			if (t1_part->t2_fifo_buf->buf_idx == T2_BLOCK_ENTRY)
				break;
			entry = (struct hash_entry *)cur->data;
			if (entry->clock_bit || entry->flying_bit) {
				entry->clock_bit = 0;
				continue;
			}
			if (entry->dirty_bit) {

				/*
				if (entry->fingerprint == 9326686948173054547) {
					//gdb_flags = 1;
					print_entry(entry, "WRITE_T2-1");
					printf("buf_idx: %d\n", t1_part->t2_fifo_buf->buf_idx);
				}
				*/
				entry_f = &t2_block->entry[t1_part->t2_fifo_buf->buf_idx++];
				entry_f->fingerprint = entry->fingerprint;
				entry_f->reserve = 0;
				entry_f->kv_size = entry->kv_size;
				entry_f->pba = entry->pba;
				entry_f->evict_bit = entry->evict_bit;
				//entry_f->ttl = entry->ttl;
			}
			if (cur == cur_bucket->clock_hand)
				advance_clock_hand(cur_bucket);
			list_delete_node(hash_list, cur);
			t1_table->cached_entry--;
			if (hash_list->size == 0) cur_bucket->clock_hand = NULL;
			free(entry);
			if(++total_collected_cnt >= T2_BLOCK_ENTRY)
				break;
			if (++collected_entry_cnt >= (target_cnt/NR_T1_BUCKET_PER_PART))
				break;
		}

		//if (t2_block_idx == T2_BLOCK_ENTRY)
		//	break;
		//if (collected_entry_cnt % (T2_BLOCK_ENTRY/NR_T1_BUCKET_PER_PART))
		//	continue;

		for (cur = hash_list->head, nxt=cur?cur->nxt:NULL; cur != cur_bucket->clock_hand; cur = nxt, nxt=cur?cur->nxt:NULL) {
			if (t1_part->t2_fifo_buf->buf_idx == T2_BLOCK_ENTRY)
				break;
			entry = (struct hash_entry *)cur->data;
			if (entry->clock_bit || entry->flying_bit) {
				entry->clock_bit = 0;
				continue;
			}
			if (entry->dirty_bit) {
				/*
				if (entry->fingerprint == 9326686948173054547) {
					print_entry(entry, "WRITE_T2-2");
					//gdb_flags = 1;
				}
				*/

				entry_f = &t2_block->entry[t1_part->t2_fifo_buf->buf_idx++];
				entry_f->fingerprint = entry->fingerprint;
				entry_f->reserve = 0;
				entry_f->kv_size = entry->kv_size;
				entry_f->pba = entry->pba;
				entry_f->evict_bit = entry->evict_bit;
				//entry_f->ttl = entry->ttl;
			}
			if (cur == cur_bucket->clock_hand)
				advance_clock_hand(cur_bucket);
			list_delete_node(hash_list, cur);
			if (hash_list->size == 0) cur_bucket->clock_hand = NULL;
			free(entry);
			t1_table->cached_entry--;
			if (++total_collected_cnt >= T2_BLOCK_ENTRY)
				break;
			if (++collected_entry_cnt >= (target_cnt/NR_T1_BUCKET_PER_PART))
				break;
		}
	}

	if (total_collected_cnt < T2_BLOCK_ENTRY)
		goto retry;

	return t2_block;
}

/*
static void * cb_t2_write (void *arg) {
	//retry_req_to_hlr(req->hlr, req);
	free(arg);
	return NULL;
}
*/

static struct t2_hash_block *write_t2_block(struct cascade *cas, struct handler *hlr, struct t1_hash_partition *t1_part, struct t2_hash_block *t2_block) {
	struct callback *cb;
	uint8_t ori_head = t1_part->t2_fifo_head;
	struct t2_hash_block *ori_t2_block, *ret = NULL;
	if (t1_part->t2_fifo_size == NR_T2_BLOCK) {
		// need warterfall
		// write a t3 block at fifo head
		ori_t2_block = (struct t2_hash_block *)aligned_alloc(MEM_ALIGN_UNIT,sizeof(struct t2_hash_block));
		hlr->sync_read(hlr, t1_part->pba + ori_head * T2_BLOCK_GRAINS, T2_BLOCK_GRAINS, (char*)ori_t2_block, cas->ops_number);
		/*
		if ((t1_part->pba + ori_head * T2_BLOCK_GRAINS) == 4096) {
			printf("938th: %lu\n", ori_t2_block->entry[938]);
		}
		*/
		t1_part->t2_fifo_head = (t1_part->t2_fifo_head + 1) % NR_T2_BLOCK;
		ret = ori_t2_block;
	}
	//if (t1_part->t2_fifo_size >= NR_T2_BLOCK)
	//	abort();

	uint64_t fifo_pba = t1_part->pba;
	uint64_t t2_block_pba;
	if (t1_part->t2_fifo_size == NR_T2_BLOCK) {
		t2_block_pba = t1_part->pba + T2_BLOCK_GRAINS * ori_head;
	} else {
		if (t1_part->t2_fifo_head)
			abort();
		t2_block_pba = t1_part->pba + T2_BLOCK_GRAINS * t1_part->t2_fifo_size;
		t1_part->t2_fifo_size++;
	}
	if (gdb_flags) {
		printf("@@@@@@@@@@@@@@@@@@@@@@@@@ 938th: %lu\n", t2_block->entry[938].fingerprint);
		gdb_flags = 0;
	}
	/*
	printf("%p part size: %d, ori_head: %d, new_head: %d, t2_block_pba: %d\n", t1_part, t1_part->t2_fifo_size, ori_head, t1_part->t2_fifo_head, t2_block_pba);
	if (t2_block_pba == 4224) {
		printf("[ERR]%lu\n",t2_block->entry[214].fingerprint);
		printf("[ERR]%lu\n",t2_block->entry[1023].fingerprint);
		printf("[ERR]%d %d %p %p\n", sizeof(t2_block->entry), sizeof(struct t2_hash_block), t2_block, t2_block->entry);
	}
	*/
	//cb = make_callback(hlr, cb_t2_write, t2_block);
	int retval = 0;
	retval = hlr->sync_write(hlr, t2_block_pba, T2_BLOCK_GRAINS, (char *)t2_block->entry, cas->ops_number);
	if (retval != T2_BLOCK_GRAINS * GRAIN_UNIT) {
		perror("???");
		//abort();
	}
	/*
	if (t2_block_pba == 4096) {
		struct t2_hash_block *tmp_t2_block = (struct t2_hash_block *)aligned_alloc(MEM_ALIGN_UNIT,sizeof(struct t2_hash_block));
		retval = hlr->sync_read(hlr, 4096, T2_BLOCK_GRAINS, (char*)tmp_t2_block);
		if (retval != T2_BLOCK_GRAINS * GRAIN_UNIT) {
			perror("???");
			//abort();
		}
		printf("tmp 938th: %lu\n", tmp_t2_block->entry[938]);
		free(tmp_t2_block);
	}
	*/

	/*
	if (t2_block_pba == 4224) {
		memset(t2_block, 0, T2_BLOCK_SIZE);
		printf("[ERR]%lu\n",t2_block->entry[214].fingerprint);
		printf("[ERR]%lu\n",t2_block->entry[1023].fingerprint);
		printf("[ERR]%d %d %p %p\n", sizeof(t2_block->entry), sizeof(struct t2_hash_block), t2_block, t2_block->entry);

		hlr->sync_read(hlr, 4224, T2_BLOCK_GRAINS, (char *)t2_block);
	printf("[YOU]%lu\n",t2_block->entry[214].fingerprint);
		printf("[YOU]%lu\n",t2_block->entry[1023].fingerprint);
		printf("[YOU]%d %d %p %p\n", sizeof(t2_block->entry), sizeof(struct t2_hash_block), t2_block, t2_block->entry);


	}
	*/
	t1_part->t2_fifo_buf->buf_idx = 0;
	//free(t2_block);
	return ret;
}

static void *cb_t2_read(void *arg);

static void *cb_t2_read_kvcmp(void *arg) {
	struct t2_read_params *t2_read_params = (struct t2_read_params *)arg;
	struct request *req = t2_read_params->req;
	struct cas_params *params = (struct cas_params *)req->params;
	struct hash_entry_f *entry_f;

	struct key_struct *key = &req->key;
	struct val_struct value;
	value.value = (char*)t2_read_params->kv_buf;
	bool is_expired;

	//if (req->key.hash_low == 9326686948173054547) {
	//	printf("ASD\n");
	//}

	if (keycmp_k_v(key, &value) == 0) {
		is_expired = is_expired_entry(req, key, &value);
		pthread_mutex_lock(&params->mutex);
		if (params->t2_target_idx > t2_read_params->batch_idx) {
			params->t2_target_idx = t2_read_params->batch_idx;
			memcpy(&params->entry_f, &t2_read_params->entry_f, sizeof(struct hash_entry_f));
			memcpy(req->value.value, t2_read_params->kv_buf, t2_read_params->entry_f.kv_size);
			params->is_expired = is_expired;
		}
		params->read_done++;
		if (t2_read_params->t2_read_cnt == params->read_done) {
			params->read_done = 0;
			if (params->is_expired) {
				params->write_step = params->read_step = CAS_STEP_EXPIRED;
			} else {
				params->write_step = params->read_step = CAS_STEP_READ_T2_SUCC;
			}
			//retry_req_to_hlr(req->hlr, req);
			retry_req_to_hlr(req->hlr, req);
		}
		pthread_mutex_unlock(&params->mutex);
	} else {
		if (t2_read_params->start_idx > 0) {
			free(value.value);
			cb_t2_read(arg);
			return NULL;
		}
		pthread_mutex_lock(&params->mutex);
		params->read_done++;
		if (t2_read_params->t2_read_cnt == params->read_done) {
			if (params->t2_target_idx != 8) {
				if (params->is_expired) {
					params->write_step = params->read_step = CAS_STEP_EXPIRED;
				} else {
					params->write_step = params->read_step = CAS_STEP_READ_T2_SUCC;
				}
			} else {
				params->write_step = CAS_STEP_READ_T2_FAIL;
				params->read_step = CAS_STEP_READ_T2_FAIL;
			}
			params->read_done = 0;
			retry_req_to_hlr(req->hlr, req);
		}
		pthread_mutex_unlock(&params->mutex);
	}
	free(t2_read_params->kv_buf);
	free(t2_read_params->buf);
	free(t2_read_params);
	return NULL;
}

static void *cb_t2_read(void *arg) {
	struct t2_read_params *t2_read_params = (struct t2_read_params *)arg;
	struct request *req = t2_read_params->req;
	struct handler *hlr = req->hlr;
	struct t2_hash_block *t2_block = (struct t2_hash_block *)t2_read_params->buf;
	struct cas_params *params = (struct cas_params *)req->params;
	struct hash_entry_f *entry_f;
	uint64_t t2_ttl_cnt = 0;

	/*
	if (t2_read_params->target_fp == 9326686948173054547) {
		printf("asd\n");
	}
	*/

	for (int i = t2_read_params->start_idx - 1; i >= 0; i--) {
		entry_f = &t2_block->entry[i];
		if (entry_f->fingerprint == t2_read_params->target_fp) {
			/*
			if (entry_f->pba == PBA_INVALID) {
				entry_f = NULL;
				continue;
			}
			*/
#ifdef TTL
			/*
			if (t2_read_params->is_read && entry_f->ttl < get_cur_sec()) {
				printf("TTL!!! T2 %lu\n", ++t2_ttl_cnt);
				//entry_f->pba = PBA_INVALID;
				//entry_f->evict_bit = 1;
				entry_f = NULL;
				continue;
			}
			*/
#endif
			//params->entry_f = (struct hash_entry_f)malloc(sizeof(struct hash_entry_f));
			memcpy(&t2_read_params->entry_f, entry_f, ENTRY_SIZE);
			t2_read_params->start_idx = i;
			break;
		}
		entry_f = NULL;
	}

	if (entry_f) {
		struct callback *cb;
		cb = make_callback(hlr, cb_t2_read_kvcmp, t2_read_params);
		t2_read_params->kv_buf = (void *)aligned_alloc(MEM_ALIGN_UNIT,entry_f->kv_size * GRAIN_UNIT);
		//memset(t2_read_params->kv_buf, 0, entry_f->kv_size * GRAIN_UNIT);
		//printf("entry_f - fp: %lu, pba: %lu, kv_size: %lu\n", entry_f->fingerprint, entry_f->pba, entry_f->kv_size);
		//printf("t2->entry_f - fp: %lu, pba: %lu, kv_size: %lu\n", t2_read_params->entry_f.fingerprint, t2_read_params->entry_f.pba, t2_read_params->entry_f.kv_size);
		//if (entry_f->kv_size != 8)
		//	abort();
		if (hlr->poller_read(hlr, entry_f->pba, entry_f->kv_size, (char *)t2_read_params->kv_buf, cb, params->cas->ops_number) < 0) {
			cb_t2_read_kvcmp(t2_read_params);
		} else {
			//req->data_lookups++;
			//req->data_lookup_bytes += entry_f->kv_size * GRAIN_UNIT;
		}
		//printf("ASdasdqwlkjnasd\n");
		//int ret;
		//ret = hlr->sync_read(hlr, entry_f->pba, entry_f->kv_size, (char *)t2_read_params->kv_buf);
		//if (ret != entry_f->kv_size * GRAIN_UNIT) {
		//	perror("???");
		//	abort();
		//}
		//cb_t2_read_kvcmp((void*)t2_read_params);
	} else {
		pthread_mutex_lock(&params->mutex);
		params->read_done++;
		if ((t2_read_params->t2_read_cnt == params->read_done)) {
			if (params->t2_target_idx == 8) {
				params->write_step = CAS_STEP_READ_T2_FAIL;
				params->read_step = CAS_STEP_READ_T2_FAIL;
			} else {
				params->write_step = CAS_STEP_READ_T2_SUCC;
				params->read_step = CAS_STEP_READ_T2_SUCC;
			}
			params->read_done = 0;
			retry_req_to_hlr(req->hlr, req);
		}
		pthread_mutex_unlock(&params->mutex);
		free(t2_block);
		free(t2_read_params);
	}
	return NULL;
}

#define MOD(a,b) ((a+b)%b)
static int read_t2_block(struct handler *hlr, struct t1_hash_partition *t1_part, struct request *req) {
	struct callback *cb;
	int batch_num = t1_part->flying_req++;
	int batch_start_idx = t1_part->t2_fifo_head;
	batch_start_idx = MOD(batch_start_idx - 1 - (batch_num * T2_PAR_UNIT), t1_part->t2_fifo_size);
	struct cas_params *cas_params= (struct cas_params *)req->params;

	//if (t1_part->t2_fifo_size > 8)
	//	printf("asd\n");

	if (batch_num * T2_PAR_UNIT >= t1_part->t2_fifo_size)
		return -1;

	int max_read;
	if (t1_part->t2_fifo_size > T2_PAR_UNIT) {
		max_read = (t1_part->t2_fifo_size < T2_PAR_UNIT * (batch_num + 1)) ? t1_part->t2_fifo_size - T2_PAR_UNIT * batch_num : T2_PAR_UNIT;
	} else {
		max_read = t1_part->t2_fifo_size;
	}
	t1_part->flying = 1;
	//printf("batch_num: %d, batch_start_idx: %d, max_read: %d\n", batch_num, batch_start_idx, max_read);
	for (int i = batch_start_idx, j = 0; j < max_read; i = MOD(i-1,t1_part->t2_fifo_size), j++) {
		struct t2_read_params *t2_read_params = (struct t2_read_params *)malloc(sizeof(struct t2_read_params));
		//t2_read_params->buf = (void *)malloc(T2_BLOCK_SIZE);
		t2_read_params->buf = (void *)aligned_alloc(MEM_ALIGN_UNIT, T2_BLOCK_SIZE);
		memset(t2_read_params->buf, 0, T2_BLOCK_SIZE);
		t2_read_params->batch_idx = j;
		t2_read_params->target_fp = req->key.hash_low;
		t2_read_params->req = req;
		t2_read_params->t2_read_cnt = max_read;
		t2_read_params->start_idx = T2_BLOCK_ENTRY;
		t2_read_params->is_read = cas_params->is_read;
		cb = make_callback(hlr, cb_t2_read, t2_read_params);
		hlr->read(hlr, t1_part->pba + i * T2_BLOCK_GRAINS, T2_BLOCK_GRAINS, (char *)t2_read_params->buf, cb, -1, cas_params->cas->ops_number);
		req->meta_lookups++;
		req->meta_lookup_bytes += T2_BLOCK_GRAINS * GRAIN_UNIT;
		//hlr->sync_read(hlr, t1_part->pba + i * T2_BLOCK_GRAINS, T2_BLOCK_GRAINS, (char *)t2_read_params->buf);
		//cb_t2_read(t2_read_params);
		/*
		if (req->key.hash_low == 9326686948173054547)
			printf("%p read_T2_block pba: %d, i: %d, size: %d\n", t1_part, t1_part->pba + i * T2_BLOCK_GRAINS, i, t1_part->t2_fifo_size);
			*/
	}
	return 0;
}

#if 0
static struct t3_hash_block *write_t3_block(struct handler *hlr, struct t1_hash_partition *t1_part, struct t2_hash_block *t2_block, struct request *req) {
	struct callback *cb;
	uint8_t ori_head = t2_fifo_head;
	struct t2_hash_block *ori_t2_block, *ret = NULL;
	if (t1_part->t2_fifo_size == NR_T2_BLOCK) {
		// need warterfall
		// write a t3 block at fifo head
		ori_t2_block = (struct t2_hash_block *)malloc(sizeof(struct t2_hash_block));
		hlr->sync_read(hlr, t1_part->pba + ori_head * T2_BLOCK_GRAINS, T2_BLOCK_GRAINS, ori_t2_block);
		t1_part->t2_fifo_size--;
		t1_part->t2_fifo_head = (t1_part->t2_fifo_head + 1) % NR_T2_BLOCK;
		ret = ori_t2_block;
	}
	if (t1_part->t2_fifo_size >= NR_T2_BLOCK)
		abort();

	uint64_t fifo_pba = t1_part->pba;
	uint64_t t2_block_pba;
	if (t1_part->t2_fifo_size == NR_T2_BLOCK - 1) {
		t2_block_pba = t1_part->pba + T2_BLOCK_GRAINS * ori_head;
	} else {
		if (t1_part->t2_fifo_head)
			abort();
		t2_block_pba = t1_part->pba + T2_BLOCK_GRAINS * t1_part->t2_fifo_size;
	}
	t1_part->t2_fifo_size++;
	//cb = make_callback(hlr, cb_t2_write, t2_block);
	hlr->sync_write(hlr, t2_block_pba * GRAIN_UNIT, T2_BLOCK_SIZE, (char *)t2_block->entry);
	free(t2_block);
	return ret;
}
#endif

static int write_t3_wide_block(struct cascade *cas, struct handler *hlr, struct t3_hash_bucket *t3_bucket) {
	struct t3_list_item *last_item = NULL;
	int num_entries = t3_bucket->buf_idx, flush_num, current_buf_idx = 0;
	char *tmp_buf = NULL;
	int aligned_last_pba, valid_cnt, copied_cnt;

	if (t3_bucket->buf_idx == 0)
		return -1;

	if (t3_bucket->size == 0) {
		last_item = (struct t3_list_item *)t3_bucket->hash_list->head->data;
		t3_bucket->last = last_item->lnode;
	} else {
		last_item = (struct t3_list_item *)(t3_bucket->last->data);
	}

	if (t3_bucket->size == NR_T3_WIDE_BLOCK) {
		printf("too many t3 wide blocks\n");
		fflush(stdout);
		//	abort();
	}

	aligned_last_pba = (t3_bucket->last_idx * ENTRY_SIZE) / T3_WIDE_MIN_UNIT * T3_WIDE_MIN_UNIT_GRAINS;
	valid_cnt = t3_bucket->last_idx % (T3_WIDE_MIN_UNIT/ENTRY_SIZE);
	copied_cnt = num_entries <= (T3_WIDE_MIN_UNIT/ENTRY_SIZE - valid_cnt) ? num_entries : T3_WIDE_MIN_UNIT/ENTRY_SIZE - valid_cnt;
	tmp_buf = (char *)aligned_alloc(MEM_ALIGN_UNIT, T3_WIDE_MIN_UNIT);
	//FIXME: it needs R-M-W
	if (valid_cnt) {
		hlr->sync_read(hlr, last_item->pba + aligned_last_pba, T3_WIDE_MIN_UNIT_GRAINS, tmp_buf, cas->ops_number);
		memcpy(tmp_buf + (valid_cnt * ENTRY_SIZE), (char*)&t3_bucket->buf->entry[current_buf_idx], copied_cnt * ENTRY_SIZE);
		hlr->sync_write(hlr, last_item->pba + aligned_last_pba, T3_WIDE_MIN_UNIT_GRAINS, tmp_buf, cas->ops_number);
		current_buf_idx += copied_cnt;
		num_entries -= copied_cnt;
		if (t3_bucket->last_idx + copied_cnt >= T3_BLOCK_ENTRY) {
			if (t3_bucket->size == NR_T3_WIDE_BLOCK) {
				last_item = (struct t3_list_item *)t3_bucket->hash_list->head->data;
				t3_bucket->last = last_item->lnode;
				//t3_bucket->last_idx = 0;
			} else {
				t3_bucket->last = last_item->lnode->nxt;
				t3_bucket->size++;
				last_item = (struct t3_list_item *)(t3_bucket->last->data);
			}
		} else if (t3_bucket->size == 0) {
			t3_bucket->size++;
			abort();
		}

		t3_bucket->last_idx = (t3_bucket->last_idx + copied_cnt) % T3_BLOCK_ENTRY;
	}


	while (num_entries > 0) {
		flush_num = T3_BLOCK_ENTRY - t3_bucket->last_idx > num_entries ? num_entries : T3_BLOCK_ENTRY - t3_bucket->last_idx;
		if (flush_num < T3_WIDE_MIN_UNIT/ENTRY_SIZE) {
			copied_cnt = flush_num;
		} else {
			copied_cnt = T3_WIDE_MIN_UNIT/ENTRY_SIZE;
		}
		//printf("t3_bucket->buf_idx: %d, flush_num: %d, copied_cnt: %d t3_bucket->last_idx %d\n", t3_bucket->buf_idx, flush_num, copied_cnt, t3_bucket->last_idx);
		memcpy(tmp_buf, (char*)&t3_bucket->buf->entry[current_buf_idx], copied_cnt * ENTRY_SIZE);
		aligned_last_pba = (t3_bucket->last_idx * ENTRY_SIZE) / T3_WIDE_MIN_UNIT * T3_WIDE_MIN_UNIT_GRAINS;
		hlr->sync_write(hlr, last_item->pba + aligned_last_pba, T3_WIDE_MIN_UNIT_GRAINS, tmp_buf, cas->ops_number);
		current_buf_idx += copied_cnt;
		num_entries -= copied_cnt;
		if (t3_bucket->last_idx + copied_cnt >= T3_BLOCK_ENTRY) {
			if (t3_bucket->size == NR_T3_WIDE_BLOCK) {
				last_item = (struct t3_list_item *)t3_bucket->hash_list->head->data;
				t3_bucket->last = last_item->lnode;
				//t3_bucket->last_idx = 0;
			} else {
				t3_bucket->last = last_item->lnode->nxt;
				t3_bucket->size++;
				last_item = (struct t3_list_item *)(t3_bucket->last->data);
			}
		} else if (t3_bucket->size == 0) {
			t3_bucket->size++;
		}
		t3_bucket->last_idx = (t3_bucket->last_idx + copied_cnt) % T3_BLOCK_ENTRY;
	}

	if (num_entries)
		abort();

	t3_bucket->buf_idx = 0;
	//free(t3_bucket->buf);
	free(tmp_buf);

	return 0;
}

static int do_bucket_compaction(struct cascade *cas, struct handler *hlr, struct t3_hash_bucket *t3_bucket, uint64_t t3_bucket_idx) {
	struct t3_wide_hash_table *t3_wide = cas->t3_wide;
	struct t3_hash_bucket *t3_narrow_bucket = t3_bucket, *t3_wide_bucket_start, *t3_wide_bucket;
	uint64_t t3_narrow_bucket_idx = t3_bucket_idx;
	uint64_t t3_wide_bucket_idx = t3_narrow_bucket_idx * T3_RATIO;
	t3_wide_bucket_start = &t3_wide->t3_bucket[t3_wide_bucket_idx];

	for (int i = 0; i < T3_RATIO; i++) {
		t3_wide_bucket = t3_wide_bucket_start + i;
		//t3_wide_bucket->buf = (struct t3_hash_block *)malloc(sizeof(struct t3_hash_block));
		t3_wide_bucket->buf = (struct t3_hash_block *)aligned_alloc(MEM_ALIGN_UNIT, sizeof(struct t3_hash_block));
		t3_wide_bucket->buf_idx = 0;
	}

	li_node *cur;
	struct t3_list_item *item;
	struct t3_hash_block *t3_block = (struct t3_hash_block *)aligned_alloc(MEM_ALIGN_UNIT, sizeof(struct t3_hash_block));
	struct hash_entry_f *entry_f, *old_entry_f;
	int j;
	static uint64_t compaction_ttl_cnt = 0;
	int iter = 0;
	list_for_each_node(t3_narrow_bucket->hash_list, cur) {
		if (t3_narrow_bucket->size < ++iter)
			break;
		item = (struct t3_list_item *)cur->data;
		hlr->sync_read(hlr, item->pba, T3_BLOCK_GRAINS, (char *)t3_block, cas->ops_number);
		for (int i = 0; i < T3_BLOCK_ENTRY; i++) {
			entry_f = &t3_block->entry[i];
#ifdef TTL
			if (entry_f->ttl < get_cur_sec() || entry_f->pba == PBA_INVALID) {
				printf("TTL!!! COMPACTION %lu\n", ++compaction_ttl_cnt);
				//entry_f->pba = PBA_INVALID;
				entry_f->evict_bit = 1;
				//continue;
			}
#endif
			//if (entry_f->fingerprint == 18069508485845357479)
			//	printf("씨빨롬 %lu\n", entry_f->pba);
			t3_wide_bucket = t3_wide_bucket_start + (entry_f->fingerprint % T3_RATIO);
			if (t3_wide_bucket->buf_idx == T3_BLOCK_ENTRY) {
				// write t3 block to t3_wide
				write_t3_wide_block(cas, hlr, t3_wide_bucket);
				t3_wide_bucket->buf_idx = 0;
			}
			memcpy(t3_wide_bucket->buf->entry + t3_wide_bucket->buf_idx++, entry_f, ENTRY_SIZE);
			/*
			for (j = 0; j < t3_wide_bucket->buf_idx; j++) {
				old_entry_f = &t3_wide_bucket->buf->entry[j];
				if (old_entry_f->fingerprint == entry_f->fingerprint)
					break;
			}
			if (j == t3_wide_bucket->buf_idx) {
				memcpy(t3_wide_bucket->buf->entry + t3_wide_bucket->buf_idx++, entry_f, ENTRY_SIZE);
			} else {
				memcpy(t3_wide_bucket->buf->entry + j, entry_f, ENTRY_SIZE);
			}
			*/
		}
	}

	for (int i = 0; i < T3_RATIO; i++) {
		t3_wide_bucket = t3_wide_bucket_start + i;
		write_t3_wide_block(cas, hlr, t3_wide_bucket);
		free(t3_wide_bucket->buf);
	}

	t3_narrow_bucket->size = 0;
	t3_narrow_bucket->buf_idx = 0;
	t3_narrow_bucket->last = NULL;

	return 0;
}

static int write_t3_block(struct cascade *cas, struct handler *hlr, struct t3_hash_bucket *t3_bucket, uint64_t t3_bucket_idx) {
	struct t3_narrow_hash_table *t3_narrow = cas->t3_narrow;
	struct t3_list_item *last_item, *item;
	li_node *cur;

	struct hash_entry_f *entry_f = NULL;
	uint64_t t3_narrow_bucket_idx;
	uint64_t t3_wide_bucket_idx;

	//printf("write_t3_block\n");

	if (t3_bucket->size == 0) {
		last_item = (struct t3_list_item *)t3_bucket->hash_list->head->data;
		t3_bucket->last = last_item->lnode;
	} else if (t3_bucket->size == NR_T3_NARROW_BLOCK - 1) {
		//printf("bucket compaction\n");
		do_bucket_compaction(cas, hlr, t3_bucket, t3_bucket_idx);
		// trigger compaction
		list_for_each_node(t3_bucket->hash_list, cur) {
			item = (struct t3_list_item *)cur->data;
			bloomfilter_init(item->bf, BF_M, BF_K);
		}
		last_item = (struct t3_list_item *)t3_bucket->hash_list->head->data;
		t3_bucket->last = last_item->lnode;
		//last_item = NULL;
	} else {
		last_item = (struct t3_list_item *)(t3_bucket->last->data);
	}

	if (!last_item) { // compaction case
		return 1;
	}

	struct bloomfilter *bf = last_item->bf;
	for (int i = 0; i < T3_BLOCK_ENTRY; i++) {
		bloomfilter_set(bf, &((t3_bucket->buf->entry + i)->fingerprint), sizeof(uint64_t));
	}


	hlr->sync_write(hlr, last_item->pba, T3_BLOCK_GRAINS, (char*)t3_bucket->buf, cas->ops_number);
	/*
	if (t3_bucket == gdb_t3_bucket) {
		printf("LAST %p, pba: %lu, size: %d, fg: %lu\n", t3_bucket, last_item->pba, t3_bucket->size, t3_bucket->buf->entry[2289]);
	}
	if (last_item->pba == 8192) {
		struct t3_hash_block *tmp_t3_block = (struct t3_hash_block *)aligned_alloc(MEM_ALIGN_UNIT,sizeof(struct t3_hash_block));
		int retval;
		retval = hlr->sync_read(hlr, 8192, T3_BLOCK_GRAINS, (char*)tmp_t3_block);
		if (retval != T3_BLOCK_GRAINS * GRAIN_UNIT) {
			perror("???");
			//abort();
		}
		printf("tmp 2289th: %lu\n", tmp_t3_block->entry[2289]);
		free(tmp_t3_block);

	}
	*/


	t3_bucket->last = last_item->lnode->nxt;
	t3_bucket->size++;

	int cnt = 0;
	for (int i = 0; i < NR_T3_NARROW; i++) {
		if (t3_narrow->t3_bucket[i].size == 0)
			cnt++;
	}
	printf("CUR T3: %p empty narrow: %d of %d\n", t3_bucket, cnt, NR_T3_NARROW);

	cnt = 0;
	for (int i = 0; i < NR_T3_WIDE; i++) {
		if (cas->t3_wide->t3_bucket[i].size == 0)
			cnt++;
	}
	printf("empty wide: %d of %d\n", cnt, NR_T3_WIDE);


	return 0;
}

static struct t3_hash_block *write_t3_buffer(struct cascade *cas, struct handler *hlr, struct t2_hash_block *t2_block) {
	struct t3_narrow_hash_table *t3_narrow = cas->t3_narrow;
	struct t3_hash_bucket *t3_bucket;
	struct t3_hash_block *buf; 

	struct hash_entry_f *entry_f = NULL;
	uint64_t t3_narrow_bucket_idx;
	uint64_t t3_wide_bucket_idx;

	for (int i = 0; i < T2_BLOCK_ENTRY; i++) {
		entry_f = &t2_block->entry[i];
		//if (entry_f->pba == PBA_INVALID)
		//	continue;
		t3_wide_bucket_idx = entry_f->fingerprint % NR_T3_WIDE;
		t3_narrow_bucket_idx = t3_wide_bucket_idx / T3_RATIO;
		t3_bucket = &t3_narrow->t3_bucket[t3_narrow_bucket_idx];
		buf = t3_bucket->buf;
		if (t3_bucket->buf_idx >= T3_BLOCK_ENTRY)
			printf("ERR???\n");

		//if (entry_f->fingerprint == 9326686948173054547) {
		//	printf("t3_bucket: %p, narrow_idx: %d, wide_idx: %d, idx: %d\n", t3_bucket, t3_narrow_bucket_idx, t3_wide_bucket_idx, t3_bucket->buf_idx);
		//	gdb_t3_bucket = t3_bucket;
		//}
		memcpy(buf->entry + t3_bucket->buf_idx++, entry_f, ENTRY_SIZE);
		if (t3_bucket->buf_idx == T3_BLOCK_ENTRY) {
			//flush t3_buf to t3_narrow
			write_t3_block(cas, hlr, t3_bucket, t3_narrow_bucket_idx);
			t3_bucket->buf_idx = 0;
		}
	}
	return NULL;
}


static struct hash_entry_f *read_t3_buffer(struct cascade *cas, struct request *req) {
	uint64_t fp = req->key.hash_low;
	uint64_t t3_wide_bucket_idx = fp % NR_T3_WIDE;
	uint64_t t3_narrow_bucket_idx = t3_wide_bucket_idx / T3_RATIO;
	struct t3_hash_bucket *t3_narrow_bucket = &cas->t3_narrow->t3_bucket[t3_narrow_bucket_idx];
	struct hash_entry_f *entry_f = NULL;
	struct hash_entry_f *res = NULL;
	//int collision_cnt = 0;
	struct cas_params *params = (struct cas_params *)req->params;
	uint64_t buf_ttl_cnt = 0;

	if (t3_narrow_bucket->buf_idx) {
		int start_idx = t3_narrow_bucket->buf_idx;
		if (params->tmp_entry_f) {
			while(start_idx > 0) {
				entry_f = t3_narrow_bucket->buf->entry + (--start_idx);
				if (entry_f == params->tmp_entry_f)
					break;
			}
		}

		entry_f = NULL;	
		for (int i = start_idx - 1; i >= 0; i--) {
			entry_f = t3_narrow_bucket->buf->entry + i;
			if ((entry_f->pba != PBA_INVALID) && (entry_f->fingerprint == fp)) {
#ifdef TTL
				/*
				if (params->is_read && entry_f->ttl < get_cur_sec()) {
					printf("TTL BUF!!! %lu\n", ++buf_ttl_cnt);
					//entry_f->pba = PBA_INVALID;
					entry_f->evict_bit = 1;
					//entry_f = NULL;
				}
				*/
#endif
				//collision_cnt++;
				res = entry_f;
				break;
			}
		}
	}
	/*
	if (collision_cnt > 1) {
		abort();
	}
	*/
	return res;
}

static void *cb_t3_read(void *arg);

static void *cb_t3_read_kvcmp(void *arg) {
	struct t3_read_params *t3_read_params = (struct t3_read_params *)arg;
	struct request *req = t3_read_params->req;
	struct cas_params *params = (struct cas_params *)req->params;
	struct hash_entry_f *entry_f;

	struct key_struct *key = &req->key;
	struct val_struct value;
	value.value = (char *)t3_read_params->kv_buf;
	bool is_expired;

	//if (key->hash_low == 18069508485845357479) {
	//	if (params->read_step == CAS_STEP_READ_T3_WIDE && req->hlr->stat.nr_read > 4350000)
	//		printf("cb_t3_read_kvcmp\n");
	//}

	if (keycmp_k_v(key, &value) == 0) {
		is_expired = is_expired_entry(req, key, &value);
		if (params->write_step == CAS_STEP_READ_T3_NARROW || params->read_step == CAS_STEP_READ_T3_NARROW) {
			params->write_step = params->read_step = CAS_STEP_READ_T3_NARROW_SUCC;
		} else {
			params->write_step = params->read_step = CAS_STEP_READ_T3_WIDE_SUCC;
		}
		memcpy(&params->entry_f, &t3_read_params->entry_f, ENTRY_SIZE);
		if (is_expired)
			params->write_step = CAS_STEP_EXPIRED;
		if (t3_read_params->buf)
			free(t3_read_params->buf);
		if (t3_read_params->kv_buf)
			free(t3_read_params->kv_buf);
		free(t3_read_params);
		retry_req_to_hlr(req->hlr, req);
	} else if (t3_read_params->nr_entry) {
		if (t3_read_params->kv_buf) {
			free(t3_read_params->kv_buf);
			t3_read_params->kv_buf = NULL;
		}

		cb_t3_read(arg);
	} else {
		li_node *prv = t3_read_params->item->lnode->prv;
		if (t3_read_params->max_read_cnt <= t3_read_params->read_cnt)
			goto not_found;
		if (prv) {
			// keep reading
			if (params->read_step == CAS_STEP_READ_T3_NARROW) {
				struct t3_list_item *item;
				while (prv) {
					item = (struct t3_list_item *)prv->data;
					if (bloomfilter_get(item->bf, &(req->key.hash_low), sizeof(uint64_t))) {
						break;
					} else {
						t3_read_params->read_cnt++;
					}
					if (prv->prv)
						prv = prv->prv;
					else 
						goto not_found;
				}

			}
			struct callback *cb;
			t3_read_params->item = (struct t3_list_item *)prv->data;
			t3_read_params->nr_entry = T3_BLOCK_ENTRY;
			t3_read_params->read_cnt++;
			cb = make_callback(req->hlr, cb_t3_read, t3_read_params);
			if (req->hlr->poller_read(req->hlr, t3_read_params->item->pba, T3_BLOCK_GRAINS, (char *)t3_read_params->buf, cb, params->cas->ops_number) < 0) {
				abort();
			} else {
				req->meta_lookups++;
				req->meta_lookup_bytes += T3_BLOCK_GRAINS * GRAIN_UNIT;
			}
		} else {
not_found:
			// not found
			if (params->write_step == CAS_STEP_READ_T3_NARROW || params->read_step == CAS_STEP_READ_T3_NARROW)
				params->write_step = params->read_step = CAS_STEP_READ_T3_NARROW_FAIL;
			else {
				params->write_step = params->read_step = CAS_STEP_READ_T3_WIDE_FAIL;
			}
			if (t3_read_params->buf)
				free(t3_read_params->buf);
			if (t3_read_params->kv_buf)
				free(t3_read_params->kv_buf);
			free(t3_read_params);
			retry_req_to_hlr(req->hlr, req);
		}
	}
	return NULL;
}


static void *cb_t3_read(void *arg) {
	struct t3_read_params *t3_read_params = (struct t3_read_params *)arg;
	struct request *req = t3_read_params->req;
	struct handler *hlr = req->hlr;
	struct t3_hash_block *t3_block = (struct t3_hash_block *)t3_read_params->buf;
	struct cas_params *params = (struct cas_params *)req->params;
	struct hash_entry_f *entry_f;
	struct callback *cb = NULL;

	int same = 0;

	//if (t3_read_params->item->pba == 8192)
	//		printf("!!!!!!!!!!!!!!!!!!!! %lu\n",t3_block->entry[2289].fingerprint);
	/*
	for (int i = t3_read_params->nr_entry - 1; i >= 0; i--) {
		entry_f = &t3_block->entry[i];
		//if (entry_f->fingerprint == 9326686948173054547)
		//	print_entry_f(entry_f, "T3-READ-1");
		if (entry_f->fingerprint == req->key.hash_low)
			same++;
	}
	if (same > 1) {
		printf("NONONONONONO\n");
		fflush(stdout);
		abort();
	}
	*/
	//if (req->key.hash_low == 18069508485845357479) {
	//	if (params->read_step == CAS_STEP_READ_T3_WIDE && req->hlr->stat.nr_read > 4350000)
	//		printf("t3_read\n");
	//}

	entry_f = get_match_entry_f(t3_block, req->key.hash_low, t3_read_params->nr_entry, params->is_read);
	if (entry_f) {

		//if (entry_f->fingerprint == 9326686948173054547)
		//	print_entry_f(entry_f, "T3-READ-2");
		memcpy(&t3_read_params->entry_f, entry_f, ENTRY_SIZE);
		cb = make_callback(hlr, cb_t3_read_kvcmp, t3_read_params);
		t3_read_params->kv_buf = (void *)aligned_alloc(MEM_ALIGN_UNIT, entry_f->kv_size * GRAIN_UNIT);
		t3_read_params->nr_entry = entry_f - t3_block->entry;
		if (hlr->poller_read(hlr, entry_f->pba, entry_f->kv_size, (char *)t3_read_params->kv_buf, cb, params->cas->ops_number) < 0) {
			cb_t3_read_kvcmp(t3_read_params);
		} else {
			//req->data_lookups++;
			//req->data_lookup_bytes += entry_f->kv_size * GRAIN_UNIT;
		}
	} else {
		li_node *prv = t3_read_params->item->lnode->prv;
		if (t3_read_params->max_read_cnt <= t3_read_params->read_cnt)
			goto not_found;
		if (prv) {
			// keep reading
			if (params->read_step == CAS_STEP_READ_T3_NARROW) {
				struct t3_list_item *item;
				while (prv) {
					item = (struct t3_list_item *)prv->data;
					if (bloomfilter_get(item->bf, &(req->key.hash_low), sizeof(uint64_t))) {
						break;
					} else {
						t3_read_params->read_cnt++;
					}
					if (prv->prv)
						prv = prv->prv;
					else 
						goto not_found;
				}

			}
			t3_read_params->item = (struct t3_list_item *)prv->data;
			t3_read_params->nr_entry = T3_BLOCK_ENTRY;
			t3_read_params->read_cnt++;
			cb = make_callback(hlr, cb_t3_read, t3_read_params);
			if (hlr->poller_read(hlr, t3_read_params->item->pba, T3_BLOCK_GRAINS, (char *)t3_read_params->buf, cb, params->cas->ops_number) < 0) {
				abort();
			} else {
				req->meta_lookups++;
				req->meta_lookup_bytes += T3_BLOCK_GRAINS * GRAIN_UNIT;
			}
		} else {
			// not found
not_found:
			if (params->write_step == CAS_STEP_READ_T3_NARROW || params->read_step == CAS_STEP_READ_T3_NARROW)
				params->write_step = params->read_step = CAS_STEP_READ_T3_NARROW_FAIL;
			else {
				params->write_step = params->read_step = CAS_STEP_READ_T3_WIDE_FAIL;
			}
			if (t3_read_params->buf)
				free(t3_read_params->buf);
			if (t3_read_params->kv_buf)
				free(t3_read_params->kv_buf);
			free(t3_read_params);
			retry_req_to_hlr(req->hlr, req);
		}
	}
	return NULL;
}

static int read_t3_narrow_block(struct cascade *cas, struct handler *hlr, struct request *req) {
	uint64_t fp = req->key.hash_low;
	uint64_t t3_wide_bucket_idx = fp % NR_T3_WIDE;
	uint64_t t3_narrow_bucket_idx = t3_wide_bucket_idx / T3_RATIO;
	struct t3_hash_bucket *t3_narrow_bucket = &cas->t3_narrow->t3_bucket[t3_narrow_bucket_idx];
	struct t3_hash_bucket *t3_wide_bucket = &cas->t3_wide->t3_bucket[t3_wide_bucket_idx];
	struct cas_params *params = (struct cas_params *)req->params;
	volatile struct hash_entry_f *entry_f = NULL;
	struct callback *cb;


	if (t3_narrow_bucket->size == 0)
		return -1;

	li_node *cur;
	struct t3_list_item *item = (struct t3_list_item *)t3_narrow_bucket->last->prv->data;
	int read_cnt = 0;
	int max_read_cnt = t3_narrow_bucket->size;
	while (item != NULL) {
		if (bloomfilter_get(item->bf, &(req->key.hash_low), sizeof(uint64_t))) {
			// need to read t3 block
			break;
		} else {
			read_cnt++;
		}

		if (item->lnode->prv)
			item = (struct t3_list_item *)item->lnode->prv->data;	
		else
			return -1;
	}

	if (read_cnt > max_read_cnt)
		abort();
	if (read_cnt == max_read_cnt)
		return -1;


	struct t3_read_params *t3_read_params = (struct t3_read_params *)malloc(sizeof(struct t3_read_params));

	t3_read_params->max_read_cnt = max_read_cnt;
	t3_read_params->read_cnt = read_cnt + 1;
	t3_read_params->buf = (void *)aligned_alloc(MEM_ALIGN_UNIT,T3_BLOCK_SIZE);
	t3_read_params->kv_buf = NULL;
	t3_read_params->t3_bucket = t3_narrow_bucket;
	t3_read_params->item = item;
	t3_read_params->nr_entry = T3_BLOCK_ENTRY;
	t3_read_params->req = req;
	cb = make_callback(hlr, cb_t3_read, t3_read_params);
	params->write_step = params->read_step = CAS_STEP_READ_T3_NARROW;
	hlr->read(hlr, t3_read_params->item->pba, T3_BLOCK_GRAINS, (char *)t3_read_params->buf, cb, -1, cas->ops_number);
	req->meta_lookups++;
	req->meta_lookup_bytes += T3_BLOCK_GRAINS * GRAIN_UNIT;
	return 0;
}

static int read_t3_wide_block(struct cascade *cas, struct handler *hlr, struct request *req) {
	uint64_t fingerprint = req->key.hash_low;
	uint64_t t3_wide_bucket_idx = fingerprint % NR_T3_WIDE;
	uint64_t t3_narrow_bucket_idx = t3_wide_bucket_idx / T3_RATIO;
	struct t3_hash_bucket *t3_narrow_bucket = &cas->t3_narrow->t3_bucket[t3_narrow_bucket_idx];
	struct t3_hash_bucket *t3_wide_bucket = &cas->t3_wide->t3_bucket[t3_wide_bucket_idx];
	struct cas_params *params = (struct cas_params *)req->params;
	volatile struct hash_entry_f *entry_f = NULL;
	struct callback *cb;


	//if (fingerprint == 18069508485845357479 && hlr->stat.nr_read > 4350000) {
	//	printf("wide\n");
	//}

	if (t3_wide_bucket->size == 0)
		return -1;

	struct t3_read_params *t3_read_params = (struct t3_read_params *)malloc(sizeof(struct t3_read_params));

	t3_read_params->buf = (void *)aligned_alloc(MEM_ALIGN_UNIT,T3_BLOCK_SIZE);
	t3_read_params->kv_buf = NULL;
	t3_read_params->t3_bucket = t3_wide_bucket;
	t3_read_params->max_read_cnt = t3_wide_bucket->size;
	t3_read_params->read_cnt = 1;
	if (t3_wide_bucket->last_idx) {
		t3_read_params->item = (struct t3_list_item *)t3_wide_bucket->last->data;
		t3_read_params->nr_entry = t3_wide_bucket->last_idx;
	} else {
		t3_read_params->item = (struct t3_list_item *)t3_wide_bucket->last->prv->data;
		t3_read_params->nr_entry = T3_BLOCK_ENTRY;
		t3_read_params->max_read_cnt--;
	}
	t3_read_params->req = req;
	cb = make_callback(hlr, cb_t3_read, t3_read_params);
	params->write_step = params->read_step = CAS_STEP_READ_T3_WIDE;
	hlr->read(hlr, t3_read_params->item->pba, T3_BLOCK_GRAINS, (char *)t3_read_params->buf, cb, -1, cas->ops_number);
	req->meta_lookups++;
	req->meta_lookup_bytes += T3_BLOCK_GRAINS * GRAIN_UNIT;
	return 0;
}

static void do_eviction(struct cascade *cas, struct handler *hlr, struct t1_hash_table *t1_table) {
	struct t2_hash_block *t2_block, *ori_t2_block;
	struct t3_hash_block *t3_block;
	for (int i = 0; i < NR_T1_PART; i++) {
		struct t1_hash_partition *t1_part = &t1_table->t1_part[i];
		t2_block = collect_t2_block(t1_table, t1_part, i);
		ori_t2_block = write_t2_block(cas, hlr, t1_part, t2_block);
		if (ori_t2_block) {
			// write t3
			write_t3_buffer(cas, hlr, ori_t2_block);
		}
	}
}

int cascade_get(struct kv_ops *ops, struct request *req) {
	int rc = 0;
	struct handler *hlr = req->hlr;
	struct cascade *cas = (struct cascade *)ops->_private;
	struct t1_hash_table *t1_table = cas->t1_table;

	uint64_t t1_bucket_idx = req->key.hash_low % NR_T1_BUCKET;
	uint64_t t1_part_idx = t1_bucket_idx / NR_T1_BUCKET_PER_PART;

	struct t1_hash_partition *t1_part = &t1_table->t1_part[t1_part_idx];
	struct t1_hash_bucket *t1_bucket = &t1_table->t1_bucket[t1_bucket_idx];

	struct hash_entry *entry = NULL;
	struct hash_entry_f *entry_f = NULL;
	struct callback *cb = NULL;


	if (!req->params) req->params = make_cas_params();
	struct cas_params *params = (struct cas_params *)req->params;

	params->is_read = true;
	params->cas = cas;

	uint64_t old_pba;
	static uint64_t ttl_cnt = 0;
	bool is_evicted = false;

	//if (strncmp(req->key.key, "user4931155956028412375513337700", 32) == 0) {
	//	puts("set here");

	//}


	switch (params->read_step) {
	case CAS_STEP_T1_MATCH:
		entry = params->entry;
		entry->clock_bit = 1;
		entry->flying_bit = 0;
		//printf("READSTEP 1\n");
		goto cas_end_req;
		break;
	case CAS_STEP_READ_T3_NARROW_SUCC:
		//printf("READSTEP 2\n");
	case CAS_STEP_READ_T3_WIDE_SUCC:
		//printf("READSTEP 3\n");
	case CAS_STEP_READ_KV_SUCC:
		//printf("READSTEP 4\n");
	case CAS_STEP_READ_T2_SUCC:
		//printf("READSTEP 5\n");
	case CAS_STEP_READ_T3_BUFFER:
		entry_f = &params->entry_f;
		goto cas_write_t1;
		break;
	case CAS_STEP_T1_MISMATCH:
		entry = params->entry;
		entry->flying_bit = 0;
		//printf("READSTEP 6\n");
		break;
	case CAS_STEP_READ_T2_FAIL:
		//printf("READSTEP 7\n");
		goto cas_read_t2;
		break;
	case CAS_STEP_READ_T3_BUFFER_FAIL:
		//printf("READSTEP 8\n");
		goto cas_read_t3_buffer;
		//goto cas_read_t3_narrow;
		break;
	case CAS_STEP_READ_T3_NARROW_FAIL:
		//printf("READSTEP 9\n");
		goto cas_read_t3_wide;
		break;
	case CAS_STEP_READ_T3_WIDE_FAIL:
		//printf("READSTEP 10\n");
		rc = -1;
		goto exit;
	case CAS_STEP_EXPIRED:
		printf("GET EXPEXP\n");
		is_evicted = true;
		entry_f = &params->entry_f;
		goto cas_write_t1;
	case CAS_STEP_T1_EXPIRED:
		entry = params->entry;
		entry->clock_bit = 0;
		entry->flying_bit = 0;
		entry->evict_bit = 1;
		entry->dirty_bit = 1;
		rc = -1;
		goto exit;
		break;
	case CAS_STEP_READ_T3_WIDE:
		abort();
	default:
		hlr->stat.nr_read++;
		break;
	}

	if (t1_part->flying) {
		if (params->read_step == CAS_STEP_INIT) {
			hlr->stat.nr_read--;
		}
		retry_req_to_hlr(hlr, req);
		if (params->read_step != CAS_STEP_INIT)
			printf("FLYING:%d\n",params->read_step);
		goto exit;
	}


cas_get_entry:
	entry = get_target_entry(cas, entry?entry->lnode->nxt:t1_bucket->hash_list->head, req->key.hash_low);
	if (entry) {
		//entry->fingerprint = req->key.hash_low;
		//entry->dirty_bit = 1;
		//entry->clock_bit = 0;
		//entry->kv_size = req->value.len / GRAIN_UNIT;
		//entry->pba = get_next_pba(hlr, req->value.len);
		//if (entry->pba == PBA_INVALID) {
		//	goto cas_get_entry;
		//}
		/*
#ifdef TTL
		if (entry->ttl < get_cur_sec()) {
			printf("TTL!!! %lu\n", ++ttl_cnt);
			entry->dirty = 1;
			entry->evict_bit = 1;
			goto cas_get_entry;
		}
#endif
		*/
		params->read_step = CAS_STEP_INIT;
		params->entry = entry;
		entry->flying_bit = 1;
		cb = make_callback(hlr, cb_keycmp, req);
		
		if (hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb, -1, cas->ops_number) < 0) {
			entry->clock_bit = 0;
			entry->flying_bit = 0;
			entry->dirty_bit = 1;
			entry->evict_bit = 1;
			memset(req->value.value, 0, 512);
			cb_keycmp(req);
		} else {
			//req->data_lookups++;
			//req->data_lookup_bytes += entry->kv_size * GRAIN_UNIT;
		}
		//printf("GET-1\n");
		goto exit;
	}

	// need empty space for insertion
	if (need_cache_eviction(cas)) {
		// write t2
		// may write t3-narrow (if t2 is full)
		// may write t3-wide (if one of t3-narrow's buckets is full)
		//printf("@@@EVICTION\n");
		do_eviction(cas, hlr, t1_table);
	}

cas_read_t2:
	//req->temp_buf = (char *)aligned_alloc(MEM_ALIGN_UNIT, PART_TABLE_SIZE);
	if (t1_part->t2_fifo_size) {

		//printf("GET-2\n");
		// read t2
		t1_part->flying = 1;
		if (read_t2_block(hlr, t1_part, req)) {
			// t2 miss
			//printf("go t3_buffer\n");
			goto cas_read_t3_buffer;
		} else {
			rc = 0;
			goto exit;
		}

	} else {
		// not existng entry
		rc = -1;
		goto exit;
	}

cas_read_t3_buffer:
	params->read_step = CAS_STEP_READ_T3_BUFFER;
	entry_f = read_t3_buffer(cas, req);
	if (entry_f) {
		//memcpy(&params->entry_f, entry_f, sizeof(struct
		params->tmp_entry_f = entry_f;
		goto cas_keycmp; 
	} else {
		params->tmp_entry_f = NULL;
	}

cas_read_t3_narrow:
	// read t3-narrow
	params->read_step = CAS_STEP_READ_T3_NARROW;
	//printf("NARROW-1!!!!\n");
	if (!read_t3_narrow_block(cas, hlr, req)) {
		//printf("NARROW-2!!!!\n");
		rc = 0;
		goto exit;
	}

cas_read_t3_wide:
	//read t3_wide
	params->read_step = CAS_STEP_READ_T3_WIDE;
	//printf("WIDE-1!!!!\n");
	if (!read_t3_wide_block(cas, hlr, req)) {
	//printf("WIDE-2!!!!\n");
		rc = 0;
		goto exit;
	} else {
	//printf("WIDE-3!!!!\n");
		rc = -1;
		goto exit;
	}
	
cas_keycmp:
	cb = make_callback(hlr, cb_keycmp, req);
	memcpy(&params->entry_f, entry_f, sizeof(struct hash_entry_f));
	if (hlr->read(hlr, entry_f->pba, entry_f->kv_size, req->value.value, cb, -1, cas->ops_number) < 0) {
		is_evicted = 1;
		if (!entry_f->evict_bit) {
#ifdef TTL
			entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, entry_f->pba, entry_f->kv_size, 1, entry_f->ttl, is_evicted);
#else
			entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, entry_f->pba, entry_f->kv_size, 0);
#endif
		}
		memset(req->value.value, 0, 512);
		cb_keycmp(req);
		rc = 0;
	} else {
		//req->data_lookups++;
		//req->data_lookup_bytes += entry_f->kv_size * GRAIN_UNIT;
	}
	goto exit;
	
cas_write_t1:
	t1_part->flying = 0;
	t1_part->flying_req = 0;
	if (!entry_f->evict_bit) {
#ifdef TTL
		entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, entry_f->pba, entry_f->kv_size, 0, entry_f->ttl, is_evicted);
#else
		entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, entry_f->pba, entry_f->kv_size, 0);
#endif
		if (is_evicted) {
			entry->evict_bit = 1;
			entry->dirty_bit = 1;
			entry->clock_bit = 0;
			entry->flying_bit = 0;
			rc = -1;
			goto exit;
		}
	} else {
		printf("EVICT ON\n");
	}
	
cas_end_req:
	req->end_req(req);

exit:
	if (rc == -1) {
		t1_part->flying = 0;
		t1_part->flying_req = 0;
		hlr->stat.nr_read_miss++;
		not_found_count[params->read_step]++;
		if ((++notnot % 10000) == 0)
			print_notnot();
	}

	return rc;
}

#if 0
int cascade_set(struct kv_ops *ops, struct request *req) {
	sw_start(sw_set);
	int rc = 0;
	struct handler *hlr = req->hlr;
	struct cascade *cas = (struct cascade *)ops->_private;
	struct t1_hash_table *t1_table = cas->t1_table;

	uint64_t t1_bucket_idx = req->key.hash_low % NR_T1_BUCKET;
	uint64_t t1_part_idx = t1_bucket_idx / NR_T1_BUCKET_PER_PART;

	struct t1_hash_partition *t1_part = &t1_table->t1_part[t1_part_idx];
	struct t1_hash_bucket *t1_bucket = &t1_table->t1_bucket[t1_bucket_idx];

	struct hash_entry *entry = NULL;
	struct hash_entry_f *entry_f = NULL;
	struct callback *cb = NULL;


	if (!req->params) req->params = make_cas_params();
	struct cas_params *params = (struct cas_params *)req->params;
	params->is_read = false;
	params->cas = cas;

	uint64_t old_pba;
	bool is_evicted = false;

	//if (strncmp(req->key.key, "user4931155956028412375513337700", 32) == 0) {
	//	puts("set here");

	//}


	switch (params->write_step) {
	case CAS_STEP_T1_MATCH:
		entry = params->entry;
		entry->clock_bit = 1;
		entry->flying_bit = 0;
		entry->evict_bit = 0;
		entry->dirty_bit = 1;
		goto cas_write_kvpair;
		break;
	case CAS_STEP_EXPIRED:
		printf("SET EXPEXP\n");
	case CAS_STEP_READ_T3_NARROW_SUCC:
	case CAS_STEP_READ_T3_WIDE_SUCC:
	case CAS_STEP_READ_KV_SUCC:
	case CAS_STEP_READ_T2_SUCC:
		entry_f = &params->entry_f;
	case CAS_STEP_READ_T3_WIDE_FAIL:
		goto cas_write_t1;
		break;
	case CAS_STEP_T1_MISMATCH:
		entry = params->entry;
		entry->flying_bit = 0;
		break;
	case CAS_STEP_READ_T2_FAIL:
		goto cas_read_t2;
		break;
	case CAS_STEP_READ_T3_BUFFER_FAIL:
		goto cas_read_t3_buffer;
		//goto cas_read_t3_narrow;
		break;
	case CAS_STEP_READ_T3_NARROW_FAIL:
		goto cas_read_t3_wide;
		break;
	case CAS_STEP_T1_EXPIRED:
		entry = params->entry;
		entry->clock_bit = 1;
		entry->flying_bit = 0;
		entry->evict_bit = 0;
		entry->dirty_bit = 1;
		goto cas_write_kvpair;
		break;
	default:
		hlr->stat.nr_write++;
		break;
	}

	//if (req->key.hash_low == 18069508485845357479) {
	//	printf("ASDASDASDASDASDASDASD\n");
	//}


	if (t1_part->flying) {
		if (params->write_step == CAS_STEP_INIT) {
			hlr->stat.nr_write--;
		}
		retry_req_to_hlr(hlr, req);
		goto exit;
	}


cas_get_entry:
	entry = get_target_entry(cas, entry?entry->lnode->nxt:t1_bucket->hash_list->head, req->key.hash_low);
	if (entry) {
		//entry->fingerprint = req->key.hash_low;
		//entry->dirty_bit = 1;
		//entry->clock_bit = 0;
		//entry->kv_size = req->value.len / GRAIN_UNIT;
		//entry->pba = get_next_pba(hlr, req->value.len);
		params->write_step = CAS_STEP_INIT;
		params->entry = entry;
		entry->flying_bit = 1;
		cb = make_callback(hlr, cb_keycmp, req);
		if (hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb, -1, cas->ops_number) < 0) {
			entry->clock_bit = 0;
			entry->flying_bit = 0;
			entry->dirty_bit = 1;
			entry->evict_bit = 1;
			memset(req->value.value, 0, 512);
			cb_keycmp(req);
		} else {
			//req->data_lookups++;
			//req->data_lookup_bytes += entry->kv_size * GRAIN_UNIT;
		}
		goto exit;
	}

	// need empty space for insertion
	if (need_cache_eviction(cas)) {
		// write t2
		// may write t3-narrow (if t2 is full)
		// may write t3-wide (if one of t3-narrow's buckets is full)
		//printf("@@@EVICTION\n");
		do_eviction(cas, hlr, t1_table);
	}

cas_read_t2:
	//req->temp_buf = (char *)aligned_alloc(MEM_ALIGN_UNIT, PART_TABLE_SIZE);
	if (t1_part->t2_fifo_size) {
		// read t2
		t1_part->flying = 1;
		if (read_t2_block(hlr, t1_part, req)) {
			// t2 miss
			goto cas_read_t3_buffer;
		}
		rc = 0;
		goto exit;

	} else {
		// not existng entry
		goto cas_write_t1;
	}

cas_read_t3_buffer:
	params->write_step = CAS_STEP_READ_T3_BUFFER;
	entry_f = read_t3_buffer(cas, req);
	if (entry_f) {
		//params->entry_f = entry_f;
		params->tmp_entry_f = entry_f;
		goto cas_keycmp; 
	} else {
		params->tmp_entry_f = NULL;
	}

cas_read_t3_narrow:
	// read t3-narrow
	params->write_step = CAS_STEP_READ_T3_NARROW;
	if (!read_t3_narrow_block(cas, hlr, req)) {
		rc = 0;
		goto exit;
	}

cas_read_t3_wide:
	//read t3_wide
	params->write_step = CAS_STEP_READ_T3_WIDE;
	if (!read_t3_wide_block(cas, hlr, req)) {
		rc = 0;
		goto exit;
	} else {
		goto cas_write_t1;
	}
	
cas_keycmp:
	cb = make_callback(hlr, cb_keycmp, req);
	if (hlr->read(hlr, entry_f->pba, entry_f->kv_size, req->value.value, cb, -1, cas->ops_number) < 0) {
		printf("SET READ -1\n");
		memset(req->value.value, 0, 512);
		retry_req_to_hlr(hlr, req);
	} else {
		//req->data_lookups++;
		//req->data_lookup_bytes += entry_f->kv_size * GRAIN_UNIT;
	}
	goto exit;
	
cas_write_t1:
	t1_part->flying = 0;
	t1_part->flying_req = 0;
#ifdef TTL
	entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, PBA_INVALID, req->value.len/GRAIN_UNIT, 1, req->req_time + req->sec, 0);
#else
	entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, PBA_INVALID, req->value.len/GRAIN_UNIT, 1);
#endif

cas_write_kvpair:
#ifdef TTL_GROUP
	entry->pba = get_next_pba(hlr, req->value.len, -1, cas->ops_number, -1);
#else
	entry->pba = get_next_pba(hlr, req->value.len, -1, cas->ops_number);
	//if (entry->fingerprint == 18069508485845357479) {
	//	printf("ASDASDASDASDASDASDASD %lu\n", entry->pba);
	//}

#endif
	cb = make_callback(hlr, req->end_req, req);
	copy_key_to_value(&req->key, &req->value, req->req_time + req->sec);
	//printf("old_pba: %lu\n", old_pba);
	//print_entry(entry, "OLD");
	//sw_start(sw_cpy);
#ifdef TTL_GROUP
	hlr->write(hlr, entry->pba, PBA_INVALID, entry->kv_size, req->value.value, cb, -1, cas->ops_number, -1);
#else
	hlr->write(hlr, entry->pba, PBA_INVALID, entry->kv_size, req->value.value, cb, -1, cas->ops_number);
#endif
	//sw_end(sw_cpy);
	//set_cpy += sw_get_usec(sw_cpy);

exit:
	sw_end(sw_set);
	set_total += sw_get_usec(sw_set);

	return rc;
}
#endif

int cascade_set(struct kv_ops *ops, struct request *req) {
	sw_start(sw_set);
	int rc = 0;
	struct handler *hlr = req->hlr;
	struct cascade *cas = (struct cascade *)ops->_private;
	struct t1_hash_table *t1_table = cas->t1_table;

	uint64_t t1_bucket_idx = req->key.hash_low % NR_T1_BUCKET;
	uint64_t t1_part_idx = t1_bucket_idx / NR_T1_BUCKET_PER_PART;

	struct t1_hash_partition *t1_part = &t1_table->t1_part[t1_part_idx];
	struct t1_hash_bucket *t1_bucket = &t1_table->t1_bucket[t1_bucket_idx];

	struct hash_entry *entry = NULL;
	struct hash_entry_f *entry_f = NULL;
	struct callback *cb = NULL;


	if (!req->params) req->params = make_cas_params();
	struct cas_params *params = (struct cas_params *)req->params;
	params->is_read = false;
	params->cas = cas;

	uint64_t old_pba;
	bool is_evicted = false;

	//if (strncmp(req->key.key, "user4931155956028412375513337700", 32) == 0) {
	//	puts("set here");

	//}


	switch (params->write_step) {
	case CAS_STEP_T1_MATCH:
		entry = params->entry;
		entry->clock_bit = 1;
		entry->flying_bit = 0;
		entry->evict_bit = 0;
		entry->dirty_bit = 1;
		goto cas_write_kvpair;
		break;
	case CAS_STEP_EXPIRED:
		printf("SET EXPEXP\n");
	case CAS_STEP_READ_KV_SUCC:
		entry_f = &params->entry_f;
		goto cas_write_t1;
		break;
	case CAS_STEP_T1_MISMATCH:
		entry = params->entry;
		entry->flying_bit = 0;
		break;
	case CAS_STEP_T1_EXPIRED:
		entry = params->entry;
		entry->clock_bit = 1;
		entry->flying_bit = 0;
		entry->evict_bit = 0;
		entry->dirty_bit = 1;
		goto cas_write_kvpair;
		break;
	default:
		hlr->stat.nr_write++;
		break;
	}

	//if (req->key.hash_low == 18069508485845357479) {
	//	printf("ASDASDASDASDASDASDASD\n");
	//}


	if (t1_part->flying) {
		if (params->write_step == CAS_STEP_INIT) {
			hlr->stat.nr_write--;
		}
		retry_req_to_hlr(hlr, req);
		goto exit;
	}


cas_get_entry:
	entry = get_target_entry(cas, entry?entry->lnode->nxt:t1_bucket->hash_list->head, req->key.hash_low);
	if (entry) {
		//entry->fingerprint = req->key.hash_low;
		//entry->dirty_bit = 1;
		//entry->clock_bit = 0;
		//entry->kv_size = req->value.len / GRAIN_UNIT;
		//entry->pba = get_next_pba(hlr, req->value.len);
		params->write_step = CAS_STEP_INIT;
		params->entry = entry;
		entry->flying_bit = 1;
		cb = make_callback(hlr, cb_keycmp, req);
		if (hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb, -1, cas->ops_number) < 0) {
			entry->clock_bit = 0;
			entry->flying_bit = 0;
			entry->dirty_bit = 1;
			entry->evict_bit = 1;
			memset(req->value.value, 0, 512);
			cb_keycmp(req);
		} else {
			//req->data_lookups++;
			//req->data_lookup_bytes += entry->kv_size * GRAIN_UNIT;
		}
		goto exit;
	}

	// need empty space for insertion
	if (need_cache_eviction(cas)) {
		// write t2
		// may write t3-narrow (if t2 is full)
		// may write t3-wide (if one of t3-narrow's buckets is full)
		//printf("@@@EVICTION\n");
		do_eviction(cas, hlr, t1_table);
	}
	
cas_write_t1:
#ifdef TTL
	entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, PBA_INVALID, req->value.len/GRAIN_UNIT, 1, req->req_time + req->sec, 0);
#else
	entry = insert_t1_entry(t1_table, t1_bucket, req->key.hash_low, PBA_INVALID, req->value.len/GRAIN_UNIT, 1);
#endif

cas_write_kvpair:
#ifdef TTL_GROUP
	entry->pba = get_next_pba(hlr, req->value.len, -1, cas->ops_number, -1);
#else
	entry->pba = get_next_pba(hlr, req->value.len, -1, cas->ops_number);
	//if (entry->fingerprint == 18069508485845357479) {
	//	printf("ASDASDASDASDASDASDASD %lu\n", entry->pba);
	//}

#endif
	cb = make_callback(hlr, req->end_req, req);
	copy_key_to_value(&req->key, &req->value, req->req_time + req->sec);
	//printf("old_pba: %lu\n", old_pba);
	//print_entry(entry, "OLD");
	//sw_start(sw_cpy);
#ifdef TTL_GROUP
	hlr->write(hlr, entry->pba, PBA_INVALID, entry->kv_size, req->value.value, cb, -1, cas->ops_number, -1);
#else
	hlr->write(hlr, entry->pba, PBA_INVALID, entry->kv_size, req->value.value, cb, -1, cas->ops_number);
#endif
	//sw_end(sw_cpy);
	//set_cpy += sw_get_usec(sw_cpy);

exit:
	sw_end(sw_set);
	set_total += sw_get_usec(sw_set);
	if (t1_part->flying) {
		if (rc == -1)
			abort();
		if (req->params) {
			if (params->write_step != CAS_STEP_INIT) {
				t1_part->is_set = 1;
				t1_part->flying_type = params->write_step;
			}
		}
	}

	return rc;
}

int cascade_delete(struct kv_ops *ops, struct request *req) {
	return 0;
}

int cascade_need_gc(struct kv_ops *ops, struct handler *hlr) {
	return dev_need_gc(hlr, ops->ops_number);
}

void *cb_idx_gc (void *arg) {
	return NULL;
}

void *cb_data_gc (void *arg) {
	return NULL;
}

static int trigger_data_gc(struct handler* hlr, struct kv_ops *ops, struct gc *gc) {
	struct cascade *cas = (struct cascade *)ops->_private;
	struct t1_hash_table *t1_table = cas->t1_table;

	uint64_t t1_bucket_idx;
	uint64_t t1_part_idx;

	struct t1_hash_partition *t1_part;
	struct t1_hash_bucket *t1_bucket;

	struct segment *victim_seg = (struct segment *)gc->_private;
	uint32_t valid_cnt = gc->valid_cnt;
	uint32_t entry_cnt = victim_seg->entry_cnt;
	char *seg_buf = (char*)gc->buf;

	uint64_t start_pba = victim_seg->start_addr / GRAIN_UNIT;
	struct callback *cb = NULL;

	uint8_t key_len; 
	uint32_t value_len;
	char *cur_buf;
	uint32_t cur_off = 0;
	char *key;
	uint64_t fingerprint;
	uint128 hash128;
	struct hash_entry *entry;
	struct hash_bucket *bucket;
	uint64_t cur_pba, old_pba;
	uint32_t ttl_sec;
	uint64_t bucket_idx;
	int move_cnt = 0;

	for (unsigned int i = 0; i < entry_cnt; i++) {
		cur_buf = seg_buf + cur_off;
		key_len = ((uint8_t *)cur_buf)[0];
		key = cur_buf + sizeof(key_len);
		value_len = ((uint32_t *)(cur_buf + sizeof(key_len) + key_len))[0];
		ttl_sec = ((uint32_t*)(cur_buf + sizeof(key_len) + key_len + sizeof(value_len)))[0];
		hash128 = hashing_key_128(key, key_len);
		t1_bucket_idx = hash128.second % NR_T1_BUCKET;
		t1_part_idx = t1_bucket_idx / NR_T1_BUCKET_PER_PART;
		t1_part = &t1_table->t1_part[t1_part_idx];
		t1_bucket = &t1_table->t1_bucket[t1_bucket_idx];
		entry = get_target_entry(cas, t1_bucket->hash_list->head, hash128.second);
		cur_pba = cur_off / GRAIN_UNIT;
		if (entry && (entry->pba == start_pba + cur_pba)) {
			// have to move hot item
			// update ttl
			//ttl_sec = seg->creation_time + ttl_sec - get_cur_sec();
#ifdef TTL
			if (ttl_sec < get_cur_sec()) {
				victim_seg->invalid_cnt++;
				continue;
			}
#endif
			old_pba = entry->pba;

			cb = make_callback(hlr, cb_data_gc, gc);
#ifdef TTL_GROUP
			entry->pba = get_next_pba(hlr, entry->kv_size * GRAIN_UNIT, -1, cas->ops_number, -1);
			hlr->write(hlr, entry->pba, old_pba, entry->kv_size, cur_buf, cb, -1, cas->ops_number, -1);
#else
			entry->pba = get_next_pba(hlr, entry->kv_size * GRAIN_UNIT, -1, cas->ops_number);
			hlr->write(hlr, entry->pba, old_pba, entry->kv_size, cur_buf, cb, -1, cas->ops_number);
#endif
			//printf("MOVE_HOT %d %d\n", entry_cnt, ++move_cnt);
			++move_cnt;
		} else {
			victim_seg->invalid_cnt++;
		}
		cur_off += value_len;
	}

	if (victim_seg->invalid_cnt != entry_cnt - move_cnt) {
		abort();
	}

	return 0;



	return 0;
}

int cascade_trigger_gc(struct kv_ops *ops, struct handler *hlr) {
	struct gc *gc = hlr->gc;

	if (!dev_read_victim_segment(hlr, ops->ops_number, gc)) {
		goto exit;
	}

	if (gc->is_idx) {
		abort();
	} else {
		trigger_data_gc(hlr, ops, gc);
	}


exit:
	reap_gc_segment(hlr, ops->ops_number, gc);
	gc->state = GC_DONE;

	return gc->valid_cnt;

	return 0;
}

int cascade_wait_gc(struct kv_ops *ops, struct handler *hlr) {
	return 0;
}
