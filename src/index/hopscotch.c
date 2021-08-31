#include "type.h"
#include "index/hopscotch.h"
#include "platform/kv_ops.h"
#include "platform/device.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static void copy_key_to_value(struct key_struct *key, struct val_struct *value) {
	memcpy(value->value, &key->len, sizeof(key->len));
	memcpy(value->value+sizeof(key->len), key->key, key->len);
}

static void hopscotch_full_idx_init (struct hopscotch *hs) {
	// Allocate tables
	hs->table=(struct hash_table *)calloc(NR_TABLE, sizeof(struct hash_table));
	for (int i = 0; i < NR_TABLE; i++) {
		// entry
		hs->table[i].entry = (struct hash_entry *)calloc(NR_ENTRY, sizeof(struct hash_entry));
		for (int j = 0; j < NR_ENTRY; j++) {
			hs->table[i].entry[j].pba = PBA_INVALID;
		}
	}
	return;
}

static void hopscotch_full_idx_free (struct hopscotch *hs) {
	// Free tables
	for (int i = 0; i < NR_TABLE; i++) {
		free(hs->table[i].entry);
	}
	free(hs->table);
	return;
}

static struct hash_entry *hopscotch_full_find_matching_tag
(struct hopscotch *hs, uint8_t dir, uint32_t idx, int *offset, uint8_t tag) {
	struct hash_table *ht = &hs->table[dir];
	int _offset = *offset;
	while (_offset < MAX_HOP) {
		int current_idx = (idx + _offset) % NR_ENTRY;
		struct hash_entry *entry = &ht->entry[current_idx];

		if ((entry->key_fp_tag == tag) && (entry->pba != PBA_INVALID)) {
			*offset = _offset;
			return entry;
		}
		++_offset;
	}
	return NULL;
}

static int hopscotch_full_find_free_entry(struct hopscotch *hs, uint8_t dir, uint32_t idx, struct request *req) {
	struct hash_table *ht = &hs->table[dir];
	int offset = 0;

	// find free entry
	while (offset < MAX_HOP) {
		int current_idx = (idx + offset) % NR_ENTRY;
		struct hash_entry *entry = &ht->entry[current_idx];
		if (entry->pba == PBA_INVALID) return offset;
		++offset;
	}

	// if not, make free entry
	offset = MAX_HOP-1;
	while (offset >= 0) {
		int current_idx = (idx + offset) % NR_ENTRY;
		struct hash_entry *entry = &ht->entry[current_idx];

		int ori_idx = (NR_ENTRY + current_idx - entry->neigh_off) % NR_ENTRY;
		for (int dis_off = entry->neigh_off+1; dis_off < MAX_HOP; dis_off++) {
			int dis_idx = (ori_idx + dis_off) % NR_ENTRY;
			struct hash_entry *dis_entry = &ht->entry[dis_idx];
			printf("make_free_entry - idx: %d, offset: %d, neigh_off: %lu", idx, offset, entry->neigh_off);
			if (dis_entry->pba == PBA_INVALID) {
				hs->fill_entry(hs, dir, ori_idx, dis_off, entry->key_fp_tag, entry->kv_size, entry->pba);
				hs->dis_cnt++;
				return offset;
			}
		}
		--offset;
	}

	// error
	puts("\n@@@ insert error point! @@@\n");
	return -1;
}

static struct hash_entry *hopscotch_full_fill_entry
(struct hopscotch *hs, uint8_t dir, uint32_t idx, uint8_t offset, uint8_t tag, uint16_t size, uint64_t pba) {
	struct hash_table *ht = &hs->table[dir];
	struct hash_entry *entry = &ht->entry[(idx+offset)%NR_ENTRY];
	*entry = (struct hash_entry){offset, tag, size, (pba & PBA_INVALID)};
	return entry;
}

void hopscotch_full_print_info (struct hopscotch *hs) {
	// population
	uint64_t population = 0;
	for (int i = 0; i < NR_TABLE; i++) {
		for (int j = 0; j < NR_ENTRY; j++) {
			if (hs->table[i].entry[j].pba != PBA_INVALID) {
				population++;
			}
		}
	}
	printf("%.2f%% populated (%lu/%u)\n", 
			(float)population/(NR_TABLE*NR_ENTRY)*100, population, NR_TABLE*NR_ENTRY);

	// cost
	uint64_t cost_sum = 0;
	for (int i = 0; i < FLASH_READ_MAX; i++) cost_sum += hs->lookup_cost[i];
	for (int i = 0; i < FLASH_READ_MAX; i++) {
		printf("%d,%lu,%.4f\n", i, hs->lookup_cost[i], (float)hs->lookup_cost[i]/cost_sum*100);
	}
	return;
}

uint32_t hopscotch_lru_retrieve_key (void *table_ptr) {
	struct hash_part *part = (struct hash_part *)table_ptr;
	return part->page_num;
}

void hopscotch_lru_data_free (void *table_ptr) {
	if (table_ptr) free(table_ptr);
	return;
}

void hopscotch_part_idx_init (struct hopscotch *hs) {
	hs->part_lru = (LRU **)malloc(NR_TABLE * sizeof(LRU *));
	for (int i = 0; i < NR_TABLE; i++) {
		lru_init(&hs->part_lru[i], hopscotch_lru_data_free, hopscotch_lru_retrieve_key);
		q_init(&hs->part_q[i], NR_PART);
	}

	for (int i = 0; i < NR_TABLE; i++) {
		for (int j = 0; j < NR_PART; j++) {
			hs->part_info[i][j].state = LRU_INVALID;
			hs->part_info[i][j].pba = PBA_INVALID;
			hs->part_info[i][j].cnt = 0;
			hs->part_info[i][j].debug = 0xff;
			hs->part_info[i][j].latest = 0xffffffff;
			hs->part_info[i][j].latest_pba = 0xffffffff;
			//q_enqueue(aligned_alloc(512, PART_SIZE), hs->part_q[i]);
			q_enqueue(aligned_alloc(512, PART_SIZE), hs->part_q[i]);
		}
	}

	return;
}

void hopscotch_part_idx_free (struct hopscotch *hs) {
	void *ptr;
	for (int i = 0; i < NR_TABLE; i++) {
		lru_free(hs->part_lru[i]);
		while((ptr = q_dequeue(hs->part_q[i])))
			free(ptr);
		q_free(hs->part_q[i]);
	}
	free(hs->part_lru);
}

void *cb_lru_retry(void *arg) {
	struct request *req = (struct request *)arg;
	struct hop_params *params = (struct hop_params *)req->params;

	switch (req->type) {
		case REQ_TYPE_GET:
			params->lookup_step = HOP_STEP_LRU_MISS;
			break;
		case REQ_TYPE_SET:
			params->insert_step = HOP_STEP_LRU_MISS;
			break;
		default:
			fprintf(stderr, "Wrong req type on cb_lru_retry");
			break;
	}

	////////// test
	if (params->lookup_step == HOP_STEP_LRU_MISS) {
		static int cnt = 0;
		if (++cnt % 1024000 == 0) {
			printf("Insert key LRU mismatch! : %d\n", cnt);
		}
	}
	//////////

	struct hash_part *part = params->part;
	struct hopscotch *hs = (struct hopscotch *)part->_private;




#ifdef PART_MEM
	struct hash_table *cp_table = &hs->table[part->dir];
	struct hash_entry *cp_entry = &cp_table->entry[part->page_num * NR_ENTRY_PER_PART];
	memcpy(part->entry, cp_entry, PART_SIZE);
#endif

#ifdef HOP_DEBUG	

	int zero = 0;
	for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
		if (part->entry[i].pba > part->pba) {
			if (part->entry[i].pba != PBA_INVALID) {
				print_part(part, "LRU");
				fflush(stdout);
				abort();
			}
		}
		if (part->entry[i].pba == 0) {
			if (++zero > 2) {
				print_part(part, "LCU ZERO");
				fflush(stdout);
				abort();
			}
		}
	}
#endif
	hs->part_info[part->dir][part->page_num].state = LRU_DONE;
	retry_req_to_hlr(req->hlr, req);
	return NULL;
}

void *cb_find_retry(void *arg) {
	struct request *req = (struct request *)arg;
	struct hop_params *params = (struct hop_params *)req->params;

	switch (req->type) {
		case REQ_TYPE_GET:
			params->lookup_step = HOP_STEP_FIND_RETRY;
			break;
		case REQ_TYPE_SET:
			params->insert_step = HOP_STEP_FIND_RETRY;
			break;
		default:
			fprintf(stderr, "Wrong req type on cb_find_retry");
			break;
	}

	struct hash_part *part = params->part;
	struct hopscotch *hs = (struct hopscotch *)part->_private;


#ifdef HOP_DEBUG	
	if (hs->part_info[part->dir][part->page_num].state != LRU_FLYING) abort();
#endif
	hs->part_info[part->dir][part->page_num].state = LRU_DONE;

	//retry_req_to_hlr(req->hlr, req);
	return NULL;
}



static struct hash_entry *hopscotch_part_find_matching_tag
(struct hopscotch *hs, uint8_t dir, uint32_t idx, int *offset, uint8_t tag) {
	LRU *lru = hs->part_lru[dir];
	int _offset = *offset;
	int current_idx = (idx + _offset) % NR_ENTRY;
	uint32_t lru_key = get_lru_key(current_idx);
	//uint32_t page_num = get_idx_page_num(idx,_offset);
	struct hash_part *part;

	part = (struct hash_part *)lru_find(lru, lru_key);

	if (!part)
		return NULL;

	if (part->page_num != lru_key) {
		struct hash_part *tmp1 = (struct hash_part *)lru_find(lru,lru_key);
		struct hash_part *tmp2 = (struct hash_part *)lru_find(lru,part->page_num);
		printf("?????????? part:%d %p lru:%d %p part: %p\n",part->page_num, tmp2, lru_key, tmp1, part);
		fflush(stdout);
		abort();
	}
	struct hash_entry *entry;
	uint32_t current_lru_key = lru_key; 

	lru_update(lru, part->lnode);

#ifdef HOP_DEBUG
	if (hs->part_info[dir][part->page_num].state == LRU_FLYING) {
		abort();
	}
#endif


	while ((current_lru_key == lru_key) && (_offset < MAX_HOP)) {
		entry = get_entry_from_part(part, current_idx);
		if ((entry->key_fp_tag == tag) && (entry->pba != PBA_INVALID)) {
			*offset = _offset;
			return entry;
		}
		++_offset;
		current_idx = (idx + _offset) % NR_ENTRY;
		current_lru_key = get_lru_key(current_idx);
	}

	if (current_lru_key != lru_key) {
		static int next_cnt = 0;
		part = (struct hash_part *)lru_find(lru, current_lru_key);
		if (part) {
			lru_update(lru, part->lnode);
			if (hs->part_info[dir][part->page_num].state == LRU_FLYING) {
				abort();
			}
			while(_offset < MAX_HOP) {
				entry = get_entry_from_part(part, current_idx);
				if ((entry->key_fp_tag == tag) && (entry->pba != PBA_INVALID)) {
					*offset = _offset;
					return entry;
				}
				++_offset;
				current_idx = (idx + _offset) % NR_ENTRY;
			}
		}

		if ((++next_cnt % 10240) == 0)
			printf("NEXT: %d\n", next_cnt);		
		/*
		   if (current_lru_key == lru_key) {
		// *offset =  _offset = MAX_HOP
		// no tag in this hop
		return NULL;
		} else if (_offset == MAX_HOP) {
		return NULL;
		} else {
		}
		}
		*/
}

	*offset = _offset;
	return NULL;
}

static int hopscotch_part_find_free_entry(struct hopscotch *hs, uint8_t dir, uint32_t idx, struct request *req) {
	LRU *lru = hs->part_lru[dir];
	volatile int offset = 0;
	volatile int current_idx = (idx + offset) % NR_ENTRY;
	volatile uint32_t lru_key = get_lru_key(current_idx);
	struct hash_part *part, *next_part = NULL;

	part = (struct hash_part *)lru_find(lru, lru_key);

	if (!part)
		abort();

	if (part->page_num != lru_key) {
		abort();
	}

	volatile struct hash_entry *entry;
	volatile uint32_t current_lru_key = lru_key;

	lru_update(lru, part->lnode);

#ifdef HOP_DEBUG
	if (hs->part_info[dir][part->page_num].state == LRU_FLYING) {
		abort();
	}
#endif


	while ((current_lru_key == lru_key) && (offset < MAX_HOP)) {
		entry = get_entry_from_part(part, current_idx);
		/*
		   if (offset > 2) {
		   printf("3 populated\n");
		   print_hop_entry(part, current_idx, offset, "FIND");
		   }
		   */

		if (entry->pba == PBA_INVALID) return offset;
#ifdef HOP_DEBUG
		if (entry->pba == 0) {
			print_hop_entry(part, current_idx, offset, "ZERO");
			fflush(stdout);
		}
#endif
		offset++;
		current_idx = (idx + offset) % NR_ENTRY;
		current_lru_key = get_lru_key(current_idx);
	}
	// From now, there is a no free entry in 'part'
	// case 1: cur_lru_key == lru_key, offset == MAXHOP -> Displace in part
	// case 2: cur_lru_key == lru_key, offset != MAXHOP -> X (upper case)
	// case 3: cur_lru_key != lru_key, offset == MAXHOP -> Displace in part
	// case 4: cur_lru_key != lru_key, offset != MAXHOP -> find free space in 'next_part'

	if ((current_lru_key != lru_key) && (offset != MAX_HOP)) { // case 4
		next_part = (struct hash_part *)lru_find(lru, current_lru_key);
		if (next_part) {
			lru_update(lru, next_part->lnode);
			while (offset < MAX_HOP) {
				entry = get_entry_from_part(next_part, current_idx);

				if (entry->pba == PBA_INVALID) return offset;
				offset++;
				current_idx = (idx + offset) % NR_ENTRY;
			}
		} else {
			abort();
		}
	}


#ifdef HOP_DEBUG
	struct hash_part *tmp_part;
	hash_part_init(&tmp_part, hs, part->dir, (idx+offset)%NR_ENTRY);
	struct callback *cb = make_callback(req->hlr, NULL, req);
	uint64_t _part_pba = hs->part_info[part->dir][part->page_num].pba;
	tmp_part->pba = _part_pba;
	//print_part(part, "MAPPING");
	int ret_val = req->hlr->read(req->hlr, _part_pba, GRAINS_PER_PART, (char *)tmp_part->entry, cb);



	int zero_num = 0, invalid_num = 0;
	for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
		if (part->entry[i].pba == 0) {
			zero_num++;
		} else if (part->entry[i].pba == PBA_INVALID) {
			invalid_num++;
		}
	}


	print_hop_entry(part, current_idx, 0, "ZERO");
	print_hop_entry(part, current_idx+32, 0, "ZERO");
	printf("%d %d\n", invalid_num, zero_num);

	print_hop_entry(tmp_part, current_idx, 0, "OREZ");
	print_hop_entry(tmp_part, current_idx+32, 0, "OREZ");
	printf("%d\n", ret_val);
	fflush(stdout);
	if (zero_num > 2) abort();
#endif

	current_idx = (idx + 2 * (MAX_HOP - 1)) % NR_ENTRY;
	current_lru_key = get_lru_key(current_idx);

	if (next_part || (current_lru_key == lru_key)) { //displace
		struct hash_part *ori_part, *target_part;
		struct hash_entry *ori_entry, *target_entry;
		volatile int ori_idx, target_idx;
		volatile uint32_t target_lru_key;
		offset = MAX_HOP-1;
		while (offset >= 0) {
			current_idx = (idx + offset) % NR_ENTRY;
			current_lru_key = get_lru_key(current_idx);
			if (current_lru_key == lru_key) {
				ori_part = part;
			} else {
				ori_part = next_part;
			}
			ori_entry = get_entry_from_part(ori_part, current_idx);
			ori_idx = (NR_ENTRY + current_idx - ori_entry->neigh_off) % NR_ENTRY;
			for (int target_off = ori_entry->neigh_off + 1; target_off < MAX_HOP; target_off++) {
				target_idx = (ori_idx + target_off) % NR_ENTRY;
				target_lru_key = get_lru_key(target_idx);
				if (target_lru_key == lru_key) {
					target_part = part;
				} else {
					target_part = next_part;
				}
				target_entry = get_entry_from_part(target_part, target_idx);
				if (target_entry->pba == PBA_INVALID) {
					hs->fill_entry(hs, dir, ori_idx, target_off, ori_entry->key_fp_tag, ori_entry->kv_size, ori_entry->pba);
					hs->dis_cnt++;
					hs->part_info[dir][target_lru_key].cnt++;
					hs->part_info[dir][lru_key].cnt--;
					return offset;
				}
			}
			--offset;
		}
		printf("full entry\n");
		//hs->print_info(hs);
		fflush(stdout);
		abort();
	}

	return -1;
}

static struct hash_entry *hopscotch_part_fill_entry
(struct hopscotch *hs, uint8_t dir, uint32_t idx, uint8_t offset, uint8_t tag, uint16_t size, uint64_t pba) {
	LRU *lru = hs->part_lru[dir];
	struct hash_part *part;
	uint32_t lru_key = get_lru_key((idx+offset)%NR_ENTRY);

	if ((part = (struct hash_part *)lru_find(lru, lru_key))) {
		//static int cnt = 0;

#ifdef HOP_DEBUG
		if (hs->part_info[dir][part->page_num].state == LRU_FLYING) {
			abort();
		}
#endif

		struct hash_entry *entry = get_entry_from_part(part, (idx+offset)%NR_ENTRY);
		*entry = (struct hash_entry){offset, tag, size, (pba & PBA_INVALID)};
		hs->part_info[dir][part->page_num].latest = ((idx + offset)%NR_ENTRY)%NR_ENTRY_PER_PART;
		hs->part_info[dir][part->page_num].latest_pba = pba & PBA_INVALID;

		//printf("make_free_entry - idx: %d, offset: %d, neigh_off: %d", idx, offset, entry->neigh_off);
		//print_hash_entry(entry, "FILL");

		hs->part_info[dir][part->page_num].state = LRU_DIRTY;
		return entry;
	} else {
		abort();
		return NULL;
	}
}

void *cb_free(void *arg) {
	if (arg) {
		struct hash_part *part = (struct hash_part *)arg;
		struct hopscotch *hs = (struct hopscotch *)part->_private;
		hs->part_info[part->dir][part->page_num].state = LRU_CLEAN;
		//hs->part_info[part->dir][part->page_num].state = LRU_DIRTY;
		q_enqueue((void*)part->entry, hs->part_q[part->dir]);
		//free(part->entry);
		free(arg);
	}
	return NULL;
}

static int hopscotch_part_push(struct hopscotch *hs, struct request *req, uint8_t dir, struct hash_part *part) {
	LRU *lru = hs->part_lru[dir];
	struct hash_part *tmp;
	uint64_t old_pba;

	if ((tmp = (struct hash_part *)lru_find(lru, part->page_num))) {
		//already exist
		static int cnt = 0;
		lru_update(lru, tmp->lnode);
		free(part->entry);
		free(part);
		printf("duplicate caching cnt: %d\n", ++cnt);
		abort();
		return -1;
	}

	part->lnode = lru_push(lru, part);

#ifdef HOP_DEBUG
	if ((hs->part_info[dir][part->page_num].state == LRU_FLYING) || (hs->part_info[dir][part->page_num].state == LRU_DIRTY)) {
		abort();
	}
#endif

	if (hs->part_info[dir][part->page_num].state == LRU_INVALID)
		hs->part_info[dir][part->page_num].state = LRU_DIRTY;
	else {
		hs->part_info[dir][part->page_num].state = LRU_CLEAN;
	}


	if (lru->size > NR_CACHED_PART) {
		struct hash_part *evict_part = (struct hash_part *)lru_pop_without_free(lru);

		if (hs->part_info[dir][evict_part->page_num].state == LRU_DIRTY) {
			struct handler *hlr = req->hlr;
			//old_pba = evict_part->pba;
			old_pba = PBA_INVALID;
			uint64_t part_pba = get_next_pba(hlr, PART_SIZE);
			//uint64_t part_pba = get_next_idx_pba(hlr, PART_SIZE);
			hs->part_info[dir][evict_part->page_num].state = LRU_FLYING;
			hs->part_info[dir][evict_part->page_num].pba = part_pba;
			evict_part->pba = part_pba;
#ifdef HOP_DEBUG
			int invalid_num = 0, zero_num = 0;
			for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
				if (evict_part->entry[i].pba == 0) {
					zero_num++;
				} else if (evict_part->entry[i].pba == PBA_INVALID) {
					invalid_num++;
				}
				if (zero_num > 2) abort();
			}
			//printf("EVICT %d\n", invalid_num);
			//print_part(evict_part, "EVICT");
			for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
				if (evict_part->entry[i].pba > evict_part->pba) {
					if (evict_part->entry[i].pba != PBA_INVALID) {
						print_part(part, "SET6");
						fflush(stdout);
						abort();
					}
				}
			}
#endif


#ifdef PART_MEM
			struct hash_table *cp_table = &hs->table[evict_part->dir];
			struct hash_entry *cp_entry = &cp_table->entry[evict_part->page_num * NR_ENTRY_PER_PART];
			memcpy(cp_entry, evict_part->entry, PART_SIZE);
#endif

			struct callback *cb = make_callback(hlr, cb_free, evict_part);
			hlr->write(hlr, part_pba, old_pba, GRAINS_PER_PART, (char *)evict_part->entry, cb);
			//cb_free(evict_part);

			//hlr->idx_write(hlr, part_pba, GRAINS_PER_PART, (char *)evict_part->entry, cb);
			return 1;
		} else {
			q_enqueue((void*)evict_part->entry, hs->part_q[evict_part->dir]);
			//free(evict_part->entry);
			free(evict_part);
			return 0;
		}
	}
	return 0;
}

void hopscotch_part_print_info (struct hopscotch *hs) {
	printf("PART_SIZE: %d\n", PART_SIZE);
	printf("NR_ENTRY_PER_PART: %d\n", NR_ENTRY_PER_PART);
	printf("NR_PART: %d\n", NR_PART);
	printf("NR_ENTRY: %d\n", NR_ENTRY);
	printf("PBA_INVALID: %ld\n", PBA_INVALID);
	// population
	//uint64_t entry_cnt = 0, part_cnt = 0;
	uint64_t part_cnt = 0;

	for (int i = 0; i < NR_TABLE; i++) {
		for (int j = 0; j < NR_PART; j++) {
			//entry_cnt = 0;
			//fot (int k = 0; k < NR_ENTRY_PER_PART; k++) {
			//	if (part
			//}
			if (hs->part_info[i][j].state != LRU_INVALID) {
				part_cnt++;
			}
			//if (j % 128) {
			printf("[ENTRY in PART%d] %.2f%% populated (%lu/%u)\n",
					j,(float)(hs->part_info[i][j].cnt)/NR_ENTRY_PER_PART, hs->part_info[i][j].cnt,NR_ENTRY_PER_PART);
			//}
		}
	}
	printf("[PART] %.2f%% populated (%lu/%u)\n", 
			(float)part_cnt/(NR_TABLE*NR_PART)*100, part_cnt, NR_TABLE*NR_PART);

	printf("[ENTRY] %.2f%% populated (%lu/%u)\n", 
			(float)hs->fill_cnt/(NR_ENTRY)*100, hs->fill_cnt, NR_ENTRY);

	printf("[ENTRY] %.2f%% displaced (%lu/%u)\n", 
			(float)hs->dis_cnt/(NR_ENTRY)*100, hs->dis_cnt, NR_ENTRY);

#if 0
	// cost
	uint64_t cost_sum = 0;
	for (int i = 0; i < FLASH_READ_MAX; i++) cost_sum += hs->lookup_cost[i];
	for (int i = 0; i < FLASH_READ_MAX; i++) {
		printf("%d,%lu,%.4f\n", i, hs->lookup_cost[i], (float)hs->lookup_cost[i]/cost_sum*100);
	}
#endif
	return;
}


int hopscotch_init(struct kv_ops *ops) {
	printf("===============================\n");
	printf(" uDepot's Hopscotch Hash-table \n");
	printf("===============================\n\n");

	printf("hopscotch_init()...\n\n");
	fflush(stdout);

	struct hopscotch *hs = (struct hopscotch *)malloc(sizeof(struct hopscotch));

#ifdef HOPSCOTCH_FULL
	hs->idx_init = hopscotch_full_idx_init;
	hs->idx_free = hopscotch_full_idx_free;
	hs->find_matching_tag = hopscotch_full_find_matching_tag;
	hs->find_free_entry = hopscotch_full_find_free_entry;
	hs->fill_entry = hopscotch_full_fill_entry;
	hs->print_info = hopscotch_full_print_info;
	hs->type = HOP_FULL;
	hs->idx_start = hs->kv_start = 0;
#elif HOPSCOTCH_PART
	hs->idx_init = hopscotch_part_idx_init;
	hs->idx_free = hopscotch_part_idx_free;
	hs->find_matching_tag = hopscotch_part_find_matching_tag;
	hs->find_free_entry = hopscotch_part_find_free_entry;
	hs->fill_entry = hopscotch_part_fill_entry;
	hs->print_info = hopscotch_part_print_info;
	hs->type = HOP_PART;
	hs->idx_start = IDX_START_IN_DEV;
	hs->kv_start = KV_START_IN_DEV;
#ifdef PART_MEM
	hopscotch_full_idx_init(hs);
#endif
	//hs->print_info(hs);
#endif

	hs->fill_cnt = hs->dis_cnt = 0;
	for (int i = 0; i < MAX_HOP; i++) {
		hs->off_cnt[i] = 0;
	}

	hs->idx_init(hs);

	ops->_private = (void *)hs;

	return 0;
}

int hopscotch_free(struct kv_ops *ops) {
	struct hopscotch *hs = (struct hopscotch *)ops->_private;

	printf("hopscotch_free()...\n\n");

	hs->print_info(hs);
	hs->idx_free(hs);
#ifdef PART_MEM
	hopscotch_full_idx_free(hs);
#endif

	free(hs);

	return 0;
}


#if 0
static void print_entry(struct hash_entry *entry) {
	printf("off:%lu, tag:0x%lx, size:%lu, pba:%lx\n",
			entry->neigh_off, entry->key_fp_tag, entry->kv_size, entry->pba);
}

static struct hash_entry *fill_entry
(struct hash_entry *entry, uint8_t offset, uint8_t tag, uint16_t size, uint64_t pba) {
	*entry = (struct hash_entry){offset, tag, size, (pba & PBA_INVALID)};
	//print_entry(entry);
	return entry;
}
#endif



static int keycmp_k_v(struct key_struct *key, struct val_struct *value) {
	if (key->len != ((uint8_t *)value->value)[0]) {
		return (key->len > ((uint8_t *)value->value)[0]) ? 1:-1;
	}
	return strncmp(key->key, value->value+(sizeof(uint8_t)), key->len);
}

void *cb_keycmp(void *arg) {
	struct request *req = (struct request *)arg;
	struct hop_params *params = (struct hop_params *)req->params;

	struct key_struct *key = &req->key;
	struct val_struct *value = &req->value;

	static int get_miss = 0;

	switch (req->type) {
		case REQ_TYPE_GET:
			if (keycmp_k_v(key, value) == 0) {
				params->lookup_step = HOP_STEP_KEY_MATCH;
				req->end_req(req);
				return NULL;
			} else {
				if (++get_miss % 10240 == 0) {
					printf("GET key missmatch! : %d\n", get_miss);
				}
				params->lookup_step = HOP_STEP_KEY_MISMATCH;
			}
			break;
		case REQ_TYPE_SET:
			if (keycmp_k_v(key, value) == 0) {
				params->insert_step = HOP_STEP_KEY_MATCH;
			} else {
				params->insert_step = HOP_STEP_KEY_MISMATCH;
			}
			break;
		default:
			fprintf(stderr, "Wrong req type on cb_keycmp");
			break;
	}

	////////// test
	if (params->insert_step == HOP_STEP_KEY_MISMATCH) {
		static int cnt = 0;
		if (++cnt % 10240 == 0) {
			printf("Insert key mismatch! : %d\n", cnt);
		}
	}
	//////////

	retry_req_to_hlr(req->hlr, req);
	return NULL;
}

// FIXME
uint64_t get_pba(uint16_t kv_size) {
	static uint64_t pba = 0;
	uint64_t ret = pba;
	pba += kv_size;
	return ret;
}

int hopscotch_set(struct kv_ops *ops, struct request *req) {
	struct hopscotch *hs = (struct hopscotch *)ops->_private;
	struct handler *hlr = req->hlr;
	int offset = 0;
	uint64_t pba;
	struct hash_part *part = NULL;
	uint64_t idx_page_num;

	hash_t h_key = req->key.hash_low;

	uint32_t idx = h_key & ((1 << IDX_BIT)-1);
	uint8_t  tag = (uint8_t)(h_key >> IDX_BIT);
	uint8_t  dir = tag & ((1 << DIR_BIT)-1);

	//struct hash_table *ht = &hs->table[dir];
	struct hash_entry *entry = NULL;
	struct callback *cb = NULL;

	if (!req->params) req->params = make_hop_params();
	struct hop_params *params = (struct hop_params *)req->params;

	uint64_t old_pba;

	//if (strncmp(req->key.key, "user765417060341", 16) == 0) {
	//	puts("here");
	//}


	switch (params->insert_step) {
		case HOP_STEP_KEY_MATCH:
			offset = params->offset;
			goto hop_insert_key_match;
			break;
		case HOP_STEP_KEY_MISMATCH:
			hlr->stat.nr_write_key_mismatch++;
			offset = params->offset + 1;
			goto hop_insert_key_mismatch;
			break;
		case HOP_STEP_FLYING:
			offset = params->offset + 1;
			params->insert_step = HOP_STEP_KEY_MISMATCH;
			goto hop_insert_key_mismatch;
			break;
		case HOP_STEP_LRU_MISS:
			hlr->stat.nr_write_cache_miss++;
			offset = params->offset;
			goto hop_lru_cache_miss;
		case HOP_STEP_FIND_RETRY:
			params->insert_step = HOP_STEP_INIT;
			goto hop_insert_key_mismatch;
			break;
		default:
			hlr->stat.nr_write++;
			goto hop_insert_key_mismatch;
			break;
	}



hop_lru_cache_miss:
	// insert the hash_part to lru cache
	part = params->part;
	if (hs->part_info[dir][part->page_num].state != LRU_DONE) {
		if (hs->part_info[dir][part->page_num].state != LRU_FLYING) abort();
		printf("MISS FYLING\n");
		hlr->stat.nr_write_cache_miss--;
		retry_req_to_hlr(req->hlr, req);
		goto exit;
	} else {
		hopscotch_part_push(hs, req, dir, part);
	}
	//print_hop_entry(part, idx, offset, "MISS");

hop_insert_key_mismatch:
	// phase 1: find matching tag
	entry = hs->find_matching_tag(hs, dir, idx, &offset, tag);
	if (entry != NULL) {
		//printf("SET 1 - idx: %llu, offset: %d\n", idx, offset);
		// read kv-pair to confirm whether it is correct key or not
		cb = make_callback(hlr, cb_keycmp, req);
		params->offset = offset;
		hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb);
		goto exit;
		//goto hop_insert_key_match;
	} else if (hs->type == HOP_PART) {
		// address index cache miss
		idx_page_num = get_idx_page_num(idx, offset);
		if (hs->part_info[dir][idx_page_num].state == LRU_INVALID) {
			hash_part_init(&part, hs, dir, (idx + offset)%NR_ENTRY);
			for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
				memset(&part->entry[i], 0, sizeof(struct hash_entry));
				part->entry[i].pba = PBA_INVALID;
			}
			hopscotch_part_push(hs, req, dir, part);
			//printf("SET 2 - idx: %llu, offset: %d, part_num: %d, part: %p\n", idx, offset, idx_page_num, part);
			goto hop_find_free_entry;
		} else if ((hs->part_info[dir][idx_page_num].state == LRU_FLYING) || (hs->part_info[dir][idx_page_num].state == LRU_DONE)) {
			params->offset = offset - 1;
			//params->insert_step = HOP_STEP_KEY_MISMATCH;
			params->insert_step = HOP_STEP_FLYING;
			//printf("SET 3 - idx: %llu, offset: %d, part_num: %d, part: %p\n", idx, offset, idx_page_num, part);
			retry_req_to_hlr(req->hlr, req);
			goto exit;
		} else if (offset != MAX_HOP) {
			// get next part
			idx_page_num = get_idx_page_num(idx, offset); // next part
			if (hs->part_info[dir][idx_page_num].state == LRU_INVALID) {
				hash_part_init(&part, hs, dir, (idx+offset)%NR_ENTRY);
				for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
					memset(&part->entry[i], 0, sizeof(struct hash_entry));
					part->entry[i].pba = PBA_INVALID;
				}
				hopscotch_part_push(hs, req, dir, part);
				//printf("SET 4 - idx: %llu, offset: %d, part_num: %d, part: %p\n", idx, offset, idx_page_num, part);
				goto hop_find_free_entry;
			} else if ((hs->part_info[dir][idx_page_num].state == LRU_FLYING) || (hs->part_info[dir][idx_page_num].state == LRU_DONE)) {
				params->offset = offset - 1;
				//params->insert_step = HOP_STEP_KEY_MISMATCH;
				params->insert_step = HOP_STEP_FLYING;
				//printf("SET 5 - idx: %llu, offset: %d, part_num: %d, part: %p\n", idx, offset, idx_page_num, part);
				retry_req_to_hlr(req->hlr,req);
				goto exit;
			} else if (hs->part_info[dir][idx_page_num].state == LRU_DIRTY) {
				abort();
			}


			hash_part_init(&part, hs, dir, (idx+offset)%NR_ENTRY);

			params->offset = offset;
			params->idx = idx;
			params->part = part;
			uint64_t part_pba = hs->part_info[dir][part->page_num].pba;
			hs->part_info[dir][part->page_num].state = LRU_FLYING;

			part->pba = part_pba;
#ifdef HOP_DEBUG
			for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
				if (part->entry[i].pba > part->pba) {
					if (part->entry[i].pba != PBA_INVALID) {
						print_part(part, "SET6");
						fflush(stdout);
						abort();
					}
				}
			}
#endif
			cb = make_callback(hlr, cb_lru_retry, req);
			hs->part_info[dir][part->page_num].debug = hlr->read(hlr, part_pba, GRAINS_PER_PART, (char *)part->entry, cb);
			goto exit;
		}
	}

hop_find_free_entry:
	// phase 2: find free entry
	offset = hs->find_free_entry(hs, dir, idx, req);
	if (offset == -1) {
		// error: must resize the table!
		//fprintf(stderr, "Insert error: Need to resize the table");
		//abort();

		idx_page_num = (get_idx_page_num(idx, 0) + 1) % NR_PART; // next part
		if (hs->part_info[dir][idx_page_num].state == LRU_INVALID) {
			hash_part_init(&part, hs, dir, 0);
			part->page_num = idx_page_num;
			for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
				memset(&part->entry[i], 0, sizeof(struct hash_entry));
				part->entry[i].pba = PBA_INVALID;
			}
			hopscotch_part_push(hs, req, dir, part);
				params->insert_step = HOP_STEP_FIND_RETRY;
			retry_req_to_hlr(req->hlr,req);
		} else if ((hs->part_info[dir][idx_page_num].state == LRU_FLYING) || (hs->part_info[dir][idx_page_num].state == LRU_DONE)) {
			params->insert_step = HOP_STEP_FIND_RETRY;
			retry_req_to_hlr(req->hlr,req);
		} else {
			hash_part_init(&part, hs, dir, 0);
			part->page_num = idx_page_num;
			cb = make_callback(hlr, cb_find_retry, req);
			params->offset = 0;
			params->idx = idx;
			params->part = part;
			uint64_t part_pba = hs->part_info[dir][part->page_num].pba;
			hs->part_info[dir][part->page_num].state = LRU_FLYING;
			hlr->stat.nr_write_find_miss++;
			hlr->read(hlr, part_pba, GRAINS_PER_PART, (char *)part->entry, cb);
		}
		goto exit;
	}

hop_insert_key_match:
	// phase 3: fill the entry
	///*
	

	//struct hash_entry *tmp_entry = &ht->entry[(idx+offset)%NR_ENTRY];
	old_pba = PBA_INVALID;

	pba = get_next_pba(hlr, req->value.len);

	entry = hs->fill_entry(hs, dir, idx, offset, tag, req->value.len/SOB, pba);
	hs->fill_cnt++;
	hs->part_info[dir][get_idx_page_num(idx,offset)].cnt++;

	cb = make_callback(hlr, req->end_req, req);
	copy_key_to_value(&req->key, &req->value);
	hlr->write(hlr, entry->pba, old_pba, entry->kv_size, req->value.value, cb);
	//*/
	/*
	   entry = hs->fill_entry(hs, dir, idx, offset, tag, req->value.len/SOB, pba);
	//pba = get_next_pba(hlr, req->value.len);
	//pba = get_next_pba(hlr, 1024);
	req->end_req(req);
	*/

exit:
	return 0;
}


#if 0
static void collect_lookup_cost(struct hopscotch *hs, int nr_read) {
	if (nr_read < FLASH_READ_MAX) hs->lookup_cost[nr_read]++;
}
#endif

int hopscotch_get(struct kv_ops *ops, struct request *req) {
	volatile int rc = 0;

	struct hopscotch *hs = (struct hopscotch *)ops->_private;
	struct handler *hlr = req->hlr;
	int offset = 0;
	struct hash_part *part = NULL;

	hash_t h_key = req->key.hash_low;

	uint32_t idx = h_key & ((1 << IDX_BIT)-1);
	uint8_t  tag = (uint8_t)(h_key >> IDX_BIT);
	uint8_t  dir = tag & ((1 << DIR_BIT)-1);

	uint64_t idx_page_num;

	//struct hash_table *ht = &hs->table[dir];
	struct hash_entry *entry = NULL;
	struct callback *cb = NULL;

	if (!req->params) req->params = make_hop_params();
	struct hop_params *params = (struct hop_params *)req->params;

	if (strncmp(req->key.key, "user765417060341", 16) == 0) {
		puts("here");
	}


	switch (params->lookup_step) {
		case HOP_STEP_KEY_MISMATCH:
			hlr->stat.nr_read_key_mismatch++;
			offset = params->offset + 1;
			goto hop_lookup_key_mismatch;
			break;
		case HOP_STEP_LRU_MISS:
			hlr->stat.nr_read_cache_miss++;
			offset = params->offset;
			goto hop_lru_cache_miss;
			break;
		case HOP_STEP_FLYING:
			offset = params->offset + 1;
			params->lookup_step = HOP_STEP_KEY_MISMATCH;
			goto hop_lookup_key_mismatch;
			break;
		case HOP_STEP_INIT:
			hlr->stat.nr_read++;
		case HOP_STEP_KEY_MATCH:
		default:
			goto hop_lookup_key_mismatch;
			break;
	}

hop_lru_cache_miss:
	part = params->part;
	if (hs->part_info[dir][part->page_num].state != LRU_DONE) {
		if (hs->part_info[dir][part->page_num].state != LRU_FLYING) abort();
		printf("MISS FYLING\n");
		hlr->stat.nr_read_cache_miss--;
		retry_req_to_hlr(req->hlr, req);
		goto exit;
	} else {
		hopscotch_part_push(hs, req, dir, part);
	}



hop_lookup_key_mismatch:
	entry = hs->find_matching_tag(hs, dir, idx, &offset, tag);
	if (entry != NULL) {
		//printf("1\n");
		//entry = &ht->entry[(idx+offset)%NR_ENTRY];
		//entry = get_entry(hs, dir, idx, offset);
		cb = make_callback(hlr, cb_keycmp, req);
		params->offset = offset;
		hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb);
		goto exit;
	} else if (hs->type == HOP_PART) {
		idx_page_num = get_idx_page_num(idx, offset);
		if (hs->part_info[dir][idx_page_num].state == LRU_INVALID) {
			//printf("2\n");
			abort();
			hash_part_init(&part, hs, dir, (idx + offset)%NR_ENTRY);
			for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
				memset(&part->entry[i], 0, sizeof(struct hash_entry));
				part->entry[i].pba = PBA_INVALID;
			}
			hopscotch_part_push(hs, req, dir, part);
			goto not_exist;
		} else if ((hs->part_info[dir][idx_page_num].state == LRU_FLYING) || (hs->part_info[dir][idx_page_num].state == LRU_DONE)) {
			//printf("3\n");
			params->offset = offset - 1;
			//params->lookup_step = HOP_STEP_KEY_MISMATCH;
			params->lookup_step = HOP_STEP_FLYING;
			retry_req_to_hlr(req->hlr, req);
			goto exit;
		} else if (offset != MAX_HOP) {
			idx_page_num = get_idx_page_num(idx, offset);
			if (hs->part_info[dir][idx_page_num].state == LRU_INVALID) {
				//printf("4\n");
				hash_part_init(&part, hs, dir, (idx+offset)%NR_ENTRY);
				for (int i = 0; i < NR_ENTRY_PER_PART; i++) {
					memset(&part->entry[i], 0, sizeof(struct hash_entry));
					part->entry[i].pba = PBA_INVALID;
				}
				hopscotch_part_push(hs, req, dir, part);
				goto not_exist;
			} else if ((hs->part_info[dir][idx_page_num].state == LRU_FLYING) || (hs->part_info[dir][idx_page_num].state == LRU_DONE)) {
				//printf("5\n");
				params->offset = offset - 1;
				//params->lookup_step = HOP_STEP_KEY_MISMATCH;
				params->lookup_step = HOP_STEP_FLYING;
				retry_req_to_hlr(req->hlr,req);
				goto exit;
			}

			hash_part_init(&part, hs, dir, (idx+offset)%NR_ENTRY);
			params->offset = offset;
			params->idx = idx;
			params->part = part;
			hs->part_info[dir][part->page_num].state = LRU_FLYING;
			uint64_t part_pba = hs->part_info[dir][part->page_num].pba;
			part->pba = part_pba;
			cb = make_callback(hlr, cb_lru_retry, req);
			hlr->read(hlr, part_pba, GRAINS_PER_PART, (char *)part->entry, cb);
			goto exit;
		}
	}

not_exist:
	// case of "not existing key"
	rc = 1;
exit:
	if (rc == 1)
		hlr->stat.nr_read_miss++;
	return rc;
}

int hopscotch_delete(struct kv_ops *ops, struct request *req) {
	// this would be implemented if necessary
	return 0;
}

int hopscotch_need_gc(struct kv_ops *ops, struct handler *hlr) {
	// this would be implemented if necessary
	return 0;
}

int hopscotch_trigger_gc(struct kv_ops *ops, struct handler *hlr) {
	// this would be implemented if necessary
	return 0;
}

int hopscotch_wait_gc(struct kv_ops *ops, struct handler *hlr) {
	// this would be implemented if necessary
	return 0;
}
