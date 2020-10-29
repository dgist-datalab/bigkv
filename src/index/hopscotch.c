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

int hopscotch_init(struct kv_ops *ops) {
	printf("===============================\n");
	printf(" uDepot's Hopscotch Hash-table \n");
	printf("===============================\n\n");

	printf("hopscotch_init()...\n\n");

	struct hopscotch *hs = (struct hopscotch *)malloc(sizeof(struct hopscotch));

	// Allocate tables
	hs->table=(struct hash_table *)calloc(NR_TABLE, sizeof(struct hash_table));
	for (int i = 0; i < NR_TABLE; i++) {
		// entry
		hs->table[i].entry = (struct hash_entry *)calloc(NR_ENTRY, sizeof(struct hash_entry));
		for (int j = 0; j < NR_ENTRY; j++) {
			hs->table[i].entry[j].pba = PBA_INVALID;
		}
	}

	ops->_private = (void *)hs;

	return 0;
}

int hopscotch_free(struct kv_ops *ops) {
	struct hopscotch *hs = (struct hopscotch *)ops->_private;

	printf("hopscotch_free()...\n\n");

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

	// Free tables
	for (int i = 0; i < NR_TABLE; i++) {
		free(hs->table[i].entry);
	}
	free(hs->table);
	free(hs);

	return 0;
}

static int find_matching_tag
(struct hash_table *ht, uint32_t idx, int offset, uint8_t tag) {
	while (offset < MAX_HOP) {
		int current_idx = (idx + offset) % NR_ENTRY;
		struct hash_entry *entry = &ht->entry[current_idx];

		if (entry->key_fp_tag == tag) return offset;
		++offset;
	}
	return -1;
}

#if 0
static void print_entry(struct hash_entry *entry) {
	printf("off:%lu, tag:0x%lx, size:%lu, pba:%lx\n",
		entry->neigh_off, entry->key_fp_tag, entry->kv_size, entry->pba);
}
#endif

static struct hash_entry *fill_entry
(struct hash_entry *entry, uint8_t offset, uint8_t tag, uint16_t size, uint64_t pba) {
	*entry = (struct hash_entry){offset, tag, size, (pba & PBA_INVALID)};
	//print_entry(entry);
	return entry;
}

static int find_free_entry(struct hash_table *ht, uint32_t idx) {
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
			if (dis_entry->pba == PBA_INVALID) {
				fill_entry(dis_entry, dis_off, entry->key_fp_tag, entry->kv_size, entry->pba);
				return offset;
			}
		}
		--offset;
	}

	// error
	puts("\n@@@ insert error point! @@@\n");
	return -1;
}

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

	switch (req->type) {
	case REQ_TYPE_GET:
		if (keycmp_k_v(key, value) == 0) {
			params->lookup_step = HOP_STEP_KEY_MATCH;
			req->end_req(req);
			return NULL;
		} else {
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

	hash_t h_key = req->key.hash_low;

	uint32_t idx = h_key & ((1 << IDX_BIT)-1);
	uint8_t  tag = (uint8_t)(h_key >> IDX_BIT);
	uint8_t  dir = tag & ((1 << DIR_BIT)-1);

	struct hash_table *ht = &hs->table[dir];
	struct hash_entry *entry = NULL;
	struct callback *cb = NULL;

	if (!req->params) req->params = make_hop_params();
	struct hop_params *params = (struct hop_params *)req->params;

	if (strncmp(req->key.key, "user765417060341", 16) == 0) {
		puts("here");
	}

	switch (params->insert_step) {
	case HOP_STEP_KEY_MATCH:
		offset = params->offset;
		goto hop_insert_key_match;
		break;
	case HOP_STEP_KEY_MISMATCH:
		offset = params->offset + 1;
		goto hop_insert_key_mismatch;
		break;
	default:
		break;
	}

hop_insert_key_mismatch:
	// phase 1: find matching tag
	offset = find_matching_tag(ht, idx, offset, tag);
	if (offset != -1) {
		// read kv-pair to confirm whether it is correct key or not
		entry = &ht->entry[(idx+offset)%NR_ENTRY];
		cb = make_callback(hlr, cb_keycmp, req);
		params->offset = offset;
		hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb);
		goto exit;
	}

	// phase 2: find free entry
	offset = find_free_entry(ht, idx);
	if (offset == -1) {
		// error: must resize the table!
		fprintf(stderr, "Insert error: Need to resize the table");
		abort();
	}

hop_insert_key_match:
	// phase 3: fill the entry
	pba = get_next_pba(hlr, req->value.len);

	entry = fill_entry(&ht->entry[(idx+offset)%NR_ENTRY], offset, tag, req->value.len/SOB, pba);

	cb = make_callback(hlr, req->end_req, req);
	copy_key_to_value(&req->key, &req->value);
	hlr->write(hlr, entry->pba, entry->kv_size, req->value.value, cb);

exit:
	return 0;
}


#if 0
static void collect_lookup_cost(struct hopscotch *hs, int nr_read) {
	if (nr_read < FLASH_READ_MAX) hs->lookup_cost[nr_read]++;
}
#endif

int hopscotch_get(struct kv_ops *ops, struct request *req) {
	int rc = 0;

	struct hopscotch *hs = (struct hopscotch *)ops->_private;
	struct handler *hlr = req->hlr;
	int offset = 0;

	hash_t h_key = req->key.hash_low;

	uint32_t idx = h_key & ((1 << IDX_BIT)-1);
	uint8_t  tag = (uint8_t)(h_key >> IDX_BIT);
	uint8_t  dir = tag & ((1 << DIR_BIT)-1);

	struct hash_table *ht = &hs->table[dir];
	struct hash_entry *entry = NULL;
	struct callback *cb = NULL;

	if (!req->params) req->params = make_hop_params();
	struct hop_params *params = (struct hop_params *)req->params;

	if (strncmp(req->key.key, "user765417060341", 16) == 0) {
		puts("here");
	}

	switch (params->lookup_step) {
	case HOP_STEP_KEY_MISMATCH:
		offset = params->offset + 1;
		goto hop_lookup_key_mismatch;
		break;
	case HOP_STEP_INIT:
	case HOP_STEP_KEY_MATCH:
	default:
		break;
	}

hop_lookup_key_mismatch:
	offset = find_matching_tag(ht, idx, offset, tag);
	if (offset != -1) {
		entry = &ht->entry[(idx+offset)%NR_ENTRY];
		cb = make_callback(hlr, cb_keycmp, req);
		params->offset = offset;
		hlr->read(hlr, entry->pba, entry->kv_size, req->value.value, cb);
		goto exit;
	}

	// case of "not existing key"
	rc = 1;
exit:
	return rc;
}

int hopscotch_delete(struct kv_ops *ops, struct request *req) {
	// this would be implemented if necessary
	return 0;
}
