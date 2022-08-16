/*
 * Key Generator Header
 *
 * Description: A 'keygen struct' holds information of generator. The generator
 * is able to create key sets depending on a key distribution setting.
 * Generated keys are maintained on memory as a string format, but there could
 * be some ways to improve this inefficiency.
 *
 */

#ifndef __KEYGEN_H__
#define __KEYGEN_H__

#include <stdint.h>
#include <pthread.h>

typedef char* kg_key_t;

enum key_dist_t {
	KEY_DIST_UNIFORM,
	KEY_DIST_LOCALITY,
	KEY_DIST_HOTSPOT, // FIXME: this is not implemented now
};

struct keygen {
	kg_key_t *key_pool;

	uint64_t nr_key;
	uint64_t nr_eff_key;
	int key_size;
	key_dist_t key_dist;
	int32_t query_ratio;
	int32_t hotset_ratio;
	int32_t insert_ratio;
	int32_t eff_ratio;
	uint64_t start_hotset;
	uint64_t nr_hotset;
	uint64_t start_coldset;
	uint64_t nr_coldset;
	uint64_t nr_insertset;
	uint64_t start_insertset;
	uint32_t seed;

	uint64_t load_cnt;

	pthread_mutex_t seq_lock;
};

struct keygen *keygen_init(uint64_t nr_key, int key_size, const char username[]);
int keygen_free(struct keygen *kg);

int set_key_dist
(struct keygen *kg, key_dist_t dist, int query_ratio, int hotset_ratio);

void set_eff_kg (struct keygen *kg, int32_t eff_ratio, int32_t insert_ratio);
void move_hotset_area (struct keygen *kg, int32_t move_ratio);
kg_key_t get_next_key_for_moving(struct keygen *kg, char req_type);

kg_key_t get_next_key(struct keygen *kg);
kg_key_t get_next_key_for_load(struct keygen *kg);

#endif
