/*
 * Key Generator Implementation
 */

#include "platform/keygen.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct keygen *keygen_init(uint64_t nr_key, int key_size) {
	struct keygen *kg = (struct keygen *)malloc(sizeof(struct keygen));

	kg->nr_key = nr_key;
	kg->key_size = key_size;

	kg->key_dist = KEY_DIST_UNIFORM;
	kg->query_ratio = 50;
	kg->hotset_ratio = 50;
	kg->nr_hotset = kg->nr_key / 100 * kg->hotset_ratio;
	kg->nr_coldset = kg->nr_key - kg->nr_hotset;

	kg->seed = 0;
	srand(kg->seed);

	kg->load_cnt = 0;

	kg->key_pool = (kg_key_t *)malloc(sizeof(kg_key_t) * kg->nr_key);
	for (size_t i = 0; i < kg->nr_key; i++) {
		kg->key_pool[i] = (kg_key_t)malloc(kg->key_size);
		strncpy(kg->key_pool[i], "user", 4);
		for (int j = 4; j < kg->key_size; j++) kg->key_pool[i][j] = '0'+(rand()%10);
	}

	pthread_mutex_init(&kg->seq_lock, NULL);

	return kg;
}

int keygen_free(struct keygen *kg) {
	for (size_t i = 0; i < kg->nr_key; i++) {
		free(kg->key_pool[i]);
	}
	free(kg->key_pool);
	free(kg);
	return 0;
}

static void shuffle_key_pool(struct keygen *kg) {
	kg_key_t temp_key_ptr = NULL;
	for (size_t i = 0; i < kg->nr_key; i++) {
		int pos = rand() % kg->nr_key;
		temp_key_ptr = kg->key_pool[i];
		kg->key_pool[i] = kg->key_pool[pos];
		kg->key_pool[pos] = temp_key_ptr;
	}
}

int set_key_dist
(struct keygen *kg, key_dist_t dist, int query_ratio, int hotset_ratio) {
	kg->key_dist = dist;
	switch (kg->key_dist) {
	case KEY_DIST_UNIFORM:
		printf("Key distribution setting: KEY_DIST_UNIFORM\n");
		break;
	case KEY_DIST_LOCALITY:
		printf("Key distribution setting: KEY_DIST_LOCALITY, %d:%d\n",
			query_ratio, hotset_ratio);
		shuffle_key_pool(kg);
		shuffle_key_pool(kg);
		shuffle_key_pool(kg);

		kg->query_ratio = query_ratio;
		kg->hotset_ratio = hotset_ratio;
		kg->nr_hotset = kg->nr_key/100*kg->hotset_ratio;
		kg->nr_coldset = kg->nr_key - kg->nr_hotset;
		break;
	default:
		fprintf(stderr, "%s: Wrong key distribution\n", __FUNCTION__);
		break;
	}
	return 0;
}

kg_key_t get_next_key_for_load(struct keygen *kg) {
	if (kg->load_cnt >= kg->nr_key) {
		fprintf(stderr, "Loading key overflow!\n");
		return NULL;
	} else {
		kg_key_t return_key;
		pthread_mutex_lock(&kg->seq_lock);
		return_key = kg->key_pool[kg->load_cnt++];
		pthread_mutex_unlock(&kg->seq_lock);
		return return_key;
	}
}

kg_key_t get_next_key(struct keygen *kg) {
	switch (kg->key_dist) {
	case KEY_DIST_UNIFORM:
		return kg->key_pool[rand()%kg->nr_key];
	case KEY_DIST_LOCALITY:
		if (rand()%100 < kg->query_ratio) { // hot
			return kg->key_pool[rand()%kg->nr_hotset];
		} else { // cold
			return kg->key_pool[
				kg->nr_hotset+(rand()%kg->nr_coldset)];
		}
		break;
	default:
		fprintf(stderr, "Wrong key-distribution!\n");
	}
	return NULL;
}
