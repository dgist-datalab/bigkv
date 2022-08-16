#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "utility/ttl.h"

static uint64_t g_timestamp = 0;;

uint64_t get_cur_min(void) {
	uint64_t cur_min;
	time_t cur_sec;
	time(&cur_sec);
	cur_min = cur_sec / 60;
#ifdef RAND_TTL
	return cur_min;
#endif
	return g_timestamp / 60;
}

void set_cur_sec (uint64_t sec) {
	g_timestamp = sec;
}

uint64_t get_cur_sec (void) {
#ifdef RAND_TTL
	time_t cur_sec;
	time(&cur_sec);
	return cur_sec;
#endif
	return g_timestamp;
}

uint64_t get_rel_min(uint64_t min) {
	uint64_t cur_min = get_cur_min();
	return cur_min - min;
}


uint32_t base_times[5] = {TTL_BIT_ONE, TTL_BIT_TWO, TTL_BIT_THREE, TTL_BIT_FOUR, TTL_BIT_FIVE};

uint64_t pack_min_to_bits(uint32_t min) {
	uint32_t time_bits[6] = {0,};
	int i;
	for (i = 4; i >= 0; i--) {
		time_bits[i] = (min / base_times[i]);
		min = min % base_times[i];
	}
	if (min)
		time_bits[0]++;

	/*
	for (i = 5; i >= 0; i--) {
		printf("%d ", time_bits[i]);
	}
	printf("\n");
	*/


	for (i = 0; i < 5; i++) {
		if (time_bits[i] > 1) {
			time_bits[i+1]++;
			time_bits[i] = 0;
		}
	}

	if (time_bits[5])
		abort();

	uint64_t bits = 0;
	bits |= time_bits[4] << 4;
	bits |= time_bits[3] << 3;
	bits |= time_bits[2] << 2;
	bits |= time_bits[1] << 1;
	bits |= time_bits[0] << 0;

	/*
	for (i = 5; i >= 0; i--) {
		printf("%d ", time_bits[i]);
	}
	printf("\n");
	*/

	if (bits == 0)
		bits = 1;

	return bits;
}

uint32_t unpack_ttl_to_min(uint64_t bits) {
	uint32_t min = 0;

	min += ((bits >> 0) & 1) * TTL_BIT_ONE;
	min += ((bits >> 1) & 1) * TTL_BIT_TWO;
	min += ((bits >> 2) & 1) * TTL_BIT_THREE;
	min += ((bits >> 3) & 1) * TTL_BIT_FOUR;
	min += ((bits >> 4) & 1) * TTL_BIT_FIVE;

	return min;
}

uint64_t get_bits_from_ttl(uint64_t base, uint32_t ttl) {
#ifndef TTL
	return 0;
#endif
	uint64_t mttl = get_rel_min(base) + ttl;
	uint64_t bits = pack_min_to_bits(mttl);
	return bits;
}

uint32_t get_expected_expiration(uint64_t base, uint64_t bits) {
	return base + unpack_ttl_to_min(bits);
}

int is_expired_ttl(uint64_t base, uint64_t bits) {
	uint32_t min = get_expected_expiration(base, bits);
	static uint64_t count = 0;
#ifndef TTL
	return 0;
#endif
	if (get_cur_min() < min)
		return 0;
	if ((++count%1000) == 0)
		printf("TTL 5bit: %lu\n", count);
	return 1;
}

uint64_t update_bits_from_new_base(uint64_t new_base, uint64_t old_base, uint64_t bits) {
	uint64_t expected_min = get_expected_expiration(old_base, bits);
	uint64_t new_ttl = expected_min - new_base;
	uint64_t new_bits = pack_min_to_bits(new_ttl);
	return new_bits;
}

#if 0
int main() {
	uint64_t cur = get_cur_min(), mcur, res;
	uint32_t ttl;
	mcur = cur - 10;
	ttl = 5;
	res = get_bits_from_ttl(mcur, ttl);
	printf("1. mcur: %lu, ttl: %u, 5bits: %lu, exact: %lu, expected: %u\n", mcur, ttl + 10, res, mcur + ttl, get_expected_expiration(mcur, res));

	mcur = cur - 100;
	ttl = 120;
	res = get_bits_from_ttl(mcur, ttl);
	printf("2. mcur: %lu, ttl: %u, 5bits: %lu, exact: %lu, expected: %u\n", mcur, ttl + 100, res, mcur + ttl, get_expected_expiration(mcur, res));

	mcur = cur - 10;
	ttl = 1440;
	res = get_bits_from_ttl(mcur, ttl);
	printf("3. mcur: %lu, ttl: %u, 5bits: %lu, exact: %lu, expected: %u\n", mcur, ttl + 10, res, mcur + ttl, get_expected_expiration(mcur, res));

	mcur = cur - 10;
	ttl = 10080;
	res = get_bits_from_ttl(mcur, ttl);
	printf("4. mcur: %lu, ttl: %u, 5bits: %lu, exact: %lu, expected: %u\n", mcur, ttl + 10, res, mcur + ttl, get_expected_expiration(mcur, res));

	mcur = cur - 1000;
	ttl = 4021;
	res = get_bits_from_ttl(mcur, ttl);
	printf("5. mcur: %lu, ttl: %u, 5bits: %lu, exact: %lu, expected: %u\n", mcur, ttl + 1000, res, mcur + ttl, get_expected_expiration(mcur, res));

	mcur = cur - 43200;
	ttl = 43200;
	res = get_bits_from_ttl(mcur, ttl);
	printf("6. mcur: %lu, ttl: %u, 5bits: %lu, exact: %lu, expected: %u\n", mcur, ttl + 43200, res, mcur + ttl, get_expected_expiration(mcur, res));

	return 0;
}
#endif
