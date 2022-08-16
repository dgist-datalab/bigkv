#ifndef __H_TIMES__
#define __H_TIMES__

#include <stdint.h>
#include <time.h>

#define TTL_BIT_ONE 12
#define TTL_BIT_TWO (TTL_BIT_ONE * 16)
#define TTL_BIT_THREE (TTL_BIT_TWO * 16)
#define TTL_BIT_FOUR (TTL_BIT_THREE * 16)
#define TTL_BIT_FIVE (TTL_BIT_FOUR * 2)

uint64_t get_cur_sec(void);
void set_cur_sec(uint64_t);
uint64_t get_cur_min(void);
uint64_t get_rel_min(uint64_t min);
uint64_t pack_min_to_bits(uint32_t min);
uint32_t unpack_ttl_to_min(uint64_t bits);
uint64_t get_bits_from_ttl(uint64_t base, uint32_t ttl);
uint32_t get_expected_expiration(uint64_t base, uint64_t bits);
int is_expired_ttl(uint64_t base, uint64_t bits);
uint64_t update_bits_from_new_base(uint64_t new_base, uint64_t old_base, uint64_t bits);
#endif
