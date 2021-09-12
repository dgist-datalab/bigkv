#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#define XXH_INLINE_ALL
#include "xxhash.h"

uint64_t hashing_key(char *key, uint8_t len) {
	return XXH3_64bits(key, len);
}

XXH128_hash_t hashing_key_128(char *key, uint8_t len) {
	return XXH3_128bits(key,len);
}
