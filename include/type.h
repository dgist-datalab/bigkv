/*
 * A header to define types
 */

#ifndef __BIGKV_TYPE_H__
#define __BIGKV_TYPE_H__

#include <stdint.h>
#include <time.h>
#include "config.h"

typedef uint64_t hash_t;

enum seg_state_t:unsigned char {
	SEG_STATE_FREE,
	SEG_STATE_STAGED,
	SEG_STATE_USED,
};

enum req_type_t:unsigned char {
	REQ_TYPE_PADDING,
	REQ_TYPE_SET,
	REQ_TYPE_GET,
	REQ_TYPE_DELETE,
	REQ_TYPE_RANGE,
	REQ_TYPE_ITERATOR,
};

enum htable_t {
	HTABLE_HOPSCOTCH,
	HTABLE_BIGKV,
};

struct netreq {
	req_type_t type;
	uint8_t keylen;
	uint32_t seq_num;
	char key[KEY_LEN];
	uint32_t kv_size;
} __attribute__((packed));

struct netack {
	uint32_t seq_num;
	req_type_t type;
	time_t elapsed_time;
};

#endif
