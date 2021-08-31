/*
 * Key-value Operation Header
 *
 * Description: It's a header to define key-value operations. Member functions
 * of 'kv_ops' struct must be mapped at init time. Currently only 3 operations
 * are required, but other operations can be added to support some additional
 * functionalities.
 *
 */

#ifndef __KV_OPS_H__
#define __KV_OPS_H__

#include <stdio.h>
#include <stdint.h>
#include "platform/request.h"

struct kv_ops_stat {
	uint64_t get_kv_cnt;
	uint64_t set_kv_cnt;
	uint64_t delete_kv_cnt;
};

struct kv_ops {
	int (*init) (struct kv_ops *);
	int (*free) (struct kv_ops *);

	int (*get_kv) (struct kv_ops *, struct request *);
	int (*set_kv) (struct kv_ops *, struct request *);
	int (*delete_kv) (struct kv_ops *, struct request *);


	int (*need_gc) (struct kv_ops *, struct handler *hlr);
	int (*trigger_gc) (struct kv_ops *, struct handler *hlr);
	int (*wait_gc) (struct kv_ops *, struct handler *hlr);

	void *_private;
	struct kv_ops_stat stat;
};

static inline void 
print_kv_ops_stat(struct kv_ops_stat *stat) {
	printf("kv_ops-stat: %lu/%lu/%lu (GET/SET/DELETE)\n",
		stat->get_kv_cnt, stat->set_kv_cnt, stat->delete_kv_cnt);
}

#endif
