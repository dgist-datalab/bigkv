/*
 * Request Header.
 *
 * Description: All requests are generated here. Each 'request' struct has its
 * own information, including key, value, elapsed time, ans so on.
 *
 */


#ifndef __REQUEST_H__
#define __REQUEST_H__

#include "config.h"
#include "type.h"
#include "platform/master.h"
#include "platform/handler.h"
#include "platform/util.h"
#include "utility/stopwatch.h"

struct key_struct {
	char		key[KEY_LEN_MAX];
	uint8_t		len;
	hash_t		hash_low, hash_high;
};

struct val_struct {
	char		*value;
	uint32_t	len;
};

struct request {
	req_type_t type;
	uint32_t seq_num;
	struct key_struct key;
	struct val_struct value;
	uint32_t sec;
	uint64_t req_time;

	stopwatch sw;
	stopwatch *sw_bd[10];

	void *(*end_req)(void *const);
	void *params;
	char *temp_buf;
	int rc;
	bool fault;
	bool repaired;
	bool re_active;
	int moved;

	uint64_t meta_lookups;
	uint64_t meta_lookup_bytes;
	uint64_t data_lookups;
	uint64_t data_lookup_bytes;

	struct handler *hlr;
	struct handler *ori_hlr;

	int cl_sock;
};

struct request *make_request_from_netreq(struct handler *hlr, struct netreq *nr, int sock);
struct request *make_request_from_redis(struct handler *hlr, struct client *cli, int sock, req_type_t type);
struct request *make_request_from_trace(struct handler *hlr);
void add_request_info(struct request *req);
void *net_end_req(void *_req);
void *redis_end_req(void *_req);
void *trace_end_req(void *_req);
int set_value(struct val_struct *value, int len, char *input_val, int is_read, struct handler *hlr);

#endif
