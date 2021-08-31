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

	stopwatch sw;

	void *(*end_req)(void *const);
	void *params;
	char *temp_buf;

	struct handler *hlr;

	int cl_sock;
};

struct request *make_request_from_netreq(struct handler *hlr, struct netreq *nr, int sock);
struct request *make_request_from_redis(struct handler *hlr, struct client *cli, int sock, req_type_t type);
void add_request_info(struct request *req);
void *net_end_req(void *_req);
void *redis_end_req(void *_req);

#endif
