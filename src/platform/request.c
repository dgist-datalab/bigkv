#include "platform/request.h"
#include <stdlib.h>
#include <string.h>

stopwatch *sw_send, *sw_free;
time_t t_send, t_free;

stopwatch sw_value;
time_t t_value;

extern queue *ack_q;

static int set_value(struct val_struct *value, int len, char *input_val) {
	int rc = 0;

	value->len = len;
#ifdef LINUX_AIO
	value->value = (char *)aligned_alloc(VALUE_ALIGN_UNIT, value->len);
	if (!value->value) {
		perror("allocating value");
		abort();
	}
	if (input_val) {
		memcpy(value->value, input_val, value->len);
	} else {
		memset(value->value, 0, value->len);
	}
#elif SPDK
	// TODO: implement SPDK allocation
#endif
	return rc;
}

struct request *
make_request_from_netreq(struct handler *hlr, struct netreq *nr, int sock) {
	//struct request *req = (struct request *)malloc(sizeof (struct request));
	struct request *req = (struct request *)q_dequeue(hlr->req_pool);

	req->type = nr->type;
	req->seq_num = nr->seq_num;

	req->key.len = nr->keylen;
	memcpy(req->key.key, nr->key, req->key.len);

	req->value.len = nr->kv_size;
	req->value.value = NULL;

	req->hlr = hlr;

	req->cl_sock = sock;

	sw_start(&req->sw);

	return req;
}

void
add_request_info(struct request *req) {
	uint128 hash128 = hashing_key_128(req->key.key, req->key.len);
	req->key.hash_low = hash128.first;
	req->key.hash_high = hash128.second;

	switch (req->type) {
	case REQ_TYPE_GET:
		set_value(&req->value, VALUE_LEN_MAX, NULL);
		break;
	case REQ_TYPE_SET:
		set_value(&req->value, req->value.len, NULL);
		break;
	case REQ_TYPE_DELETE:
	case REQ_TYPE_RANGE:
	case REQ_TYPE_ITERATOR:
	default:
		break;
	}

	req->end_req = net_end_req;
	req->params = NULL;
	req->temp_buf = NULL;
}

void *net_end_req(void *_req) {
	struct request *req = (struct request *)_req;
	struct handler *hlr = req->hlr;
	//struct netack *ack = (struct netack *)malloc(sizeof(struct netack));
	struct netack ack;

	sw_end(&req->sw);
	ack.seq_num = req->seq_num; // TODO
	ack.type = req->type;
	ack.elapsed_time = sw_get_usec(&req->sw);

	send_ack(req->cl_sock, &ack);
	//q_enqueue((void *)ack, ack_q);

	cl_release(hlr->flying);

	//sw_start(sw_free);
	if (req->params) free(req->params);
	free(req->value.value);
	q_enqueue((void *)req, hlr->req_pool);
	//sw_end(sw_free);
	//t_free += sw_get_usec(sw_free);

	return NULL;
}
