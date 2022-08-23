#include "platform/request.h"
#include "platform/redis.h"
#include "platform/dev_spdk.h"
#include "index/cascade.h"
#include <stdlib.h>
#include <string.h>


extern bool fault_occurs;
extern bool repaired;
extern bool re_activated;

stopwatch *sw_send, *sw_free;
time_t t_send, t_free;

stopwatch sw_value;
time_t t_value;

extern queue *ack_q;

int set_value(struct val_struct *value, int len, char *input_val, int is_read, struct handler *hlr) {
	int rc = 0;

	value->len = len;
	if (!value->value) {
		if (is_read) {
			value->value = (char *)q_dequeue(hlr->value_pool);
		} else {
#ifdef DEV_SPDK
			value->value = (char *)spdk_dma_malloc(value->len, VALUE_ALIGN_UNIT, NULL);
#else
			value->value = (char *)aligned_alloc(VALUE_ALIGN_UNIT, value->len);
#endif
		}
	}
	if (!value->value) {
		perror("allocating value");
		abort();
	}
	if (input_val) {
		memcpy(value->value, input_val, value->len);
	} else {
		memset(value->value, 0, 512);
	}
	return rc;
}

struct request *
make_request_from_trace(struct handler *hlr) {
	uint128 hash128;
	struct request *req;
	while ((req = (struct request *)q_dequeue(hlr->req_pool)) == NULL);
	memset(req, 0, sizeof(*req));

	req->seq_num = 0;

	req->value.value = NULL;

	req->hlr = hlr;
	

	req->end_req = trace_end_req;
	req->params = NULL;
	req->temp_buf = NULL;


	//sw_start(&req->sw);

	return req;
}

struct request *
make_request_from_redis(struct handler *hlr, struct client *cli, int sock, req_type_t type) {
	uint128 hash128;
	struct request *req;
	while ((req = (struct request *)q_dequeue(hlr->req_pool)) == NULL);
	memset(req, 0, sizeof(*req));

	req->type = type;
	req->seq_num = 0;

	// args[1]: key
	req->key.len = cli->args_size[1];
	memcpy(req->key.key, cli->args[1], req->key.len);
	hash128 = hashing_key_128(req->key.key, req->key.len);
	req->key.hash_high = hash128.first;
	req->key.hash_low = hash128.second;

	switch (req->type) {
	case REQ_TYPE_GET:
		break;
	case REQ_TYPE_SET:
#ifndef TTL
		req->sec = 0;
#else
#ifdef RAND_TTL
		req->sec = (uint32_t)((double)rand()/((double)RAND_MAX + 1) * 43200) + 1;
#endif
#endif
		req->value.len = cli->args_size[3];
		break;
	case REQ_TYPE_DELETE:
	case REQ_TYPE_RANGE:
	case REQ_TYPE_ITERATOR:
	default:
		break;
	}

	req->value.value = NULL;

	req->hlr = hlr;
	
	req->cl_sock = sock;

	req->end_req = redis_end_req;
	req->params = NULL;
	req->temp_buf = NULL;

	//sw_start(&req->sw);

	return req;
}

struct request *
make_request_from_netreq(struct handler *hlr, struct netreq *nr, int sock) {
	struct request *req;
	while ((req = (struct request *)q_dequeue(hlr->req_pool)) == NULL);
	memset(req, 0, sizeof(*req));

	req->type = nr->type;
	req->seq_num = nr->seq_num;

	req->key.len = nr->keylen;
	memcpy(req->key.key, nr->key, req->key.len);

#ifndef TTL
	req->sec = 0;
#else
#ifdef RAND_TTL
	req->sec = (uint32_t)((double)rand()/((double)RAND_MAX + 1) * 43200) + 1;
#endif
#endif


	req->value.len = nr->kv_size;
	req->value.value = NULL;

	req->hlr = hlr;

	req->cl_sock = sock;

	//sw_start(&req->sw);

#ifdef BREAKDOWN
	if (req->type == REQ_TYPE_GET) {
		for (int i = 0; i < 10; i++)
			req->sw_bd[i] = sw_create();
		sw_start(req->sw_bd[0]);
		sw_start(req->sw_bd[1]);
	}
#endif
		

	return req;
}

void
add_request_info(struct request *req) {
	uint128 hash128 = hashing_key_128(req->key.key, req->key.len);
	req->key.hash_high = hash128.first;
	req->key.hash_low = hash128.second;

	switch (req->type) {
	case REQ_TYPE_GET:
		set_value(&req->value, VALUE_LEN_MAX, NULL, 1, req->hlr);
		break;
	case REQ_TYPE_SET:
		set_value(&req->value, req->value.len, NULL, 0, req->hlr);
		break;
	case REQ_TYPE_DELETE:
	case REQ_TYPE_RANGE:
	case REQ_TYPE_ITERATOR:
	default:
		break;
	}
#ifdef REDIS
	req->end_req = redis_end_req;
#elif TRACE
	req->end_req = trace_end_req;
#else
	req->end_req = net_end_req;
#endif
	req->params = NULL;
	req->temp_buf = NULL;
}

void *net_end_req(void *_req) {
	struct request *req = (struct request *)_req;
	struct handler *hlr = req->hlr;
	struct netack ack;
	time_t elapsed_time;

	sw_end(&req->sw);
	ack.seq_num = req->seq_num; // TODO
	ack.type = req->type;
	ack.elapsed_time = sw_get_usec(&req->sw);

	elapsed_time = sw_get_usec(&req->sw);
	
	switch (req->type) {
	case REQ_TYPE_GET:
#ifdef CDF
		collect_latency(hlr->cdf_table, elapsed_time);
#endif
#ifdef AMF_CDF
		collect_io_bytes(hlr->amf_bytes_cdf_table, req->meta_lookup_bytes);
		collect_io_count(hlr->amf_cdf_table, req->meta_lookups);
#endif
		hlr->nr_query++;
		break;
	case REQ_TYPE_SET:
		break;
	case REQ_TYPE_DELETE:
	case REQ_TYPE_RANGE:
	case REQ_TYPE_ITERATOR:
	default:
		break;
	}

	send_ack(req->cl_sock, &ack);

	cl_release(hlr->flying);

#ifdef BREAKDOWN
	if (req->type == REQ_TYPE_GET) {
		sw_end(req->sw_bd[0]);
		for (int i = 0; i < 10; i++) {
			hlr->sw_total[i] += sw_get_usec(req->sw_bd[i]);
			sw_destroy(req->sw_bd[i]);
		}
	}
#endif


	//sw_start(sw_free);
	if (req->params) free(req->params);
	if (req->type == REQ_TYPE_GET) {
		q_enqueue((void *)req->value.value, hlr->value_pool);
	} else {
#ifdef DEV_SPDK
		spdk_dma_free(req->value.value);
#else
		free(req->value.value);
#endif
	}
	q_enqueue((void *)req, hlr->req_pool);
	//sw_end(sw_free);

	return NULL;
}

void *redis_end_req(void *_req) {
	struct request *req = (struct request *)_req;
	struct handler *hlr = req->hlr;
	time_t elapsed_time;

	static int move_cnt = 0;
	sw_end(&req->sw);
	elapsed_time = sw_get_usec(&req->sw);
	
	switch (req->type) {
	case REQ_TYPE_GET:
#ifdef CDF
		collect_latency(hlr->cdf_table, elapsed_time);
#endif
#ifdef AMF_CDF
		collect_io_bytes(hlr->amf_bytes_cdf_table, req->meta_lookup_bytes);
		collect_io_count(hlr->amf_cdf_table, req->meta_lookups);
#endif
		hlr->nr_query++;
		redis_write_empty_array(req->cl_sock);
		if (req->rc == 1) {
			//NOT found!!
		} else {
			if (repaired && !re_activated) {
				if ((req->hlr->number != FAULT_HLR) && req->fault && req->repaired) {
					move_req_to_other(req->ori_hlr, req);
					if ((++move_cnt % 10000) == 0)
						printf("MOVING %d\n", move_cnt);
					if (move_cnt == 1000000) {
						printf("REACTIVE\n");
						re_activated = 1;
					}
				}
			}
			//found!!
		}
		break;
	case REQ_TYPE_SET:
		if (!req->moved)
			redis_write_ok(req->cl_sock);
		break;
	case REQ_TYPE_DELETE:
	case REQ_TYPE_RANGE:
	case REQ_TYPE_ITERATOR:
	default:
		break;
	}

	cl_release(hlr->flying);

	//sw_start(sw_free);
	if (req->params) free(req->params);
	if (req->type == REQ_TYPE_GET) {
		q_enqueue((void *)req->value.value, hlr->value_pool);
	} else {
#ifdef DEV_SPDK
		spdk_dma_free(req->value.value);
#else
		free(req->value.value);
#endif
	}
	q_enqueue((void *)req, hlr->req_pool);
	//sw_end(sw_free);
	//t_free += sw_get_usec(sw_free);

	return NULL;
}

void *trace_end_req(void *_req) {
	struct request *req = (struct request *)_req;
	struct handler *hlr = req->hlr;
	time_t elapsed_time;

	sw_end(&req->sw);
	elapsed_time = sw_get_usec(&req->sw);
	
	switch (req->type) {
	case REQ_TYPE_GET:
#ifdef CDF
		collect_latency(hlr->cdf_table, elapsed_time);
#endif
#ifdef AMF_CDF
		collect_io_bytes(hlr->amf_bytes_cdf_table, req->meta_lookup_bytes);
		collect_io_count(hlr->amf_cdf_table, req->meta_lookups);
#endif
		hlr->nr_query++;
		break;
	case REQ_TYPE_SET:
		break;
	case REQ_TYPE_DELETE:
	case REQ_TYPE_RANGE:
	case REQ_TYPE_ITERATOR:
	default:
		break;
	}


	cl_release(hlr->flying);

	//sw_start(sw_free);
	if (req->params) {
		free(req->params);
		req->params = NULL;
	}
	if (req->type == REQ_TYPE_GET) {
		q_enqueue((void *)req->value.value, hlr->value_pool);
	} else if (req->type == REQ_TYPE_SET) {
#ifdef DEV_SPDK
		spdk_dma_free(req->value.value);
#else
		free(req->value.value);
#endif
	}
	q_enqueue((void *)req, hlr->req_pool);

	return NULL;
}
