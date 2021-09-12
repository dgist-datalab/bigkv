/*
 * Header for utility functions
 */

#ifndef __DFHASH_UTIL_H__
#define __DFHASH_UTIL_H__

#include "config.h"
#include "type.h"
#include "master.h"
#include "xxhash.h"

#include <stdint.h>
#include <time.h>
#include <sys/types.h>
#include <semaphore.h>

typedef unsigned __int128 uint128_t;

/*struct net_req {
	req_type_t type;
	uint8_t keylen;
	uint32_t seq_num;
	char key[KEY_LEN];
	uint32_t kv_size;
} __attribute__((packed));

struct net_ack {
	uint32_t seq_num;
	req_type_t type;
	time_t elapsed_time;
}; */

uint64_t hashing_key(char *key, uint8_t len);
XXH128_hash_t hashing_key_128(char *key, uint8_t len);

ssize_t read_sock(int sock, void *buf, ssize_t count);
ssize_t read_sock_bulk(int sock, void *buf, ssize_t count, ssize_t align);
ssize_t read_sock_bulk_circular(int sock, void *buf, ssize_t buf_size, ssize_t buf_start, ssize_t buf_end);
static inline ssize_t move_circular_buf(void *buf, ssize_t buf_size, ssize_t buf_start, ssize_t buf_len) {
	ssize_t buf_end = buf_start + buf_len;
	if (buf_size - buf_end <= MASTER_SOCK_MOVE) {
		memcpy(buf, ((char *)buf + buf_start), buf_len);
		return buf_len;
	}
	return 0;
}

ssize_t send_request(int sock, struct netreq *nr);
ssize_t send_request_bulk(int sock, struct netreq *nr_arr, int count);
ssize_t recv_request(int sock, struct netreq *nr);
ssize_t send_ack(int sock, struct netack *na);
ssize_t recv_ack(int sock, struct netack *na);

#ifdef SEND_ACK_BUFFERING
ssize_t ack_buf_flush(int sock);
#endif

void collect_latency(uint64_t table[], time_t latency);
void print_cdf(uint64_t table[], uint64_t nr_query);

int req_in(sem_t *sem_lock);
int req_out(sem_t *sem_lock);
void wait_until_finish(sem_t *sem_lock, int qdepth);

#endif
