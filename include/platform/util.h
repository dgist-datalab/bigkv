/*
 * Header for utility functions
 */

#ifndef __DFHASH_UTIL_H__
#define __DFHASH_UTIL_H__

#include "config.h"
#include "type.h"
#include "master.h"
#include <stdint.h>
#include <time.h>
#include <sys/types.h>
#include <semaphore.h>
#include <city.h>

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
uint128 hashing_key_128(char *key, uint8_t len);

ssize_t read_sock(int sock, void *buf, ssize_t count);
ssize_t read_sock_bulk(int sock, void *buf, ssize_t count, ssize_t align);

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
