#include "config.h"
#include "city.h"
#include "platform/util.h"

#include <string.h>
#include <unistd.h>
#include <stdio.h>

//#define SEND_ACK_BUFFERING
#define ACK_BUF_MAX 64

uint64_t hashing_key(char *key, uint8_t len) {
	return CityHash64(key, len);
}

uint128 hashing_key_128(char *key, uint8_t len) {
	return CityHash128(key,len);
}

ssize_t read_sock(int sock, void *buf, ssize_t count) {
	ssize_t readed = 0, len;
	while (readed < count) {
		len = read(sock, &(((char *)buf)[readed]), count-readed);
		if (len == -1) {
			if (readed == 0) return -1;
			else continue;
		} else if (len == 0) {
			if (readed > 0) {
				fprintf(stderr, "sock buf remain!\n");
			}
			return 0;
		}
		readed += len;
	}
	return readed;
}

int num_syscalls;

ssize_t read_sock_bulk(int sock, void *buf, ssize_t max_objs, ssize_t align) {
	ssize_t readed = 0, len;
	ssize_t max_count = align * max_objs;
	while (readed < max_count) {
		len = read(sock, &(((char *)buf)[readed]), max_count-readed);
		if (len == -1) {
			if (readed == 0) return -1;
			else continue;
		} else if (len == 0) {
			if (readed > 0) {
				fprintf(stderr, "sock buf remain!\n");
			}
			return 0;
		}
		readed += len;

		if (readed % align == 0) break;
	}
	return readed;
}


ssize_t send_request(int sock, struct netreq *nr) {
	return write(sock, nr, sizeof(struct netreq));
}

ssize_t send_request_bulk(int sock, struct netreq *nr_arr, int count) {
	return write(sock, nr_arr, sizeof(struct netreq) * count);
}

ssize_t recv_request(int sock, struct netreq *nr) {
	return read_sock(sock, nr, sizeof(struct netreq));
}

#ifdef SEND_ACK_BUFFERING
#ifdef YCSB
__thread uint32_t ack_buf[ACK_BUF_MAX];
#else
__thread struct netack ack_buf[ACK_BUF_MAX];
#endif
__thread int buf_cnt = 0;
#endif

#ifdef SEND_ACK_BUFFERING
ssize_t ack_buf_flush(int sock) {
	ssize_t len = write(sock, ack_buf, sizeof(ack_buf[0]) * buf_cnt);
	buf_cnt = 0;
	return len;
}
#endif

ssize_t send_ack(int sock, struct netack *na) {

#ifdef SEND_ACK_BUFFERING
	if (na->type == REQ_TYPE_GET) {
#ifdef YCSB
		return write(sock, &na->seq_num, sizeof(uint32_t));
#else
		return write(sock, na, sizeof(struct netack));
#endif
	}

#ifdef YCSB
	ack_buf[buf_cnt++] = na->seq_num;
#else
	ack_buf[buf_cnt++] = *na;
#endif
	if (buf_cnt == ACK_BUF_MAX) {
		return ack_buf_flush(sock);
	} else {
		return 0;
	}
#else

#ifdef YCSB
	return write(sock, &na->seq_num, sizeof(uint32_t));
#else
	return write(sock, na, sizeof(struct netack));
#endif

#endif
}

ssize_t recv_ack(int sock, struct netack *na) {
	return read_sock(sock, na, sizeof(struct netack));
}

void collect_latency(uint64_t table[], time_t latency) {
	if (latency != -1) table[latency/10]++;
}

void print_cdf(uint64_t table[], uint64_t nr_query) {
	uint64_t cdf_sum = 0;
	printf("#latency,cnt,cdf\n");
	for (int i = 0; i < CDF_TABLE_MAX && cdf_sum != nr_query; i++) {
		if (table[i] != 0) {
			cdf_sum += table[i];
			printf("%d,%lu,%.6f\n",
				i*10, table[i], (float)cdf_sum/nr_query);
			table[i] = 0;
		}
	}
}

int req_in(sem_t *sem_lock) {
	return sem_wait(sem_lock);
}

int req_out(sem_t *sem_lock) {
	return sem_post(sem_lock);
}

void wait_until_finish(sem_t *sem_lock, int qdepth) {
	int nr_finished_reqs = 0;
	while (nr_finished_reqs != qdepth) {
		sleep(1);
		sem_getvalue(sem_lock, &nr_finished_reqs);
	}
}
