/*
 * Master Thread Header
 *
 * Description: A master thread works for request delivery. A master thread
 * should be located to a NIC. Its main role is to accept network requests and
 * convert it to internal key-value requests.
 *
 * Current network back-end is implemented as epoll(), but this would be
 * overheads due to the OS network stack. Since almost high-performance NICs
 * support DPDK, it could be improved by changing it to DPDK.
 *
 */

#ifndef __BIGKV_MASTER_H__
#define __BIGKV_MASTER_H__

#include "type.h"
#include "config.h"
#include "platform/handler.h"
#include "platform/client.h"

#include <time.h>
#include <pthread.h>

#define MAX_NETREQ_BUF 1024
#define SOCK_BUF_SIZE (1024 * 1024 * 2L)
#define MASTER_SOCK_MOVE (1024 * 16L)
#define CLIENT_MAX_NUM 128



struct master {
	pthread_t tid;

	int num_dev;
	char **device;

	struct handler *hlr[32];

	int fd;
	int epfd;

	struct netreq netreq_buf[MAX_NETREQ_BUF];
	struct client *clients[128];
	/*
	char *buf;
	ssize_t buf_start;
	ssize_t buf_end;
	ssize_t buf_len;
	ssize_t buf_size;

	char *tmp_err;
	const char **args;
	int* args_size;
	int args_len;
	int args_cap;
	*/

	int processed_req;
#ifdef CDF
	uint64_t *cdf_table;
#endif

	int (*parse_and_make_request)(struct master *mas, int fd);
};


struct master *master_init(int num_dev, char **device);
void master_free(struct master *mas);
void *master_thread(void *input);
int master_parse_and_make_request(struct master *mas, int fd);

#endif
