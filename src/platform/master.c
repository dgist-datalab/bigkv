#include "platform/master.h"
#include "platform/util.h"
#include "platform/request.h"
#include "platform/redis.h"
#include "platform/dev_spdk.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <signal.h>
#include <errno.h>

#define MAX_EVENTS 1024

int first = 0;
int dev_cap = 0;

bool fault_occurs = false;
bool repaired = false;
bool re_activated = false;
uint64_t re_active_num = 0;
uint64_t G_count = 0;

stopwatch *mas_sw;
stopwatch *print_sw;

static int __socket_init() {
	int fd;
	//int flags;
	int option;
	struct sockaddr_in sockaddr;

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd == -1) {
		perror("socket()");
		goto error;
	}

	// setting socket to non-blocking
	/*
	flags = fcntl(fd, F_GETFL);
	flags |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0) {
		perror("fcntl()");
		goto close_fd;
	}
	*/

	// setting socket to enable reusing address and port
	option = true;
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option)) < 0) {
		perror("setsockopt()");
		goto close_fd;
	}

	memset(&sockaddr, 0, sizeof(sockaddr));
	sockaddr.sin_family      = AF_INET;
	sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); // FIXME: assign an address per master thread
	sockaddr.sin_port        = htons(PORT);

	if (bind(fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) == -1) {
		perror("bind()");
		goto close_fd;
	}

	if (listen(fd, 2048) == -1) {
		perror("listen()");
		goto close_fd;
	}

	return fd;

close_fd:
	close(fd);
error:
	return -1;
}

#ifndef TRACE
static int master_sock_init (struct master *mas) {
	mas->fd = __socket_init();

	mas->epfd = epoll_create1(0);
	if (mas->epfd < 0) {
		perror("epoll_create1()");
		return -1;
	}
	/*
	mas->buf = (char *)malloc(MASTER_BUF_SIZE);
	mas->buf_start = mas->buf_len = 0;
	mas->buf_size = MASTER_BUF_SIZE;
	mas->args_cap = mas->args_len = 0;
	*/
	for (int i = 0; i < CLIENT_MAX_NUM; i++) {
		mas->clients[i] = NULL;
	}

	struct epoll_event events;
	//events.events  = EPOLLIN | EPOLLET;
	events.events  = EPOLLIN;
	events.data.fd = mas->fd;

	if (epoll_ctl(mas->epfd, EPOLL_CTL_ADD, mas->fd, &events)) {
		perror("epoll_ctl()");
		close(mas->epfd);
		return -1;
	}

	return 0;
}
#else
static FILE *get_next_trace_fd (struct master *mas) {
	char file_path[100] = {0,};
	int cluster_num, file_idx;
	fclose(mas->trace_fd);
	if (mas->current_trace_idx < mas->traces[mas->current_trace_num].num_files) {
		cluster_num = mas->traces[mas->current_trace_num].cluster_num;
		file_idx = ++(mas->current_trace_idx);
	} else if (mas->current_trace_num < mas->num_traces - 1) {
		cluster_num = mas->traces[++(mas->current_trace_num)].cluster_num;
		file_idx = mas->current_trace_idx = 0;
	} else {
		// end
		mas->trace_fd = NULL;
		goto exit;
	}
	snprintf(file_path, 100, "/zfs/cluster%d/cluster%d.%d", cluster_num, cluster_num, file_idx);
	mas->trace_fd = fopen(file_path, "r");
	printf("\nTrace: %s is opened\n", file_path);

exit:
	return mas->trace_fd;
}

static void master_trace_close (struct master *mas) {
	if (mas->num_traces - 1 != mas->current_trace_num)
		abort();
}

static int master_trace_init (struct master *mas) {
	char file_path[100] = {0,};
	mas->current_trace_num = 0;
	mas->current_trace_idx = 0;
	snprintf(file_path, 100, "/zfs/cluster%d/cluster%d.%d", mas->traces[mas->current_trace_num].cluster_num, mas->traces[mas->current_trace_num].cluster_num, 0);
	mas->trace_fd = fopen(file_path, "r");
	printf("Trace: %s is opened\n", file_path);

	if (mas->trace_fd == NULL)
		return -1;

	return 0;
}
#endif

struct master *master_init(int num_hlr, int num_dev, char *device[], char *core_mask, int num_traces, struct trace *traces) {
	struct master *mas = (struct master *)calloc(1, sizeof(struct master));
	struct handler *hlrs = (struct handler *)malloc(sizeof(struct handler) * num_hlr);
	int num_dev_per_hlr = num_dev / num_hlr;

	// TODO: init master variables
	mas->num_hlr = num_hlr;
	mas->num_dev = num_dev;
	mas->device = device;
	strncpy(mas->core_mask, core_mask, strlen(core_mask));

#ifndef TRACE
	master_sock_init(mas);
#else
	mas->num_traces = num_traces;
	mas->traces = traces;
	master_trace_init(mas);
#endif

	pthread_barrier_init(&mas->barrier, NULL, num_hlr + 1);
	pthread_create(&mas->tid, NULL, &master_thread, mas);
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(first, &cpuset);
	pthread_setaffinity_np(mas->tid, sizeof(cpu_set_t), &cpuset);

#ifdef DEV_SPDK
	dev_spdk_env_init(mas->core_mask);
	mas->sctx = (struct spdk_ctx *)malloc(sizeof(struct spdk_ctx) * num_dev);
	for (int i = 0; i < num_dev; i++) {
		dev_spdk_open(mas->sctx + i, device[i]);
	}
#endif

	for (int i = 0; i < num_hlr; i++) {
		printf("HANDLER[%d] INIT\n", i);
		mas->hlr[i] = handler_init(mas, hlrs + i, device + i * num_dev_per_hlr, num_dev, num_dev_per_hlr, num_hlr);
	}

	mas->parse_and_make_request = master_parse_and_make_request;
	mas->processed_req = 0;

#ifdef CDF
	mas->cdf_table = (uint64_t *)malloc(CDF_TABLE_MAX * sizeof(uint64_t));
	memset(mas->cdf_table, 0, CDF_TABLE_MAX * sizeof (uint64_t));
#endif

#ifdef AMF_CDF
	mas->amf_cdf_table = (uint64_t *)malloc(CDF_TABLE_MAX * sizeof(uint64_t));
	memset(mas->amf_cdf_table, 0, CDF_TABLE_MAX * sizeof (uint64_t));
	mas->amf_bytes_cdf_table = (uint64_t *)malloc(CDF_TABLE_MAX * sizeof(uint64_t));
	memset(mas->amf_bytes_cdf_table, 0, CDF_TABLE_MAX * sizeof (uint64_t));
#endif

	return mas;

error:
	free(mas);
	return NULL;
}

void master_free(struct master *mas) {
	// TODO: free master variables
	for (int i = 0; i < mas->num_hlr; i++) {
		free(mas->hlr[i]);
	}
#ifndef TRACE
	close(mas->epfd);
#else
	master_trace_close(mas);
#endif
#ifdef DEV_SPDK
	free(mas->sctx);
#endif

#ifdef CDF
	free(mas->cdf_table);
#endif
#ifdef AMF_CDF
	free(mas->amf_cdf_table);
	free(mas->amf_bytes_cdf_table);
#endif

	free(mas);
}

void master_reset (struct master *mas) {
	for (int i = 0; i < mas->num_dev; i++) {
		for (int j = 0; j < CDF_TABLE_MAX; j++) {
#ifdef CDF
			mas->hlr[i]->cdf_table[j] = 0;
#endif
#ifdef AMF_CDF
			mas->hlr[i]->amf_cdf_table[j] = 0;
			mas->hlr[i]->amf_bytes_cdf_table[j] = 0;
#endif
		}
		memset(&mas->hlr[i]->stat, 0, sizeof(struct stats));
	}	

}


static int __accept_new_client(struct master *mas) {
	int fd;
	int socklen;
	struct sockaddr_in sockaddr;
	//int flags;
	struct client *cli;

	socklen = sizeof(sockaddr);
	fd = accept(mas->fd, (struct sockaddr *)&sockaddr, (socklen_t *)&socklen);
	if (fd < 0) {
		perror("accept()");
		goto error;
	}

	/*
	flags = fcntl(fd, F_GETFL);
	flags |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0) {
		perror("fcntl()");
		close(fd);
		goto error;
	}
	*/

	struct epoll_event events;
	//events.events  = EPOLLIN | EPOLLET;
	events.events  = EPOLLIN;
	events.data.fd = fd;

	if (epoll_ctl(mas->epfd, EPOLL_CTL_ADD, fd, &events) < 0) {
		perror("epoll_ctl()");
		close(fd);
		goto error;
	}

	if (mas->clients[fd]) {
		abort();
	} else {
		cli = client_init(fd);
		mas->clients[fd] = cli;
	}

	return fd;

error:
	return -1;
}

/*
static int __process_request(struct master *mas, int fd) {
	int rc = 0;
	int len;
	int n_obj;
	uint64_t hash_high;
	int hlr_idx;

	len = read_sock_bulk(fd, mas->netreq_buf, MAX_NETREQ_BUF, sizeof(struct netreq));
	if (len == -1) {
		goto exit;
	} else if (len == 0) {
		close(fd);
		epoll_ctl(mas->epfd, EPOLL_CTL_DEL, fd, NULL);
		printf("A client (fd:%d) is disconnected ...\n", fd);
		rc = 1;
		goto exit;
	}
	mas->buf_len += len;

	n_obj = len / sizeof(struct netreq);

	for (int i = 0; i < n_obj; i++) {
		// TODO: request handling
		struct netreq *nr = &mas->netreq_buf[i];
		hash_high = hashing_key_128(nr->key, nr->keylen).first;
		hlr_idx = (hash_high >> 32) % mas->num_dev;

		struct request *req = make_request_from_netreq(mas->hlr[hlr_idx], nr, fd);

		rc = forward_req_to_hlr(mas->hlr[hlr_idx], req);
		if (rc) {
			goto exit;
		}
	}

exit:
	return rc;
}
*/

static int __netreq_parse_and_make_request (struct master *mas, int fd) {
	uint64_t hash_high;
	int hlr_idx;
	int rc;
	struct client *cli = mas->clients[fd];
	int n_obj = cli->buf_len / sizeof(struct netreq);
	struct netreq *netreq_buf = ((struct netreq *)cli->buf) + (cli->buf_start / sizeof(struct netreq));

	
	for (int i = 0; i < n_obj; i++) {
		// TODO: request handling
		struct netreq *nr = netreq_buf + i;
		hash_high = hashing_key_128(nr->key, nr->keylen).first;
		hlr_idx = (hash_high >> 32) % mas->num_hlr;

		struct request *req = make_request_from_netreq(mas->hlr[hlr_idx], nr, fd);
#ifdef BREAKDOWN
		if (req->type == REQ_TYPE_GET)
			sw_start(req->sw_bd[6]);
#endif
		//printf("hash_high: %lu, hlr_idx: %lu\n", hash_high, hlr_idx);
		rc = forward_req_to_hlr(mas->hlr[hlr_idx], req);
#ifdef BREAKDOWN
		if (req->type == REQ_TYPE_GET)
			sw_end(req->sw_bd[6]);
#endif
		if (rc) {
			goto exit;
		}
	}
	cli->buf_start += n_obj * sizeof(struct netreq);
	cli->buf_len -= n_obj * sizeof(struct netreq);
	

exit:
	return rc;
}

static int __redis_parse_and_make_request (struct master *mas, int fd) {
	int rc;
	uint64_t hash_high;
	int hlr_idx;
	req_type_t type;
	struct client *cli = mas->clients[fd];


	bool fault_req = false;



	while (1) {
		fault_req = false;
		error err = redis_read_command(cli);
		//redis_print_args(mas);
		if (err != NULL) {
			if ((char *)err == (char *)ERR_INCOMPLETE) {
				rc = 0;
				goto exit;
			}
			return -1;
		}
		type = redis_convert_type(cli);
		if (type == REQ_TYPE_ITERATOR) { //FIXME
			rc = 0;
			goto exit;
		} else if (type == REQ_TYPE_UNKNOWN) {
			redis_print_args(cli);
		}

		// args[1]: key
		hash_high = hashing_key_128((char*)cli->args[1], cli->args_size[1]).first;
		hlr_idx = (hash_high >> 32) % mas->num_hlr;

		if (fault_occurs) {
			if (fault_occurs && !re_activated)
				if (hlr_idx == FAULT_HLR) {
					hlr_idx = (hash_high >> 32) % (mas->num_hlr-1);
					fault_req = true;
				}
		}

		struct request *req = make_request_from_redis(mas->hlr[hlr_idx], cli, fd, type);

		if (fault_req) {
			req->fault = fault_occurs;
			req->repaired = repaired;
			req->ori_hlr = mas->hlr[FAULT_HLR];
		}

		if (re_activated) {
			if (hlr_idx == FAULT_HLR) {
				req->re_active = 1;
				req->fault = 1;
			}
		}

		rc = forward_req_to_hlr(mas->hlr[hlr_idx], req);
		if (rc) {
			goto exit;
		}
		cli->reqs++;

	static double old_nr_read = 0, old_nr_read_miss = 0, miss_rate;

	//if ((++count%10000) == 0) {
	sw_end(print_sw);
	if (sw_get_sec(print_sw) >= 1) {
		printf("Processed requests ITER: %lu\n", ++G_count);
		for (int i = 0; i < mas->num_hlr; i++) {
			mas->stat.nr_read += mas->hlr[i]->stat.nr_read;	
			mas->stat.nr_read_cache_miss += mas->hlr[i]->stat.nr_read_cache_miss;	
			mas->stat.nr_read_miss += mas->hlr[i]->stat.nr_read_miss;	
			mas->stat.nr_read_key_mismatch += mas->hlr[i]->stat.nr_read_key_mismatch;	
			mas->stat.nr_read_find_miss += mas->hlr[i]->stat.nr_read_find_miss;	
			mas->stat.nr_read_expired += mas->hlr[i]->stat.nr_read_expired;	
			mas->stat.nr_read_evicted += mas->hlr[i]->stat.nr_read_evicted;	
			mas->stat.nr_write += mas->hlr[i]->stat.nr_write;
			mas->stat.nr_write_cache_miss += mas->hlr[i]->stat.nr_write_cache_miss;
			mas->stat.nr_write_key_mismatch += mas->hlr[i]->stat.nr_write_key_mismatch;
			mas->stat.nr_write_find_miss += mas->hlr[i]->stat.nr_write_find_miss;
			mas->stat.nr_write_expired += mas->hlr[i]->stat.nr_write_expired;
			mas->stat.nr_write_evicted += mas->hlr[i]->stat.nr_write_evicted;
		}
		print_mas_stat(mas);
		miss_rate = ((double)(mas->stat.nr_read_miss - old_nr_read_miss)) / ((double)((mas->stat.nr_read) - old_nr_read));
		printf("ITER %lu NR_READ: %lu HIT_RATE: %.6f\n", G_count, mas->stat.nr_read, 1-miss_rate);
		memset(&mas->stat, 0, sizeof(struct stats));
		fflush(stdout);
		sw_start(print_sw);
	}

	}
exit:
	return rc;
}

static int __trace_parse_and_make_request (struct master *mas, int fd) {
	char *buf = mas->trace_buf;
	int rc = 0;
	uint64_t hash_high;
	int hlr_idx;
	req_type_t type;
	struct request *req;
	char *tokens[10] = {0,};
	int cnt = 0;
	char *pos = NULL;
	uint128 hash128;
	char *token = strtok(buf, ",");
	static uint64_t count = 0, sets = 0, gets = 0;


	while(token != NULL) {
		tokens[cnt++] = token;
		token = strtok(NULL, ",");
	}

	if (strncmp(tokens[5], "get", 3) == 0) {
		type = REQ_TYPE_GET;
		gets++;
	} else if (strncmp(tokens[5], "set", 3) == 0) {
		type = REQ_TYPE_SET;
		sets++;
	} else if (strncmp(tokens[5], "add", 3) == 0) {
		type = REQ_TYPE_SET;
		sets++;
	} else {
		goto exit;
	}


	hash128 = hashing_key_128(tokens[1], atoi(tokens[2]));
	hash_high = hash128.first;

	hlr_idx = (hash_high >> 32) % mas->num_hlr;
	req = make_request_from_trace(mas->hlr[hlr_idx]);

#ifndef TTL
	req->sec = 0;
#endif
	req->req_time = atoi(tokens[0]); // seconds
	req->sec = atoi(tokens[6]); // ttl (seconds)
	req->key.hash_high = hash128.first;
	req->key.hash_low = hash128.second;
	req->key.len = atoi(tokens[2]);
	memcpy(req->key.key, tokens[1], req->key.len);
	//req->value.len = atoi(tokens[3]);
	req->value.len = 32768;
	req->type = type;

	/*
	if (strncmp(tokens[5], "get", 3) == 0)
		req->type = REQ_TYPE_GET;
	else if (strncmp(tokens[5], "set", 3) == 0)
		req->type = REQ_TYPE_SET;
	else if (strncmp(tokens[5], "add", 3) == 0)
		req->type = REQ_TYPE_SET;
	else {
		trace_end_req(req);
		goto exit;
	}
	*/

	rc = forward_req_to_hlr(mas->hlr[hlr_idx], req);
	if (rc) {
		goto exit;
	}
	if ((++count%1000000) == 0) {
		sw_end(mas_sw);
		printf("Processed requests TOTAL: %lu, Throughput: %.4f ops/s (%f seconds), SETS: %lu (%.4f), GETS: %lu (%.4f)\n", count, (double)count/sw_get_sec(mas_sw), sw_get_sec(mas_sw), sets, (double)sets/count, gets, (double)gets/count);
		for (int i = 0; i < mas->num_hlr; i++) {
			mas->stat.nr_read += mas->hlr[i]->stat.nr_read;	
			mas->stat.nr_read_cache_miss += mas->hlr[i]->stat.nr_read_cache_miss;	
			mas->stat.nr_read_miss += mas->hlr[i]->stat.nr_read_miss;	
			mas->stat.nr_read_key_mismatch += mas->hlr[i]->stat.nr_read_key_mismatch;	
			mas->stat.nr_read_find_miss += mas->hlr[i]->stat.nr_read_find_miss;	
			mas->stat.nr_read_expired += mas->hlr[i]->stat.nr_read_expired;	
			mas->stat.nr_read_evicted += mas->hlr[i]->stat.nr_read_evicted;	
			mas->stat.nr_write += mas->hlr[i]->stat.nr_write;
			mas->stat.nr_write_cache_miss += mas->hlr[i]->stat.nr_write_cache_miss;
			mas->stat.nr_write_key_mismatch += mas->hlr[i]->stat.nr_write_key_mismatch;
			mas->stat.nr_write_find_miss += mas->hlr[i]->stat.nr_write_find_miss;
			mas->stat.nr_write_expired += mas->hlr[i]->stat.nr_write_expired;
			mas->stat.nr_write_evicted += mas->hlr[i]->stat.nr_write_evicted;
			print_hlr_stat(mas->hlr[i]);
		}
		fflush(stdout);
	}


exit:
	return rc;
}


int master_parse_and_make_request (struct master *mas, int fd) {
#ifdef REDIS
	return __redis_parse_and_make_request(mas, fd);
#elif TRACE
	return __trace_parse_and_make_request(mas, fd);
#else
	return __netreq_parse_and_make_request(mas, fd);
#endif 
}

static int __process_request_circular(struct master *mas, int fd) {
	int rc = 0;
	int len;
	//int n_obj;
	//uint64_t hash_high;
	ssize_t ret;
	//int hlr_idx;
	//struct netreq *netreq_buf;
	struct client *cli = mas->clients[fd];

	len = read_sock_bulk_circular(fd, cli->buf, cli->buf_size, cli->buf_start, cli->buf_len);
	if (len == -1) {
		printf("shutdown now[%d] %d\n", fd, cli->reqs);
		//shutdown(fd, SHUT_RD);
		goto exit;
	} else if (len == 0) {
		close(fd);
		epoll_ctl(mas->epfd, EPOLL_CTL_DEL, fd, NULL);
		client_free(cli);
		mas->clients[fd] = NULL;
		printf("A client (fd:%d) is disconnected ...\n", fd);
		rc = 0;
		goto exit;
	}
	cli->buf_len += len;

	// parse buffer & make reqeusts
	
	mas->parse_and_make_request(mas, fd);
	
	// move circular buffer
	if (cli->buf_len == 0) {
		cli->buf_start = 0;
	} else {
		if ((ret = move_circular_buf(cli->buf, cli->buf_size, cli->buf_start, cli->buf_len))) {
			cli->buf_start = 0;
			if (ret != cli->buf_len) {
				fprintf(stderr, "buf_len differs!\n");
			}
		}
	}

exit:
	return rc;

}

#ifndef TRACE
void *master_thread(void *input) {
	int rc;

	struct master *mas = (struct master *)input;

	struct epoll_event ep_events[MAX_EVENTS];
	int ep_event_count = 0;
	int ep_timeout = -1; // 0 for busy-loop, -1 for sleep


	print_sw = sw_create();
	sw_start(print_sw);

	pthread_barrier_wait(&mas->barrier);

	while (1) {
		ep_event_count = epoll_wait(mas->epfd, ep_events, MAX_EVENTS, ep_timeout);

		if (ep_event_count == 0) {
			continue;
		} else if (ep_event_count < 0) {
			if (errno == EINTR)
				continue;
			perror("epoll_wait()");
			break;
		}

		for (int i = 0; i < ep_event_count; i++) {
			if (ep_events[i].data.fd == mas->fd) {
				rc = __accept_new_client(mas);
				if (rc < 0) {
					goto exit;
				}
				printf("Accepted cilent fd: %d\n", rc);
			} else {
				//rc = __process_request(mas, ep_events[i].data.fd);
				rc = __process_request_circular(mas, ep_events[i].data.fd);
				if (rc) {
					goto exit;
				}
			}
		}
	}

exit:
	master_free(mas);
	return NULL;
}
#else
void trace_exit(int);
void *master_thread(void *input) {
	int rc;
	char *buffer = (char *)malloc(1024);

	struct master *mas = (struct master *)input;
	mas->trace_buf = buffer;

	pthread_barrier_wait(&mas->barrier);

	mas_sw = sw_create();
	sw_start(mas_sw);


	print_sw = sw_create();
	sw_start(print_sw);

	while (1) {
		if (fgets(buffer, 1024, mas->trace_fd) == NULL) {
			if (feof(mas->trace_fd)) {
				if (get_next_trace_fd(mas) == NULL)
					goto exit;
				else
					continue;
			} else {
				printf("fgets() failed\n");
				abort();
			}
		}
		buffer[strlen(buffer)] = 0;

		rc = mas->parse_and_make_request(mas, mas->fd);

		if (rc) {
			goto exit;
		}
	}

exit:
	for (int i = 0; i < mas->num_hlr; i++) {
		printf("hlr[%d]\n", i);
		for (int j = 0; j < 10; j++) {
			mas->sw_total[j] += mas->hlr[i]->sw_total[j];
		}
		print_hlr_stat(mas->hlr[i]);
		//print_hlr_sw(mas->hlr[i]);
		fflush(stdout);
	}
	print_mas_sw(mas);
	fflush(stdout);
	uint64_t nr_query = 0;
#ifdef CDF
	for (int i = 0; i < mas->num_dev; i++) {
		for (int j = 0; j < CDF_TABLE_MAX; j++) {
			nr_query += mas->hlr[i]->cdf_table[j];
			mas->cdf_table[j] += mas->hlr[i]->cdf_table[j];
		}
		//nr_query += mas->hlr[i]->nr_query;
	}	
	print_cdf(mas->cdf_table, nr_query);
	fflush(stdout);
#endif
	nr_query = 0;
#ifdef AMF_CDF
	uint64_t nr_query_cnt = 0, nr_query_bytes = 0;
	for (int i = 0; i < mas->num_dev; i++) {
		for (int j = 0; j < CDF_TABLE_MAX; j++) {
			mas->amf_cdf_table[j] += mas->hlr[i]->amf_cdf_table[j];
			nr_query_cnt += mas->hlr[i]->amf_cdf_table[j];
			mas->amf_bytes_cdf_table[j] += mas->hlr[i]->amf_bytes_cdf_table[j];
			nr_query_bytes += mas->hlr[i]->amf_bytes_cdf_table[j];
		}
		//nr_query += mas->hlr[i]->nr_query;
	}	
	print_io_cdf(mas->amf_cdf_table, nr_query_cnt);
	print_io_bytes_cdf(mas->amf_bytes_cdf_table, nr_query_bytes);
	fflush(stdout);
#endif

	for (int i = 0; i < mas->num_hlr; i++) {
		handler_free(mas->hlr[i]);
	}
	
	free(mas->trace_buf);
	master_free(mas);
	return NULL;
}

#endif

/*void *ack_sender(void *input) {
	struct master *mas = (struct master *)input;
	q_init(&ack_q, QSIZE);

	struct netack *ack;

	while (1) {
		if (ack_sender_stop) break;

		ack = (struct netack *)q_dequeue(ack_q);
		if (!ack) continue;
		send_ack(mas->
	}

	return NULL;
}*/
