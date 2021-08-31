#include "platform/master.h"
#include "platform/util.h"
#include "platform/request.h"
#include "platform/redis.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <signal.h>

#define MAX_EVENTS 1024

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

	if (listen(fd, 128) == -1) {
		perror("listen()");
		goto close_fd;
	}

	return fd;

close_fd:
	close(fd);
error:
	return -1;
}

struct master *master_init(int num_dev, char *device[]) {
	struct master *mas = (struct master *)calloc(1, sizeof(struct master));

	// TODO: init master variables
	mas->num_dev = num_dev;
	mas->device = device;

	mas->fd = __socket_init();

	mas->epfd = epoll_create1(0);
	if (mas->epfd < 0) {
		perror("epoll_create1()");
		goto error;
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
		goto error;
	}

	pthread_create(&mas->tid, NULL, &master_thread, mas);
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(0, &cpuset);
	pthread_setaffinity_np(mas->tid, sizeof(cpu_set_t), &cpuset);

	for (int i = 0; i < num_dev; i++) {
		mas->hlr[i] = handler_init(device[i]);
	}

	mas->parse_and_make_request = master_parse_and_make_request;
	mas->processed_req = 0;

#ifdef CDF
	mas->cdf_table = (uint64_t *)malloc(CDF_TABLE_MAX * sizeof(uint64_t));
	memset(mas->cdf_table, 0, CDF_TABLE_MAX * sizeof (uint64_t));
#endif

	return mas;

error:
	free(mas);
	return NULL;
}

void master_free(struct master *mas) {
	// TODO: free master variables
	close(mas->epfd);
#ifdef CDF
	free(mas->cdf_table);
#endif
	free(mas);
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
		hlr_idx = (hash_high >> 32) % mas->num_dev;

		struct request *req = make_request_from_netreq(mas->hlr[hlr_idx], nr, fd);

		rc = forward_req_to_hlr(mas->hlr[hlr_idx], req);
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

	while (1) {
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
		hlr_idx = (hash_high >> 32) % mas->num_dev;

		struct request *req = make_request_from_redis(mas->hlr[hlr_idx], cli, fd, type);

		rc = forward_req_to_hlr(mas->hlr[hlr_idx], req);
		cli->reqs++;
		if (rc) {
			goto exit;
		}
	}
exit :
	return rc;
}

int master_parse_and_make_request (struct master *mas, int fd) {
#ifdef REDIS
	return __redis_parse_and_make_request(mas, fd);
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

void *master_thread(void *input) {
	int rc;

	struct master *mas = (struct master *)input;

	struct epoll_event ep_events[MAX_EVENTS];
	int ep_event_count = 0;
	int ep_timeout = -1; // 0 for busy-loop, -1 for sleep

	while (1) {
		ep_event_count = epoll_wait(mas->epfd, ep_events, MAX_EVENTS, ep_timeout);

		if (ep_event_count == 0) {
			continue;
		} else if (ep_event_count < 0) {
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
