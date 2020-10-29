#include "platform/master.h"
#include "platform/util.h"
#include "platform/request.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>

#define MAX_EVENTS 1024

static int __socket_init() {
	int fd;
	int flags;
	int option;
	struct sockaddr_in sockaddr;

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd == -1) {
		perror("socket()");
		goto error;
	}

	// setting socket to non-blocking
	flags = fcntl(fd, F_GETFL);
	flags |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0) {
		perror("fcntl()");
		goto close_fd;
	}

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

	struct epoll_event events;
	events.events  = EPOLLIN | EPOLLET;
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

	return mas;

error:
	free(mas);
	return NULL;
}

void master_free(struct master *mas) {
	// TODO: free master variables
	close(mas->epfd);
	free(mas);
}


static int __accept_new_client(struct master *mas) {
	int fd;
	int socklen;
	struct sockaddr_in sockaddr;
	int flags;

	socklen = sizeof(sockaddr);
	fd = accept(mas->fd, (struct sockaddr *)&sockaddr, (socklen_t *)&socklen);
	if (fd < 0) {
		perror("accept()");
		goto error;
	}

	flags = fcntl(fd, F_GETFL);
	flags |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, flags) < 0) {
		perror("fcntl()");
		close(fd);
		goto error;
	}

	struct epoll_event events;
	events.events  = EPOLLIN | EPOLLET;
	events.data.fd = fd;

	if (epoll_ctl(mas->epfd, EPOLL_CTL_ADD, fd, &events) < 0) {
		perror("epoll_ctl()");
		close(fd);
		goto error;
	}

	return fd;

error:
	return -1;
}

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
			} else {
				rc = __process_request(mas, ep_events[i].data.fd);
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
