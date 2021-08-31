#include "platform/master.h"
#include "platform/util.h"
#include "platform/request.h"
#include "platform/redis.h"
#include "platform/client.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>

struct client *client_init(int sock) {
	struct client *cli = (struct client *)calloc(1, sizeof(struct client));

	cli->fd = sock;

	cli->buf = (char *)malloc(SOCK_BUF_SIZE);
	cli->buf_start = cli->buf_len = 0;
	cli->buf_size = SOCK_BUF_SIZE;
	cli->args_cap = cli->args_len = 0;

	cli->reqs = 0;

	return cli;
}

void client_free(struct client *cli) {
	// TODO: free master variables
	free(cli->buf);
	if (cli->buf_len) {
		abort();
	}
	free(cli);
}
