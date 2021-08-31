#ifndef __CLIENT_H__
#define __CLIENT_H__

#include "type.h"
#include "config.h"

#include <time.h>
#include <pthread.h>
#include <sys/types.h>


struct client {
	int fd;

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

	int reqs;
};


struct client *client_init(int sock);
void client_free(struct client *cli);

#endif
