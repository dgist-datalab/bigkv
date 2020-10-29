#include "platform/util.h"
#include "platform/handler.h"
#include "platform/request.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>

struct server {
	int num_dev;
	char *device[32];
	struct master *mas;
} server;

static void server_exit(int sig) {
	exit(1);
}

static int signal_add() {
	struct sigaction sa;
	sa.sa_handler = server_exit;
	sigaction(SIGINT, &sa, NULL);
	return 0;
}

static int server_init(struct server *srv) {
	srv->mas = master_init(srv->num_dev, srv->device);
	return 0;
}

static int 
server_getopt(int argc, char *argv[], struct server *const srv) {
	int c;
	bool dev_flag = false;

	while (1) {
		static struct option long_options[] = {
			{"devices", required_argument, 0, 'd'},
			{0, 0, 0, 0}
		};

		int option_index = 0;

		c = getopt_long(argc, argv, "d:", long_options, &option_index);

		if (c == -1) break;

		switch (c) {
		case 'd':
			dev_flag = true;
			srv->num_dev = atoi(optarg);
			for (int i = 0; i < srv->num_dev; i++) {
				srv->device[i] = (char *)calloc(32, sizeof(char));
				strncpy(srv->device[i], argv[optind+i],
					strlen(argv[optind+i]));
			}
			break;
		case '?':
			// Error
			break;
		default:
			abort();
		}
	}

	if (!dev_flag) {
		fprintf(stderr, "(%s:%d) No devices specified!\n", 
			__FILE__, __LINE__);
		exit(1);
	}
	return 0;
}

int main(int argc, char *argv[]) {
	signal_add();

	server_getopt(argc, argv, &server);

	server_init(&server);

	pthread_join(server.mas->tid, NULL);

	return 0;
}
