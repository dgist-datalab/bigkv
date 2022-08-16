#include "platform/client.h"
#include "platform/error.h"
#include "platform/redis.h"
#include "type.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>


#if 0
redis *redis_new(){
	redis *c = (redis*)calloc(1, sizeof(redis));
	if (!c){
		err(1, "malloc");
	}
	c->mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
	c->buf = (char*)malloc(BUFSIZE);
	c->output = (char*)malloc(OUTPUT_BUFSIZE);
	pthread_mutex_init(c->mutex, NULL); 
	printf("clnt new\n");
	return c;
}

void redis_free(redis *c){
	if (!c){
		return;
	}
	if (c->buf){
		free(c->buf);
	}
	if (c->args){
		free(c->args);
	}
	if (c->args_size){
		free(c->args_size);
	}
	if (c->output){
		free(c->output);
	}
	if (c->tmp_err){
		free(c->tmp_err);
	}
	if (c->mutex) {
		pthread_mutex_destroy(c->mutex);
	}
	free(c);
}


void redis_close(redis *c){
	redis_free(c);
}
#endif

const char *ERR_INCOMPLETE = "incomplete";
const char *ERR_QUIT = "quit";

inline void redis_output_require(struct client *cli, size_t siz){
	/*
	if (c->output_cap < siz){
		while (c->output_cap < siz){
			if (c->output_cap == 0){
				c->output_cap = 1;
			}else{
				c->output_cap *= 2;
			}
		}
		c->output = (char*)realloc(c->output, c->output_cap);
		if (!c->output){
			err(1, "malloc");
		}
	}
	*/
}
#if 0
void redis_write(struct client *cli, const char *data, int n){
	redis_output_require(cli, cli->output_len+n);
	memcpy(cli->output+cli->output_len, data, n);	
	cli->output_len+=n;
}

void redis_write_data(struct client *cli, const char *data, int n){
	redis_output_require(cli, cli->output_len+n);
	memcpy(cli->output+cli->output_len, data, n);	
	cli->output_len+=n;
}

void redis_clear(struct client *cli){
	cli->output_len = 0;
	cli->output_offset = 0;
}

void redis_write_byte(struct client *cli, char b){
	redis_output_require(cli, cli->output_len+1);
	cli->output[cli->output_len++] = b;
}

void redis_write_bulk(struct client *cli, const char *data, int n){
	char h[32];
	sprintf(h, "$%d\r\n", n);
	redis_write(cli, h, strlen(h));
	redis_write_data(cli, data, n);
	redis_write_byte(cli, '\r');
	redis_write_byte(cli, '\n');
}

void redis_write_multibulk(struct client *cli, int n){
	char h[32];
	sprintf(h, "*%d\r\n", n);
	redis_write(cli, h, strlen(h));
}

void redis_write_int(struct client *cli, int n){
	char h[32];
	sprintf(h, ":%d\r\n", n);
	redis_write(cli, h, strlen(h));
}

void redis_write_error(struct client *cli, error err){
	redis_write(cli, "-ERR ", 5);
	redis_write(cli, err, strlen(err));
	redis_write_byte(cli, '\r');
	redis_write_byte(cli, '\n');
}


void redis_flush_offset(struct client *cli, int offset){
	if (cli->output_len-offset <= 0){
		return;
	}
	ssize_t ret;
	size_t sent = 0;
	size_t size = cli->output_len - offset;
	while ( sent < size ) {
		ret = write(cli->fd, cli->output + offset, cli->output_len - offset);
		if (ret < 0 && errno != EWOULDBLOCK) {
			perror("flush_offset");
			exit(1);
		}
		sent += ret;
		offset += ret;
	}
	cli->output_len = 0;
}


void redis_flush(struct client *cli){
	redis_flush_offset(cli, 0);
}
#endif

static void redis_write(int sock, const char *data, int n){
	if (write(sock, data, n) < 0)
		abort();
}

void redis_write_multibulk(int sock, int n){
	//char h[32];
	//sprintf(h, "*%d\r\n", n);
	//redis_write(sock, h, strlen(h));

	if (write(sock, "*1\r\n$-1\r\n", 9) < 0)
		abort();
}

void redis_write_ok(int sock) {
	if (write(sock, "+OK\r\n", 5) < 0)
		abort();
}

void redis_write_empty_array(int sock) {
	if (write(sock, "*0\r\n", 4) < 0)
		abort();
}


void redis_err_alloc(struct client *cli, int n){
	if (cli->tmp_err){
		free(cli->tmp_err);
	}
	cli->tmp_err = (char*)malloc(n);
	if (!cli->tmp_err){
		//err(1, "malloc");
	}
	memset(cli->tmp_err, 0, n);
}

error redis_err_expected_got(struct client *cli, char c1, char c2){
	redis_err_alloc(cli, 64);
	sprintf(cli->tmp_err, "Protocol error: expected '%c', got '%c'", c1, c2);
	return cli->tmp_err;
}

error redis_err_unknown_command(struct client *cli, const char *name, int count){
	redis_err_alloc(cli, count+64);
	cli->tmp_err[0] = 0;
	strcat(cli->tmp_err, "unknown command '");
	strncat(cli->tmp_err, name, count);
	strcat(cli->tmp_err, "'");
	return cli->tmp_err;
}

void redis_append_arg(struct client *cli, const char *data, int nbyte){
	if (cli->args_cap==cli->args_len){
		if (cli->args_cap==0){
			cli->args_cap=1;
		}else{
			cli->args_cap*=2;
		}
		cli->args = (const char**)realloc(cli->args, cli->args_cap*sizeof(const char *));
		printf("alloc!!!\n");
		if (!cli->args){
			//err(1, "malloc");
		}
		cli->args_size = (int*)realloc(cli->args_size, cli->args_cap*sizeof(int));
		if (!cli->args_size){
			//err(1, "malloc");
		}
	}
	cli->args[cli->args_len] = data;
	cli->args_size[cli->args_len] = nbyte;
	cli->args_len++;
}

error redis_parse_telnet_command(struct client *cli){
	size_t i = cli->buf_start;
	size_t z = cli->buf_start + cli->buf_len;
	if (i >= z){
		return ERR_INCOMPLETE;
	}
	cli->args_len = 0;
	size_t s = i;
	bool first = true;
	for (;i<z;i++){
		if (cli->buf[i]=='\'' || cli->buf[i]=='\"'){
			if (!first){
				return "Protocol error: unbalanced quotes in request";
			}
			char b = cli->buf[i];
			i++;
			s = i;
			for (;i<z;i++){
				if (cli->buf[i] == b){
					if (i+1>=z||cli->buf[i+1]==' '||cli->buf[i+1]=='\r'||cli->buf[i+1]=='\n'){
						redis_append_arg(cli, cli->buf+s, i-s);
						i--;
					}else{
						return "Protocol error: unbalanced quotes in request";
					}
					break;
				}
			}
			i++;
			continue;
		}
		if (cli->buf[i] == '\n'){
			if (!first){
				size_t e;
				if (i>s && cli->buf[i-1] == '\r'){
					e = i-1;
				}else{
					e = i;
				}
				redis_append_arg(cli, cli->buf+s, e-s);
			}
			i++;
			cli->buf_len -= i-cli->buf_start;
			if (cli->buf_len == 0){
				cli->buf_start = 0;
			}else{
				cli->buf_start = i;
			}
			return NULL;
		}
		if (cli->buf[i] == ' '){
			if (!first){
				redis_append_arg(cli, cli->buf+s, i-s);
				first = true;
			}
		}else{
			if (first){
				s = i;
				first = false;
			}
		}
	}
	return ERR_INCOMPLETE;
}

error redis_read_command(struct client *cli){
	cli->args_len = 0;
	size_t i = cli->buf_start;
	size_t z = cli->buf_start + cli->buf_len;
	int args_len, bulk_flag = 0;
//	printf("first buf_idx : %d buf_len : %d\n", c->buf_idx, c->buf_len);
	if (i >= z){
		return ERR_INCOMPLETE;
	}
	if ( (cli->buf[i] != '*') && (cli->buf[i] != '$') && (cli->buf[i] != '+')) {
		//printf("first buf_idx : %d buf_len : %d\n%s", c->buf_idx, c->buf_len, &c->buf[c->buf_idx]);
		return redis_parse_telnet_command(cli);
	}
	if (cli->buf[i] == '$') {
		args_len = 1;
		bulk_flag = 1;
	}
	else if (cli->buf[i] == '+') {
		i++;
		if (i >=z) {
			return ERR_INCOMPLETE;
		}
		if (cli->buf_len < 5) {
			return ERR_INCOMPLETE;
		}
		//memcpy(str, &c->buf[i],4);
		if (strncmp(&cli->buf[i], "OK\r\n", 4) == 0) {
			redis_append_arg(cli, "OK", 2);
			cli->buf_len -= 5;
			if (cli->buf_len == 0) {
				cli->buf_start = 0;
			}
			else {
				cli->buf_start = i+4;
			}
			return NULL;
		}
		else if (strncmp(&cli->buf[i], "KO\r\n", 4) == 0) {
			redis_append_arg(cli, "KO", 2);
			cli->buf_len -= 5;
			if (cli->buf_len == 0) {
				cli->buf_start = 0;
			}
			else {
				cli->buf_start = i+4;
			}
			return NULL;
		}
	}
	else {
		i++;
		args_len = 0;
		size_t s = i;
		for (;i < z;i++){
			if (cli->buf[i]=='\n'){
				if (cli->buf[i-1] !='\r'){
					return "Protocol error: invalid multibulk length";
				}
				cli->buf[i-1] = 0;
				args_len = atoi(cli->buf+s);
				cli->buf[i-1] = '\r';
				if (args_len <= 0){
					if (args_len < 0 || i-s != 2){
						return "Protocol error: invalid multibulk length";
					}
				}
				i++;
				break;
			}
		}
	}
	if (i >= z){
		return ERR_INCOMPLETE;
	}
	for (int j=0;j<args_len;j++){
		if (i >= z){
			return ERR_INCOMPLETE;
		}
		if (cli->buf[i] != '$'){
			printf("err\n");
			return redis_err_expected_got(cli, '$', cli->buf[i]);
		}
		i++;
		int nsiz = 0;
		size_t s = i;
		for (;i < z;i++){
			if (cli->buf[i]=='\n'){
				if (cli->buf[i-1] !='\r'){
					return "Protocol error: invalid bulk length";
				}
				cli->buf[i-1] = 0;
				nsiz = atoi(cli->buf+s);
				cli->buf[i-1] = '\r';
				if (nsiz <= 0){
					if (nsiz == -1) {
						redis_append_arg(cli, "NOT", 3);
						bulk_flag = 0;
						i += nsiz+2;
						break;
					}
					else if (nsiz < 0 || i-s != 2){
						return "Protocol error: invalid bulk length";
					}
				}
				i++;
				if (z-i < (size_t)nsiz+2){
					return ERR_INCOMPLETE;
				}
				s = i;
				if (cli->buf[s+nsiz] != '\r'){
					return "Protocol error: invalid bulk data";
				}
				if (cli->buf[s+nsiz+1] != '\n'){
					return "Protocol error: invalid bulk data";
				}
				if (bulk_flag) {
					redis_append_arg(cli, "BULK", 4);
					bulk_flag = 0;
				}
				redis_append_arg(cli, cli->buf+s, nsiz);
				i += nsiz+2;
				break;
			}
		}
	}
	cli->buf_len -= i-cli->buf_start;
	if (cli->buf_len == 0){
		cli->buf_start = 0;
	}
	else{
		cli->buf_start = i;
	}
	return NULL;
}

void redis_print_args(struct client *cli){
	printf("args[%d]:", cli->args_len);
	for (int i=0;i<cli->args_len;i++){
		printf("[len: %d] ", cli->args_size[i]);
		printf(" [");
		for (int j=0;j<cli->args_size[i];j++){
			printf("%c", cli->args[i][j]);
		}
		printf("]");
	}
	printf("\n");
}


int redis_parse_commands(struct client *cli){
	for (;;){
		error err = redis_read_command(cli);
		redis_print_args(cli);
		if (err != NULL){
			if ((char*)err == (char*)ERR_INCOMPLETE){
				return 1;
			}
			//redis_write_error(cli, err);
			return 0;
		}
		//redis_print_args(cli);
		//err = redis_exec_command(cli);
		if (err != NULL){
			if (err == ERR_QUIT){
				return 0;
			}
			//redis_write_error(cli, err);
			return 1;
		}
	}
	return 1;
}
