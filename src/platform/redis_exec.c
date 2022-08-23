#include "platform/master.h"
#include "platform/error.h"
#include "platform/redis.h"
#include "type.h"
#include <pthread.h>

static bool islstr(struct client *cli, int arg_idx, const char *str){
	int i = 0;
	for (;i<cli->args_size[arg_idx];i++){
		if (cli->args[arg_idx][i] != str[i] && cli->args[arg_idx][i] != str[i]-32){
			return false;
		}
	}
	return !str[i];
}

static bool iscmd(struct client *cli, const char *cmd){
	return islstr(cli, 0, cmd);
}

req_type_t redis_convert_type(struct client *cli){
	if (cli->args_len==0||(cli->args_len==1&&cli->args_size[0]==0)){
		return REQ_TYPE_UNKNOWN;
	}

	if (iscmd(cli, "HMSET")) {
		return REQ_TYPE_SET;
	} else if (iscmd(cli, "HGETALL")) {
		return REQ_TYPE_GET;
	} else if (iscmd(cli, "ZADD")) {
		return REQ_TYPE_ITERATOR;
	}

	return REQ_TYPE_UNKNOWN;
}

