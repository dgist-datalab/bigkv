#ifndef __REDIS_H__
#define __REDIS_H__

#include "platform/error.h"

int redis_parse_commands(struct client *cli);
void redis_print_args(struct client *cli);
error redis_read_command(struct client *cli);
req_type_t redis_exec_command(struct client *cli);
error redis_err_unknown_command(struct client *cli, const char *name, int count);
req_type_t redis_convert_type(struct client *cli);
void redis_write_ok(int sock);
void redis_write_int(int sock, int number);
void redis_write_empty_array(int sock);
void redis_write_multibulk(int sock, int n);

#endif
