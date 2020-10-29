/*
 * Stopwatch Timer Header.
 *
 * Description: A utility to measure elapsed time. All functions are written as
 * inline.
 *
 * If you want to create timer from heap, use create/destroy.  'start' and
 * 'end' functions log current time, and other functions measure elapsed time
 * from logged time.
 *
 */

#ifndef __STOPWATCH_H__
#define __STOPWATCH_H__

#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
	struct timeval start, end;
	time_t lap_sum;
} stopwatch;

static inline stopwatch *sw_create() {
	stopwatch *sw = (stopwatch *)calloc(1, sizeof(stopwatch));
	return sw;
}

static inline int sw_destroy(stopwatch *sw) {
	free(sw);
	return 0;
}

static inline int sw_start(stopwatch *sw) {
	gettimeofday(&sw->start, NULL);
	return 0;
}

static inline int sw_end(stopwatch *sw) {
	gettimeofday(&sw->end, NULL);
	return 0;
}

static inline int sw_print(stopwatch *sw) {
	time_t usec = (sw->end.tv_sec - sw->start.tv_sec) * 1000000;
	usec += sw->end.tv_usec - sw->start.tv_usec;

	printf("%ld:%ld:%ld (sec:msec:usec)\n",
		usec/1000000, (usec/1000)%1000, usec%1000);

	return 0;
}

static inline time_t sw_get_usec(stopwatch *sw) {
	time_t usec = (sw->end.tv_sec - sw->start.tv_sec) * 1000000;
	usec += sw->end.tv_usec - sw->start.tv_usec;

	return usec;
}

static inline double sw_get_sec(stopwatch *sw) {
	time_t sec = sw->end.tv_sec - sw->start.tv_sec;
	time_t usec = sw->end.tv_usec - sw->start.tv_usec;

	double ret = sec;
	ret += (double)usec/1000000;

	return ret;
}

static inline int sw_lap(stopwatch *sw) {
	sw_end(sw);
	sw->lap_sum += sw_get_usec(sw);
	return 0;
}

static inline time_t sw_get_lap_sum(stopwatch *sw) {
	return sw->lap_sum;
}

#endif
