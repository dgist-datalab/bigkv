#include <stdio.h>
#include <stdint.h>

#define NR_TTL_GROUPS 32

static uint64_t ttl_group_std[NR_TTL_GROUPS] = {
	60,
	120,
	180,
	240,
	300,
	360,
	420,
	480,
	540,
	600, // 10 mins, 9
	1200, // 20 mins
	1800, // 30 mins
	2400, // 40 mins
	3000, // 50 mins
	3600, // 1 hour, 14
	7200, // 2 hours
	10800, // 3 hours
	14400, // 4
	18000, // 5
	21600, // 6
	43200, // 12 hours, 20
	86400, // 1 day
	172800, // 2 day
	259200, // 3 day
	345600, // 4 day
	432000, // 5 day
	518400, // 6 day
	604800, // 7 day, 27
	1209600, // 14 day
	2592000, // 30 day
	5184000, // 60 day
	8640000 // 100 day
};

uint32_t get_ttl_idx (uint32_t ttl) {
	int l = 0, r = NR_TTL_GROUPS, m;
	while (l < r) {
		m = (l + r) / 2;
		if (ttl_group_std[m] >= ttl)
			r = m;
		else
			l = m + 1;
	}
	return r;
}

int main () {
	uint64_t a[10] = {2, 64, 120, 492, 3600, 129123, 1923940, 172800, 43200, 402382};
	int i;
	for (i = 0; i < 10; i++) {
		printf("%lu is at arr[%u] (%lu)\n", a[i], get_ttl_idx(a[i]), ttl_group_std[get_ttl_idx(a[i])]);
	}
	return 0;
}
