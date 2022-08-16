# usage:
# $ python parser.py "log_dir_name" "dat_dir_name" "metric_name: perf, ycsb"

import os
import sys

def do_parse (src_file_name, dst_file_name, start_time, end_time):
    src_file = open(src_file_name, 'r')
#    dst_file = open(dst_file_name, 'w')

    old_nr_read = 0
    old_nr_read_miss = 0
    cur_nr_read = 0
    cur_nr_read_miss = 0

    while True:
        line = src_file.readline()
        #print(line)
        if not line:
            break
        raw = line.split()
        if 'ITER:' not in line:
            continue

        cur_iter = (int)(raw[-1])

        if cur_iter < start_time:
            continue
        if cur_iter > end_time:
            continue
        #print(line)
        tmp = src_file.readline()
        cur_nr_read = float(src_file.readline().split()[-1])
        tmp = src_file.readline()
        tmp = src_file.readline()
        tmp = src_file.readline()
        cur_nr_read_miss = float(src_file.readline().split()[-1])

        #print(cur_nr_read)
        #print(cur_nr_read_miss)

        if old_nr_read == 0:
            old_nr_read = cur_nr_read
            old_nr_read_miss = cur_nr_read_miss
            continue

        hit_ratio = 1 - (cur_nr_read_miss - old_nr_read_miss) / (cur_nr_read - old_nr_read)
        print (str(cur_iter) + ' ' + str(hit_ratio))

        old_nr_read = cur_nr_read
        old_nr_read_miss = cur_nr_read_miss
    src_file.close()
#    dst_file.close()


file_name = sys.argv[1]
start_time = int(sys.argv[2])
end_time = int(sys.argv[3])

do_parse(file_name, ' ', start_time, end_time)
