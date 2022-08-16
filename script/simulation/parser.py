# usage:
# $ python parser.py "log_dir_name" "dat_dir_name" "metric_name: perf, ycsb"

import os
import sys

# hopscotch: swap
# hopscotch_part: caching
# hopscotch_mem: OPT
# bigkv: bigkv
indexing_list = ["hopscotch", "hopscotch_part", "bigkv", "cascade"]  
#indexing_list = ["hopscotch", "bigkv", "hopscotch_swap"]  
#indexing_list = ["hopscotch_swap"]  
dist_list = ["default", "hot_0.1_0.9", "hot_0.05_0.95", "hot_0.01_0.99"]  
#dist_list = ["default", "hot_0.01_0.99"]
#workload_load_list = ["workloada-load", "workloadb-load", "workloadc-load", "workloadd-load", "workloadf-load"] 
workload_load_list = ["workloada-load", "workloadb-load", "workloadc-load"] 
#workload_run_list = ["workloada-bulk-run", "workloadb-bulk-run", "workloadc-bulk-run", "workloadd-bulk-run", "workloadf-bulk-run"]  
workload_run_list = ["workloada-run", "workloadb-run", "workloadc-run", "workloadd-run", "workloadf-run"]  
#workload_run_list = ["workloada-run", "workloadb-run", "workloadc-run"]

def search (log_dir_name, metric):
    full_workload_list = list()
    indexing_dist_set = set()
    indexing_names = os.listdir(log_dir_name)
    for indexing_name in indexing_names:
        if indexing_name == "totallog":
            continue
        full_indexing_name = os.path.join(log_dir_name, indexing_name)
        dist_names = os.listdir(full_indexing_name)
        for dist_name in dist_names:
            indexing_dist_name = indexing_name
            full_dist_name = os.path.join(full_indexing_name, dist_name)
            full_perf_name = full_dist_name + "/" + metric
            workload_names = os.listdir(full_perf_name)
            indexing_dist_name += "/" + dist_name
            indexing_dist_set.add(indexing_dist_name)
            for workload_name in workload_names:
                full_workload_name = os.path.join(full_perf_name, workload_name)
                full_workload_list.append(full_workload_name);
#                print(full_workload_name)
    return full_workload_list, indexing_dist_set

def make_dat_dirs (dat_dir_name, indexing_dist_set):
    for indexing_dist_name in indexing_dist_set:
        dist_dir_name = os.path.abspath(dat_dir_name) + "/" + indexing_dist_name + "/"
        print(dist_dir_name)
        os.makedirs(dist_dir_name, exist_ok=True)

def cut_latency_data (src_file_name, dst_file_name, pivot_str):
    src_file = open(src_file_name, 'r')
    os.makedirs(os.path.dirname(dst_file_name), exist_ok=True)
    dst_file = open(dst_file_name, 'w')


    print(src_file_name)
    print(dst_file_name)

    while True:
        line = src_file.readline()
        #print(line)
        if not line:
            break
        if pivot_str not in line:
            continue
        else:
            while True:
                dst_file.write(line)
                line = src_file.readline()
                if not line:
                    break
        break
    src_file.close()
    dst_file.close()

def cut_throughput_data (src_file_name, dst_file_name, pivot_str, indexing_name, dist_name, workload_name, throughput_dict):
    src_file = open(src_file_name, 'r')
    tmp_str = str()

#    print(src_file_name)
#    print(dst_file_name)
    print("a")
    while True:
        line = src_file.readline()
        #print(line)
        if not line:
            break
        if pivot_str not in line:
            continue
        else:
            tmp_str = line
            throughput = tmp_str.split()[-1]
            key = indexing_name + dist_name + workload_name
            throughput_dict[key]= throughput
            print(key)
            #print(throughput_dict[indexing_name])
#                line = src_file.readline()
#                if not line:
            break
    return throughput_dict

    src_file.close()

def write_throughput_data(dst_file_base, throughput_dict):
    for indexing_name in indexing_list:
        for dist_name in dist_list:
            dst_file_name = dst_file_base + "/" + dist_name + "/" + indexing_name + "-load.dat"
            print(dst_file_name)
            os.makedirs(os.path.dirname(dst_file_name), exist_ok=True)
            dst_file = open(dst_file_name, 'w')
            for workload_name in workload_load_list:
                key = indexing_name + dist_name + workload_name
                dst_file.write("YCSB-" + workload_name[8:9].upper() + "\t" + throughput_dict[key] + "\n")
                print(throughput_dict[key])
            #print(throughput_dict[indexing_name])
            dst_file.close()

    for indexing_name in indexing_list:
        for dist_name in dist_list:
            dst_file_name = dst_file_base + "/" + dist_name + "/" + indexing_name + "-run.dat"
            print(dst_file_name)
            os.makedirs(os.path.dirname(dst_file_name), exist_ok=True)
            dst_file = open(dst_file_name, 'w')
            for workload_name in workload_run_list:
                key = indexing_name + dist_name + workload_name
                dst_file.write("YCSB-" + workload_name[8:9].upper() + "\t" + throughput_dict[key] + "\n")
                print(throughput_dict[key])
            #print(throughput_dict[indexing_name])
            dst_file.close()



def parse_perf (dat_dir_name, indexing_dist_set, full_workload_list, metric_name):
    #make_dat_dirs(dat_dir_name, indexing_dist_set)
    throughput_dict = dict()
    for workload_full_name in full_workload_list:
        workload_chunks = workload_full_name.split("/")
        indexing_name = workload_chunks[-4]
        dist_name = workload_chunks[-3]
        workload_name = workload_chunks[-1]
        if metric_name == "perf":
            dst_file_name = os.path.abspath(dat_dir_name) + "/latency" + "/" + indexing_name + "/" + dist_name + "/" + workload_name + ".dat"
            cut_latency_data(workload_full_name, dst_file_name, "latency")
        if metric_name == "ycsb":
            dst_file_name = os.path.abspath(dat_dir_name) + "/throughput" 
            throughput_dict = cut_throughput_data(workload_full_name, dst_file_name, "Throughput", indexing_name, dist_name, workload_name, throughput_dict)

    if metric_name == "ycsb":
        write_throughput_data(dst_file_name, throughput_dict)
#        print(dst_file_name)
#        print(dist_dir_name)
        #dst_dat_dir_name = os.path.join(os.path.abspath(dat_dir_name), os.path.split(workload_name))
#        print(os.path.abspath(dat_dir_name))
#        print(os.path.dirname(workload_name))
#        print(dst_dat_dir_name)
#        os.makedirs(workload_name


log_dir_name = sys.argv[1]
dat_dir_name = sys.argv[2]
metric_name = sys.argv[3]

print(os.path.abspath(dat_dir_name))

full_workload_list, indexing_dist_set = search(log_dir_name, metric_name)
parse_perf(dat_dir_name, indexing_dist_set, full_workload_list, metric_name)
