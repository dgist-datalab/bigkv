#!/bin/bash

if [ -z "$1" ]; then
  echo "Please specify workload name for logging"
  exit 1
fi

ycsb_dir="/home/koo/src/YCSB"
ycsb_workloads_dir="${ycsb_dir}/workloads"
bigkv_dir="/home/koo/src/ae/bigkv"
log_path="${bigkv_dir}/exp/ycsb/log/ycsb-$1-$(date +'%Y%m%d-%H%M%S')"
dev_path="/dev/nvme7n1"

mkdir -p $log_path

echo "Saving output to $log_path/totallog"

case "$2" in
    -f)
        ;;
    *)
        echo "Daemonizing script"
        $0 $1 -f < /dev/null &> /dev/null & disown
        exit 0
        ;;
esac

exec > $log_path/totallog 2>&1

flush() {
	sync
	echo 3 > /proc/sys/vm/drop_caches
	echo 1 > /proc/sys/vm/compact_memory
	echo 3 > /proc/sys/vm/drop_caches
	echo 1 > /proc/sys/vm/compact_memory
}

start_udepot-opt() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/bin/udepot-opt -c 3 -h 1 -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_udepot-opt() {
	sleep 5
	ps -ef | grep udepot-opt | grep -v grep | awk '{print "kill -2 " $2}' | sh
	sleep 5
}

reset_udepot-opt() {
	sleep 5
	ps -ef | grep udepot-opt | grep -v grep | awk '{print "kill -10 " $2}' | sh
	sleep 5
}

print_udepot-opt() {
	sleep 5
	ps -ef | grep udepot-opt | grep -v grep | awk '{print "kill -12 " $2}' | sh
	sleep 5
}

reset_print_udepot-opt() {
	sleep 5
	ps -ef | grep udepot-opt | grep -v grep | awk '{print "kill -18 " $2}' | sh
	sleep 5
}


start_udepot-cache() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/bin/udepot-cache -c 3 -h 1 -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_udepot-cache() {
	sleep 5
	ps -ef | grep udepot-cache | grep -v grep | awk '{print "kill -2 " $2}' | sh
	sleep 5
}

reset_udepot-cache() {
	sleep 5
	ps -ef | grep udepot-cache | grep -v grep | awk '{print "kill -10 " $2}' | sh
	sleep 5
}

print_udepot-cache() {
	sleep 5
	ps -ef | grep udepot-cache | grep -v grep | awk '{print "kill -12 " $2}' | sh
	sleep 5
}

reset_print_udepot-cache() {
	sleep 5
	ps -ef | grep udepot-cache | grep -v grep | awk '{print "kill -18 " $2}' | sh
	sleep 5
}

start_bigkv() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/bin/bigkv -c 3 -h 1 -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_bigkv() {
	sleep 5
	ps -ef | grep bigkv | grep -v grep | awk '{print "kill -2 " $2}' | sh
	sleep 5
}

reset_bigkv() {
	sleep 5
	ps -ef | grep bigkv | grep -v grep | awk '{print "kill -10 " $2}' | sh
	sleep 5
}

print_bigkv() {
	sleep 5
	ps -ef | grep bigkv | grep -v grep | awk '{print "kill -12 " $2}' | sh
	sleep 5
}

reset_print_bigkv() {
	sleep 5
	ps -ef | grep bigkv | grep -v grep | awk '{print "kill -18 " $2}' | sh
	sleep 5
}

start_slickcache() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/bin/slickcache -c 3 -h 1 -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 120
}

kill_slickcache() {
	sleep 5
	#killall -2 slickcache
	ps -ef | grep slickcache | grep -v grep | awk '{print "kill -2 " $2}' | sh
	sleep 5
}

reset_slickcache() {
	sleep 5
	#killall -10 slickcache
	ps -ef | grep slickcache | grep -v grep | awk '{print "kill -10 " $2}' | sh
	sleep 5
}

print_slickcache() {
	sleep 5
	#killall -12 slickcache
	ps -ef | grep slickcache | grep -v grep | awk '{print "kill -12 " $2}' | sh
	sleep 5
}

reset_print_slickcache() {
	sleep 5
	#killall -12 slickcache
	ps -ef | grep slickcache | grep -v grep | awk '{print "kill -18 " $2}' | sh
	sleep 5
}

setup_log() {
	echo "=============================================="
	echo "${test}-${mem}-${dist}-${workload}-${phase}"
	mkdir -p \
		"$output_dir_org_stat" \
		"$output_dir_org_vmstat" \
		"$output_dir_org_slab" \
		"$output_dir_org_dmesg" \
		"$output_dir_org_ycsb" 
	output_file_stat="${output_dir_org_stat}/${workload}-${phase}"
	output_file_vmstat="${output_dir_org_vmstat}/${workload}-${phase}"
	output_file_slab="${output_dir_org_slab}/${workload}-${phase}"
	output_file_dmesg="${output_dir_org_dmesg}/${workload}-${phase}"
	output_file_ycsb="${output_dir_org_ycsb}/${workload}-${phase}"
}

do_load() {
	phase="load"
	setup_log

	dmesg -c > /dev/null 2>&1

	flush
	sleep 5

	iostat -c -d -x ${dev_path} 1 -m > ${output_file_stat} &
	vmstat 1 | gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > ${output_file_vmstat} &
	dmesg -w > ${output_file_dmesg} &

	sleep 5


	cd ${ycsb_dir} 
	./bin/ycsb load redis -s -P ${ycsb_workloads_dir}/${workload} -p redis.host=127.0.0.1 -p redis.port=5556 -p redis.timeout=10000000  -threads 100 > ${output_file_ycsb} 2>&1
	cd -

	slabtop -o --sort=c > ${output_file_slab} 2>&1

	ps -ef | grep iostat | grep -v grep | awk '{print "kill -9 " $2}' | sh
	ps -ef | grep vmstat | grep -v grep | awk '{print "kill -9 " $2}' | sh

	echo End Run

	ps -ef | grep dmesg | grep -v grep | awk '{print "kill -15 " $2}' | sh
	while pgrep -f dmesg > /dev/null; do sleep 1; done
	dmesg -c > /dev/null 2>&1

	sleep 5
}

do_run() {
	phase="run"
	setup_log

	dmesg -c > /dev/null 2>&1

	flush
	sleep 5

	iostat -c -d -x ${dev_path} 1 -m > ${output_file_stat} &
	vmstat 1 | gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > ${output_file_vmstat} &
	dmesg -w > ${output_file_dmesg} &

	sleep 5


	cd ${ycsb_dir} 
	./bin/ycsb run redis -s -P ${ycsb_workloads_dir}/${workload} -p redis.host=127.0.0.1 -p redis.port=5556 -p redis.timeout=10000000 -threads 1 > ${output_file_ycsb} 2>&1
	cd -


	slabtop -o --sort=c > ${output_file_slab} 2>&1

	ps -ef | grep iostat | grep -v grep | awk '{print "kill -9 " $2}' | sh
	ps -ef | grep vmstat | grep -v grep | awk '{print "kill -9 " $2}' | sh

	echo End Run

	ps -ef | grep dmesg | grep -v grep | awk '{print "kill -15 " $2}' | sh
	while pgrep -f dmesg > /dev/null; do sleep 1; done
	dmesg -c > /dev/null 2>&1

	sleep 5
}

do_bulk_run() {
	phase="bulk-run"
	setup_log

	dmesg -c > /dev/null 2>&1

	flush
	sleep 5

	iostat -c -d -x ${dev_path} 1 -m > ${output_file_stat} &
	vmstat 1 | gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > ${output_file_vmstat} &
	dmesg -w > ${output_file_dmesg} &

	sleep 5


	cd ${ycsb_dir} 
	./bin/ycsb run redis -s -P ${ycsb_workloads_dir}/${workload} -p redis.host=127.0.0.1 -p redis.port=5556 -p redis.timeout=10000000 -threads 100 > ${output_file_ycsb} 2>&1
	cd -


	slabtop -o --sort=c > ${output_file_slab} 2>&1

	ps -ef | grep iostat | grep -v grep | awk '{print "kill -9 " $2}' | sh
	ps -ef | grep vmstat | grep -v grep | awk '{print "kill -9 " $2}' | sh

	echo End Run

	ps -ef | grep dmesg | grep -v grep | awk '{print "kill -15 " $2}' | sh
	while pgrep -f dmesg > /dev/null; do sleep 1; done
	dmesg -c > /dev/null 2>&1

	sleep 5
}


do_ycsb() {
	for workload in workloada
	do
		phase="thread1"
		${start_server}
		do_load
		do_run
		${kill_server}
	done
}

do_bulk_ycsb() {
	for workload in workloada
	do
		phase="thread100"
		${start_server}
		do_load
		do_bulk_run
		${kill_server}
	done
}

do_all_ycsb() {
	for workload in workloada
	do
		phase="thread100"
		${start_server}
		do_load
		do_bulk_run
		do_run
		${kill_server}
	done
}

prepare_workloads() {
	cp ${bigkv_dir}/exp/ycsb/ycsb-example ${ycsb_workloads_dir}
}

for test in udepot-opt udepot-cache bigkv slickcache
do
	prepare_workloads
	for mem in 128MB
	do
		start_server="start_${test}"
		kill_server="kill_${test}"

		output_dir_org_perf="$log_path/$test/"
		workload=example
		phase=server

		${start_server}

		for dist in hot_0.2_0.8
		do
			output_dir_org_stat="$log_path/$test/$mem/$dist/iostat"
			output_dir_org_vmstat="$log_path/$test/$mem/$dist/vmstat"
			output_dir_org_slab="$log_path/$test/$mem/$dist/slab"
			output_dir_org_dmesg="$log_path/$test/$mem/$dist/dmesg"
			output_dir_org_ycsb="$log_path/$test/$mem/$dist/ycsb"
	
			for workload in ycsb-example
			do
				do_load
				do_bulk_run
			done
		done
		${kill_server}
	done
done
