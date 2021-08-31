#!/bin/bash

if [ -z "$1" ]; then
  echo "Please specify workload name for logging"
  exit 1
fi

bigkv_dir="/home/koo/src/bigkv_nrg"
log_path="/log/ycsb-$1-$(date +'%Y%m%d-%H%M%S')"
dev_path="/dev/nvme1n1"
swap_dev_path="/dev/nvme3n1"

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

switch_dev() {
	if [[ "$dev_path" == "/dev/nvme1n1" ]]; then
		dev_path="/dev/nvme2n1"
	else
		dev_path="/dev/nvme1n1"
	fi
	echo ${dev_path}
}

start_hopscotch_swap() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	mkdir /sys/fs/cgroup/memory/bigkv
	echo $((256 * 1024 * 1024)) > /sys/fs/cgroup/memory/bigkv/memory.limit_in_bytes
	bash -c "echo "'$$'" > /sys/fs/cgroup/memory/bigkv/cgroup.procs; ${bigkv_dir}/hopscotch -d 1 ${dev_path} >> ${output_file_perf} 2>&1" &
	sleep 5
}

kill_hopscotch_swap() {
	killall -2 hopscotch
	sleep 5
}

start_hopscotch() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/hopscotch -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_hopscotch() {
	killall -2 hopscotch
	sleep 5
}

start_hopscotch_part() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/hopscotch_part -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_hopscotch_part() {
	killall -2 hopscotch_part
	sleep 5
}

start_hopscotch_part_128gb() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/hopscotch_part_128gb -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_hopscotch_part_128gb() {
	killall -2 hopscotch_part_128gb
	sleep 5
}

start_hopscotch_part_256gb() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/hopscotch_part_256gb -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_hopscotch_part_256gb() {
	killall -2 hopscotch_part_256gb
	sleep 5
}


start_bigkv() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/bigkv -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_bigkv() {
	killall -2 bigkv
	sleep 5
}

start_bigkv_256gb() {
	mkdir -p "$output_dir_org_perf"
	output_file_perf="${output_dir_org_perf}/${workload}-${phase}"
	${bigkv_dir}/bigkv_256gb -d 1 ${dev_path} >> ${output_file_perf} 2>&1 &
	sleep 5
}

kill_bigkv_256gb() {
	killall -2 bigkv_256gb
	sleep 5
}

# Kukania logs are saved at copy workload
setup_log() {
	echo "=============================================="
	echo "${test}-${dist}-${workload}-${phase}"
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

	iostat -c -d -x ${dev_path} ${swap_dev_path} 1 -m > ${output_file_stat} &
	vmstat 1 | gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > ${output_file_vmstat} &
	dmesg -w > ${output_file_dmesg} &

	sleep 5


	ssh root@10.150.21.54 "cd /home/koo/src/YCSB; ./bin/ycsb load redis -s -P workloads/bigkv/${dist}/${workload} -p redis.host=10.150.21.55 -p redis.port=5556 -p redis.timeout=10000000 -threads 100" > ${output_file_ycsb} 2>&1

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

	iostat -c -d -x ${dev_path} ${swap_dev_path} 1 -m > ${output_file_stat} &
	vmstat 1 | gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > ${output_file_vmstat} &
	dmesg -w > ${output_file_dmesg} &

	sleep 5


	ssh root@10.150.21.54 "cd /home/koo/src/YCSB; ./bin/ycsb run redis -s -P workloads/bigkv/${dist}/${workload} -p redis.host=10.150.21.55 -p redis.port=5556 -p redis.timeout=10000000 -threads 1" > ${output_file_ycsb} 2>&1


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

	iostat -c -d -x ${dev_path} ${swap_dev_path} 1 -m > ${output_file_stat} &
	vmstat 1 | gawk '{now=strftime("%Y-%m-%d %T "); print now $0}' > ${output_file_vmstat} &
	dmesg -w > ${output_file_dmesg} &

	sleep 5


	ssh root@10.150.21.54 "cd /home/koo/src/YCSB; ./bin/ycsb run redis -s -P workloads/bigkv/${dist}/${workload} -p redis.host=10.150.21.55 -p redis.port=5556 -p redis.timeout=10000000 -threads 100" > ${output_file_ycsb} 2>&1


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
	#for workload in workloada workloadb workloadc workloadd workloadf
	for workload in workloadc
	do
		phase="thread1"
		${start_server}
		switch_dev
		do_load
		do_run
		${kill_server}
	done
}

do_bulk_ycsb() {
	for workload in workloada workloadb workloadc workloadd workloadf
	do
		phase="thread100"
		${start_server}
		switch_dev
		do_load
		do_bulk_run
		${kill_server}
	done
}



#for test in hopscotch_part
#for test in bigkv hopscotch hopscotch_swap hopscotch_part
#for test in hopscotch_swap 
for test in bigkv_256gb hopscotch_part_256gb hopscotch_part_128gb
do
	#for dist in small
	#for dist in hot_0.01_0.99 hot_0.05_0.95 hot_0.1_0.9 default
	for dist in default
	do
		output_dir_org_perf="$log_path/$test/$dist/perf"
		output_dir_org_stat="$log_path/$test/$dist/iostat"
		output_dir_org_vmstat="$log_path/$test/$dist/vmstat"
		output_dir_org_slab="$log_path/$test/$dist/slab"
		output_dir_org_dmesg="$log_path/$test/$dist/dmesg"
		output_dir_org_ycsb="$log_path/$test/$dist/ycsb"

		start_server="start_${test}"
		kill_server="kill_${test}"

		do_ycsb
		#do_bulk_ycsb
	done
done

