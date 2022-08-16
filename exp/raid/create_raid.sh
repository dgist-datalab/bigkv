#!/bin/bash
## usage: ./create_raid {start dev num} {# of devices} {level} {size of each device (can be empty)}
# usage: ./create_raid {# of devices} {level} {size of each device (can be empty)}

dev=$1
#devices="/dev/nvme${dev}n1"
devices="/dev/nvme4n1 /dev/nvme5n1 /dev/nvme7n1 /dev/nvme13n1"
idx=0

#for ((i=1; i<$2; i++))
#do
#	dev=$(($dev+1))
#	devices=$devices" /dev/nvme${dev}n1 "
#done

if [ ! -z "$3" ]; then
  for i in $devices; do
    echo "o
y
n


+$3

w
y" | gdisk $i
  done

  devices="$(echo $devices | sed s/n1/n1p1/g)"
  ls -d /dev/md[0-9]* | while read f; do mdadm -S $f; done
fi

sleep 0.5

echo "mdadm -C /dev/md0 --level=$2 --raid-device=$1 ${devices}"
mdadm -C /dev/md0 --level=$2 --raid-device=$1 ${devices}

sysctl dev.raid.speed_limit_max=-1
