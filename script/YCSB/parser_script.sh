#!/bin/bash

python3 ./parser.py $1 $2 latency 
python3 ./parser.py $1 $2 throughput
python3 ./parser.py $1 $2 io_bytes
python3 ./parser.py $1 $2 io_count
