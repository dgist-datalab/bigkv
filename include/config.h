/*
 * Configuration Header
 *
 * Description: It's a header to define global configurations which is
 * determined at compile time.
 *
 * ex) Queue depth, page size, IP, port, ...
 * 
 */

#ifndef __DFHASH_CONFIG_H__
#define __DFHASH_CONFIG_H__

#define Ki (1024)
#define Mi (1024*Ki)
#define Gi (1024*Mi)
#define Ti (1024*Gi)

//#define PAGESIZE (4*Ki)
#define PAGESIZE (16*Ki)
//#define AVAIL_MEMORY (1 << 29) // 1TB에 4KB 최소 기준, 512MB DRAM
//#define AVAIL_MEMORY (1 << 28) // 1TB에 4KB 최소 기준, 256MB DRAM
#define AVAIL_MEMORY (1 << 27) // 1TB에 4KB 최소 기준, 128MB DRAM

#ifdef CASCADE
#define TEST_GC_CAPACITY (8LLU * Gi)
#else
#define TEST_GC_CAPACITY (32LLU * Gi)
#endif
#define CAPACITY 

#define SEGMENT_SIZE (2*Mi)
#define GRAIN_UNIT 1024
#define NR_GRAIN_IN_SEG (SEGMENT_SIZE/GRAIN_UNIT)
#define MAX_SEG_AGE 32

#define SOB GRAIN_UNIT

#define KEY_LEN 32
#define VALUE_LEN (16*Ki)

#define KEY_LEN_MAX 256
#define VALUE_LEN_MAX (2*Mi)

//#define VALUE_ALIGN_UNIT VALUE_LEN
//#define MEM_ALIGN_UNIT (4*Ki)
#define VALUE_ALIGN_UNIT 4096
#define MEM_ALIGN_UNIT 4096

#define CDF_TABLE_MAX 10000000

#define QSIZE 1024
#define QDEPTH 512

//#define IP "169.254.130.123"
#define IP "127.0.0.1"
#define PORT 5556

#define FAULT_HLR 7

#endif
