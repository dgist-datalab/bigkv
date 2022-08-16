#ifndef __H_LFQUEUE_Q_
#define __H_LFQUEUE_Q_
	
#include <pthread.h>

typedef struct lfq_node{
	void *n;
	struct lfq_node *next;
}lfq_node;

typedef struct lfqueue{
	int size;
	int m_size;
	lfq_node *head;
	lfq_node *tail;
}lfqueue;

void lfq_init(lfqueue**,int);
int lfq_enqueue(void *,lfqueue*);
void *lfq_dequeue(lfqueue*);
void lfq_free(lfqueue*);

#endif
