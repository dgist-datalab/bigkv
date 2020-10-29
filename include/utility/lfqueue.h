#ifndef __H_QUEUE_Q_
#define __H_QUEUE_Q_
	
#include <pthread.h>

typedef struct node{
	void *n;
	struct node *next;
}node;

typedef struct queue{
	int size;
	int m_size;
	node *head;
	node *tail;
}queue;

void lfq_init(queue**,int);
int lfq_enqueue(void *,queue*);
void *lfq_dequeue(queue*);
void lfq_free(queue*);

#endif
