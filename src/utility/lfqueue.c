#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include "utility/lfqueue.h"

void lfq_init(struct lfqueue** q, int size){
	(*q)=(lfqueue*)malloc(sizeof(lfqueue));
	(*q)->head=(lfq_node*)malloc(sizeof(lfq_node));
	(*q)->tail=(*q)->head;
	(*q)->head->n=NULL;
	(*q)->head->next=NULL;

	(*q)->size=0;
	(*q)->m_size=size;
}

int lfq_enqueue(void *val,struct lfqueue* q){
	struct lfq_node *n;
	struct lfq_node *_node=(lfq_node*)malloc(sizeof(lfq_node));
	_node->n=val;
	_node->next=NULL;
	
	while (1) {
		n = q->tail;
		if(__sync_bool_compare_and_swap((&q->size),q->m_size,q->m_size)){
			free(_node);
			return 0;
		}
		if (__sync_bool_compare_and_swap(&(n->next), NULL, _node)) {
			break;
		} else {
			__sync_bool_compare_and_swap(&(q->tail), n, n->next);
		}
	}
	__sync_bool_compare_and_swap(&(q->tail), n, _node);
	__sync_fetch_and_add(&q->size,1);
	return 1;
}

void *lfq_dequeue(struct lfqueue *q){
	struct lfq_node* n;
	void *val;
	while(1){
		n=q->head;
		if(n->next==NULL){
			return NULL;
		}
		if(__sync_bool_compare_and_swap(&(q->head),n,n->next)){
			break;
		}
	}

	val=(void*)n->next->n;
	free(n);
	__sync_fetch_and_sub(&q->size,1);
	return val;
}

void lfq_free(struct lfqueue *q){
	while(lfq_dequeue(q)){}
	free(q);
}
