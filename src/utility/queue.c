#include <stdlib.h>
#include "utility/queue.h"

void q_init(queue **q,int qsize){
	*q=(queue*)malloc(sizeof(queue));
	(*q)->size=0;
	(*q)->head=(*q)->tail=NULL;
	pthread_mutex_init(&((*q)->q_lock),NULL);
	pthread_rwlock_init(&((*q)->rw_lock), NULL);
	(*q)->firstFlag=true;
	(*q)->m_size=qsize;
}

bool q_enqueue(void* req, queue* q){
	node *new_node=(node*)malloc(sizeof(node));
	pthread_mutex_lock(&q->q_lock);
	//pthread_rwlock_rdlock(&q->rw_lock);
	if(q->size==q->m_size){
		pthread_mutex_unlock(&q->q_lock);
		//pthread_rwlock_unlock(&q->rw_lock);
		free(new_node);
		return false;
	}
	//pthread_rwlock_unlock(&q->rw_lock);
	//pthread_rwlock_wrlock(&q->rw_lock);

	new_node->d.req=req;
	new_node->next=NULL;
	if(q->size==0){
		q->head=q->tail=new_node;
	}
	else{
		q->tail->next=new_node;
		q->tail=new_node;
	}
	q->size++;
	pthread_mutex_unlock(&q->q_lock);
	//pthread_rwlock_unlock(&q->rw_lock);
	return true;
}

bool q_enqueue_front(void *req, queue*q){
	node *new_node=(node*)malloc(sizeof(node));
	pthread_mutex_lock(&q->q_lock);
	if(q->size==q->m_size){	
		pthread_mutex_unlock(&q->q_lock);
		free(new_node);
		return false;
	}
	new_node->d.req=req;
	new_node->next=NULL;
	if(q->size==0){
		q->head=q->tail=new_node;
	}
	else{
		new_node->next=q->head;
		q->head=new_node;
	}
	q->size++;
	pthread_mutex_unlock(&q->q_lock);
	return true;
}

void* q_dequeue(queue *q){
	pthread_mutex_lock(&q->q_lock);
	//pthread_rwlock_rdlock(&q->rw_lock);
	if(!q->head || q->size==0){
		pthread_mutex_unlock(&q->q_lock);
		//pthread_rwlock_unlock(&q->rw_lock);
		return NULL;
	}
	//pthread_rwlock_unlock(&q->rw_lock);
	//pthread_rwlock_wrlock(&q->rw_lock);
	node *target_node;
	target_node=q->head;
	q->head=q->head->next;

	void *res=target_node->d.req;
	q->size--;
	pthread_mutex_unlock(&q->q_lock);
	//pthread_rwlock_unlock(&q->rw_lock);
	free(target_node);
	return res;
}

void* q_pick(queue *q){
	pthread_mutex_lock(&q->q_lock);
	if(!q->head || q->size==0){
		pthread_mutex_unlock(&q->q_lock);
		return NULL;
	}
	node *target_node;
	target_node=q->head;
	void *res=target_node->d.req;
	pthread_mutex_unlock(&q->q_lock);
	return res;
}

void q_free(queue* q){
	while(q_dequeue(q)){}
	pthread_mutex_destroy(&q->q_lock);
	free(q);
}

bool q_enqueue_int(int req, queue* q){
	node *new_node=(node*)malloc(sizeof(node));
	pthread_mutex_lock(&q->q_lock);
	if(q->size==q->m_size){
		pthread_mutex_unlock(&q->q_lock);
		free(new_node);
		return false;
	}

	new_node->d.data=req;
	new_node->next=NULL;
	if(q->size==0){
		q->head=q->tail=new_node;
	}
	else{
		q->tail->next=new_node;
		q->tail=new_node;
	}
	q->size++;
	pthread_mutex_unlock(&q->q_lock);
	return true;
}

int q_dequeue_int(queue* q){
	pthread_mutex_lock(&q->q_lock);
	if(!q->head || q->size==0){
		pthread_mutex_unlock(&q->q_lock);
		return 0;
	}
	node *target_node;
	target_node=q->head;
	q->head=q->head->next;

	int res=target_node->d.data;
	q->size--;
	pthread_mutex_unlock(&q->q_lock);
	free(target_node);
	return res;
}
