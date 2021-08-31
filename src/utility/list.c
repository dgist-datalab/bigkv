#include"utility/list.h"
#include <stdlib.h>

list* list_init(){
	list *res=(list*)malloc(sizeof(list));

	res->size=0;
	res->head=res->tail=NULL;
	return res;
}

inline li_node *new_li_node(void *data){
	li_node* res=(li_node*)malloc(sizeof(li_node));
	res->data=data;
	res->prv=res->nxt=NULL;
	return res;
}


li_node *list_insert(list *li, void *data){
	li_node *t=new_li_node(data);
	li->size++;
	if(!li->head){
		li->head=li->tail=t;
		return t;
	}
	
	t->prv=li->tail;
	li->tail->nxt=t;
	li->tail=t;
	return t;
}

void list_move_to_tail(list *li, li_node* t){
	if (t==li->tail)
		return;
	else if (t==li->head) {
		li->head=li->head->nxt;
		if(li->head)
			li->head->prv=NULL;
	} else {
		li_node *prv=t->prv, *nxt=t->nxt;
		prv->nxt=nxt;
		nxt->prv=prv;
	}
	t->prv=li->tail;
	t->nxt=NULL;
	li->tail->nxt=t;
	li->tail=t;
}

void list_move_to_head(list *li, li_node* t){
	if (t==li->head)
		return;
	else if (t==li->tail) {
		li->tail=li->tail->prv;
		if(li->tail)
			li->tail->nxt=NULL;
	} else {
		li_node *prv=t->prv, *nxt=t->nxt;
		prv->nxt=nxt;
		nxt->prv=prv;
	}
	t->nxt=li->head;
	t->prv=NULL;
	li->head->prv=t;
	li->head=t;
}



void list_delete_node(list *li, li_node* t){
	if(t==li->head){
		li->head=li->head->nxt;
		if(li->head)
			li->head->prv=NULL;
	}
	else if(t==li->tail){
		li->tail=li->tail->prv;
		if(li->tail)
			li->tail->nxt=NULL;
	}
	else{
		li_node *prv=t->prv, *nxt=t->nxt;
		prv->nxt=nxt;
		nxt->prv=prv;	
	}	
	li->size--;
	free(t);
}

void list_free(list *li){
	li_node *now, *nxt;
	if(li->size){
		list_for_each_node_safe(li,now,nxt){
			list_delete_node(li,now);
		}
	}
	free(li);
}
