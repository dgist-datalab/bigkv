#ifndef __H_LIST_H
#define __H_LIST_H

#include "type.h"

typedef struct list_node{
	void *data;
	struct list_node *prv;
	struct list_node *nxt;
}li_node;

typedef struct list{
	int size;
	struct list_node *head;
	struct list_node *tail;
}list;


list* list_init();
li_node *list_insert(list *, void *data);
void list_move_to_tail(list *, li_node*);
void list_move_to_head(list *, li_node*);
void list_delete_node(list *, li_node*);
void list_free(list *li);

#define list_for_each_node(li, ln)\
	for(ln=li->head; ln!=NULL; ln=ln->nxt)

#define list_for_each_node_safe(li, ln, lp)\
	for(ln=li->head, lp=li->head->nxt; ln!=NULL; ln=lp, lp=ln?ln->nxt:NULL)

#endif
