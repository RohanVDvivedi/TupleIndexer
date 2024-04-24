#ifndef LINKED_PAGE_LIST_ITERATOR_PUBLIC_H
#define LINKED_PAGE_LIST_ITERATOR_PUBLIC_H

typedef struct linked_page_list_iterator linked_page_list_iterator;

typedef enum linked_page_list_state linked_page_list_state;
enum linked_page_list_state
{
	HEAD_ONLY_LINKED_PAGE_LIST = 1, // linked_page_list has exactly 1 node
	DUAL_NODE_LINKED_PAGE_LIST = 2, // linked_page_list has exactly 2 nodes
	MANY_NODE_LINKED_PAGE_LIST = 3  // linked_page_list has 3 or more nodes
};

#endif