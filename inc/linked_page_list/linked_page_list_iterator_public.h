#ifndef LINKED_PAGE_LIST_ITERATOR_PUBLIC_H
#define LINKED_PAGE_LIST_ITERATOR_PUBLIC_H

/*
** You need to ensure that that is no concurrent writes using the linked_page_list_iterator
** As this may result in deadlocks.
*/

typedef struct linked_page_list_iterator linked_page_list_iterator;

typedef enum linked_page_list_state linked_page_list_state;
enum linked_page_list_state
{
	HEAD_ONLY_LINKED_PAGE_LIST = 1, // linked_page_list has exactly 1 node
	DUAL_NODE_LINKED_PAGE_LIST = 2, // linked_page_list has exactly 2 nodes
	MANY_NODE_LINKED_PAGE_LIST = 3  // linked_page_list has 3 or more nodes
};

linked_page_list_iterator* get_new_linked_page_list_iterator(uint64_t head_page_id, int lock_type, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error);

// get the current state that the linked_page_list is in
linked_page_list_state get_state_for_linked_page_list(linked_page_list_iterator* lpli_p);

// if state == HEAD_ONLY_LINKED_PAGE_LIST and the curr_page has tuple_count == 0
int is_empty_linked_page_list(linked_page_list_iterator* lpli_p);

// curr_page.page_id == head_page_id
int is_at_head_page_linked_page_list_iterator(linked_page_list_iterator* lpli_p);

// curr_pahe.next_page_id = head_page_id
int is_at_tail_page_linked_page_list_iterator(linked_page_list_iterator* lpli_p);

// !is_empty && is_at_head_page && curr_tuple_index == 0
int is_at_head_tuple_linked_page_list_iterator(linked_page_list_iterator* lpli_p);

// !is_empty && is_at_tail_page && curr_tuple_index == (curr_page.tuple_count - 1)
int is_at_tail_tuple_linked_page_list_iterator(linked_page_list_iterator* lpli_p);

void delete_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error);

#endif