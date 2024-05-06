#ifndef LINKED_PAGE_LIST_ITERATOR_PUBLIC_H
#define LINKED_PAGE_LIST_ITERATOR_PUBLIC_H

/*
** You need to ensure that there are no concurrent writes using the linked_page_list_iterator, as this may result in deadlocks.
** It is assumed that this datastructure should be used when you only tend to insert at the head or tail, OR concurrently read the whole linked_page_list to find the correct one.
*/

typedef struct linked_page_list_iterator linked_page_list_iterator;

typedef enum linked_page_list_state linked_page_list_state;
enum linked_page_list_state
{
	HEAD_ONLY_LINKED_PAGE_LIST = 1, // linked_page_list has exactly 1 node
	DUAL_NODE_LINKED_PAGE_LIST = 2, // linked_page_list has exactly 2 nodes
	MANY_NODE_LINKED_PAGE_LIST = 3  // linked_page_list has 3 or more nodes
};

linked_page_list_iterator* get_new_linked_page_list_iterator(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error);

// this returns 1, if the curr_page is locked with a WRITE_LOCK
int is_writable_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// get the current state that the linked_page_list is in
linked_page_list_state get_state_for_linked_page_list(const linked_page_list_iterator* lpli_p);

// if state == HEAD_ONLY_LINKED_PAGE_LIST and the curr_page has tuple_count == 0
int is_empty_linked_page_list(const linked_page_list_iterator* lpli_p);

// curr_page.page_id == head_page_id
int is_at_head_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// curr_page.next_page_id = head_page_id
int is_at_tail_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// !is_empty && is_at_head_page && curr_tuple_index == 0
int is_at_head_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// !is_empty && is_at_tail_page && curr_tuple_index == (curr_page.tuple_count - 1)
int is_at_tail_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// get tuple at curr_tuple_index of the curr_page for the linked_page_list iterator
const void* get_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

void delete_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error);

typedef enum linked_page_list_relative_insert_pos linked_page_list_relative_insert_pos;
enum linked_page_list_relative_insert_pos
{
	INSERT_BEFORE_LINKED_PAGE_LIST_ITERATOR = 0,
	INSERT_AFTER_LINKED_PAGE_LIST_ITERATOR = 1,
};

// insert tuple in linked_page_list at the position the linked_page_list_iterator points to, either INSERT_BEFORE or INSERT_AFTER
// on an abort error, lock on the curr_page is also released, then you only need to call delete_linked_page_list_iterator
int insert_at_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* tuple, linked_page_list_relative_insert_pos rel_pos, const void* transaction_id, int* abort_error);

// go to the next tuple of the current tuple
// returns 1 for success, it returns 0, if there are no tuples in the linked_page_list
// on an abort error, lock on the curr_page is also released, then you only need to call delete_linked_page_list_iterator
int next_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error);

// go to the prev tuple of the current tuple
// returns 1 for success, it returns 0, if there are no tuples in the linked_page_list
// on an abort error, lock on the curr_page is also released, then you only need to call delete_linked_page_list_iterator
int prev_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error);

typedef enum linked_page_list_go_after_operation linked_page_list_go_after_operation;
enum linked_page_list_go_after_operation
{
	GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION = 0,
	GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION = 1,
};

// remove the tuple that the linked_page_list_iterator is currently pointing at
// on an abort error, lock on the curr_page is also released, then you only need to call delete_linked_page_list_iterator
int remove_from_linked_page_list_iterator(linked_page_list_iterator* lpli_p, linked_page_list_go_after_operation aft_op, const void* transaction_id, int* abort_error);

// update the curr_tuple being pointed at by the linked_page_list_iterator with the parameter tuple
// on an abort error, lock on the curr_page is also released, then you only need to call delete_linked_page_list_iterator
int update_at_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* tuple, const void* transaction_id, int* abort_error);

// update a column inplace at the place that the linked_page_list_iterator is pointing to
// ADVISED 	:: only update columns that do not change the tuple size on the page, else the page may become less than half full and this can not be fixed without a merge, and you can not mrege with an iterator
//			:: also attempting to update to a element value that can increase the tuple size, may even fail, because the slot for the tuple is not big enough
// on an abort error, lock on the curr_page is also released, then you only need to call delete_linked_page_list_iterator
int update_element_in_place_at_linked_page_list_iterator(linked_page_list_iterator* lpli_p, uint32_t element_index, const user_value* element_value, const void* transaction_id, int* abort_error);

#endif