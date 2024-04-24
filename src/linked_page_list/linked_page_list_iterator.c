#include<linked_page_list_iterator.h>

#include<persistent_page_functions.h>
#include<linked_page_list_node_util.h>

#include<stdlib.h>

linked_page_list_iterator* get_new_linked_page_list_iterator(uint64_t head_page_id, int lock_type, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(lpltd_p == NULL || pam_p == NULL)
		return NULL;

	// allocate enough memory
	linked_page_list_iterator* lpli_p = malloc(sizeof(linked_page_list_iterator));
	if(lpli_p == NULL)
		exit(-1);

	lpli_p->curr_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, lock_type, abort_error);
	if(*abort_error)
	{
		free(lpli_p);
		return NULL;
	}
	lpli_p->curr_tuple_index = 0;
	lpli_p->head_page_id = head_page_id;
	lpli_p->lpltd_p = lpltd_p;
	lpli_p->pam_p = pam_p;

	return lpli_p;
}

int is_writable_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return is_persistent_page_write_locked(&(lpli_p->curr_page));
}

linked_page_list_state get_state_for_linked_page_list(const linked_page_list_iterator* lpli_p)
{
	if(is_only_head_linked_page_list(&(lpli_p->curr_page), lpli_p->lpltd_p))
		return HEAD_ONLY_LINKED_PAGE_LIST;
	else if(is_dual_node_linked_page_list(&(lpli_p->curr_page), lpli_p->lpltd_p))
		return DUAL_NODE_LINKED_PAGE_LIST;
	else
		return MANY_NODE_LINKED_PAGE_LIST;
}

// if state == HEAD_ONLY_LINKED_PAGE_LIST and the curr_page has tuple_count == 0
int is_empty_linked_page_list(const linked_page_list_iterator* lpli_p)
{
	return HEAD_ONLY_LINKED_PAGE_LIST == get_state_for_linked_page_list(lpli_p)
		&& 0 == get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def));
}

// curr_page.page_id == head_page_id
int is_at_head_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// curr_page.next_page_id = head_page_id
int is_at_tail_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// !is_empty && is_at_head_page && curr_tuple_index == 0
int is_at_head_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

// !is_empty && is_at_tail_page && curr_tuple_index == (curr_page.tuple_count - 1)
int is_at_tail_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p);

void delete_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	if(!is_persistent_page_NULL(&(lpli_p->curr_page), lpli_p->pam_p))
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			// even if there is an abort_error, there is nothing else that you need to do here
			// you must ofcourse free the allocated memory
		}
	}
	free(lpli_p);
}