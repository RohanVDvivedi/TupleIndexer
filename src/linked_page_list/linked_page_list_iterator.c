#include<linked_page_list_iterator.h>

#include<persistent_page_functions.h>
#include<linked_page_list_node_util.h>
#include<linked_page_list_page_header.h>

#include<stdlib.h>

linked_page_list_iterator* get_new_linked_page_list_iterator(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(lpltd_p == NULL || pam_p == NULL)
		return NULL;

	// allocate enough memory
	linked_page_list_iterator* lpli_p = malloc(sizeof(linked_page_list_iterator));
	if(lpli_p == NULL)
		exit(-1);

	lpli_p->curr_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, ((pmm_p == NULL) ? READ_LOCK : WRITE_LOCK), abort_error);
	if(*abort_error)
	{
		free(lpli_p);
		return NULL;
	}
	lpli_p->curr_tuple_index = 0;
	lpli_p->head_page_id = head_page_id;
	lpli_p->lpltd_p = lpltd_p;
	lpli_p->pam_p = pam_p;
	lpli_p->pmm_p = pmm_p;

	return lpli_p;
}

int is_writable_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return lpli_p->pmm_p != NULL;
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

int is_empty_linked_page_list(const linked_page_list_iterator* lpli_p)
{
	// check if the only page of the linked_page_list is empty
	return HEAD_ONLY_LINKED_PAGE_LIST == get_state_for_linked_page_list(lpli_p)
		&& 0 == get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def));
}

int is_at_head_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return is_head_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p);
}

int is_at_tail_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return is_tail_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p);
}

int is_at_head_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	// check that the linked_page_list is not empty
	// and we must be pointing to the first tuple on the head page
	return (!is_empty_linked_page_list(lpli_p)) && is_at_head_page_linked_page_list_iterator(lpli_p)
		&& (lpli_p->curr_tuple_index == 0);
}

// !is_empty && is_at_tail_page && curr_tuple_index == (curr_page.tuple_count - 1)
int is_at_tail_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	// check that the linked_page_list is not empty
	// and we must be pointing to the last tuple on the tail page
	return (!is_empty_linked_page_list(lpli_p)) && is_at_tail_page_linked_page_list_iterator(lpli_p)
		&& (lpli_p->curr_tuple_index == (get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1));
}

const void* get_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return get_nth_tuple_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), lpli_p->curr_tuple_index);
}

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

int insert_at_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* tuple, linked_page_list_relative_insert_pos rel_pos, const void* transaction_id, int* abort_error)
{
	// fail if this is not a writable iterator
	if(!is_writable_linked_page_list_iterator(lpli_p))
		return 0;

	// can not insert a tuple greater than the record def on the page
	// if tuple == NULL, then it can always be inserted
	if(tuple != NULL && lpli_p->lpltd_p->max_record_size < get_tuple_size(lpli_p->lpltd_p->record_def, tuple))
		return 0;

	// if the linked_page_list is empty, the directly perform an insert and quit
	if(is_empty_linked_page_list(lpli_p))
	{
		append_tuple_on_persistent_page_resiliently(lpli_p->pmm_p, transaction_id, &(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), tuple, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		lpli_p->curr_tuple_index = 0;
		return 1;
	}

	// calculate insertion index for this new tuple
	uint32_t insert_at_pos = lpli_p->curr_tuple_index + rel_pos;

	// update the curr_tuple_index
	lpli_p->curr_tuple_index += (1U - rel_pos);

	// perform a resilient insert at that index
	int inserted = insert_tuple_on_persistent_page_resiliently(lpli_p->pmm_p, transaction_id, &(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), insert_at_pos, tuple, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	// if inserted, return success
	if(inserted)
		return 1;

	// a resilient insert failed, so now we will need to split this page

	// if the page is head page and not a tail page, then keep FULL_LOWER_HALF
	// else if the page is tail page and not a head page, then keep FULL_UPPER_HALF
	int split_organization = EQUAL_SPLIT;
	if(is_head_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p) && !is_tail_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p))
		split_organization = FULL_LOWER_HALF;
	else if(!is_head_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p) && is_tail_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p))
		split_organization = FULL_UPPER_HALF;

	// split the contents of the curr_page, with the later/tail contents going to new_page
	persistent_page new_page = split_insert_bplus_tree_interior_page(&(lpli_p->curr_page), tuple, insert_at_pos, SPLIT_LOWER_HALF, split_organization, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// add new_page into the linked_page_list, right after the curr_page
	switch(get_state_for_linked_page_list(lpli_p))
	{
		case HEAD_ONLY_LINKED_PAGE_LIST :
		{
			insert_page_in_between_linked_page_list(&(lpli_p->curr_page), &(lpli_p->curr_page),  &new_page, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}
			break;
		}
		case DUAL_NODE_LINKED_PAGE_LIST :
		case MANY_NODE_LINKED_PAGE_LIST :
		{
			// grab lock on the next_page of the curr_page
			persistent_page next_page = lock_and_get_next_page_in_linked_page_list(&(lpli_p->curr_page), WRITE_LOCK, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// insert new_page, between curr_page and next_page
			insert_page_in_between_linked_page_list(&(lpli_p->curr_page), &next_page,  &new_page, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}

			// release lock on next_page
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
				goto ABORT_ERROR;
			}
			break;
		}
	}

	// decide which of lpli_p->curr_page or new_page becomes the new curr_page
	uint32_t new_curr_page_tuple_count = ;

	// if curr_tuple_index is within bounds, then the curr_page remains as is
	if(lpli_p->curr_tuple_index < new_curr_page_tuple_count)
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}
	// else new_page becomes the curr_page
	else
	{
		lpli_p->curr_tuple_index -= new_curr_page_tuple_count;
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &new_page, NONE_OPTION, abort_error);
			return 0;
		}

		lpli_p->curr_page = new_page;
		new_page = get_NULL_persistent_page(lpli_p->pam_p);
	}

	return 1;

	// on an abort error, relase lock on curr_page and exit
	ABORT_ERROR:
	release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
	return 0;
}