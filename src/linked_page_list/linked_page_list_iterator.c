#include<linked_page_list_iterator.h>

#include<persistent_page_functions.h>
#include<linked_page_list_page_header.h>
#include<linked_page_list_node_util.h>
#include<linked_page_list_page_tuples_util.h>

#include<stdlib.h>

// this function will never modify page_ref or the iterator
#define get_from_ref(page_ref) (((page_ref)->points_to_iterator_head) ? (&(lpli_p->head_page)) : (&((page_ref)->non_head_page)))

#define get_NULL_persistent_page_reference(pam_p) ((persistent_page_reference){.points_to_iterator_head = 0, .non_head_page = get_NULL_persistent_page(pam_p)})

static int release_lock_on_reference_while_holding_head_lock(persistent_page_reference* page_ref, const linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	if(page_ref->points_to_iterator_head) // make it not point to the iterator head
	{
		page_ref->points_to_iterator_head = 0;
		page_ref->non_head_page = get_NULL_persistent_page(lpli_p->pam_p);
		return 1;
	}
	else
		return release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(page_ref->non_head_page), NONE_OPTION, abort_error);
}

static persistent_page_reference lock_and_get_next_page_reference(persistent_page_reference* page_ref, int lock_type, linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	// if the user is requesting a head page, then return a reference to it
	if(is_next_of_in_linked_page_list(get_from_ref(page_ref), &(lpli_p->head_page), lpli_p->lpltd_p))
		return (persistent_page_reference){
			.points_to_iterator_head = 1,
			.non_head_page = get_NULL_persistent_page(lpli_p->pam_p)
		};
	else
		return (persistent_page_reference){
			.points_to_iterator_head = 0,
			.non_head_page = lock_and_get_next_page_in_linked_page_list(get_from_ref(page_ref), lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error)
		};
}

static persistent_page_reference lock_and_get_prev_page_reference(persistent_page_reference* page_ref, int lock_type, linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	// if the user is requesting a head page, then return a reference to it
	if(is_prev_of_in_linked_page_list(get_from_ref(page_ref), &(lpli_p->head_page), lpli_p->lpltd_p))
		return (persistent_page_reference){
			.points_to_iterator_head = 1,
			.non_head_page = get_NULL_persistent_page(lpli_p->pam_p)
		};
	else
		return (persistent_page_reference){
			.points_to_iterator_head = 0,
			.non_head_page = lock_and_get_prev_page_in_linked_page_list(get_from_ref(page_ref), lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error)
		};
}

linked_page_list_iterator* get_new_linked_page_list_iterator(uint64_t head_page_id, const linked_page_list_tuple_defs* lpltd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// the following 2 must be present
	if(lpltd_p == NULL || pam_p == NULL)
		return NULL;

	// allocate enough memory
	linked_page_list_iterator* lpli_p = malloc(sizeof(linked_page_list_iterator));
	if(lpli_p == NULL)
		exit(-1);

	lpli_p->head_page = acquire_persistent_page_with_lock(pam_p, transaction_id, head_page_id, ((pmm_p == NULL) ? READ_LOCK : WRITE_LOCK), abort_error);
	if(*abort_error)
	{
		free(lpli_p);
		return NULL;
	}
	lpli_p->curr_page = (persistent_page_reference){.points_to_iterator_head = 1, .non_head_page = get_NULL_persistent_page(pam_p)};
	lpli_p->curr_tuple_index = 0;
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
	if(is_only_head_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p))
		return HEAD_ONLY_LINKED_PAGE_LIST;
	else if(is_dual_node_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p))
		return DUAL_NODE_LINKED_PAGE_LIST;
	else
		return MANY_NODE_LINKED_PAGE_LIST;
}

int is_empty_linked_page_list(const linked_page_list_iterator* lpli_p)
{
	// check if the only page of the linked_page_list is empty
	return HEAD_ONLY_LINKED_PAGE_LIST == get_state_for_linked_page_list(lpli_p)
		&& 0 == get_tuple_count_on_persistent_page(get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def));
}

int is_at_head_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return is_head_page_of_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->head_page.page_id, lpli_p->lpltd_p);
}

int is_at_tail_page_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return is_tail_page_of_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->head_page.page_id, lpli_p->lpltd_p);
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
		&& (lpli_p->curr_tuple_index == (get_tuple_count_on_persistent_page(get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1));
}

const void* get_tuple_linked_page_list_iterator(const linked_page_list_iterator* lpli_p)
{
	return get_nth_tuple_on_persistent_page(get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), lpli_p->curr_tuple_index);
}

void delete_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	if(!is_persistent_page_NULL(&(lpli_p->head_page), lpli_p->pam_p))
	{
		release_lock_on_reference_while_holding_head_lock(&(lpli_p->curr_page), lpli_p, transaction_id, abort_error);
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->head_page), NONE_OPTION, abort_error);
		// even if any of the above function gives an abort error, we still will need to release the other lock, so not handling abort error here is the best thing to be done
	}
	free(lpli_p);
}

// before calling this function, we assume that lpli_p->curr_tuple_index has been set to the correct value as if the insertion has already been done
// the tuple will be inserted at insert_at_pos on the curr_page
// after insertion OR split insertion, the linked_page_list iterator is adjusted to make the iterator point to curr_tuple_index
// the curr_tuple_index and the insert_at_pos, but must be in range [0, total_tuple_count), where total_tuple_count is the tuple count of the curr_page after a successfull insertion
// on an abort error, it releases all locks including the lock on the curr_page of the iterator, making the iterator unusable
int insert_OR_split_insert_on_page_of_linked_page_list(linked_page_list_iterator* lpli_p, uint32_t insert_at_pos, const void* tuple, const void* transaction_id, int* abort_error)
{
	// the curr_tuple_index and the insert_at_pos, but must be in range [0, total_tuple_count)
	// total_tuple_count is the tuple count of the curr_page after a successfull insertion
	// total_tuple_count = tuple_count(curr_page) + 1
	// passing a wrong value for these parameters defeats the purpose of this function
	uint32_t total_tuple_count = get_tuple_count_on_persistent_page(get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) + 1;
	if(total_tuple_count <= insert_at_pos)
		return 0;
	if(total_tuple_count <= lpli_p->curr_tuple_index)
		return 0;

	// perform a resilient insert at that index
	int inserted = insert_tuple_on_persistent_page_resiliently(lpli_p->pmm_p, transaction_id, get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), insert_at_pos, tuple, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	// if inserted, return success
	if(inserted)
		return 1;

	// a resilient insert failed, so now we will need to split this page

	// if the page is head page and not a tail page, then keep FULL_LOWER_HALF
	// else if the page is tail page and not a head page, then keep FULL_UPPER_HALF
	int split_organization = EQUAL_SPLIT;
	if(is_head_page_of_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->head_page.page_id, lpli_p->lpltd_p) && !is_tail_page_of_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->head_page.page_id, lpli_p->lpltd_p))
		split_organization = FULL_LOWER_HALF;
	else if(!is_head_page_of_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->head_page.page_id, lpli_p->lpltd_p) && is_tail_page_of_linked_page_list(get_from_ref(&(lpli_p->curr_page)), lpli_p->head_page.page_id, lpli_p->lpltd_p))
		split_organization = FULL_UPPER_HALF;

	// split the contents of the curr_page, with the later/tail contents going to new_page
	persistent_page_reference new_page = {
		.points_to_iterator_head = 0,
		.non_head_page = split_insert_bplus_tree_interior_page(get_from_ref(&(lpli_p->curr_page)), tuple, insert_at_pos, SPLIT_LOWER_HALF, split_organization, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error)
	};
	if(*abort_error)
		goto ABORT_ERROR;

	// add new_page into the linked_page_list, right after the curr_page
	switch(get_state_for_linked_page_list(lpli_p))
	{
		case HEAD_ONLY_LINKED_PAGE_LIST :
		{
			insert_page_in_between_linked_page_list(get_from_ref(&(lpli_p->curr_page)), get_from_ref(&(lpli_p->curr_page)), get_from_ref(&new_page), lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_reference_while_holding_head_lock(&new_page, lpli_p, transaction_id, abort_error);
				goto ABORT_ERROR;
			}
			break;
		}
		case DUAL_NODE_LINKED_PAGE_LIST :
		case MANY_NODE_LINKED_PAGE_LIST :
		{
			// grab lock on the next_page of the curr_page
			persistent_page_reference next_page = lock_and_get_next_page_reference(&(lpli_p->curr_page), WRITE_LOCK, lpli_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_reference_while_holding_head_lock(&new_page, lpli_p, transaction_id, abort_error);
				goto ABORT_ERROR;
			}

			// insert new_page, between curr_page and next_page
			insert_page_in_between_linked_page_list(get_from_ref(&(lpli_p->curr_page)), get_from_ref(&next_page),  get_from_ref(&new_page), lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_reference_while_holding_head_lock(&next_page, lpli_p, transaction_id, abort_error);
				release_lock_on_reference_while_holding_head_lock(&new_page, lpli_p, transaction_id, abort_error);
				goto ABORT_ERROR;
			}

			// release lock on next_page
			release_lock_on_reference_while_holding_head_lock(&next_page, lpli_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_reference_while_holding_head_lock(&new_page, lpli_p, transaction_id, abort_error);
				goto ABORT_ERROR;
			}
			break;
		}
	}

	// decide which of lpli_p->curr_page or new_page becomes the new curr_page

	// for this grab the new tuple_count on curr_page
	uint32_t new_curr_page_tuple_count = get_tuple_count_on_persistent_page(get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def));

	// if curr_tuple_index is within bounds, then the curr_page remains as is
	if(lpli_p->curr_tuple_index < new_curr_page_tuple_count)
	{
		release_lock_on_reference_while_holding_head_lock(&new_page, lpli_p, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}
	// else new_page becomes the curr_page
	else
	{
		lpli_p->curr_tuple_index -= new_curr_page_tuple_count;
		release_lock_on_reference_while_holding_head_lock(&(lpli_p->curr_page), lpli_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_lock_on_reference_while_holding_head_lock(&new_page, lpli_p, transaction_id, abort_error);
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->head_page), NONE_OPTION, abort_error);
			return 0;
		}

		// such an assignment for persistent_page_reference works
		lpli_p->curr_page = new_page;
		new_page = get_NULL_persistent_page_reference(lpli_p->pam_p);
	}

	return 1;

	// on an abort error, relase lock on curr_page and exit
	ABORT_ERROR:
	release_lock_on_reference_while_holding_head_lock(&(lpli_p->curr_page), lpli_p, transaction_id, abort_error);
	release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->head_page), NONE_OPTION, abort_error);
	return 0;
}

int insert_at_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* tuple, linked_page_list_relative_insert_pos rel_pos, const void* transaction_id, int* abort_error)
{
	// fail if this is not a writable iterator
	if(!is_writable_linked_page_list_iterator(lpli_p))
		return 0;

	// can not insert a tuple greater than the max_record_size on the page
	// if tuple == NULL, then it can always be inserted
	if(tuple != NULL && lpli_p->lpltd_p->max_record_size < get_tuple_size(lpli_p->lpltd_p->record_def, tuple))
		return 0;

	// if the linked_page_list is empty, the directly perform an insert and quit
	if(is_empty_linked_page_list(lpli_p))
	{
		append_tuple_on_persistent_page_resiliently(lpli_p->pmm_p, transaction_id, get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), tuple, abort_error);
		if(*abort_error)
		{
			release_lock_on_reference_while_holding_head_lock(&(lpli_p->curr_page), lpli_p, transaction_id, abort_error);
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->head_page), NONE_OPTION, abort_error);
			return 0;
		}
		lpli_p->curr_tuple_index = 0;
		return 1;
	}

	// calculate insert_at_pos and curr_tuple_index as if the tuple has already been inserted

	// calculate insertion index for this new tuple
	uint32_t insert_at_pos = lpli_p->curr_tuple_index + rel_pos;

	// update the curr_tuple_index
	lpli_p->curr_tuple_index += (1U - rel_pos);

	// perform a insert / split_insert at that index
	// only an abort error or a invalid parameter can result in failure
	int result = insert_OR_split_insert_on_page_of_linked_page_list(lpli_p, insert_at_pos, tuple, transaction_id, abort_error);
	if(*abort_error)
		return 0;
	return result;
}

// merges DUAL_NODE_LINKED_PAGE_LIST's pages in to a HEAD_ONLY_LINKED_PAGE_LIST
// it will also make curr_page point to the only head, and curr_tuple_index point to the same tuple it was pointing to (if it was valid prior to this call)
// on an abort error, it releases all locks including the lock on the curr_page of the iterator, making the iterator unusable
static int merge_dual_nodes_into_only_head(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	// fail if this is not a writable iterator
	if(!is_writable_linked_page_list_iterator(lpli_p))
		return 0;

	// this function only meant to deal with DUAL_NODE_LINKED_PAGE_LIST, hence the check
	if(get_state_for_linked_page_list(lpli_p) != DUAL_NODE_LINKED_PAGE_LIST)
		return 0;

	// take lock on the other page, this will be the next/prev page of the curr_page
	persistent_page other_page = lock_and_get_next_page_in_linked_page_list(&(lpli_p->curr_page), WRITE_LOCK, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		return 0;
	}

	// check if they can be merged, they are already adjacent pages
	if(!can_merge_linked_page_list_pages(&(lpli_p->curr_page), &other_page, lpli_p->lpltd_p))
	{
		// if not release lock on the other page and quit with 0
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &other_page, NONE_OPTION, abort_error);
		return 0;
	}

	// now we are sure that curr_page and other_page can be merged
	// we will merge both the pages and put them into the head page
	// and this head page becomes the curr_page of this linked_page_list_iterator

	// so now we make head_page the curr_page, and tail page the other_page
	// the curr_page must be the head_page, if not swap curr_page and other_page
	if(!is_head_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p))
	{
		persistent_page temp = lpli_p->curr_page;
		lpli_p->curr_page = other_page;
		other_page = temp;

		// now curr_page is head_page and other_page is tail_page

		// if the curr_page is not the head page, then curr_tuple_index += get_tuple_count(&head_page),
		// as we know now head_page is the curr_page
		// this is the final value of curr_tuple_index after the merge
		// note:: we do not need to do this if the curr_page was already the head_page
		lpli_p->curr_tuple_index += get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def));
	}

	// now merge the curr_page and other_page, into the curr_page
	merge_linked_page_list_pages(&(lpli_p->curr_page), &other_page, MERGE_INTO_PAGE1, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &other_page, NONE_OPTION, abort_error);
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		return 0;
	}

	// remove other_page from the linked_page_list
	remove_page_from_between_linked_page_list(&(lpli_p->curr_page), &other_page, &(lpli_p->curr_page), lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &other_page, NONE_OPTION, abort_error);
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		return 0;
	}

	// now we may free the other_page
	release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &other_page, FREE_PAGE, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &other_page, NONE_OPTION, abort_error);
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		return 0;
	}

	// we already fixed the curr_tuple_index, remember??

	return 1;
}

// discards the curr_page of the linked_page_list_iterator if it is empty
// this function fails if the curr_page is not empty, OR if the empty page is the only head of the linked_page_list
// after the empty page is discarded it will make the iterator point to first_tuple after discarded page if aft_op = GO_NEXT_AFTER_*_OPERATION,
// else it will point to the immediate previous tuple
// on an abort error, it releases all locks including the lock on the curr_page of the iterator, making the iterator unusable
static int discard_curr_page_if_empty(linked_page_list_iterator* lpli_p, linked_page_list_go_after_operation aft_op, const void* transaction_id, int* abort_error)
{
	// fail if this is not a writable iterator
	if(!is_writable_linked_page_list_iterator(lpli_p))
		return 0;

	// if the page is not empty, then quit with 0
	if(0 != get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)))
		return 0;

	switch(get_state_for_linked_page_list(lpli_p))
	{
		// head of a head only empty linked_page_list, can not be removed
		case HEAD_ONLY_LINKED_PAGE_LIST :
			return 0;
		case DUAL_NODE_LINKED_PAGE_LIST :
		{
			// on an abort error here, nothing needs to be done
			merge_dual_nodes_into_only_head(lpli_p, transaction_id, abort_error);
			if(*abort_error)
				return 0;

			// fix the curr_tuple_index
			switch(aft_op)
			{
				case GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION :
				{
					lpli_p->curr_tuple_index = 0;
					break;
				}
				case GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION :
				{
					lpli_p->curr_tuple_index = get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1;
					break;
				}
			}

			return 1;
		}
		case MANY_NODE_LINKED_PAGE_LIST :
		{
			// if curr_page is head page, then clone contents of the 2nd page into the head page and remove that 2nd page instead
			if(is_head_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p))
			{
				// grab lock on the next_page
				persistent_page next_page = lock_and_get_next_page_in_linked_page_list(&(lpli_p->curr_page), WRITE_LOCK, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// grab lock on the next_next_page
				persistent_page next_next_page = lock_and_get_next_page_in_linked_page_list(&next_page, WRITE_LOCK, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// move contents of next_page to curr_page
				merge_linked_page_list_pages(&(lpli_p->curr_page), &next_page, MERGE_INTO_PAGE1, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// remove next_page from the linked_page_list
				remove_page_from_between_linked_page_list(&(lpli_p->curr_page), &next_page, &next_next_page, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// free the next_page
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, FREE_PAGE, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// next_page is not freed and must not be accessed

				// release lock on the next_next_page
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// now you only have lock on the curr_page, which is infact the next page
				switch(aft_op)
				{
					// make iterator point to head of the linked_page_list
					case GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION :
					{
						lpli_p->curr_tuple_index = 0;
						break;
					}
					case GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION :
					{
						// grab lock on the prev_page
						persistent_page prev_page = lock_and_get_prev_page_in_linked_page_list(&(lpli_p->curr_page), WRITE_LOCK, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
						if(*abort_error)
						{
							release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
							return 0;
						}

						// release lock on the curr_page
						release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
						if(*abort_error)
						{
							release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
							return 0;
						}

						lpli_p->curr_page = prev_page; prev_page = get_NULL_persistent_page(lpli_p->pam_p);
						lpli_p->curr_tuple_index = get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1;
						break;
					}
				}

				return 1;
			}
			// else lock prev_page and curr_page, then discard the curr_page from their between
			else
			{
				// grab lock on the next_page
				persistent_page next_page = lock_and_get_next_page_in_linked_page_list(&(lpli_p->curr_page), WRITE_LOCK, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// grab lock on the prev_page
				persistent_page prev_page = lock_and_get_prev_page_in_linked_page_list(&(lpli_p->curr_page), WRITE_LOCK, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// remove the curr_page from the between prev_page and next_page
				remove_page_from_between_linked_page_list(&prev_page, &(lpli_p->curr_page), &next_page, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// free the curr_page
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), FREE_PAGE, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// now we only have lock on the next_page and prev_page, the iterator's curr_page is freed
				// we will make the iterator point accoding to the passed aft_op option

				// fix the curr_tuple_index and curr_page (curr_page is NULL_persistent_page as of now)
				switch(aft_op)
				{
					case GO_NEXT_AFTER_LINKED_PAGE_ITERATOR_OPERATION :
					{
						// lock on prev_page is not needed
						release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
						if(*abort_error)
						{
							release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
							return 0;
						}

						lpli_p->curr_page = next_page; next_page = get_NULL_persistent_page(lpli_p->pam_p);
						lpli_p->curr_tuple_index = 0;
						break;
					}
					case GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION :
					{
						// lock on next_page is not needed
						release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
						if(*abort_error)
						{
							release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
							return 0;
						}

						lpli_p->curr_page = prev_page; prev_page = get_NULL_persistent_page(lpli_p->pam_p);
						lpli_p->curr_tuple_index = get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1;
						break;
					}
				}

				return 1;
			}
		}
	}

	return 0;
}

int next_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	// if the linked_page_list is empty, the fail
	if(is_empty_linked_page_list(lpli_p))
		return 0;

	// increment curr_tuple_index
	lpli_p->curr_tuple_index++;

	// if the curr_tuple_index is within bounds, then return with a success
	if(0 <= lpli_p->curr_tuple_index && lpli_p->curr_tuple_index < get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)))
		return 1;

	// if it is HEAD_ONLY_LINKED_PAGE_LIST, then we don't have to go next
	if(get_state_for_linked_page_list(lpli_p) == HEAD_ONLY_LINKED_PAGE_LIST)
	{
		lpli_p->curr_tuple_index = 0;
		return 1;
	}

	// merge must not be attempted, for a HEAD_ONLY linked_page_list OR a read-only iterator OR if the curr_page is tail
	int must_not_attempt_merge = (!is_writable_linked_page_list_iterator(lpli_p)) ||
								(is_tail_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p));

	int may_attempt_merge = !must_not_attempt_merge;

	// lock types to acquire
	int lock_type = is_writable_linked_page_list_iterator(lpli_p) ? WRITE_LOCK : READ_LOCK;

	switch(get_state_for_linked_page_list(lpli_p))
	{
		case DUAL_NODE_LINKED_PAGE_LIST :
		{
			if(may_attempt_merge)
			{
				// revert back to original state of the curr_tuple_index, before this next call
				lpli_p->curr_tuple_index--;

				// attempt a merge
				int merged = merge_dual_nodes_into_only_head(lpli_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;

				// if merged, now it is HEAD_ONLY_LINKED_PAGE_LIST, we can delegate the call
				if(merged)
				{
					next_linked_page_list_iterator(lpli_p, transaction_id, abort_error);
					if(*abort_error)
						return 0;
					return 1;
				}
				else
					lpli_p->curr_tuple_index++;
			}

			// if somehow we couldn't merge, then simply get next_page and point to its first tuple

			// grab lock on the next page
			persistent_page next_page = lock_and_get_next_page_in_linked_page_list(&(lpli_p->curr_page), lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}

			// now we can release lock on the curr_page
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
				return 0;
			}

			// make curr_page the next_page
			lpli_p->curr_page = next_page; next_page = get_NULL_persistent_page(lpli_p->pam_p);
			lpli_p->curr_tuple_index = 0;

			return 1;
		}
		case MANY_NODE_LINKED_PAGE_LIST :
		{
			// grab lock on the next page
			persistent_page next_page = lock_and_get_next_page_in_linked_page_list(&(lpli_p->curr_page), lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}

			// test if we can merge
			if(may_attempt_merge && can_merge_linked_page_list_pages(&(lpli_p->curr_page), &next_page, lpli_p->lpltd_p))
			{
				// grab lock on the next_next_page
				persistent_page next_next_page = lock_and_get_next_page_in_linked_page_list(&next_page, lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// merge curr_page and next_page into curr_page
				// since we already tested that they can be merged, we are sure that it either succeeds or aborts
				merge_linked_page_list_pages(&(lpli_p->curr_page), &next_page, MERGE_INTO_PAGE1, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// remove next_page from between curr_page and next_next_page
				remove_page_from_between_linked_page_list(&(lpli_p->curr_page), &next_page, &next_next_page, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// free next_page
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, FREE_PAGE, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// release lock on next_next_page
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_next_page, NONE_OPTION, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// curr_tuple_index remains as is

				return 1;
			}

			// now we can release lock on the curr_page
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
				return 0;
			}

			// make curr_page the next_page
			lpli_p->curr_page = next_page; next_page = get_NULL_persistent_page(lpli_p->pam_p);
			lpli_p->curr_tuple_index = 0;

			return 1;
		}
		default : // this will never occur
			return 0;
	}
}

int prev_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* transaction_id, int* abort_error)
{
	// if the linked_page_list is empty, the fail
	if(is_empty_linked_page_list(lpli_p))
		return 0;

	// decrement curr_tuple_index
	lpli_p->curr_tuple_index--;

	// if the curr_tuple_index is within bounds, then return with a success
	if(0 <= lpli_p->curr_tuple_index && lpli_p->curr_tuple_index < get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)))
		return 1;

	// if it is HEAD_ONLY_LINKED_PAGE_LIST, then we don't have to go prev
	if(get_state_for_linked_page_list(lpli_p) == HEAD_ONLY_LINKED_PAGE_LIST)
	{
		lpli_p->curr_tuple_index = get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1;
		return 1;
	}

	// merge must not be attempted, for a HEAD_ONLY linked_page_list OR a read-only iterator OR if the curr_page is head
	int must_not_attempt_merge = (!is_writable_linked_page_list_iterator(lpli_p)) ||
								(is_head_page_of_linked_page_list(&(lpli_p->curr_page), lpli_p->head_page_id, lpli_p->lpltd_p));

	int may_attempt_merge = !must_not_attempt_merge;

	// lock types to acquire
	int lock_type = is_writable_linked_page_list_iterator(lpli_p) ? WRITE_LOCK : READ_LOCK;

	switch(get_state_for_linked_page_list(lpli_p))
	{
		case DUAL_NODE_LINKED_PAGE_LIST :
		{
			if(may_attempt_merge)
			{
				// revert back to original state of the curr_tuple_index, before this next call
				lpli_p->curr_tuple_index++;

				// attempt a merge
				int merged = merge_dual_nodes_into_only_head(lpli_p, transaction_id, abort_error);
				if(*abort_error)
					return 0;

				// if merged, now it is HEAD_ONLY_LINKED_PAGE_LIST, we can delegate the call
				if(merged)
				{
					prev_linked_page_list_iterator(lpli_p, transaction_id, abort_error);
					if(*abort_error)
						return 0;
					return 1;
				}
				else
					lpli_p->curr_tuple_index--;
			}

			// if somehow we couldn't merge, then simply get prev_page and point to its last tuple

			// grab lock on the prev page
			persistent_page prev_page = lock_and_get_prev_page_in_linked_page_list(&(lpli_p->curr_page), lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}

			// now we can release lock on the curr_page
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
				return 0;
			}

			// make curr_page the next_page
			lpli_p->curr_page = prev_page; prev_page = get_NULL_persistent_page(lpli_p->pam_p);
			lpli_p->curr_tuple_index = get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1;

			return 1;
		}
		case MANY_NODE_LINKED_PAGE_LIST :
		{
			// grab lock on the prev page
			persistent_page prev_page = lock_and_get_prev_page_in_linked_page_list(&(lpli_p->curr_page), lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}

			// test if we can merge
			if(may_attempt_merge && can_merge_linked_page_list_pages(&prev_page, &(lpli_p->curr_page), lpli_p->lpltd_p))
			{
				// cahce the tuple_count of the prev_page
				uint32_t original_tuple_count_prev_page = get_tuple_count_on_persistent_page(&prev_page, lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def));

				// grab lock on the prev_prev_page
				persistent_page prev_prev_page = lock_and_get_prev_page_in_linked_page_list(&prev_page, lock_type, lpli_p->lpltd_p, lpli_p->pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// merge prev_page and curr_page into curr_page
				merge_linked_page_list_pages(&prev_page, &(lpli_p->curr_page), MERGE_INTO_PAGE2, lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// remove prev_page from between prev_prev_page and curr_page
				remove_page_from_between_linked_page_list(&prev_prev_page, &prev_page, &(lpli_p->curr_page), lpli_p->lpltd_p, lpli_p->pam_p, lpli_p->pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// free prev_page
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, FREE_PAGE, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// release lock on prev_prev_page
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_prev_page, NONE_OPTION, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
					return 0;
				}

				// adjust curr_tuple_index to point to the immediate new_tuple
				lpli_p->curr_tuple_index = original_tuple_count_prev_page - 1;

				return 1;
			}

			// now we can release lock on the curr_page
			release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
				return 0;
			}

			// make curr_page the next_page
			lpli_p->curr_page = prev_page; prev_page = get_NULL_persistent_page(lpli_p->pam_p);
			lpli_p->curr_tuple_index = get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1;

			return 1;
		}
		default : // this will never occur
			return 0;
	}
}

int remove_from_linked_page_list_iterator(linked_page_list_iterator* lpli_p, linked_page_list_go_after_operation aft_op, const void* transaction_id, int* abort_error)
{
	// if not writable, we can not discard the current tuple
	if(!is_writable_linked_page_list_iterator(lpli_p))
		return 0;

	// if the linked_page_list is empty then fail, removal
	if(is_empty_linked_page_list(lpli_p))
		return 0;

	// discard current tuple
	discard_tuple_on_persistent_page(lpli_p->pmm_p, transaction_id, &(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), lpli_p->curr_tuple_index, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		return 0;
	}

	// handle case if the page becomes newly empty
	if(0 == get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)))
	{
		// since currently there is no tuple to point to, we will just reset the curr_tuple_index
		lpli_p->curr_tuple_index = 0;
		// below function only discards the page, if there are other pages in the linked_page_list
		// i.e. if the linked_page_list itself became empty, then the below function call is a NOP, returning 0
		discard_curr_page_if_empty(lpli_p, aft_op, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		return 1;
	}

	// we by default went next, upon discarding the tuple on the page, so if we are required to go prev, we do that adjusting the tuple
	if(aft_op == GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION)
		lpli_p->curr_tuple_index--;

	// if the curr_tuple_index is within bounds, then return with a success
	if(0 <= lpli_p->curr_tuple_index && lpli_p->curr_tuple_index < get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)))
		return 1;

	// the curr_tuple_index is not within bounds
	// so appropriately fix it
	if(aft_op == GO_PREV_AFTER_LINKED_PAGE_ITERATOR_OPERATION)
	{
		lpli_p->curr_tuple_index = 0;
		prev_linked_page_list_iterator(lpli_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		return 1;
	}
	else
	{
		lpli_p->curr_tuple_index = get_tuple_count_on_persistent_page(&(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def)) - 1;
		next_linked_page_list_iterator(lpli_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		return 1;
	}
}

int update_at_linked_page_list_iterator(linked_page_list_iterator* lpli_p, const void* tuple, const void* transaction_id, int* abort_error)
{
	// fail if this is not a writable iterator
	if(!is_writable_linked_page_list_iterator(lpli_p))
		return 0;

	// if the linked_page_list is empty then fail, removal
	if(is_empty_linked_page_list(lpli_p))
		return 0;

	// can not insert a tuple greater than the max_record_size on the page
	// if tuple == NULL, then it can always be inserted
	if(tuple != NULL && lpli_p->lpltd_p->max_record_size < get_tuple_size(lpli_p->lpltd_p->record_def, tuple))
		return 0;

	// try update_resiliently, first
	int updated = update_tuple_on_persistent_page_resiliently(lpli_p->pmm_p, transaction_id, get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), lpli_p->curr_tuple_index, tuple, abort_error);
	if(*abort_error)
	{
		release_lock_on_reference_while_holding_head_lock(&(lpli_p->curr_page), lpli_p, transaction_id, abort_error);
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->head_page), NONE_OPTION, abort_error);
		return 0;
	}
	if(updated)
		return 1;

	// discard tuple at curr_tuple_index
	int discarded = discard_tuple_on_persistent_page(lpli_p->pmm_p, transaction_id, get_from_ref(&(lpli_p->curr_page)), lpli_p->lpltd_p->pas_p->page_size, &(lpli_p->lpltd_p->record_def->size_def), lpli_p->curr_tuple_index, abort_error);
	if(*abort_error)
	{
		release_lock_on_reference_while_holding_head_lock(&(lpli_p->curr_page), lpli_p, transaction_id, abort_error);
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->head_page), NONE_OPTION, abort_error);
		return 0;
	}
	if(!discarded) // this must never occur
		return 0;

	// then call split_insert with same curr_tuple_index and insert_at_pos
	// only an abort error or a invalid parameter can result in failure
	int result = insert_OR_split_insert_on_page_of_linked_page_list(lpli_p, lpli_p->curr_tuple_index, tuple, transaction_id, abort_error);
	if(*abort_error)
		return 0;
	return result;
}

int update_element_in_place_at_linked_page_list_iterator(linked_page_list_iterator* lpli_p, uint32_t element_index, const user_value* element_value, const void* transaction_id, int* abort_error)
{
	// fail if this is not a writable iterator
	if(!is_writable_linked_page_list_iterator(lpli_p))
		return 0;

	// if the linked_page_list is empty then fail, removal
	if(is_empty_linked_page_list(lpli_p))
		return 0;

	// perform the inplace update, on an abort release lock on the curr_page and fail
	int updated = set_element_in_tuple_in_place_on_persistent_page(lpli_p->pmm_p, transaction_id, &(lpli_p->curr_page), lpli_p->lpltd_p->pas_p->page_size, lpli_p->lpltd_p->record_def, lpli_p->curr_tuple_index, element_index, element_value, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(lpli_p->pam_p, transaction_id, &(lpli_p->curr_page), NONE_OPTION, abort_error);
		return 0;
	}

	return updated;
}