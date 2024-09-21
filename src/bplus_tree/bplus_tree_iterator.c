#include<bplus_tree_iterator.h>

#include<persistent_page_functions.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_walk_down.h>

#include<stdlib.h>

static persistent_page* get_curr_leaf_page(bplus_tree_iterator* bpi_p)
{
	if(bpi_p->is_stacked)
	{
		locked_page_info* curr_leaf_page = get_top_of_locked_pages_stack(&(bpi_p->lps));
		if(curr_leaf_page == NULL)
			return NULL;
		return &(curr_leaf_page->ppage);
	}
	else
	{
		if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p))
			return NULL;
		return &(bpi_p->curr_page);
	}
}

// makes the iterator point to next page of the curr_leaf_page
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_next_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(bpi_p->is_stacked == 0) // iterate forward using the next_page pointer on the leaf
	{
		// get the next_page_id
		uint64_t next_page_id = get_next_page_id_of_bplus_tree_leaf_page(&(bpi_p->curr_page), bpi_p->bpttd_p);

		// attempt to lock the next_leaf_page
		persistent_page next_leaf_page = get_NULL_persistent_page(bpi_p->pam_p);
		if(next_page_id != bpi_p->bpttd_p->pas_p->NULL_PAGE_ID)
		{
			next_leaf_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, next_page_id, (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK), abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}
		}

		// release lock on the curr_page
		release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			// on an abort error release lock on next_leaf_page if it is not NULL
			if(!is_persistent_page_NULL(&next_leaf_page, bpi_p->pam_p))
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &next_leaf_page, NONE_OPTION, abort_error);
			return 0;
		}

		// update the curr_page
		bpi_p->curr_page = next_leaf_page;

		// goto_next was a success if next_leaf_page is not null
		return !is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p);
	}
	else // iterate forward using the pointers on the parent pages that are stacked
		return walk_down_next_locking_parent_pages_for_stacked_iterator(&(bpi_p->lps), bpi_p->lock_type, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
}

// makes the iterator point to prev page of the curr_leaf_page
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_prev_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(bpi_p->is_stacked == 0) // iterate forward using the prev_page pointer on the leaf
	{
		// get the prev_page_id
		uint64_t prev_page_id = get_prev_page_id_of_bplus_tree_leaf_page(&(bpi_p->curr_page), bpi_p->bpttd_p);

		// attempt to lock the prev_leaf_page
		persistent_page prev_leaf_page = get_NULL_persistent_page(bpi_p->pam_p);
		if(prev_page_id != bpi_p->bpttd_p->pas_p->NULL_PAGE_ID)
		{
			prev_leaf_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, prev_page_id, (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK), abort_error);
			if(*abort_error)
			{
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}
		}

		// release lock on the curr_page
		release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			// on an abort error release lock on next_leaf_page if it is not NULL
			if(!is_persistent_page_NULL(&prev_leaf_page, bpi_p->pam_p))
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &prev_leaf_page, NONE_OPTION, abort_error);
			return 0;
		}

		// update the curr_page
		bpi_p->curr_page = prev_leaf_page;

		// goto_next was a success if next_leaf_page is not null
		return !is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p);
	}
	else // iterate forward using the pointers on the parent pages that are stacked
		return walk_down_prev_locking_parent_pages_for_stacked_iterator(&(bpi_p->lps), bpi_p->lock_type, bpi_p->bpttd_p, bpi_p->pam_p, transaction_id, abort_error);
}

bplus_tree_iterator* get_new_bplus_tree_iterator(persistent_page curr_page, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	// the following 2 must be present
	if(bpttd_p == NULL || pam_p == NULL)
		return NULL;

	// the top most page must be a leaf page
	if(!is_bplus_tree_leaf_page(&curr_page, bpttd_p))
		return NULL;

	// if pmm_p == NULL, then the top page must be read locked, otherwise return NULL
	// and if pmm_p != NULL, then the top page must be write locked, otherwise return NULL
	if((pmm_p == NULL && is_persistent_page_write_locked(&curr_page)) ||
		(pmm_p != NULL && (!is_persistent_page_write_locked(&curr_page))))
		return NULL;

	bplus_tree_iterator* bpi_p = malloc(sizeof(bplus_tree_iterator));
	if(bpi_p == NULL)
		exit(-1);

	bpi_p->is_stacked = 0;
	bpi_p->curr_page = curr_page;
	bpi_p->curr_tuple_index = curr_tuple_index;
	bpi_p->bpttd_p = bpttd_p;
	bpi_p->pam_p = pam_p;
	bpi_p->pmm_p = pmm_p;

	persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);
	if(bpi_p->curr_tuple_index == LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE)
	{
		uint32_t tuple_count_on_curr_leaf_page = get_tuple_count_on_persistent_page(curr_leaf_page, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
		if(tuple_count_on_curr_leaf_page == 0)
			bpi_p->curr_tuple_index = 0;
		else
			bpi_p->curr_tuple_index = tuple_count_on_curr_leaf_page - 1;
	}

	return bpi_p;
}

bplus_tree_iterator* get_new_bplus_tree_stacked_iterator(locked_pages_stack lps, uint32_t curr_tuple_index, int lock_type, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p)
{
	// the following 2 must be present
	if(bpttd_p == NULL || pam_p == NULL)
		return NULL;

	// and the lps size must not be zero
	if(get_element_count_locked_pages_stack(&lps) == 0)
		return NULL;

	// the top most page must be a leaf page
	if(!is_bplus_tree_leaf_page(&(get_top_of_locked_pages_stack(&lps)->ppage), bpttd_p))
		return NULL;

	// if pmm_p == NULL, then the top page must be read locked, otherwise return NULL
	// and if pmm_p != NULL, then the top page must be write locked, otherwise return NULL
	if((pmm_p == NULL && is_persistent_page_write_locked(&(get_top_of_locked_pages_stack(&lps)->ppage))) ||
		(pmm_p != NULL && (!is_persistent_page_write_locked(&(get_top_of_locked_pages_stack(&lps)->ppage)))))
		return NULL;

	bplus_tree_iterator* bpi_p = malloc(sizeof(bplus_tree_iterator));
	if(bpi_p == NULL)
		exit(-1);

	bpi_p->is_stacked = 1;
	bpi_p->lps = lps;
	bpi_p->lock_type = lock_type;
	bpi_p->curr_tuple_index = curr_tuple_index;
	bpi_p->bpttd_p = bpttd_p;
	bpi_p->pam_p = pam_p;
	bpi_p->pmm_p = pmm_p;

	persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);
	if(bpi_p->curr_tuple_index == LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE)
	{
		uint32_t tuple_count_on_curr_leaf_page = get_tuple_count_on_persistent_page(curr_leaf_page, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
		if(tuple_count_on_curr_leaf_page == 0)
			bpi_p->curr_tuple_index = 0;
		else
			bpi_p->curr_tuple_index = tuple_count_on_curr_leaf_page - 1;
	}

	return bpi_p;
}

bplus_tree_iterator* clone_bplus_tree_iterator(const bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(is_writable_bplus_tree_iterator(bpi_p))
		return NULL;

	bplus_tree_iterator* clone_p = malloc(sizeof(bplus_tree_iterator));
	if(clone_p == NULL)
		exit(-1);

	clone_p->is_stacked = bpi_p->is_stacked;
	clone_p->curr_tuple_index = bpi_p->curr_tuple_index;
	clone_p->bpttd_p = bpi_p->bpttd_p;
	clone_p->pam_p = bpi_p->pam_p;
	clone_p->pmm_p = bpi_p->pmm_p;

	if(bpi_p->is_stacked)
	{
		clone_p->lock_type = bpi_p->lock_type;
		if(!initialize_locked_pages_stack(&(clone_p->lps), get_capacity_locked_pages_stack(&(bpi_p->lps))))
			exit(-1);

		// clone all the locks of bpi_p->lps in bottom first order and insert them to clone_p->lps
		for(uint32_t i = 0; i < get_element_count_locked_pages_stack(&(bpi_p->lps)); i++)
		{
			locked_page_info* locked_page_info_to_clone = get_from_bottom_of_locked_pages_stack(&(bpi_p->lps), i);
			persistent_page cloned_page = acquire_persistent_page_with_lock(clone_p->pam_p, transaction_id, locked_page_info_to_clone->ppage.page_id, READ_LOCK, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			push_to_locked_pages_stack(&(clone_p->lps), &INIT_LOCKED_PAGE_INFO(cloned_page, locked_page_info_to_clone->child_index));
		}
	}
	else
	{
		if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p))
			clone_p->curr_page = get_NULL_persistent_page(bpi_p->pam_p);
		else
		{
			clone_p->curr_page = acquire_persistent_page_with_lock(clone_p->pam_p, transaction_id, bpi_p->curr_page.page_id, READ_LOCK, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	return clone_p;

	ABORT_ERROR:
	if(bpi_p->is_stacked)
		release_all_locks_and_deinitialize_stack_reenterable(&(clone_p->lps), clone_p->pam_p, transaction_id, abort_error);
	free(clone_p);
	return NULL;
}

int is_writable_bplus_tree_iterator(const bplus_tree_iterator* bpi_p)
{
	return bpi_p->pmm_p != NULL;
}

int is_stacked_bplus_tree_iterator(const bplus_tree_iterator* bpi_p)
{
	return bpi_p->is_stacked;
}

int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	{
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		// if the page itself is invalid, then quit
		if(curr_leaf_page == NULL)
			return 0;

		// increment the current tuple count, if the next tuple we point to will be on this page
		if(bpi_p->curr_tuple_index + 1 < get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
		{
			bpi_p->curr_tuple_index++;
			return 1;
		}
	}

	// else we keep visiting the next pages until we reach a page that has atleast a tuple or if the page is a NULL page
	while(1)
	{
		// iterate to next leaf page
		if(!goto_next_leaf_page(bpi_p, transaction_id, abort_error))
			return 0;
		if(*abort_error)
			return 0;

		// get curr_leaf_page we are pointing to
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		// if we reached the end of the scan (curr_leaf_page == NULL), quit with 0
		if(curr_leaf_page == NULL)
			return 0;

		uint32_t curr_leaf_page_tuple_count = get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

		// a valid leaf page has atleast a tuple
		if(curr_leaf_page_tuple_count > 0)
		{
			bpi_p->curr_tuple_index = 0;
			break;
		}
	}

	return 1;
}

const void* get_tuple_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);
	if(curr_leaf_page == NULL || bpi_p->curr_tuple_index >= get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
		return NULL;
	return get_nth_tuple_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def), bpi_p->curr_tuple_index);
}

int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	{
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		// if the page itself is invalid, then quit
		if(curr_leaf_page == NULL)
			return 0;

		// decrement the current tuple count, if the next tuple we point to will be on this page
		if(bpi_p->curr_tuple_index != 0)
		{
			bpi_p->curr_tuple_index--;
			return 1;
		}
	}

	// else we keep visiting the previous pages until we reach a page that has atleast a tuple or if the page is a NULL page
	while(1)
	{
		// iterate to prev leaf page
		if(!goto_prev_leaf_page(bpi_p, transaction_id, abort_error))
			return 0;
		if(*abort_error)
			return 0;

		// get curr_leaf_page we are pointing to
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		// if we reached the end of the scan (curr_leaf_page == NULL), quit with 0
		if(curr_leaf_page == NULL)
			return 0;

		uint32_t curr_leaf_page_tuple_count = get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

		// a valid page has atleast a tuple
		if(curr_leaf_page_tuple_count > 0)
		{
			bpi_p->curr_tuple_index = curr_leaf_page_tuple_count - 1;
			break;
		}
	}

	return 1;
}

void delete_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(bpi_p->is_stacked)
		release_all_locks_and_deinitialize_stack_reenterable(&(bpi_p->lps), bpi_p->pam_p, transaction_id, abort_error);
	else
	{
		if(!is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->pam_p))
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
	}
	free(bpi_p);
}

int update_non_key_element_in_place_at_bplus_tree_iterator(bplus_tree_iterator* bpi_p, uint32_t element_index, const user_value* element_value, const void* transaction_id, int* abort_error)
{
	// cannot update non WRITE_LOCKed bplus_tree_iterator
	if(!is_writable_bplus_tree_iterator(bpi_p))
		return 0;

	// make sure that the element that the user is trying to update in place is not a key for the bplus_tree
	// if you allow so, it could be a disaster
	for(uint32_t i = 0; i < bpi_p->bpttd_p->key_element_count; i++)
		if(bpi_p->bpttd_p->key_element_ids[i] == element_index)
			return 0;

	// get current leaf page that the bplus_tree_iterator is pointing to
	persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

	// if the iterator is at the end OR is not pointing to a valid tuple, then fail
	if(curr_leaf_page == NULL || bpi_p->curr_tuple_index >= get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
		return 0;

	// perform the inplace update, on an abort release all locks
	int updated = set_element_in_tuple_in_place_on_persistent_page(bpi_p->pmm_p, transaction_id, curr_leaf_page, bpi_p->bpttd_p->pas_p->page_size, bpi_p->bpttd_p->record_def, bpi_p->curr_tuple_index, element_index, element_value, abort_error);
	if(*abort_error)
	{
		while(get_element_count_locked_pages_stack(&(bpi_p->lps)) > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
		}
		return 0;
	}

	return updated;
}