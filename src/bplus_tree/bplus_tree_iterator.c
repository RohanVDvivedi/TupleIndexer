#include<bplus_tree_iterator.h>

#include<persistent_page_functions.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_util.h>

#include<stdlib.h>

static persistent_page* get_curr_leaf_page(bplus_tree_iterator* bpi_p)
{
	locked_page_info* curr_leaf_page = get_top_of_locked_pages_stack(&(bpi_p->lps));
	if(curr_leaf_page == NULL)
		return NULL;
	return &(curr_leaf_page->ppage);
}

// makes the iterator point to next page of the curr_leaf_page
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_next_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(get_element_count_locked_pages_stack(&(bpi_p->lps)) == 0)
		return 0;

	if(bpi_p->is_stacked == 0) // iterate forward using the next_page pointer on the leaf
	{
		// get the next_page_id
		uint64_t next_page_id;
		{
			persistent_page* curr_leaf_page = &(get_top_of_locked_pages_stack(&(bpi_p->lps))->ppage);
			next_page_id = get_next_page_id_of_bplus_tree_leaf_page(curr_leaf_page, bpi_p->bpttd_p);
		}

		// attempt to lock the next_leaf_page, if locked successfully, push it onto the stack
		if(next_page_id != bpi_p->bpttd_p->pas_p->NULL_PAGE_ID)
		{
			persistent_page next_leaf_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, next_page_id, (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK), abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			push_to_locked_pages_stack(&(bpi_p->lps), &INIT_LOCKED_PAGE_INFO(next_leaf_page, INVALID_TUPLE_INDEX));
		}

		// pop one from the bottom, this was the old curr_leaf_page
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}
	else // iterate forward using the pointers on the parent pages that are stacked
	{
		// pop the top of the stack, which will happen to be curr_leaf_page, exposing us to the parent pages underneath
		{
			locked_page_info* top = get_top_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
			pop_from_locked_pages_stack(&(bpi_p->lps));
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// loop over stack to reach the next
		while(get_element_count_locked_pages_stack(&(bpi_p->lps)) > 0)
		{
			locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(&(bpi_p->lps));

			if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpi_p->bpttd_p))
				break;

			uint32_t curr_page_level = get_level_of_bplus_tree_page(&(curr_locked_page->ppage), bpi_p->bpttd_p);

			// for an interior page,
			// if it's child_index + 1 is out of bounds then release lock on it and pop it
			// else lock the child_page on it's child_index + 1 entry and push this child_page onto the lps stack
			
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->index_def->size_def));

			// increment its child_index
			curr_locked_page->child_index++;

			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count) // if child_index is within bounds, push this child_page onto the lps stack
			{
				// then lock the page at child_index, and push it onto the stack
				uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpi_p->bpttd_p);
				persistent_page child_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, child_page_id, ((curr_page_level == 1) ? (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK) : READ_LOCK), abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				// push a locked_page_info for child_page pointing to its (first_child_index - 1)
				locked_page_info child_locked_page_info = INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX);
				if(!is_bplus_tree_leaf_page(&(child_locked_page_info.ppage), bpi_p->bpttd_p)) // when not leaf
					child_locked_page_info.child_index = ALL_LEAST_KEYS_CHILD_INDEX-1; // this is so that when this becomes initially the top for this loop, it will get incremented to its ALL_LEAST_KEYS_CHILD_INDEX
				push_to_locked_pages_stack(&(bpi_p->lps), &child_locked_page_info);
			}
			else // pop release lock on the curr_locked_page, and pop it
			{
				// pop it from the stack and unlock it
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
				pop_from_locked_pages_stack(&(bpi_p->lps));
				if(*abort_error)
					goto ABORT_ERROR;
			}
		}
	}

	if(get_element_count_locked_pages_stack(&(bpi_p->lps)) == 0) // this implies end of scan
		return 0;

	// on success we would have the curr_leaf_page on the stack
	return 1;

	ABORT_ERROR:;
	while(get_element_count_locked_pages_stack(&(bpi_p->lps)) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
		release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
	}
	return 0;
}

// makes the iterator point to prev page of the curr_leaf_page
// this function releases all locks on an abort_error
// on abort_error OR on reaching end, all page locks are released, and return value is 0
static int goto_prev_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	if(get_element_count_locked_pages_stack(&(bpi_p->lps)) == 0)
		return 0;

	if(bpi_p->is_stacked == 0) // iterate backward using the prev_page pointer on the leaf
	{
		// get the prev_page_id
		uint64_t prev_page_id;
		{
			persistent_page* curr_leaf_page = &(get_top_of_locked_pages_stack(&(bpi_p->lps))->ppage);
			prev_page_id = get_prev_page_id_of_bplus_tree_leaf_page(curr_leaf_page, bpi_p->bpttd_p);
		}

		// attempt to lock the prev_leaf_page, if locked successfully, push it onto the stack
		if(prev_page_id != bpi_p->bpttd_p->pas_p->NULL_PAGE_ID)
		{
			persistent_page prev_leaf_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, prev_page_id, (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK), abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			push_to_locked_pages_stack(&(bpi_p->lps), &INIT_LOCKED_PAGE_INFO(prev_leaf_page, INVALID_TUPLE_INDEX));
		}

		// pop one from the bottom, this was the old curr_leaf_page
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}
	else // iterate backward using the pointers on the parent pages that are stacked
	{
		// pop the top of the stack, which will happen to be curr_leaf_page, exposing us to the parent pages underneath
		{
			locked_page_info* top = get_top_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
			pop_from_locked_pages_stack(&(bpi_p->lps));
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// loop over stack to reach the prev
		while(get_element_count_locked_pages_stack(&(bpi_p->lps)) > 0)
		{
			locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(&(bpi_p->lps));

			if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpi_p->bpttd_p))
				break;

			uint32_t curr_page_level = get_level_of_bplus_tree_page(&(curr_locked_page->ppage), bpi_p->bpttd_p);

			// for an interior page,
			// if it's child_index - 1 is out of bounds then release lock on it and pop it
			// else lock the child_page on it's child_index - 1 entry and push this child_page onto the lps stack

			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->index_def->size_def));

			// decrement its child_index
			curr_locked_page->child_index--;

			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count) // if child_index is within bounds, push this child_page onto the lps stack
			{
				// then lock the page at child_index, and push it onto the stack
				uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpi_p->bpttd_p);
				persistent_page child_page = acquire_persistent_page_with_lock(bpi_p->pam_p, transaction_id, child_page_id, ((curr_page_level == 1) ? (is_writable_bplus_tree_iterator(bpi_p) ? WRITE_LOCK : READ_LOCK) : READ_LOCK), abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				// push a locked_page_info for child_page, making its last child + 1,
				locked_page_info child_locked_page_info = INIT_LOCKED_PAGE_INFO(child_page, INVALID_TUPLE_INDEX);
				if(!is_bplus_tree_leaf_page(&(child_locked_page_info.ppage), bpi_p->bpttd_p))
					child_locked_page_info.child_index = get_tuple_count_on_persistent_page(&(child_locked_page_info.ppage), bpi_p->bpttd_p->pas_p->page_size, &(bpi_p->bpttd_p->index_def->size_def)); // this is so that when this becomes initially the top for this loop, it will get decremented to its last_child_index, which is at (tuple_count-1)
				push_to_locked_pages_stack(&(bpi_p->lps), &child_locked_page_info);
			}
			else // pop release lock on the curr_locked_page, and pop it
			{
				// pop it from the stack and unlock it
				release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
				pop_from_locked_pages_stack(&(bpi_p->lps));
				if(*abort_error)
					goto ABORT_ERROR;
			}
		}
	}

	if(get_element_count_locked_pages_stack(&(bpi_p->lps)) == 0) // this implies end of scan
		return 0;

	// on success we would have the curr_leaf_page on the stack
	return 1;

	ABORT_ERROR:;
	while(get_element_count_locked_pages_stack(&(bpi_p->lps)) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
		release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
	}
	return 0;
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

	bpt_p->is_stacked = 0;
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

	bpt_p->is_stacked = 1;
	bpi_p->lps = lps;
	bpt_p->lock_type = lock_type;
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

	if(!initialize_locked_pages_stack(&(clone_p->lps), get_capacity_locked_pages_stack(&(bpi_p->lps))))
		exit(-1);

	clone_p->curr_tuple_index = bpi_p->curr_tuple_index;
	clone_p->is_stacked = bpi_p->is_stacked;
	clone_p->bpttd_p = bpi_p->bpttd_p;
	clone_p->pam_p = bpi_p->pam_p;
	clone_p->pmm_p = bpi_p->pmm_p;

	// clone all the locks of bpi_p->lps in bottom first order and insert them to clone_p->lps
	for(uint32_t i = 0; i < get_element_count_locked_pages_stack(&(bpi_p->lps)); i++)
	{
		locked_page_info* locked_page_info_to_clone = get_from_bottom_of_locked_pages_stack(&(bpi_p->lps), i);
		persistent_page cloned_page = acquire_persistent_page_with_lock(clone_p->pam_p, transaction_id, locked_page_info_to_clone->ppage.page_id, READ_LOCK, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
		push_to_locked_pages_stack(&(clone_p->lps), &INIT_LOCKED_PAGE_INFO(cloned_page, locked_page_info_to_clone->child_index));
	}

	return clone_p;

	ABORT_ERROR:
	while(get_element_count_locked_pages_stack(&(clone_p->lps)) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(clone_p->lps));
		release_lock_on_persistent_page(clone_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&(clone_p->lps));
	}
	deinitialize_locked_pages_stack(&(clone_p->lps));
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
		goto_next_leaf_page(bpi_p, transaction_id, abort_error);
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
		goto_prev_leaf_page(bpi_p, transaction_id, abort_error);
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
	while(get_element_count_locked_pages_stack(&(bpi_p->lps)) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
		release_lock_on_persistent_page(bpi_p->pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
	}
	deinitialize_locked_pages_stack(&(bpi_p->lps));
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