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
		if(next_page_id != bpi_p->bpttd_p->NULL_PAGE_ID)
		{
			persistent_page next_leaf_page = acquire_persistent_page_with_lock(bpi_p->dam_p, transaction_id, next_page_id, bpi_p->leaf_lock_type, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			push_to_locked_pages_stack(&(bpi_p->lps), &INIT_LOCKED_PAGE_INFO(next_leaf_page));
		}

		// pop one from the bottom, this was the old curr_leaf_page
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
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
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
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

			// for an interior page,
			// if it's child_index + 1 is out of bounds then release lock on it and pop it
			// else lock the child_page on it's child_index + 1 entry and push this child_page onto the lps stack
			
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->index_def->size_def));

			// increment its child_index
			curr_locked_page->child_index++;

			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count) // if child_index is within bounds, push this child_page onto the lps stack
			{
				// then lock the page at child_index, and push it onto the stack
				uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpi_p->bpttd_p);
				persistent_page child_page = acquire_persistent_page_with_lock(bpi_p->dam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				// push a locked_page_info for child_page pointing to its (first_child_index - 1)
				locked_page_info child_locked_page_info = INIT_LOCKED_PAGE_INFO(child_page);
				if(!is_bplus_tree_leaf_page(&(child_locked_page_info.ppage), bpi_p->bpttd_p))
					child_locked_page_info.child_index = -2; // this is so that when this becomes initially the top for this loop, it will get incremented to its first_child_index
				push_to_locked_pages_stack(&(bpi_p->lps), &child_locked_page_info);
			}
			else // pop release lock on the curr_locked_page, and pop it
			{
				// pop it from the stack and unlock it
				release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
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
		release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
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
			prev_page_id = get_next_page_id_of_bplus_tree_leaf_page(curr_leaf_page, bpi_p->bpttd_p);
		}

		// attempt to lock the prev_leaf_page, if locked successfully, push it onto the stack
		if(prev_page_id != bpi_p->bpttd_p->NULL_PAGE_ID)
		{
			persistent_page prev_leaf_page = acquire_persistent_page_with_lock(bpi_p->dam_p, transaction_id, prev_page_id, bpi_p->leaf_lock_type, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			push_to_locked_pages_stack(&(bpi_p->lps), &INIT_LOCKED_PAGE_INFO(prev_leaf_page));
		}

		// pop one from the bottom, this was the old curr_leaf_page
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
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
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
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

			// for an interior page,
			// if it's child_index - 1 is out of bounds then release lock on it and pop it
			// else lock the child_page on it's child_index - 1 entry and push this child_page onto the lps stack

			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->index_def->size_def));

			// decrement its child_index
			curr_locked_page->child_index--;

			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count) // if child_index is within bounds, push this child_page onto the lps stack
			{
				// then lock the page at child_index, and push it onto the stack
				uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpi_p->bpttd_p);
				persistent_page child_page = acquire_persistent_page_with_lock(bpi_p->dam_p, transaction_id, child_page_id, READ_LOCK, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				// push a locked_page_info for child_page, making its last child + 1,
				locked_page_info child_locked_page_info = INIT_LOCKED_PAGE_INFO(child_page);
				if(!is_bplus_tree_leaf_page(&(child_locked_page_info.ppage), bpi_p->bpttd_p))
					child_locked_page_info.child_index = get_tuple_count_on_persistent_page(&(child_locked_page_info.ppage), bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->index_def->size_def)); // this is so that when this becomes initially the top for this loop, it will get decremented to its last_child_index
				push_to_locked_pages_stack(&(bpi_p->lps), &child_locked_page_info);
			}
			else // pop release lock on the curr_locked_page, and pop it
			{
				// pop it from the stack and unlock it
				release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(curr_locked_page->ppage), NONE_OPTION, abort_error);
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
		release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
	}
	return 0;
}

bplus_tree_iterator* get_new_bplus_tree_iterator(locked_pages_stack lps, uint32_t curr_tuple_index, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// the following 2 must be present
	if(bpttd_p == NULL || dam_p == NULL)
		return NULL;

	// and the lps size must not be zero
	if(get_element_count_locked_pages_stack(&lps) == 0)
		return NULL;

	bplus_tree_iterator* bpi_p = malloc(sizeof(bplus_tree_iterator));
	if(bpi_p == NULL)
		exit(-1);

	bpi_p->lps = lps;
	bpi_p->curr_tuple_index = curr_tuple_index;
	bpi_p->bpttd_p = bpttd_p;
	bpi_p->dam_p = dam_p;

	persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);
	if(bpi_p->curr_tuple_index == LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE)
	{
		uint32_t tuple_count_on_curr_leaf_page = get_tuple_count_on_persistent_page(curr_leaf_page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));
		if(tuple_count_on_curr_leaf_page == 0)
			bpi_p->curr_tuple_index = 0;
		else
			bpi_p->curr_tuple_index = tuple_count_on_curr_leaf_page - 1;
	}

	bpi_p->leaf_lock_type = is_persistent_page_write_locked(curr_leaf_page) ? WRITE_LOCK : READ_LOCK;
	bpi_p->is_stacked = get_element_count_locked_pages_stack(&lps) > 1;

	return bpi_p;
}

int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	{
		persistent_page* curr_leaf_page = get_curr_leaf_page(bpi_p);

		// if the page itself is invalid, then quit
		if(curr_leaf_page == NULL)
			return 0;

		// increment the current tuple count, if the next tuple we point to will be on this page
		if(bpi_p->curr_tuple_index + 1 < get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
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

		uint32_t curr_leaf_page_tuple_count = get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

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
	if(curr_leaf_page == NULL || bpi_p->curr_tuple_index >= get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
		return NULL;
	return get_nth_tuple_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def), bpi_p->curr_tuple_index);
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

		uint32_t curr_leaf_page_tuple_count = get_tuple_count_on_persistent_page(curr_leaf_page, bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

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
		release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
	}
	deinitialize_locked_pages_stack(&(bpi_p->lps));
	free(bpi_p);
}
