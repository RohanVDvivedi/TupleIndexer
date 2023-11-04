#include<bplus_tree_iterator.h>

#include<persistent_page_functions.h>
#include<bplus_tree_leaf_page_header.h>

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
		int result = 0;

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
			result = 1;
		}

		// pop one from the bottom, this was the old curr_leaf_page
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
			if(*abort_error)
				goto ABORT_ERROR;
		}

		return result;
	}
	else // iterate forward using the pointers on the parent pages that are stacked
	{
		// pop the top of the stack, which will happen to be curr_leaf_page, exposing us to the parent pages underneath
		{
			locked_page_info* top = get_top_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
			pop_from_locked_pages_stack(locked_pages_stack_p);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// loop over stack to reach the next
		// TODO
	}

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
		int result = 0;

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
			result = 1;
		}

		// pop one from the bottom, this was the old curr_leaf_page
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
			if(*abort_error)
				goto ABORT_ERROR;
		}

		return result;
	}
	else // iterate backward using the pointers on the parent pages that are stacked
	{
		// pop the top of the stack, which will happen to be curr_leaf_page, exposing us to the parent pages underneath
		{
			locked_page_info* top = get_top_of_locked_pages_stack(&(bpi_p->lps));
			release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(top->ppage), NONE_OPTION, abort_error);
			pop_from_locked_pages_stack(locked_pages_stack_p);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// loop over stack to reach the prev
		// TODO
	}

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
