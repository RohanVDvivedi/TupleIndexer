#include<bplus_tree_iterator.h>

#include<persistent_page_functions.h>
#include<bplus_tree_leaf_page_header.h>

#include<stdlib.h>

static persistent_page* get_curr_leaf_page(bplus_tree_iterator* bpi_p)
{
	locked_page_info* curr_page = get_bottom_of_locked_pages_stack(&(bpi_p->lps));
	if(curr_page == NULL)
		return NULL;
	return &(curr_page->ppage);
}

static int goto_next_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

static int goto_prev_leaf_page(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error);

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

	persistent_page* curr_page = get_curr_leaf_page(bpi_p);
	if(bpi_p->curr_tuple_index == LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE)
	{
		uint32_t tuple_count_on_curr_page = get_tuple_count_on_persistent_page(curr_page, bpttd_p->page_size, &(bpttd_p->record_def->size_def));
		if(tuple_count_on_curr_page == 0)
			bpi_p->curr_tuple_index = 0;
		else
			bpi_p->curr_tuple_index = tuple_count_on_curr_page - 1;
	}

	bpi_p->leaf_lock_type = is_persistent_page_write_locked(curr_page) ? WRITE_LOCK : READ_LOCK;
	bpi_p->is_stacked = get_element_count_locked_pages_stack(&lps) > 1;

	return bpi_p;
}

int next_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	// if the page itself is invalid, then quit
	if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->dam_p))
		return 0;

	// increment the current tuple count, if the next tuple we point to will be on this page
	if(bpi_p->curr_tuple_index + 1 < get_tuple_count_on_persistent_page(&(bpi_p->curr_page), bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
	{
		bpi_p->curr_tuple_index++;
		return 1;
	}

	// else we keep visiting the next pages until we reach a page that has atleast a tuple or if the page is a NULL page
	while(1)
	{
		// get reader lock on the next page
		uint64_t next_page_id = get_next_page_id_of_bplus_tree_leaf_page(&(bpi_p->curr_page), bpi_p->bpttd_p);

		persistent_page next_page = get_NULL_persistent_page(bpi_p->dam_p);
		if(next_page_id != bpi_p->bpttd_p->NULL_PAGE_ID)
		{
			next_page = acquire_persistent_page_with_lock(bpi_p->dam_p, transaction_id, next_page_id, READ_LOCK, abort_error);
			if(*abort_error) // on an abort, release curr_page lock and exit
			{
				release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}
		}

		// release lock on the curr_page
		release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			if(!is_persistent_page_NULL(&next_page, bpi_p->dam_p))
				release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &next_page, NONE_OPTION, abort_error);
			return 0;
		}

		bpi_p->curr_page = next_page;

		// if we reached the end of the scan, quit with 0
		if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->dam_p))
			return 0;

		uint32_t curr_page_tuple_count = get_tuple_count_on_persistent_page(&(bpi_p->curr_page), bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

		// or a valid page has atleast a tuple
		if(curr_page_tuple_count > 0)
		{
			bpi_p->curr_tuple_index = 0;
			break;
		}
	}

	return 1;
}

const void* get_tuple_bplus_tree_iterator(bplus_tree_iterator* bpi_p)
{
	persistent_page* curr_page = get_curr_leaf_page(bpi_p);
	if(curr_page == NULL || bpi_p->curr_tuple_index >= get_tuple_count_on_persistent_page(&curr_page, bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def)))
		return NULL;
	return get_nth_tuple_on_persistent_page(curr_page, bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def), bpi_p->curr_tuple_index);
}

int prev_bplus_tree_iterator(bplus_tree_iterator* bpi_p, const void* transaction_id, int* abort_error)
{
	// if the page itself is invalid, then quit
	if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->dam_p))
		return 0;

	// increment the current tuple count, if the tuple that we are pointing to is not the first tuple on the page
	if(bpi_p->curr_tuple_index != 0)
	{
		bpi_p->curr_tuple_index--;
		return 1;
	}

	// else we keep visiting the previous pages until we reach a page that has atleast a tuple or if the page is a NULL page
	while(1)
	{
		// get reader lock on the prev page
		uint64_t prev_page_id = get_prev_page_id_of_bplus_tree_leaf_page(&(bpi_p->curr_page), bpi_p->bpttd_p);

		persistent_page prev_page = get_NULL_persistent_page(bpi_p->dam_p);
		if(prev_page_id != bpi_p->bpttd_p->NULL_PAGE_ID)
		{
			prev_page = acquire_persistent_page_with_lock(bpi_p->dam_p, transaction_id, prev_page_id, READ_LOCK, abort_error);
			if(*abort_error) // on an abort, release curr_page lock and exit
			{
				release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
				return 0;
			}
		}

		// release lock on the curr_page
		release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &(bpi_p->curr_page), NONE_OPTION, abort_error);
		if(*abort_error)
		{
			if(!is_persistent_page_NULL(&prev_page, bpi_p->dam_p))
				release_lock_on_persistent_page(bpi_p->dam_p, transaction_id, &prev_page, NONE_OPTION, abort_error);
			return 0;
		}

		bpi_p->curr_page = prev_page;

		// if we reached the end of the scan, quit with 0
		if(is_persistent_page_NULL(&(bpi_p->curr_page), bpi_p->dam_p))
			return 0;

		uint32_t curr_page_tuple_count = get_tuple_count_on_persistent_page(&(bpi_p->curr_page), bpi_p->bpttd_p->page_size, &(bpi_p->bpttd_p->record_def->size_def));

		// or a valid page has atleast a tuple
		if(curr_page_tuple_count > 0)
		{
			bpi_p->curr_tuple_index = curr_page_tuple_count - 1;
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
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&(bpi_p->lps));
	}
	deinitialize_locked_pages_stack(&(bpi_p->lps));
	free(bpi_p);
}
