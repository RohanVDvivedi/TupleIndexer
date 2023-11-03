#include<bplus_tree.h>

#include<locked_pages_stack.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<persistent_page_functions.h>
#include<tuple.h>

#include<stdlib.h>

// order of the enum values in find_type must remain the same
typedef enum find_type find_type;
enum find_type
{
	MIN_TUPLE,
	LESSER_THAN_KEY,
	LESSER_THAN_EQUALS_KEY,
	GREATER_THAN_EQUALS_KEY,
	GREATER_THAN_KEY,
	MAX_TUPLE,
};

// walks down for find, using the key, f_type and key_element_count_concerned (what ever is necessary)
// lock_type indicates the lock to be acquired for leaf pages, and locked_parents tells it if all parents of the leaf must be locked
// on an abort error, all pages locked in this function are unlocked and locked_pages_stack is deinitialized -> i.e. on abort_error nothing needs to be done
// on success (*abort_error) == 0, all pages requested/required are locked in the locked_pages_stack
// NOTE: if locked_parents is set, then the parents are locked only in READ_LOCK mode
locked_pages_stack walk_down_for_find_using_key(uint64_t root_page_id, const void* key, find_type f_type, uint32_t key_element_count_concerned, int lock_type, int locked_parents, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
	// create a stack of capacity = levels (locked_parents = 1) or capacity = 2 (locked_parents = 0)
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});

	{
		// get lock on the root page of the bplus_tree in mode (referred by lock_type), this is because root_page can be a leaf page
		persistent_page root_page = acquire_persistent_page_with_lock(dam_p, transaction_id, root_page_id, lock_type, abort_error);
		if(*abort_error)
			return ((locked_pages_stack){});

		// pre cache level of the root_page
		uint32_t root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// create a stack of capacity = levels (when locked_parents = 1) or capacity = 2 (when locked_parents = 0)
		if(locked_parents) // locked_parents -> lock all level pages that lead to leaf
		{
			if(!initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1))
				exit(-1);
		}
		else // we only need to lock the page and its parent at any point
		{
			if(!initialize_locked_pages_stack(locked_pages_stack_p, 2))
				exit(-1);
		}

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page));

		// root_page now has been pushed to stack, it must not be accessed directly from here on

		if(root_page_level == 0) // if root is the leaf page, then return
			return (*locked_pages_stack_p);
		else
		{ // downgrade lock on the root page
			locked_page_info* root_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
			downgrade_to_reader_lock_on_persistent_page(dam_p, transaction_id, &(root_locked_page->ppage), NONE_OPTION, abort_error);

			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// pre cache level of the curr_locked_page
		uint32_t curr_locked_page_level = get_level_of_bplus_tree_page(&(curr_locked_page->ppage), bpttd_p);

		// figure out which child page to go to next, based on f_type, key and key_element_count_concerned
		switch(f_type)
		{
			case MIN_TUPLE :
			{
				curr_locked_page->child_index = -1;
				break;
			}
			case LESSER_THAN_KEY :
			case LESSER_THAN_EQUALS_KEY :
			case GREATER_THAN_EQUALS_KEY :
			case GREATER_THAN_KEY :
			{
				curr_locked_page->child_index = find_child_index_for_key(&(curr_locked_page->ppage), key, key_element_count_concerned, bpttd_p);
				break;
			}
			case MAX_TUPLE :
			{
				curr_locked_page->child_index = get_tuple_count_on_persistent_page(&(curr_locked_page->ppage), bpttd_p->page_size, &(bpttd_p->index_def->size_def)) - 1;
				break;
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		// only the leaf_page (if the parent's level == 1) is locked with lock_type, all other pages (the parent_pages) are locked in READ_LOCK mode
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(dam_p, transaction_id, child_page_id, ((curr_locked_page_level == 1) ? lock_type : READ_LOCK), abort_error);

		// exit for abort_error
		if(*abort_error)
			goto ABORT_ERROR;

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page));

		// if parents are not to be locked, then unlock the immidiate parent after we have locked the child
		if(!locked_parents)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			// on abort_error just go to ABORT_ERROR
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	// on success return locked_pages_stack_p by value
	return *locked_pages_stack_p;

	ABORT_ERROR :;
	// on an abort_error release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	// deinitialize locked_pages_stack
	deinitialize_locked_pages_stack(locked_pages_stack_p);

	// return an empty locked_pages_stack on abort_error
	return ((locked_pages_stack){});
}

bplus_tree_iterator* find_in_bplus_tree(uint64_t root_page_id, const void* key, uint32_t key_element_count_concerned, find_position find_pos, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
	// if the user wants to consider all the key elements then
	// set key_element_count_concerned to bpttd_p->key_element_count
	if(key_element_count_concerned == KEY_ELEMENT_COUNT)
		key_element_count_concerned = bpttd_p->key_element_count;

	find_type f_type;

	if(key == NULL && (find_pos == GREATER_THAN || find_pos == GREATER_THAN_EQUALS))
		f_type = MIN_TUPLE;
	else if(key == NULL && (find_pos == LESSER_THAN || find_pos == LESSER_THAN_EQUALS))
		f_type = MAX_TUPLE;
	else
		f_type = LESSER_THAN_KEY + (find_pos - LESSER_THAN);

	locked_pages_stack lps = walk_down_for_find_using_key(root_page_id, key, f_type, key_element_count_concerned, READ_LOCK, 0, bpttd_p, dam_p, transaction_id, abort_error);
	if(*abort_error) // on abort, all pages would have already been locked by this function
		return NULL;

	// unlock all parent pages, and extract the top page
	while(get_element_count_locked_pages_stack(&lps) > 1)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(&lps);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(&lps);

		// on abort error, relase all locks, deinitialize lps stack and exit
		if(*abort_error)
		{
			while(get_element_count_locked_pages_stack(&lps) > 0)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(&lps);
				release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
				pop_bottom_from_locked_pages_stack(&lps);
			}
			deinitialize_locked_pages_stack(&lps);
			return NULL;
		}
	}
	persistent_page leaf_page = get_top_of_locked_pages_stack(&lps)->ppage;
	deinitialize_locked_pages_stack(&lps);

	// lps mus not be accessed from this point on

	// find the leaf_tuple_index for the iterator to start with
	uint32_t leaf_tuple_index = 0;

	switch(f_type)
	{
		case MIN_TUPLE :
		{
			leaf_tuple_index = 0;
			break;
		}
		case LESSER_THAN_KEY :
		{
			leaf_tuple_index = find_preceding_in_sorted_packed_page(
									&leaf_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : 0;
			break;
		}
		case LESSER_THAN_EQUALS_KEY :
		{
			leaf_tuple_index = find_preceding_equals_in_sorted_packed_page(
									&leaf_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : 0;
			break;
		}
		case GREATER_THAN_EQUALS_KEY :
		{
			leaf_tuple_index = find_succeeding_equals_in_sorted_packed_page(
									&leaf_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
		case GREATER_THAN_KEY :
		{
			leaf_tuple_index = find_succeeding_in_sorted_packed_page(
									&leaf_page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, key_element_count_concerned,
									key, bpttd_p->key_def, NULL
								);

			leaf_tuple_index = (leaf_tuple_index != NO_TUPLE_FOUND) ? leaf_tuple_index : LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
		case MAX_TUPLE :
		{
			leaf_tuple_index = LAST_TUPLE_INDEX_BPLUS_TREE_LEAF_PAGE;
			break;
		}
	}

	bplus_tree_iterator* bpi_p = get_new_bplus_tree_iterator(leaf_page, leaf_tuple_index, bpttd_p, dam_p);

	// iterate next or previous in bplus_tree_iterator, based on the f_type
	// this is not required for MIN_TUPLE and MAX_TUPLE
	switch(f_type)
	{
		case LESSER_THAN_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) >= 0)
			{
				prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case LESSER_THAN_EQUALS_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) > 0)
			{
				prev_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN_EQUALS_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) < 0)
			{
				next_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		case GREATER_THAN_KEY :
		{
			const void* tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			while(tuple_to_skip != NULL && compare_tuples(tuple_to_skip, bpttd_p->record_def, bpttd_p->key_element_ids, key, bpttd_p->key_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned) <= 0)
			{
				next_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
				if(*abort_error)
				{
					delete_bplus_tree_iterator(bpi_p, transaction_id, abort_error);
					return NULL;
				}
				tuple_to_skip = get_tuple_bplus_tree_iterator(bpi_p);
			}
			break;
		}
		default :
		case MIN_TUPLE :
		case MAX_TUPLE :
			break;
	}

	return bpi_p;
}