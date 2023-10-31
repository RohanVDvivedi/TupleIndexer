#include<bplus_tree_find_for_update_util.h>

#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>
#include<persistent_page_functions.h>

#include<tuple.h>

int walk_down_locking_parent_pages_for_update_using_record(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, uint32_t* release_for_split, uint32_t* release_for_merge, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
	// initialize relase_for_* to zeros
	(*release_for_merge) = 0;
	(*release_for_split) = 0;

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no split requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// figure out which child page to go to next
		curr_locked_page->child_index = find_child_index_for_record(&(curr_locked_page->ppage), record, bpttd_p->key_element_count, bpttd_p);

		// if you reach here, then curr_locked_page is not a leaf page
		// if curr_locked_page will not require a split, then we can release locks on all the parent pages of curr_locked_page
		if(!may_require_split_for_insert_for_bplus_tree(&(curr_locked_page->ppage), bpttd_p->page_size, bpttd_p->index_def))
		{
			// (do not release lock on the curr_locked_page)
			(*release_for_split) = get_element_count_locked_pages_stack(locked_pages_stack_p) - 1;
		}

		// if the interior page index record, at child_index in curr_locked_page if deleted, will the curr_locked_page require merging
		// if not then release all locks above curr_locked_page
		// mind well we still need lock on curr_locked_page, as merge on its child will require us to delete corresponding 1 index entry from curr_locked_page
		if(curr_locked_page->ppage.page_id != root_page_id && !may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(&(curr_locked_page->ppage), bpttd_p->page_size, bpttd_p->index_def, curr_locked_page->child_index) )
		{
			// we can release locks on all the pages in stack except for the the curr_locked_page
			(*release_for_merge) = get_element_count_locked_pages_stack(locked_pages_stack_p) - 1;
		}

		// release minimum of the two number of locks min((*release_for_split), (*release_for_merge))
		uint32_t max_locks_we_can_release = min((*release_for_split), (*release_for_merge));
		while(max_locks_we_can_release > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
					goto ABORT_ERROR;

			(*release_for_split)--;
			(*release_for_merge)--;
			max_locks_we_can_release--;
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(dam_p, transaction_id, child_page_id, WRITE_LOCK, abort_error);

		if(*abort_error)
			goto ABORT_ERROR;

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page));
	}

	// if we reach here, then the top page in the locked_pages_stack_p is likely the leaf page

	// but since record may not be the actual record that gets inserted, deleted or updated, we can no longer infer anything about what to do further

	return 1;

	ABORT_ERROR:;
	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	// initialize relase_for_* to zeros, since we are aborting
	(*release_for_merge) = 0;
	(*release_for_split) = 0;

	return 0;
}