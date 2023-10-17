#include<bplus_tree_merge_util.h>

#include<locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<persistent_page_functions.h>
#include<tuple.h>

int walk_down_locking_parent_pages_for_merge_using_key(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// figure out which child page to go to next
		curr_locked_page->child_index = find_child_index_for_key(&(curr_locked_page->ppage), key, bpttd_p->key_element_count, bpttd_p);

		// if the interior page index record, at child_index in curr_locked_page if deleted, will the curr_locked_page require merging
		// if not then release all locks above curr_locked_page
		// mind well we still need lock on curr_locked_page, as merge on its child will require us to delete corresponding 1 index entry from curr_locked_page
		if(curr_locked_page->ppage.page_id != root_page_id && !may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(&(curr_locked_page->ppage), bpttd_p->page_size, bpttd_p->index_def, curr_locked_page->child_index) )
		{
			// release locks on all the pages in stack except for the the curr_locked_page
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
				release_lock_on_persistent_page(dam_p, &(bottom->ppage), NONE_OPTION);
				pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(dam_p, child_page_id, WRITE_LOCK);

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page));
	}

	return 1;
}

int walk_down_locking_parent_pages_for_merge_using_record(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// break out of this loop on reaching a leaf page
		if(is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p))
			break;

		// figure out which child page to go to next
		curr_locked_page->child_index = find_child_index_for_record(&(curr_locked_page->ppage), record, bpttd_p->key_element_count, bpttd_p);

		// if the interior page index record, at child_index in curr_locked_page if deleted, will the curr_locked_page require merging
		// if not then release all locks above curr_locked_page
		// mind well we still need lock on curr_locked_page, as merge on its child will require us to delete corresponding 1 index entry from curr_locked_page
		if(curr_locked_page->ppage.page_id != root_page_id && !may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(&(curr_locked_page->ppage), bpttd_p->page_size, bpttd_p->index_def, curr_locked_page->child_index) )
		{
			// release locks on all the pages in stack except for the the curr_locked_page
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
				release_lock_on_persistent_page(dam_p, &(bottom->ppage), NONE_OPTION);
				pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
			}
		}

		// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
		uint64_t child_page_id = get_child_page_id_by_child_index(&(curr_locked_page->ppage), curr_locked_page->child_index, bpttd_p);
		persistent_page child_page = acquire_persistent_page_with_lock(dam_p, child_page_id, WRITE_LOCK);

		// push this child page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page));
	}

	return 1;
}