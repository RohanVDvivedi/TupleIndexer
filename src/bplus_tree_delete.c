#include<bplus_tree.h>

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

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
{
	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = &((locked_pages_stack){});
	uint32_t root_page_level;

	{
		// get lock on the root page of the bplus_tree
		persistent_page root_page = acquire_persistent_page_with_lock(dam_p, root_page_id, WRITE_LOCK);

		// pre cache level of the root_page
		root_page_level = get_level_of_bplus_tree_page(&root_page, bpttd_p);

		// create a stack of capacity = levels
		initialize_locked_pages_stack(locked_pages_stack_p, root_page_level + 1);

		// push the root page onto the stack
		push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page));
	}

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		if(!is_bplus_tree_leaf_page(&(curr_locked_page->ppage), bpttd_p)) // is not a leaf page
		{
			// figure out which child page to go to next
			curr_locked_page->child_index = find_child_index_for_key(&(curr_locked_page->ppage), key, bpttd_p->key_element_count, bpttd_p);

			// check if a merge happens at child_index of this curr_locked_page, will this page be required to be merged aswell
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
		else
			break;
	}

	// deleted will be set if the record, was deleted
	int deleted = 0;

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info curr_locked_page = *get_top_of_locked_pages_stack(locked_pages_stack_p);
		pop_from_locked_pages_stack(locked_pages_stack_p);

		if(is_bplus_tree_leaf_page(&(curr_locked_page.ppage), bpttd_p)) // is a leaf page, perform delete in the leaf page
		{
			// find index of last record that has the given key on the page
			uint32_t found_index = find_last_in_sorted_packed_page(
												&(curr_locked_page.ppage), bpttd_p->page_size,
												bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
												key, bpttd_p->key_def, NULL
											);

			// if no such record can be found, we break and exit
			if(NO_TUPLE_FOUND == found_index)
			{
				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);
				break;
			}

			// perform a delete operation on the found index in this page
			deleted = delete_in_sorted_packed_page(
								&(curr_locked_page.ppage), bpttd_p->page_size,
								bpttd_p->record_def,
								found_index,
								pmm_p
							);

			if(!deleted) // THIS IS AN ERR, WE CANT RECOVER FROM
			{
				// ABORT
				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);
				break;
			}

			// go ahead with merging only if the page is lesser than or equal to half full AND is not root
			// i.e. we can not merge a page which is root OR is more than half full
			if(curr_locked_page.ppage.page_id == root_page_id || is_page_more_than_half_full(&(curr_locked_page.ppage), bpttd_p->page_size, bpttd_p->record_def))
			{
				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);
				break;
			}

			// We will now check to see if the page can be merged

			locked_page_info* parent_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
			uint32_t parent_tuple_count = get_tuple_count_on_persistent_page(&(parent_locked_page->ppage), bpttd_p->page_size, &(bpttd_p->index_def->size_def));

			// will be set if the page has been merged
			int merged = 0;

			// attempt a merge with next page of curr_locked_page, if it has a next page with same parent
			if(!merged && parent_locked_page->child_index + 1 < parent_tuple_count)
			{
				merged = merge_bplus_tree_leaf_pages(&(curr_locked_page.ppage), bpttd_p, dam_p, pmm_p);

				// if merged we need to delete entry at child_index in the parent page
				if(merged)
					parent_locked_page->child_index += 1;
			}

			// attempt a merge with prev page of curr_locked_page, if it has a prev page with same parent
			if(!merged && parent_locked_page->child_index < parent_tuple_count)
			{
				// release lock on the curr_locked_page
				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);

				// make the previous of curr_locked_page as the curr_locked_page
				{
					uint64_t prev_child_page_id = get_child_page_id_by_child_index(&(parent_locked_page->ppage), parent_locked_page->child_index - 1, bpttd_p);
					persistent_page prev_child_page = acquire_persistent_page_with_lock(dam_p, prev_child_page_id, WRITE_LOCK);
					curr_locked_page = INIT_LOCKED_PAGE_INFO(prev_child_page);
				}

				merged = merge_bplus_tree_leaf_pages(&(curr_locked_page.ppage), bpttd_p, dam_p, pmm_p);

				// if merged we need to delete entry at child_index in the parent page

				if(!merged)
					release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);
			}

			// release lock on the curr_locked_page, if not released yet
			if(!is_persistent_page_NULL(&(curr_locked_page.ppage), dam_p))
				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);

			if(!merged)
				break;
		}
		else // check if the curr_locked_page needs to be merged, if yes then merge it with either previous or next page
		{
			// perform a delete operation on the found index in this page
			deleted = delete_in_sorted_packed_page(
								&(curr_locked_page.ppage), bpttd_p->page_size,
								bpttd_p->index_def,
								curr_locked_page.child_index,
								pmm_p
							);

			if(!deleted) // THIS IS AN ERR, WE CANT RECOVER FROM
			{
				// ABORT
				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);
				break;
			}

			if(curr_locked_page.ppage.page_id == root_page_id)
			{
				// need to handle empty root parent page
				// we clone the contents of the <only child of the root page> to the <root page>, to reduce the level of the page
				// we can do this only if the root is an interior page i.e. root_page_level > 0
				while(root_page_level > 0 && get_tuple_count_on_persistent_page(&(curr_locked_page.ppage), bpttd_p->page_size, &(bpttd_p->index_def->size_def)) == 0)
				{
					uint64_t only_child_page_id = get_child_page_id_by_child_index(&(curr_locked_page.ppage), -1, bpttd_p);
					persistent_page only_child_page = acquire_persistent_page_with_lock(dam_p, only_child_page_id, READ_LOCK);

					// clone the only_child_page in to the curr_locked_page
					if(is_bplus_tree_leaf_page(&(only_child_page), bpttd_p))
						clone_persistent_page(pmm_p, &(curr_locked_page.ppage), bpttd_p->page_size, &(bpttd_p->record_def->size_def), &(only_child_page));
					else
						clone_persistent_page(pmm_p, &(curr_locked_page.ppage), bpttd_p->page_size, &(bpttd_p->index_def->size_def), &(only_child_page));

					// free and unlock only_child_page
					release_lock_on_persistent_page(dam_p, &only_child_page, FREE_PAGE);

					// root_page_level will now be what was the level of its child (root_page_level -= 1, should have sufficed here)
					root_page_level = get_level_of_bplus_tree_page(&(curr_locked_page.ppage), bpttd_p);
				}

				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);
				break;
			}

			// go ahead with merging only if the page is lesser or equal to than half full
			// i.e. we can not merge a page which is more than half full
			if(is_page_more_than_half_full(&(curr_locked_page.ppage), bpttd_p->page_size, bpttd_p->index_def))
			{
				release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);
				break;
			}

			locked_page_info* parent_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
			uint32_t parent_tuple_count = get_tuple_count_on_persistent_page(&(parent_locked_page->ppage), bpttd_p->page_size, &(bpttd_p->index_def->size_def));

			// will be set if the page has been merged
			int merged = 0;

			// attempt a merge with next page of curr_locked_page, if it has a next page with same parent
			if(!merged && parent_locked_page->child_index + 1 < parent_tuple_count)
			{
				persistent_page* child_page1 = &(curr_locked_page.ppage);

				uint64_t child_page2_page_id = get_child_page_id_by_child_index(&(parent_locked_page->ppage), parent_locked_page->child_index + 1, bpttd_p);
				persistent_page child_page2 = acquire_persistent_page_with_lock(dam_p, child_page2_page_id, WRITE_LOCK);

				const void* separator_parent_tuple = get_nth_tuple_on_persistent_page(&(parent_locked_page->ppage), bpttd_p->page_size, &(bpttd_p->index_def->size_def), parent_locked_page->child_index + 1);

				merged = merge_bplus_tree_interior_pages(child_page1, separator_parent_tuple, &child_page2, bpttd_p, dam_p, pmm_p);

				// if merged we need to delete entry at child_index in the parent page, and free child_page2
				if(merged)
				{
					parent_locked_page->child_index += 1;

					release_lock_on_persistent_page(dam_p, &child_page2, FREE_PAGE);
				}
				else // release lock on the page that is not curr_locked_page
					release_lock_on_persistent_page(dam_p, &child_page2, NONE_OPTION);
			}

			// attempt a merge with prev page of curr_locked_page, if it has a prev page with same parent
			if(!merged && parent_locked_page->child_index < parent_tuple_count)
			{
				persistent_page* child_page2 = &(curr_locked_page.ppage);

				uint64_t child_page1_page_id = get_child_page_id_by_child_index(&(parent_locked_page->ppage), parent_locked_page->child_index - 1, bpttd_p);
				persistent_page child_page1 = acquire_persistent_page_with_lock(dam_p, child_page1_page_id, WRITE_LOCK);

				const void* separator_parent_tuple = get_nth_tuple_on_persistent_page(&(parent_locked_page->ppage), bpttd_p->page_size, &(bpttd_p->index_def->size_def), parent_locked_page->child_index);

				merged = merge_bplus_tree_interior_pages(&child_page1, separator_parent_tuple, child_page2, bpttd_p, dam_p, pmm_p);

				// if merged we need to delete entry at child_index in the parent page, and free child_page2
				if(merged)
				{
					release_lock_on_persistent_page(dam_p, child_page2, FREE_PAGE);

					curr_locked_page.ppage = child_page1;
				}
				else // release lock on the page that is not curr_locked_page
					release_lock_on_persistent_page(dam_p, &child_page1, NONE_OPTION);
			}

			// release lock on the curr_locked_page
			release_lock_on_persistent_page(dam_p, &(curr_locked_page.ppage), NONE_OPTION);

			if(!merged)
				break;
		}
	}

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, &(bottom->ppage), NONE_OPTION);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	deinitialize_locked_pages_stack(locked_pages_stack_p);

	return deleted;
}