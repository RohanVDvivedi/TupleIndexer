#include<bplus_tree.h>

#include<bplus_tree_locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<page_layout.h>

#include<arraylist.h>

#include<stdlib.h>

uint64_t get_new_bplus_tree(const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	uint64_t root_page_id;
	void* root_page = dam_p->get_new_page_with_write_lock(dam_p->context, &root_page_id);

	// if init_page fails
	if(!init_bplus_tree_leaf_page(root_page, bpttd_p))
	{
		dam_p->release_writer_lock_and_free_page(dam_p->context, root_page);
		return bpttd_p->NULL_PAGE_ID;
	}

	dam_p->release_writer_lock_on_page(dam_p->context, root_page, 1);
	return root_page_id;
}

bplus_tree_cursor* find_in_bplus_tree(uint64_t root_page_id, const void* key, int scan_dir, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

int insert_in_bplus_tree(uint64_t root_page_id, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	locked_page_info* curr_locked_page = lock_page_and_get_new_locked_page_info(root_page_id, 1, 1, bpttd_p, dam_p);

	// create a stack of capacity = levels + 3 
	arraylist locked_pages_stack;
	initialize_arraylist(&locked_pages_stack, curr_locked_page->level + 4);

	// push the root page onto the stack
	push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, curr_locked_page);

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no split requiring) in the interim
	while(1)
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		if(curr_locked_page->level > 0) // is not a leaf page
		{
			// figure out which child page to go to next
			curr_locked_page->child_index = find_child_index_for_record(curr_locked_page->page, record, bpttd_p);

			uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index, bpttd_p);

			// get lock on the next child page (this page is surely not the root page)
			locked_page_info* child_locked_page = lock_page_and_get_new_locked_page_info(child_page_id, 1, 0, bpttd_p, dam_p);

			// if child page will not require a split, then release locks on all the parent pages
			if( (child_locked_page->level == 0 && !may_require_split_for_insert_for_bplus_tree(child_locked_page->page, bpttd_p->page_size, bpttd_p->record_def))
			||  (child_locked_page->level  > 0 && !may_require_split_for_insert_for_bplus_tree(child_locked_page->page, bpttd_p->page_size, bpttd_p->index_def )) )
			{
				fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);
			}

			// push this child page onto the stack
			push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, child_locked_page);
		}
		else // break this loop on reaching a leaf page
			break;
	}

	// inserted will be set if the record, was inserted
	int inserted = 0;

	// a tuple that needs to be inserted in to the parent page
	// this happens upon a split
	const void* parent_insert = NULL;

	while(!is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		if(curr_locked_page->level == 0) // is a leaf page, insert / split_insert to the leaf page
		{
			// check if the record already exists in this leaf page
			int found = (NO_TUPLE_FOUND != find_first_in_sorted_packed_page(
												curr_locked_page->page, bpttd_p->page_size, 
												bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
												record, bpttd_p->record_def, bpttd_p->key_element_ids
											));

			// if it does not already exist then try to insert it
			if(!found)
			{
				uint32_t insertion_point;

				inserted = insert_to_sorted_packed_page(
										curr_locked_page->page, bpttd_p->page_size, 
										bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
										record, 
										&insertion_point
									);

				// if the insertion fails
				if(!inserted)
				{
					// check if the insert can succeed on compaction
					if(can_insert_this_tuple_without_split_for_bplus_tree(curr_locked_page->page, bpttd_p->page_size, bpttd_p->record_def, record))
					{
						run_page_compaction(curr_locked_page->page, bpttd_p->page_size, bpttd_p->record_def, 0, 1);

						inserted = insert_at_in_sorted_packed_page(
																curr_locked_page->page, bpttd_p->page_size, 
																bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
																record, 
																insertion_point
															);
					}

					// if it still fails then call the split insert
					if(!inserted)
					{
						if(curr_locked_page->is_root)
						{
							// get a new page to insert between the root and its children
							uint64_t root_least_keys_child_id;
							void* root_least_keys_child = dam_p->get_new_page_with_write_lock(dam_p->context, &root_least_keys_child_id);

							// clone root page contents into the new root_least_keys_child
							clone_page(root_least_keys_child, bpttd_p->page_size, bpttd_p->record_def, 1, curr_locked_page->page);

							// re intialize root page as an interior page
							init_bplus_tree_interior_page(curr_locked_page->page, ++curr_locked_page->level, bpttd_p);
							set_least_keys_page_id_of_bplus_tree_interior_page(curr_locked_page->page, root_least_keys_child_id, bpttd_p);

							// create new locked_page_info for the root_least_keys_child and push it onto the stack
							locked_page_info* root_least_keys_child_info = get_new_locked_page_info(root_least_keys_child, root_least_keys_child_id, 1, 0, bpttd_p);
							root_least_keys_child_info->child_index = curr_locked_page->child_index;
							curr_locked_page->child_index = -1; // the root page only has a single child at the moment
							push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, root_least_keys_child_info);

							// get new top of the stack
							curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
						}

						parent_insert = split_insert_bplus_tree_leaf_page(curr_locked_page->page, curr_locked_page->page_id, record, insertion_point, bpttd_p, dam_p);
						
						// if split insert was successfull
						if(parent_insert != NULL)
							inserted = 1;
						else // THIS IS AN ERROR WE CANT RECOVER FROM
							break;
					}
				}
			}

			// if an insertion was done on this page then lock on this page should be released with modification
			if(inserted)
			{
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
				unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);
			}

			// found or the record was inserted without requiring a split, then no need to iterate further in the loop
			if(parent_insert == NULL)
				break;
		}
		else // parent_insert needs to be inserted in to this page and we need to pop curr_locked_page
		{
			// here (curr_locked_page->level > 0) is a MUST condition

			int parent_tuple_inserted = 0;

			// this will be set to an appropriate index tuple that we need to insert to a parent page
			const void* new_parent_insert = NULL;

			uint32_t insertion_point = curr_locked_page->child_index + 1;
			parent_tuple_inserted = insert_at_in_sorted_packed_page(
									curr_locked_page->page, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									parent_insert, 
									insertion_point
								);

			if(!parent_tuple_inserted)
			{

				// check if the insert can succeed on compaction
				if(can_insert_this_tuple_without_split_for_bplus_tree(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, parent_insert))
				{
					run_page_compaction(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, 0, 1);

					parent_tuple_inserted = insert_at_in_sorted_packed_page(
											curr_locked_page->page, bpttd_p->page_size, 
											bpttd_p->index_def, NULL, bpttd_p->key_element_count,
											parent_insert, 
											insertion_point
										);
				}

				// if it still fails then call the split insert
				if(!parent_tuple_inserted)
				{
					if(curr_locked_page->is_root)
					{
						// get a new page to insert between the root and its children
						uint64_t root_least_keys_child_id;
						void* root_least_keys_child = dam_p->get_new_page_with_write_lock(dam_p->context, &root_least_keys_child_id);

						// clone root page contents into the new root_least_keys_child
						clone_page(root_least_keys_child, bpttd_p->page_size, bpttd_p->index_def, 1, curr_locked_page->page);

						// re intialize root page as an interior page
						init_bplus_tree_interior_page(curr_locked_page->page, ++curr_locked_page->level, bpttd_p);
						set_least_keys_page_id_of_bplus_tree_interior_page(curr_locked_page->page, root_least_keys_child_id, bpttd_p);

						// create new locked_page_info for the root_least_keys_child and push it onto the stack
						locked_page_info* root_least_keys_child_info = get_new_locked_page_info(root_least_keys_child, root_least_keys_child_id, 1, 0, bpttd_p);
						root_least_keys_child_info->child_index = curr_locked_page->child_index;
						curr_locked_page->child_index = -1; // the root page only has a single child at the moment
						push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, root_least_keys_child_info);

						// get new top of the stack
						curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
					}

					new_parent_insert = split_insert_bplus_tree_interior_page(curr_locked_page->page, curr_locked_page->page_id, parent_insert, insertion_point, bpttd_p, dam_p);
					
					if(new_parent_insert != NULL)
						parent_tuple_inserted = 1;
					else // THIS IS AN ERROR WE CANT RECOVER FROM
						break;
				}
			}

			// delete the old parent insert and update it
			free((void*)parent_insert);
			parent_insert = new_parent_insert;

			// if an insertion was done on this page then lock on this page should be released with modification
			if(parent_tuple_inserted) // if this condition fails then there is HUGE INTERNAL FAILURE
			{
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
				unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);
			}

			// if parent_tuple was inserted without a split, then no need to iterate further in the loop
			if(parent_insert == NULL)
				break;
		}
	}

	// release locks on all the pages, we had locks on until now
	fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);

	deinitialize_arraylist(&locked_pages_stack);

	return inserted;
}

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	locked_page_info* curr_locked_page = lock_page_and_get_new_locked_page_info(root_page_id, 1, 1, bpttd_p, dam_p);

	// create a stack of capacity = levels + 3 
	arraylist locked_pages_stack;
	initialize_arraylist(&locked_pages_stack, curr_locked_page->level + 4);

	// push the root page onto the stack
	push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, curr_locked_page);

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(!is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		if(curr_locked_page->level > 0) // is not a leaf page
		{
			// figure out which child page to go to next
			curr_locked_page->child_index = find_child_index_for_key(curr_locked_page->page, key, bpttd_p);

			// check if a merge happens at child_index of this curr_locked_page, will this page be required to be merged aswell
			if(!curr_locked_page->is_root && !may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, curr_locked_page->child_index) )
			{
				// release locks on all the pages in stack except the curr_locked_page

				// pop the curr_locked_page
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

				// unloc all its parent pages
				fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);

				// push the curr_locked_page back in the stack
				push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, curr_locked_page);
			}

			uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index, bpttd_p);

			// get lock on the next child page (this page is surely not the root page)
			locked_page_info* child_locked_page = lock_page_and_get_new_locked_page_info(child_page_id, 1, 0, bpttd_p, dam_p);

			// push this child page onto the stack
			push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, child_locked_page);
		}
		else
			break;
	}

	// deleted will be set if the record, was deleted
	int deleted = 0;

	while(!is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		if(curr_locked_page->level == 0) // is a leaf page, perform delete in the leaf page
		{
			// find first index of first record that has the given key
			uint32_t found_index = find_first_in_sorted_packed_page(
												curr_locked_page->page, bpttd_p->page_size,
												bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
												key, bpttd_p->key_def, NULL
											);

			// if no such record can be found, we break and exit
			if(NO_TUPLE_FOUND == found_index)
				break;

			// perform a delete operation on the found index in this page
			deleted = delete_in_sorted_packed_page(
								curr_locked_page->page, bpttd_p->page_size,
								bpttd_p->record_def,
								found_index
							);

			if(deleted)
			{
				// pop the curr_locked_page, this is the page we would be merging
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

				// go ahead with merging only if the page is lesser than half full AND is not root
				if(curr_locked_page->is_root || !is_page_lesser_than_half_full(curr_locked_page->page, bpttd_p->page_size, bpttd_p->record_def))
				{
					unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);
					break;
				}

				locked_page_info* parent_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
				uint32_t parent_tuple_count = get_tuple_count(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

				// will be set of the page has been merged
				int merged = 0;

				// attempt a merge with next page of curr_locked_page, if it has a next page with same parent
				if(!merged && parent_locked_page->child_index + 1 < parent_tuple_count)
				{
					merged = merge_bplus_tree_leaf_pages(curr_locked_page->page, curr_locked_page->page_id, bpttd_p, dam_p);

					// if merged delete entry at child_index+1 in the parent page
					if(merged)
						delete_in_sorted_packed_page(
								parent_locked_page->page, bpttd_p->page_size, 
								bpttd_p->index_def, 
								parent_locked_page->child_index + 1
							);
				}

				// attempt a merge with prev page of curr_locked_page, if it has a prev page with same parent
				if(!merged && parent_locked_page->child_index < parent_tuple_count)
				{
					// release lock on the curr_locked_page
					unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);

					// make the previous of curr_locked_page as the curr_locked_page
					uint32_t prev_child_page_id = find_child_page_id_by_child_index(parent_locked_page->page, parent_locked_page->child_index - 1, bpttd_p);
					curr_locked_page = lock_page_and_get_new_locked_page_info(prev_child_page_id, 1, 0, bpttd_p, dam_p);

					merged = merge_bplus_tree_leaf_pages(curr_locked_page->page, curr_locked_page->page_id, bpttd_p, dam_p);

					// if merged delete entry at child_index+1 in the parent page
					if(merged)
					{
						delete_in_sorted_packed_page(
								parent_locked_page->page, bpttd_p->page_size, 
								bpttd_p->index_def, 
								parent_locked_page->child_index
							);
					}
					else
					{
						unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 0, dam_p);
						curr_locked_page = NULL;
					}
				}

				// after merging is completed curr_locked_page points to an existing page

				// if merge was successfull, then update the parent_tuple_count
				if(merged)
					parent_tuple_count = get_tuple_count(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

				// need to handle empty root parent page 
				if(merged && parent_locked_page->is_root && parent_tuple_count == 0)
				{
					// clone the curr_locked_page in to the parent_locked_page
					clone_page(parent_locked_page->page, bpttd_p->page_size, bpttd_p->record_def, 1, curr_locked_page->page);

					// free and unlock curr_locked_page
					unlock_page_and_delete_locked_page_info(curr_locked_page, 1, 1, dam_p);

					// pop parent_locked_page
					pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

					// make parent locked page as the current page
					curr_locked_page = parent_locked_page;
					parent_locked_page = NULL;
				}

				// release lock on the curr_locked_page
				if(curr_locked_page != NULL)
					unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);

				if(!merged)
					break;
			}
			else // this case should never occur
				break;
		}
		else // check if the curr_locked_page needs to be merged, if yes then merge it with either previous or next page
		{
			// pop the curr_locked_page, this is the page we would be merging
			pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

			// go ahead with merging only if the page is lesser than half full AND is not root
			if(curr_locked_page->is_root || !is_page_lesser_than_half_full(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def))
			{
				unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);
				break;
			}

			locked_page_info* parent_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
			uint32_t parent_tuple_count = get_tuple_count(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

			// will be set if the page has been merged
			int merged = 0;

			// attempt a merge with next page of curr_locked_page, if it has a next page with same parent
			if(!merged && parent_locked_page->child_index + 1 < parent_tuple_count)
			{
				locked_page_info* child_page1 = curr_locked_page;

				uint64_t page2_id = find_child_page_id_by_child_index(parent_locked_page->page, parent_locked_page->child_index + 1, bpttd_p);
				locked_page_info* child_page2 = lock_page_and_get_new_locked_page_info(page2_id, 1, 0, bpttd_p, dam_p);

				const void* separator_parent_tuple = get_nth_tuple(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, parent_locked_page->child_index + 1);

				merged = merge_bplus_tree_interior_pages(child_page1->page, child_page1->page_id, separator_parent_tuple, child_page2->page, child_page2->page_id, bpttd_p, dam_p);

				// if merged delete entry at child_index+1 in the parent page, and free child_page2
				if(merged)
				{
					delete_in_sorted_packed_page(
							parent_locked_page->page, bpttd_p->page_size, 
							bpttd_p->index_def, 
							parent_locked_page->child_index + 1
						);

					unlock_page_and_delete_locked_page_info(child_page2, 1, 0, dam_p);

					curr_locked_page = child_page1;
				}
				else // release lock on the page that is not curr_locked_page
					unlock_page_and_delete_locked_page_info(child_page2, 0, 0, dam_p);
			}

			// attempt a merge with prev page of curr_locked_page, if it has a prev page with same parent
			if(!merged && parent_locked_page->child_index < parent_tuple_count)
			{
				locked_page_info* child_page2 = curr_locked_page;

				uint64_t page1_id = find_child_page_id_by_child_index(parent_locked_page->page, parent_locked_page->child_index - 1, bpttd_p);
				locked_page_info* child_page1 = lock_page_and_get_new_locked_page_info(page1_id, 1, 0, bpttd_p, dam_p);

				const void* separator_parent_tuple = get_nth_tuple(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, parent_locked_page->child_index);

				merged = merge_bplus_tree_interior_pages(child_page1->page, child_page1->page_id, separator_parent_tuple, child_page2->page, child_page2->page_id, bpttd_p, dam_p);

				// if merged delete entry at child_index+1 in the parent page, and free child_page2
				if(merged)
				{
					delete_in_sorted_packed_page(
							parent_locked_page->page, bpttd_p->page_size, 
							bpttd_p->index_def, 
							parent_locked_page->child_index
						);

					unlock_page_and_delete_locked_page_info(child_page2, 1, 0, dam_p);

					curr_locked_page = child_page1;
				}
				else // release lock on the page that is not curr_locked_page
					unlock_page_and_delete_locked_page_info(child_page1, 0, 0, dam_p);
			}

			// after merging is completed curr_locked_page points to an existing page

			// if merge was successfull, then update the parent_tuple_count
			if(merged)
				parent_tuple_count = get_tuple_count(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

			// need to handle empty root parent page 
			if(merged && parent_locked_page->is_root && parent_tuple_count == 0)
			{
				// clone the curr_locked_page in to the parent_locked_page
				clone_page(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, 1, curr_locked_page->page);

				// free and unlock curr_locked_page
				unlock_page_and_delete_locked_page_info(curr_locked_page, 1, 1, dam_p);

				// pop parent_locked_page
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

				// make parent locked page as the current page
				curr_locked_page = parent_locked_page;
				parent_locked_page = NULL;
			}

			// release lock on the curr_locked_page
			unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);

			if(!merged)
				break;
		}
	}

	// release locks on all the pages, we had locks on until now
	fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);

	deinitialize_arraylist(&locked_pages_stack);

	return deleted;
}

int destroy_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	locked_page_info* curr_locked_page = lock_page_and_get_new_locked_page_info(root_page_id, 0, 1, bpttd_p, dam_p);

	// edge case : if the root itself is a leaf page then free it, no locked_pages_stack required
	if(curr_locked_page->level == 0)
	{
		unlock_page_and_delete_locked_page_info(curr_locked_page, 1, 0, dam_p);
		return 1;
	}

	// create a stack of capacity = levels
	arraylist locked_pages_stack;
	initialize_arraylist(&locked_pages_stack, curr_locked_page->level + 1);

	// push the root page onto the stack
	push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, curr_locked_page);

	while(!is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		// if this is an interior page whose all child pages are leaf pages, i.e. level = 1
		if(curr_locked_page->level == 1)
		{
			// free all child leaf pages of this page
			// without acquiring lock on this pages

			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

			// free the child leaf page id at index -1
			uint64_t child_leaf_page_id = find_child_page_id_by_child_index(curr_locked_page->page, -1, bpttd_p);;
			dam_p->free_page(dam_p->context, child_leaf_page_id);

			// free the child leaf page id at index [0, tuple_count)
			for(uint32_t i = 0; i < tuple_count; i++)
			{
				child_leaf_page_id = find_child_page_id_by_child_index(curr_locked_page->page, i, bpttd_p);;
				dam_p->free_page(dam_p->context, child_leaf_page_id);
			}

			// pop it from the stack and unlock it and free it aswell
			pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
			unlock_page_and_delete_locked_page_info(curr_locked_page, 1, 0, dam_p);
		}
		// handle free-ing on pages at level >= 2
		else
		{
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

			// if child index is -1 or lesser than tuple_count
			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count)
			{
				// then push it's child at child_index onto the stack (with child_index = -1), while incrementing its child index
				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index, bpttd_p);
				locked_page_info* locked_child_page_info = lock_page_and_get_new_locked_page_info(child_page_id, 0, 0, bpttd_p, dam_p);
				locked_child_page_info->child_index = -1;
				curr_locked_page->child_index++;

				push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, locked_child_page_info);
			}
			else // we have free all its children, now we free this page
			{
				// here the child_index of the page is tuple_count

				// pop it from the stack and unlock it and free it aswell
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
				unlock_page_and_delete_locked_page_info(curr_locked_page, 1, 0, dam_p);
			}
		}
	}

	// release locks on all the pages, we had locks on until now
	fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);

	deinitialize_arraylist(&locked_pages_stack);

	return 1;
}

void print_bplus_tree(uint64_t root_page_id, int only_leaf_pages, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// print the root page id of the bplsu tree
	printf("\n\nBplus_tree @ root_page_id = %lu\n\n", root_page_id);

	// get lock on the root page of the bplus_tree
	locked_page_info* curr_locked_page = lock_page_and_get_new_locked_page_info(root_page_id, 0, 1, bpttd_p, dam_p);

	// create a stack of capacity = levels
	arraylist locked_pages_stack;
	initialize_arraylist(&locked_pages_stack, curr_locked_page->level + 1);

	// push the root page onto the stack
	push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, curr_locked_page);

	while(!is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		// print current page as a leaf page
		if(curr_locked_page->level == 0)
		{
			// print this page and its page_id
			printf("page_id : %llu\n\n", (unsigned long long int)curr_locked_page->page_id);
			print_bplus_tree_leaf_page(curr_locked_page->page, bpttd_p);
			printf("xxxxxxxxxxxxx\n\n");

			// pop it from the stack and unlock it
			pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
			unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 0, dam_p);
		}
		// print current page as an interior page
		else
		{
			// get tuple_count of the page
			uint32_t tuple_count = get_tuple_count(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

			// if child index is -1 or lesser than tuple_count
			if(curr_locked_page->child_index == -1 || curr_locked_page->child_index < tuple_count)
			{
				// then push it's child at child_index onto the stack (with child_index = -1), while incrementing its child index
				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index, bpttd_p);
				locked_page_info* locked_child_page_info = lock_page_and_get_new_locked_page_info(child_page_id, 0, 0, bpttd_p, dam_p);
				locked_child_page_info->child_index = -1;
				curr_locked_page->child_index++;

				push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, locked_child_page_info);
			}
			else // we have printed all its children, now we print this page
			{
				// here the child_index of the page is tuple_count

				if(!only_leaf_pages)
				{
					// print this page and its page_id
					printf("page_id : %llu\n\n", (unsigned long long int)curr_locked_page->page_id);
					print_bplus_tree_interior_page(curr_locked_page->page, bpttd_p);
					printf("xxxxxxxxxxxxx\n");
				}

				// pop it from the stack and unlock it
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);
				unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 0, dam_p);
			}
		}
	}

	// release locks on all the pages, we had locks on until now
	fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);

	deinitialize_arraylist(&locked_pages_stack);
}