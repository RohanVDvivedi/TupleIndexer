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

	// inserted will be set if the record, was inserted
	int inserted = 0;

	// a tuple that needs to be inserted in to the parent page
	// this happens upon a split
	const void* parent_insert = NULL;

	while(!is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		if(!inserted) // go deeper towards the leaf of the tree, pushing curr_locked_page's child on the stack
		{
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

				// found or the record was inserted without requiring a split
				if(parent_insert != NULL) // the split needs to be propogated to the parent pages
				{
					pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

					unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);
				}
				else
					break;
			}
			else // is an interior page, grab its child and push it into the stack
			{
				// figure out which child page to go to next
				curr_locked_page->child_index = find_child_index_for_record(curr_locked_page->page, record, bpttd_p);

				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index, bpttd_p);

				// get lock on the next child page (this page is surely not the root page)
				locked_page_info* child_locked_page = lock_page_and_get_new_locked_page_info(child_page_id, 1, 0, bpttd_p, dam_p);

				// if it will not require a split, then release locks on all the parent pages
				if( (child_locked_page->level == 0 && !may_require_split_for_insert_for_bplus_tree(child_locked_page->page, bpttd_p->page_size, bpttd_p->record_def))
				||  (child_locked_page->level  > 0 && !may_require_split_for_insert_for_bplus_tree(child_locked_page->page, bpttd_p->page_size, bpttd_p->index_def )) )
				{
					fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);
				}

				// push this child page onto the stack
				push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, child_locked_page);
			}
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

					new_parent_insert = split_insert_bplus_tree_interior_page(curr_locked_page->page, curr_locked_page->page_id, record, insertion_point, bpttd_p, dam_p);
					
					if(new_parent_insert != NULL)
						parent_tuple_inserted = 1;
					else // THIS IS AN ERROR WE CANT RECOVER FROM
						break;
				}
			}

			// delete the old parent insert and update it
			free((void*)parent_insert);
			parent_insert = new_parent_insert;

			// if parent_tuple was inserted with a split
			if(parent_insert != NULL) // the split needs to be propogated to the parent pages
			{
				pop_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

				unlock_page_and_delete_locked_page_info(curr_locked_page, 0, 1, dam_p);
			}
			else
				break;
		}
	}

	// release locks on all the pages, we had locks on until now
	fifo_unlock_all_bplus_tree_unmodified_locked_pages_stack(&locked_pages_stack, dam_p);

	deinitialize_arraylist(&locked_pages_stack);

	return inserted;
}

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

int destroy_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p);

void print_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	locked_page_info* curr_locked_page = lock_page_and_get_new_locked_page_info(root_page_id, 0, 1, bpttd_p, dam_p);

	// create a stack of capacity = levels
	arraylist locked_pages_stack;
	initialize_arraylist(&locked_pages_stack, curr_locked_page->level + 1);

	// push the root page onto the stack
	push_stack_bplus_tree_locked_pages_stack(&locked_pages_stack, curr_locked_page);

	while(is_empty_arraylist(&locked_pages_stack))
	{
		curr_locked_page = get_top_stack_bplus_tree_locked_pages_stack(&locked_pages_stack);

		// print current page as a leaf page
		if(curr_locked_page->level == 0)
		{
			// print this page and its page_id
			printf("page_id : %llu\n\n", (unsigned long long int)curr_locked_page->page_id);
			print_bplus_tree_leaf_page(curr_locked_page->page, bpttd_p);
			printf("xxxxxxxxxxxxx\n");

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

				// print this page and its page_id
				printf("page_id : %llu\n\n", (unsigned long long int)curr_locked_page->page_id);
				print_bplus_tree_interior_page(curr_locked_page->page, bpttd_p);
				printf("xxxxxxxxxxxxx\n");

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