#include<bplus_tree.h>

#include<bplus_tree_locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

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
	// the tuple to be inserted must not exceed a certain size
	if(get_tuple_size(bpttd_p->record_def, record) > get_maximum_insertable_record_size(bpttd_p))
		return 0;

	// get lock on the root page of the bplus_tree
	void* root_page = dam_p->acquire_page_with_writer_lock(dam_p->context, root_page_id);

	// pre cache level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(root_page, bpttd_p->page_size);

	// create a stack of capacity = levels + 1
	locked_pages_stack* locked_pages_stack_p = new_locked_pages_stack(root_page_level + 2);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, root_page_id));

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no split requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		if(!is_bplus_tree_leaf_page(curr_locked_page->page, bpttd_p->page_size))
		{
			// figure out which child page to go to next
			curr_locked_page->child_index = find_child_index_for_record(curr_locked_page->page, record, bpttd_p);

			// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
			uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index, bpttd_p);
			void* child_page = dam_p->acquire_page_with_writer_lock(dam_p->context, child_page_id);

			// if child page will not require a split, then release locks on all the parent pages
			if( ( is_bplus_tree_leaf_page(child_page, bpttd_p->page_size) && !may_require_split_for_insert_for_bplus_tree(child_page, bpttd_p->page_size, bpttd_p->record_def))
			||  (!is_bplus_tree_leaf_page(child_page, bpttd_p->page_size) && !may_require_split_for_insert_for_bplus_tree(child_page, bpttd_p->page_size, bpttd_p->index_def )) )
			{
				while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
				{
					locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
					dam_p->release_writer_lock_on_page(dam_p->context, bottom->page, 0);
					pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
				}
			}

			// push this child page onto the stack
			push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, child_page_id));
		}
		else // break this loop on reaching a leaf page
			break;
	}

	// inserted will be set if the record, was inserted
	int inserted = 0;

	// a tuple that needs to be inserted in to the parent page
	// this happens upon a split
	void* parent_insert = NULL;

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info curr_locked_page = *get_top_of_locked_pages_stack(locked_pages_stack_p);
		pop_from_locked_pages_stack(locked_pages_stack_p);

		if(is_bplus_tree_leaf_page(curr_locked_page.page, bpttd_p->page_size)) // is a leaf page, insert / split_insert record to the leaf page
		{
			// check if the record already exists in this leaf page
			int found = (NO_TUPLE_FOUND != find_first_in_sorted_packed_page(
												curr_locked_page.page, bpttd_p->page_size, 
												bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
												record, bpttd_p->record_def, bpttd_p->key_element_ids
											));

			if(found)
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 0);
				break;
			}

			// if it does not already exist then try to insert it
			uint32_t insertion_point;
			inserted = insert_to_sorted_packed_page(
									curr_locked_page.page, bpttd_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
									record, 
									&insertion_point
								);

			if(inserted)
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
				break;
			}

			// if the insertion fails
			// check if the insert can succeed on compaction
			if(can_insert_this_tuple_without_split_for_bplus_tree(curr_locked_page.page, bpttd_p->page_size, bpttd_p->record_def, record))
			{
				run_page_compaction(curr_locked_page.page, bpttd_p->page_size, bpttd_p->record_def, 0, 1);

				inserted = insert_at_in_sorted_packed_page(
														curr_locked_page.page, bpttd_p->page_size, 
														bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
														record, 
														insertion_point
													);
			}

			if(inserted)
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
				break;
			}

			// if it still fails then call the split insert

			// but before calling split insert we make sure that the page to be split is not a root page
			if(curr_locked_page.page_id == root_page_id)
			{
				// get a new page to insert between the root and its children
				uint64_t root_least_keys_child_id;
				void* root_least_keys_child = dam_p->get_new_page_with_write_lock(dam_p->context, &root_least_keys_child_id);

				// clone root page contents into the new root_least_keys_child
				clone_page(root_least_keys_child, bpttd_p->page_size, bpttd_p->record_def, 1, curr_locked_page.page);

				// re intialize root page as an interior page
				init_bplus_tree_interior_page(curr_locked_page.page, ++root_page_level, 1, bpttd_p);
				set_least_keys_page_id_of_bplus_tree_interior_page(curr_locked_page.page, root_least_keys_child_id, bpttd_p);

				// create new locked_page_info for the root_least_keys_child
				locked_page_info root_least_keys_child_info = INIT_LOCKED_PAGE_INFO(root_least_keys_child, root_least_keys_child_id);
				root_least_keys_child_info.child_index = curr_locked_page.child_index;
				curr_locked_page.child_index = -1; // the root page only has a single child at the moment

				push_to_locked_pages_stack(locked_pages_stack_p, &curr_locked_page);
				curr_locked_page = root_least_keys_child_info;
			}

			if(is_fixed_sized_tuple_def(bpttd_p->index_def))
				parent_insert = malloc(bpttd_p->index_def->size);
			else
			{
				uint32_t largest_index_tuple_size = (get_space_to_be_allotted_to_all_tuples(sizeof_INTERIOR_PAGE_HEADER(bpttd_p), bpttd_p->page_size, bpttd_p->index_def) / 2) - get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->index_def);
				parent_insert = malloc(largest_index_tuple_size);
			}

			inserted = split_insert_bplus_tree_leaf_page(curr_locked_page.page, curr_locked_page.page_id, record, insertion_point, bpttd_p, dam_p, parent_insert);

			// if an insertion was done (at this point a split was also performed), on this page
			// then lock on this page should be released with modification
			// and in the next loop we continue to insert parent_insert to parent page
			if(inserted)
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
			else // THIS IS AN ERR, WE CANT RECOVER FROM
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 0);
				break;
			}
		}
		else
		{
			int parent_tuple_inserted = 0;

			uint32_t insertion_point = curr_locked_page.child_index + 1;
			parent_tuple_inserted = insert_at_in_sorted_packed_page(
									curr_locked_page.page, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									parent_insert, 
									insertion_point
								);

			if(parent_tuple_inserted)
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
				break;
			}

			// check if the insert can succeed on compaction
			if(can_insert_this_tuple_without_split_for_bplus_tree(curr_locked_page.page, bpttd_p->page_size, bpttd_p->index_def, parent_insert))
			{
				run_page_compaction(curr_locked_page.page, bpttd_p->page_size, bpttd_p->index_def, 0, 1);

				parent_tuple_inserted = insert_at_in_sorted_packed_page(
										curr_locked_page.page, bpttd_p->page_size, 
										bpttd_p->index_def, NULL, bpttd_p->key_element_count,
										parent_insert, 
										insertion_point
									);
			}

			if(parent_tuple_inserted)
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
				break;
			}

			// if it still fails then call the split insert

			// but before calling split insert we make sure that the page to be split is not a root page
			if(curr_locked_page.page_id == root_page_id)
			{
				// get a new page to insert between the root and its children
				uint64_t root_least_keys_child_id;
				void* root_least_keys_child = dam_p->get_new_page_with_write_lock(dam_p->context, &root_least_keys_child_id);

				// clone root page contents into the new root_least_keys_child
				clone_page(root_least_keys_child, bpttd_p->page_size, bpttd_p->index_def, 1, curr_locked_page.page);

				// re intialize root page as an interior page
				init_bplus_tree_interior_page(curr_locked_page.page, ++root_page_level, 1, bpttd_p);
				set_least_keys_page_id_of_bplus_tree_interior_page(curr_locked_page.page, root_least_keys_child_id, bpttd_p);

				// create new locked_page_info for the root_least_keys_child
				locked_page_info root_least_keys_child_info = INIT_LOCKED_PAGE_INFO(root_least_keys_child, root_least_keys_child_id);
				root_least_keys_child_info.child_index = curr_locked_page.child_index;
				curr_locked_page.child_index = -1; // the root page only has a single child at the moment

				// push curr back on to the stack
				push_to_locked_pages_stack(locked_pages_stack_p, &curr_locked_page);
				curr_locked_page = root_least_keys_child_info;
			}

			parent_tuple_inserted = split_insert_bplus_tree_interior_page(curr_locked_page.page, curr_locked_page.page_id, parent_insert, insertion_point, bpttd_p, dam_p, parent_insert);

			// if an insertion was done (at this point a split was also performed), on this page
			// then lock on this page should be released with modification
			// and in the next loop we continue to insert parent_insert to parent page
			if(parent_tuple_inserted)
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
			else // THIS IS AN ERR, WE CANT RECOVER FROM
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 0);
				break;
			}
		}
	}

	if(parent_insert != NULL)
		free(parent_insert);

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		dam_p->release_writer_lock_on_page(dam_p->context, bottom->page, 0);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	delete_locked_pages_stack(locked_pages_stack_p);

	return inserted;
}

int delete_from_bplus_tree(uint64_t root_page_id, const void* key, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	void* root_page = dam_p->acquire_page_with_writer_lock(dam_p->context, root_page_id);

	// pre cache level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(root_page, bpttd_p->page_size);

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = new_locked_pages_stack(root_page_level + 1);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, root_page_id));

	// perform a downward pass until you reach the leaf locking all the pages, unlocking all the safe pages (no merge requiring) in the interim
	while(1)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		if(!is_bplus_tree_leaf_page(curr_locked_page->page, bpttd_p->page_size)) // is not a leaf page
		{
			// figure out which child page to go to next
			curr_locked_page->child_index = find_child_index_for_key(curr_locked_page->page, key, bpttd_p);

			// check if a merge happens at child_index of this curr_locked_page, will this page be required to be merged aswell
			if(curr_locked_page->page_id != root_page_id && !may_require_merge_or_redistribution_for_delete_for_bplus_tree_interior_page(curr_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, curr_locked_page->child_index) )
			{
				// release locks on all the pages in stack except for the the curr_locked_page
				while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1)
				{
					locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
					dam_p->release_writer_lock_on_page(dam_p->context, bottom->page, 0);
					pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
				}
			}

			// get lock on the child page (this page is surely not the root page) at child_index in curr_locked_page
			uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index, bpttd_p);
			void* child_page = dam_p->acquire_page_with_writer_lock(dam_p->context, child_page_id);

			// push this child page onto the stack
			push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, child_page_id));
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

		if(is_bplus_tree_leaf_page(curr_locked_page.page, bpttd_p->page_size)) // is a leaf page, perform delete in the leaf page
		{
			// find first index of first record that has the given key
			uint32_t found_index = find_first_in_sorted_packed_page(
												curr_locked_page.page, bpttd_p->page_size,
												bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_element_count,
												key, bpttd_p->key_def, NULL
											);

			// if no such record can be found, we break and exit
			if(NO_TUPLE_FOUND == found_index)
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 0);
				break;
			}

			// perform a delete operation on the found index in this page
			deleted = delete_in_sorted_packed_page(
								curr_locked_page.page, bpttd_p->page_size,
								bpttd_p->record_def,
								found_index
							);

			if(!deleted) // THIS IS AN ERR, WE CANT RECOVER FROM
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 0);
				break;
			}

			// go ahead with merging only if the page is lesser than half full AND is not root
			// i.e. we can not merge a page which is root OR is more than half full
			if(curr_locked_page.page_id == root_page_id || is_page_more_than_half_full(curr_locked_page.page, bpttd_p->page_size, bpttd_p->record_def))
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
				break;
			}

			// We will now check to see if the page can be merged

			locked_page_info* parent_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
			uint32_t parent_tuple_count = get_tuple_count(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

			// will be set of the page has been merged
			int merged = 0;

			// attempt a merge with next page of curr_locked_page, if it has a next page with same parent
			if(!merged && parent_locked_page->child_index + 1 < parent_tuple_count)
			{
				merged = merge_bplus_tree_leaf_pages(curr_locked_page.page, curr_locked_page.page_id, bpttd_p, dam_p);

				// if merged we need to delete entry at child_index in the parent page
				if(merged)
					parent_locked_page->child_index += 1;
			}

			// attempt a merge with prev page of curr_locked_page, if it has a prev page with same parent
			if(!merged && parent_locked_page->child_index < parent_tuple_count)
			{
				// release lock on the curr_locked_page
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);

				// make the previous of curr_locked_page as the curr_locked_page
				uint32_t prev_child_page_id = find_child_page_id_by_child_index(parent_locked_page->page, parent_locked_page->child_index - 1, bpttd_p);
				void* prev_child_page = dam_p->acquire_page_with_writer_lock(dam_p->context, prev_child_page_id);
				curr_locked_page = INIT_LOCKED_PAGE_INFO(prev_child_page, prev_child_page_id);

				merged = merge_bplus_tree_leaf_pages(curr_locked_page.page, curr_locked_page.page_id, bpttd_p, dam_p);

				// if merged we need to delete entry at child_index in the parent page

				if(!merged)
				{
					dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 0);

					// mark curr_locked_page as empty / locks already released
					curr_locked_page = INIT_LOCKED_PAGE_INFO(NULL, bpttd_p->NULL_PAGE_ID);
				}
			}

			// release lock on the curr_locked_page, if not released yet
			if(curr_locked_page.page_id != bpttd_p->NULL_PAGE_ID)
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);

			if(!merged)
				break;
		}
		else // check if the curr_locked_page needs to be merged, if yes then merge it with either previous or next page
		{
			// perform a delete operation on the found index in this page
			deleted = delete_in_sorted_packed_page(
								curr_locked_page.page, bpttd_p->page_size,
								bpttd_p->index_def,
								curr_locked_page.child_index
							);

			if(!deleted) // THIS IS AN ERR, WE CANT RECOVER FROM
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 0);
				break;
			}

			if(curr_locked_page.page_id == root_page_id)
			{
				// need to handle empty root parent page
				// we clone the contents of the <only child of the root page> to the <root page>, to reduce the level of the page
				// we can do this only if the root is an interior page i.e. root_page_level > 0
				while(root_page_level > 0 && get_tuple_count(curr_locked_page.page, bpttd_p->page_size, bpttd_p->index_def) == 0)
				{
					uint64_t only_child_page_id = find_child_page_id_by_child_index(curr_locked_page.page, -1, bpttd_p);
					void* only_child_page = dam_p->acquire_page_with_reader_lock(dam_p->context, only_child_page_id);

					// clone the only_child_page in to the curr_locked_page
					if(is_bplus_tree_leaf_page(only_child_page, bpttd_p->page_size))
						clone_page(curr_locked_page.page, bpttd_p->page_size, bpttd_p->record_def, 1, only_child_page);
					else
						clone_page(curr_locked_page.page, bpttd_p->page_size, bpttd_p->index_def, 1, only_child_page);

					// free and unlock only_child_page
					dam_p->release_reader_lock_and_free_page(dam_p->context, only_child_page);

					// root_page_level will now be what was the level of its child (root_page_level -= 1, should have sufficed here)
					root_page_level = get_level_of_bplus_tree_page(curr_locked_page.page, bpttd_p->page_size);
				}

				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
				break;
			}

			// go ahead with merging only if the page is lesser than half full
			// i.e. we can not merge a page which is more than half full
			if(is_page_more_than_half_full(curr_locked_page.page, bpttd_p->page_size, bpttd_p->index_def))
			{
				dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);
				break;
			}

			locked_page_info* parent_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);
			uint32_t parent_tuple_count = get_tuple_count(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def);

			// will be set if the page has been merged
			int merged = 0;

			// attempt a merge with next page of curr_locked_page, if it has a next page with same parent
			if(!merged && parent_locked_page->child_index + 1 < parent_tuple_count)
			{
				locked_page_info child_page1 = curr_locked_page;

				uint64_t page2_id = find_child_page_id_by_child_index(parent_locked_page->page, parent_locked_page->child_index + 1, bpttd_p);
				void* page2 = dam_p->acquire_page_with_writer_lock(dam_p->context, page2_id);
				locked_page_info child_page2 = INIT_LOCKED_PAGE_INFO(page2, page2_id);

				const void* separator_parent_tuple = get_nth_tuple(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, parent_locked_page->child_index + 1);

				merged = merge_bplus_tree_interior_pages(child_page1.page, child_page1.page_id, separator_parent_tuple, child_page2.page, child_page2.page_id, bpttd_p, dam_p);

				// if merged we need to delete entry at child_index in the parent page, and free child_page2
				if(merged)
				{
					parent_locked_page->child_index += 1;

					dam_p->release_writer_lock_and_free_page(dam_p->context, child_page2.page);
				}
				else // release lock on the page that is not curr_locked_page
					dam_p->release_writer_lock_on_page(dam_p->context, child_page2.page, 0);
			}

			// attempt a merge with prev page of curr_locked_page, if it has a prev page with same parent
			if(!merged && parent_locked_page->child_index < parent_tuple_count)
			{
				locked_page_info child_page2 = curr_locked_page;

				uint64_t page1_id = find_child_page_id_by_child_index(parent_locked_page->page, parent_locked_page->child_index - 1, bpttd_p);
				void* page1 = dam_p->acquire_page_with_writer_lock(dam_p->context, page1_id);
				locked_page_info child_page1 = INIT_LOCKED_PAGE_INFO(page1, page1_id);

				const void* separator_parent_tuple = get_nth_tuple(parent_locked_page->page, bpttd_p->page_size, bpttd_p->index_def, parent_locked_page->child_index);

				merged = merge_bplus_tree_interior_pages(child_page1.page, child_page1.page_id, separator_parent_tuple, child_page2.page, child_page2.page_id, bpttd_p, dam_p);

				// if merged we need to delete entry at child_index in the parent page, and free child_page2
				if(merged)
				{
					dam_p->release_writer_lock_and_free_page(dam_p->context, child_page2.page);

					curr_locked_page = child_page1;
				}
				else // release lock on the page that is not curr_locked_page
					dam_p->release_writer_lock_on_page(dam_p->context, child_page1.page, 0);
			}

			// release lock on the curr_locked_page
			dam_p->release_writer_lock_on_page(dam_p->context, curr_locked_page.page, 1);

			if(!merged)
				break;
		}
	}

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		dam_p->release_writer_lock_on_page(dam_p->context, bottom->page, 0);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	delete_locked_pages_stack(locked_pages_stack_p);

	return deleted;
}

int destroy_bplus_tree(uint64_t root_page_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// get lock on the root page of the bplus_tree
	void* root_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root_page_id);

	// pre cache level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(root_page, bpttd_p->page_size);

	// edge case : if the root itself is a leaf page then free it, no locked_pages_stack required
	if(is_bplus_tree_leaf_page(root_page, bpttd_p->page_size))
	{
		dam_p->release_reader_lock_and_free_page(dam_p->context, root_page);
		return 1;
	}

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = new_locked_pages_stack(root_page_level + 1);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, root_page_id));

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// if this is an interior page whose all child pages are leaf pages, i.e. level = 1
		if(get_level_of_bplus_tree_page(curr_locked_page->page, bpttd_p->page_size) == 1)
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

			// free it and pop it from the stack
			dam_p->release_reader_lock_and_free_page(dam_p->context, curr_locked_page->page);
			pop_from_locked_pages_stack(locked_pages_stack_p);
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
				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index++, bpttd_p);
				void* child_page = dam_p->acquire_page_with_reader_lock(dam_p->context, child_page_id);

				push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, child_page_id));
			}
			else // we have free all its children, now we free this page
			{
				// free it and pop it from the stack
				dam_p->release_reader_lock_and_free_page(dam_p->context, curr_locked_page->page);
				pop_from_locked_pages_stack(locked_pages_stack_p);
			}
		}
	}

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		dam_p->release_reader_lock_on_page(dam_p->context, bottom->page);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	delete_locked_pages_stack(locked_pages_stack_p);

	return 1;
}

void print_bplus_tree(uint64_t root_page_id, int only_leaf_pages, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// print the root page id of the bplsu tree
	printf("\n\nBplus_tree @ root_page_id = %"PRIu64"\n\n", root_page_id);

	// get lock on the root page of the bplus_tree
	void* root_page = dam_p->acquire_page_with_reader_lock(dam_p->context, root_page_id);

	// pre cache level of the root_page
	uint32_t root_page_level = get_level_of_bplus_tree_page(root_page, bpttd_p->page_size);

	// create a stack of capacity = levels
	locked_pages_stack* locked_pages_stack_p = new_locked_pages_stack(root_page_level + 1);

	// push the root page onto the stack
	push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(root_page, root_page_id));

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* curr_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

		// print current page as a leaf page
		if(is_bplus_tree_leaf_page(curr_locked_page->page, bpttd_p->page_size))
		{
			// print this page and its page_id
			printf("page_id : %"PRIu64"\n\n", curr_locked_page->page_id);
			print_bplus_tree_leaf_page(curr_locked_page->page, bpttd_p);
			printf("xxxxxxxxxxxxx\n\n");

			// unlock it and pop it from the stack
			dam_p->release_reader_lock_on_page(dam_p->context, curr_locked_page->page);
			pop_from_locked_pages_stack(locked_pages_stack_p);
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
				uint64_t child_page_id = find_child_page_id_by_child_index(curr_locked_page->page, curr_locked_page->child_index++, bpttd_p);
				void* child_page = dam_p->acquire_page_with_reader_lock(dam_p->context, child_page_id);

				push_to_locked_pages_stack(locked_pages_stack_p, &INIT_LOCKED_PAGE_INFO(child_page, child_page_id));
			}
			else // we have printed all its children, now we print this page
			{
				// here the child_index of the page is tuple_count

				if(!only_leaf_pages)
				{
					// print this page and its page_id
					printf("page_id : %"PRIu64"\n\n", curr_locked_page->page_id);
					print_bplus_tree_interior_page(curr_locked_page->page, bpttd_p);
					printf("xxxxxxxxxxxxx\n");
				}

				// pop it from the stack and unlock it
				dam_p->release_reader_lock_on_page(dam_p->context, curr_locked_page->page);
				pop_from_locked_pages_stack(locked_pages_stack_p);
			}
		}
	}

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		dam_p->release_reader_lock_on_page(dam_p->context, bottom->page);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	delete_locked_pages_stack(locked_pages_stack_p);
}