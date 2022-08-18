#include<bplus_tree.h>

#include<locked_pages_stack.h>
#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>

#include<page_layout.h>
#include<tuple.h>

#include<stdlib.h>

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
			curr_locked_page->child_index = find_child_index_for_record(curr_locked_page->page, record, bpttd_p->key_element_count, TOWARDS_LAST_WITH_KEY, bpttd_p);

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