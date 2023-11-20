#include<bplus_tree_split_insert_util.h>

#include<storage_capacity_page_util.h>
#include<bplus_tree_leaf_page_util.h>
#include<bplus_tree_interior_page_util.h>
#include<bplus_tree_page_header.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_interior_page_header.h>
#include<sorted_packed_page_util.h>
#include<persistent_page_functions.h>

#include<tuple.h>

#include<stdlib.h>

int walk_down_locking_parent_pages_for_split_insert_using_record(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const void* transaction_id, int* abort_error)
{
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
		// if curr_locked_page will not require a split, then release locks on all the parent pages of curr_locked_page
		if(!may_require_split_for_insert_for_bplus_tree(&(curr_locked_page->ppage), bpttd_p->pas.page_size, bpttd_p->index_def))
		{
			while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1) // (do not release lock on the curr_locked_page)
			{
				locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
				release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
				pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

				if(*abort_error)
					goto ABORT_ERROR;
			}
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

	// get leaf from locked_pages_stack
	locked_page_info* leaf_locked_page = get_top_of_locked_pages_stack(locked_pages_stack_p);

	// if the leaf_locked_page must not split on insertion of record, then release all locks on all the parent pages
	if(!must_split_for_insert_bplus_tree_leaf_page(&(leaf_locked_page->ppage), record, bpttd_p))
	{
		while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 1) // (do not release lock on the leaf_locked_page)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);

			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	return 1;

	ABORT_ERROR:;
	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	return 0;
}

int split_insert_and_unlock_pages_up(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// inserted will be set if the record, was inserted
	int inserted = 0;

	// a tuple that needs to be inserted in to the parent page
	// this happens upon a split
	void* parent_insert = NULL;

	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info curr_locked_page = *get_top_of_locked_pages_stack(locked_pages_stack_p);
		pop_from_locked_pages_stack(locked_pages_stack_p);

		if(is_bplus_tree_leaf_page(&(curr_locked_page.ppage), bpttd_p)) // is a leaf page, insert / split_insert record to the leaf page
		{
			// check if the record already exists in this leaf page
			int found = (NO_TUPLE_FOUND != find_last_in_sorted_packed_page(
												&(curr_locked_page.ppage), bpttd_p->pas.page_size, 
												bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
												record, bpttd_p->record_def, bpttd_p->key_element_ids
											));

			if(found)
			{
				release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			// if it does not already exist then try to insert it
			uint32_t insertion_point;
			inserted = insert_to_sorted_packed_page(
									&(curr_locked_page.ppage), bpttd_p->pas.page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									record, 
									&insertion_point,
									pmm_p,
									transaction_id,
									abort_error
								);

			if((*abort_error) || inserted)
			{
				release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			// if it fails then call the split insert

			// but before calling split insert we make sure that the page to be split is not a root page
			if(curr_locked_page.ppage.page_id == root_page_id)
			{
				// curr_locked_page is the root_page, get its level
				uint32_t root_page_level = get_level_of_bplus_tree_page(&(curr_locked_page.ppage), bpttd_p);

				// get a new page to insert between the root and its children
				persistent_page root_least_keys_child = get_new_persistent_page_with_write_lock(dam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// clone root page contents into the new root_least_keys_child
				clone_persistent_page(pmm_p, transaction_id, &(root_least_keys_child), bpttd_p->pas.page_size, &(bpttd_p->record_def->size_def), &(curr_locked_page.ppage), abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// re intialize root page as an interior page
				init_bplus_tree_interior_page(&(curr_locked_page.ppage), ++root_page_level, 1, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// then set its least_keys_page_id to this root_least_keys_child.page_id
				bplus_tree_interior_page_header root_page_header = get_bplus_tree_interior_page_header(&(curr_locked_page.ppage), bpttd_p);
				root_page_header.least_keys_page_id = root_least_keys_child.page_id;
				set_bplus_tree_interior_page_header(&(curr_locked_page.ppage), &root_page_header, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// create new locked_page_info for the root_least_keys_child
				locked_page_info root_least_keys_child_info = INIT_LOCKED_PAGE_INFO(root_least_keys_child);
				root_least_keys_child_info.child_index = curr_locked_page.child_index;
				curr_locked_page.child_index = -1; // the root page only has a single child at the moment

				push_to_locked_pages_stack(locked_pages_stack_p, &curr_locked_page);
				curr_locked_page = root_least_keys_child_info;
			}

			// make parent_insert hold enough memory to build any interior page record possible by this bplus_tree
			parent_insert = malloc(bpttd_p->max_index_record_size);
			if(parent_insert == NULL)
				exit(-1);

			inserted = split_insert_bplus_tree_leaf_page(&(curr_locked_page.ppage), record, insertion_point, bpttd_p, dam_p, pmm_p, transaction_id, abort_error, parent_insert);
			if(*abort_error)
			{
				release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
			if(*abort_error)
				break;
		}
		else
		{
			int parent_tuple_inserted = 0;

			uint32_t insertion_point = curr_locked_page.child_index + 1;
			parent_tuple_inserted = insert_at_in_sorted_packed_page(
									&(curr_locked_page.ppage), bpttd_p->pas.page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									parent_insert, 
									insertion_point,
									pmm_p,
									transaction_id,
									abort_error
								);

			if((*abort_error) || parent_tuple_inserted)
			{
				release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			// if it fails then call the split insert

			// but before calling split insert we make sure that the page to be split is not a root page
			if(curr_locked_page.ppage.page_id == root_page_id)
			{
				// curr_locked_page is the root_page, get its level
				uint32_t root_page_level = get_level_of_bplus_tree_page(&(curr_locked_page.ppage), bpttd_p);

				// get a new page to insert between the root and its children
				persistent_page root_least_keys_child = get_new_persistent_page_with_write_lock(dam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// clone root page contents into the new root_least_keys_child
				clone_persistent_page(pmm_p, transaction_id, &root_least_keys_child, bpttd_p->pas.page_size, &(bpttd_p->index_def->size_def), &(curr_locked_page.ppage), abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// re intialize root page as an interior page
				init_bplus_tree_interior_page(&(curr_locked_page.ppage), ++root_page_level, 1, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// then set its least_keys_page_id to this root_least_keys_child.page_id
				bplus_tree_interior_page_header root_page_header = get_bplus_tree_interior_page_header(&(curr_locked_page.ppage), bpttd_p);
				root_page_header.least_keys_page_id = root_least_keys_child.page_id;
				set_bplus_tree_interior_page_header(&(curr_locked_page.ppage), &root_page_header, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(dam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// create new locked_page_info for the root_least_keys_child
				locked_page_info root_least_keys_child_info = INIT_LOCKED_PAGE_INFO(root_least_keys_child);
				root_least_keys_child_info.child_index = curr_locked_page.child_index;
				curr_locked_page.child_index = -1; // the root page only has a single child at the moment

				// push curr back on to the stack
				push_to_locked_pages_stack(locked_pages_stack_p, &curr_locked_page);
				curr_locked_page = root_least_keys_child_info;
			}

			parent_tuple_inserted = split_insert_bplus_tree_interior_page(&(curr_locked_page.ppage), parent_insert, insertion_point, bpttd_p, dam_p, pmm_p, transaction_id, abort_error, parent_insert);
			if(*abort_error)
			{
				release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			release_lock_on_persistent_page(dam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
			if(*abort_error)
				break;
		}
	}

	if(parent_insert != NULL)
		free(parent_insert);

	// release locks on all the pages, we had locks on until now
	while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
	{
		locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
		release_lock_on_persistent_page(dam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
		pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
	}

	if(*abort_error)
		return 0;

	return inserted;
}