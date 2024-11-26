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

int split_insert_and_unlock_pages_up(uint64_t root_page_id, locked_pages_stack* locked_pages_stack_p, const void* record, uint32_t insertion_index, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
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
			// if insertion_index is not provided, then find it
			if(insertion_index == INVALID_TUPLE_INDEX)
			{
				insertion_index = find_insertion_point_in_sorted_packed_page(
									&(curr_locked_page.ppage), bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									record
								);
			}
			else // if it was provided then make sure that it is correct, in keeping the sorted ordering correct
			{
				// this will happen only if you do not provide the inputs improperly, hence must never happen
				if(!is_correct_insertion_index_for_insert_at_in_sorted_packed_page(
									&(curr_locked_page.ppage), bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									record, 
									insertion_index
								))
					return 0;
			}

			// if it does not already exist then try to insert it
			uint32_t insertion_point = insertion_index;
			inserted = insert_at_in_sorted_packed_page(
									&(curr_locked_page.ppage), bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									record, 
									insertion_point,
									pmm_p,
									transaction_id,
									abort_error
								);

			// the above function may fail only because of space requirements, because we already checked the insertion_point

			if((*abort_error) || inserted)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			// if it fails then call the split insert

			// but before calling split insert we make sure that the page to be split is not a root page
			if(curr_locked_page.ppage.page_id == root_page_id)
			{
				// curr_locked_page is the root_page, get its level
				uint32_t root_page_level = get_level_of_bplus_tree_page(&(curr_locked_page.ppage), bpttd_p);

				// get a new page to insert between the root and its children
				persistent_page root_least_keys_child = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// clone root page contents into the new root_least_keys_child
				clone_persistent_page(pmm_p, transaction_id, &(root_least_keys_child), bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def), &(curr_locked_page.ppage), abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// re intialize root page as an interior page
				init_bplus_tree_interior_page(&(curr_locked_page.ppage), ++root_page_level, 1, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// then set its least_keys_page_id to this root_least_keys_child.page_id
				bplus_tree_interior_page_header root_page_header = get_bplus_tree_interior_page_header(&(curr_locked_page.ppage), bpttd_p);
				root_page_header.least_keys_page_id = root_least_keys_child.page_id;
				set_bplus_tree_interior_page_header(&(curr_locked_page.ppage), &root_page_header, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// create new locked_page_info for the root_least_keys_child
				locked_page_info root_least_keys_child_info = INIT_LOCKED_PAGE_INFO(root_least_keys_child, INVALID_TUPLE_INDEX);
				root_least_keys_child_info.child_index = curr_locked_page.child_index;
				curr_locked_page.child_index = ALL_LEAST_KEYS_CHILD_INDEX; // the root page only has a single child at the moment

				push_to_locked_pages_stack(locked_pages_stack_p, &curr_locked_page);
				curr_locked_page = root_least_keys_child_info;
			}

			// make parent_insert hold enough memory to build any interior page record possible by this bplus_tree
			parent_insert = malloc(bpttd_p->max_index_record_size);
			if(parent_insert == NULL)
				exit(-1);

			inserted = split_insert_bplus_tree_leaf_page(&(curr_locked_page.ppage), record, insertion_point, bpttd_p, pam_p, pmm_p, transaction_id, abort_error, parent_insert);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
			if(*abort_error)
				break;
		}
		else
		{
			int parent_tuple_inserted = 0;

			uint32_t insertion_point = curr_locked_page.child_index + 1;
			parent_tuple_inserted = insert_at_in_sorted_packed_page(
									&(curr_locked_page.ppage), bpttd_p->pas_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									parent_insert, 
									insertion_point,
									pmm_p,
									transaction_id,
									abort_error
								);

			if((*abort_error) || parent_tuple_inserted)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			// if it fails then call the split insert

			// but before calling split insert we make sure that the page to be split is not a root page
			if(curr_locked_page.ppage.page_id == root_page_id)
			{
				// curr_locked_page is the root_page, get its level
				uint32_t root_page_level = get_level_of_bplus_tree_page(&(curr_locked_page.ppage), bpttd_p);

				// get a new page to insert between the root and its children
				persistent_page root_least_keys_child = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// clone root page contents into the new root_least_keys_child
				clone_persistent_page(pmm_p, transaction_id, &root_least_keys_child, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def), &(curr_locked_page.ppage), abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// re intialize root page as an interior page
				init_bplus_tree_interior_page(&(curr_locked_page.ppage), ++root_page_level, 1, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// then set its least_keys_page_id to this root_least_keys_child.page_id
				bplus_tree_interior_page_header root_page_header = get_bplus_tree_interior_page_header(&(curr_locked_page.ppage), bpttd_p);
				root_page_header.least_keys_page_id = root_least_keys_child.page_id;
				set_bplus_tree_interior_page_header(&(curr_locked_page.ppage), &root_page_header, bpttd_p, pmm_p, transaction_id, abort_error);
				if(*abort_error)
				{
					release_lock_on_persistent_page(pam_p, transaction_id, &root_least_keys_child, NONE_OPTION, abort_error);
					release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
					break;
				}

				// create new locked_page_info for the root_least_keys_child
				locked_page_info root_least_keys_child_info = INIT_LOCKED_PAGE_INFO(root_least_keys_child, INVALID_TUPLE_INDEX);
				root_least_keys_child_info.child_index = curr_locked_page.child_index;
				curr_locked_page.child_index = ALL_LEAST_KEYS_CHILD_INDEX; // the root page only has a single child at the moment

				// push curr back on to the stack
				push_to_locked_pages_stack(locked_pages_stack_p, &curr_locked_page);
				curr_locked_page = root_least_keys_child_info;
			}

			parent_tuple_inserted = split_insert_bplus_tree_interior_page(&(curr_locked_page.ppage), parent_insert, insertion_point, bpttd_p, pam_p, pmm_p, transaction_id, abort_error, parent_insert);
			if(*abort_error)
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
				break;
			}

			release_lock_on_persistent_page(pam_p, transaction_id, &(curr_locked_page.ppage), NONE_OPTION, abort_error);
			if(*abort_error)
				break;
		}
	}

	if(parent_insert != NULL)
		free(parent_insert);

	// release locks on all the pages, we had locks on until now in case of an abort
	if(*abort_error)
	{
		while(get_element_count_locked_pages_stack(locked_pages_stack_p) > 0)
		{
			locked_page_info* bottom = get_bottom_of_locked_pages_stack(locked_pages_stack_p);
			release_lock_on_persistent_page(pam_p, transaction_id, &(bottom->ppage), NONE_OPTION, abort_error);
			pop_bottom_from_locked_pages_stack(locked_pages_stack_p);
		}
		return 0;
	}

	return inserted;
}