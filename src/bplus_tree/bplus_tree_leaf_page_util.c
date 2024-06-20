#include<bplus_tree_leaf_page_util.h>

#include<sorted_packed_page_util.h>
#include<bplus_tree_leaf_page_header.h>
#include<bplus_tree_index_tuple_functions_util.h>

#include<persistent_page_functions.h>
#include<virtual_unsplitted_persistent_page.h>

#include<tuple.h>

#include<stdlib.h>

int init_bplus_tree_leaf_page(persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, bpttd_p->pas_p->page_size, sizeof_BPLUS_TREE_LEAF_PAGE_HEADER(bpttd_p), &(bpttd_p->record_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	bplus_tree_leaf_page_header hdr = get_bplus_tree_leaf_page_header(ppage, bpttd_p);
	hdr.parent.type = BPLUS_TREE_LEAF_PAGE;
	hdr.next_page_id = bpttd_p->pas_p->NULL_PAGE_ID;
	hdr.prev_page_id = bpttd_p->pas_p->NULL_PAGE_ID;
	set_bplus_tree_leaf_page_header(ppage, &hdr, bpttd_p, pmm_p, transaction_id, abort_error);
	if((*abort_error))
		return 0;

	return 1;
}

void print_bplus_tree_leaf_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_leaf_page_header(ppage, bpttd_p);
	print_persistent_page(ppage, bpttd_p->pas_p->page_size, bpttd_p->record_def);
}

uint32_t find_greater_equals_for_key_bplus_tree_leaf_page(const persistent_page* ppage, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	return find_succeeding_equals_in_sorted_packed_page(
									ppage, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);
}

uint32_t find_lesser_equals_for_key_bplus_tree_leaf_page(const persistent_page* ppage, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	return find_preceding_equals_in_sorted_packed_page(
									ppage, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);
}

// this will the tuples that will remain in the page_info after after the complete split operation
static uint32_t calculate_final_tuple_count_of_page_to_be_split(const persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p)
{
	// construct a virtual unsplitted persistent page to work on
	virtual_unsplitted_persistent_page vupp = get_virtual_unsplitted_persistent_page(page1, bpttd_p->pas_p->page_size, tuple_to_insert, tuple_to_insert_at, bpttd_p->record_def);

	// get total tuple count that we would be dealing with
	uint32_t total_tuple_count = get_tuple_count_on_virtual_unsplitted_persistent_page(&vupp);

	if(is_fixed_sized_tuple_def(bpttd_p->record_def))
	{
		// if the new tuple is to be inserted after the last tuple in the last leaf page
		if(get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p) == bpttd_p->pas_p->NULL_PAGE_ID && tuple_to_insert_at == total_tuple_count-1)
			return total_tuple_count - 1;	// i.e. only 1 tuple goes to the new page
		else // else equal split
			return total_tuple_count / 2;
	}
	else
	{
		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));

		// this is the result number of tuple that should stay on this page
		uint32_t result = 0;

		// if new tuple is to be inserted after the last tuple in the last leaf page => split it such that it is almost full
		if(get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p) == bpttd_p->pas_p->NULL_PAGE_ID && tuple_to_insert_at == total_tuple_count-1)
		{
			uint32_t limit = space_allotted_to_tuples;

			uint32_t space_occupied_until = 0;

			for(uint32_t i = 0; i < total_tuple_count; i++)
			{
				// process the ith tuple
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples_on_virtual_unsplitted_persistent_page(&vupp, i, i);
				space_occupied_until = space_occupied_by_ith_tuple + space_occupied_until;
				if(space_occupied_until <= limit)
					result++;
				if(space_occupied_until >= limit)
					break;
			}
		}
		else // else => result is the number of tuples that will take the page occupancy just above or equal to 50%
		{
			uint32_t limit = space_allotted_to_tuples / 2;

			uint32_t space_occupied_until = 0;

			for(uint32_t i = 0; i < total_tuple_count; i++)
			{
				// process the ith tuple
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples_on_virtual_unsplitted_persistent_page(&vupp, i, i);
				space_occupied_until = space_occupied_by_ith_tuple + space_occupied_until;
				result++;
				if(space_occupied_until > limit)
					break;
			}
		}

		return result;
	}
}

int must_split_for_insert_bplus_tree_leaf_page(const persistent_page* page1, const void* tuple_to_insert, const bplus_tree_tuple_defs* bpttd_p)
{
	// do not perform a split if the page can accomodate the new tuple
	return !can_append_tuple_on_persistent_page_if_done_resiliently(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def), tuple_to_insert);
}

#define USE_SUFFIX_TRUNCATION

// for this function it is assumed, that last_tuple_page1 <= first_tuple_page2, on comparing key elements (at indices key_element_ids) sorted by key_compare_direction
static int build_index_entry_from_record_tuples_for_split(const bplus_tree_tuple_defs* bpttd_p, const void* last_tuple_page1, const void* first_tuple_page2, uint64_t child_page_id, void* index_entry)
{
	// directly build_index_entry from the (first_tuple_page2, child_page_id)
	return build_index_entry_from_record_tuple_using_bplus_tree_tuple_definitions(bpttd_p, first_tuple_page2, child_page_id, index_entry);
}

// for this function it is assumed, that last_tuple_page1 <= first_tuple_page2, on comparing key elements (at indices key_element_ids) sorted by key_compare_direction
// This is the suffix_truncated version for the above function
// At the moment this code is buggy (i.e. has bug), it does not generate correct result if the key_compare_direction has DESC
static int build_suffix_truncated_index_entry_from_record_tuples_for_split(const bplus_tree_tuple_defs* bpttd_p, const void* last_tuple_page1, const void* first_tuple_page2, uint64_t child_page_id, void* index_entry)
{
	// init the index_entry
	init_tuple(bpttd_p->index_def, index_entry);

	// loop iterator, also the elements processed
	uint32_t i = 0;

	int cmp_ltp1_ie = 0; // result of comparison between last_tuple_page1 and index_entry for the first i elements
	int cmp_ie_ftp2 = 0; // result of comparison between last_tuple_page1 and index_entry for the first i elements
	// we finally want the inequality last_tuple_page1 < index_entry <= first_tuple_page1 to be satisfied

	// as we iterate over the key columns given by key_element_ids
	// the index entry will first diverge from the last_tuple_page1 in the first loop
	// and then will diverge from first_tuple_page2 in the second loop

	while(i < bpttd_p->key_element_count && cmp_ltp1_ie == 0)
	{
		// comparison results with ith element having the uninitialized values in index_entry
		int new_cmp_ltp1_ie = cmp_ltp1_ie;
		if(new_cmp_ltp1_ie == 0)
			new_cmp_ltp1_ie = compare_elements_of_tuple(last_tuple_page1, bpttd_p->record_def, bpttd_p->key_element_ids[i], index_entry, bpttd_p->index_def, i) * bpttd_p->key_compare_direction[i];
		int new_cmp_ie_ftp2 = cmp_ie_ftp2;
		if(new_cmp_ie_ftp2 == 0)
			new_cmp_ie_ftp2 = compare_elements_of_tuple(index_entry, bpttd_p->index_def, i, first_tuple_page2, bpttd_p->record_def, bpttd_p->key_element_ids[i]) * bpttd_p->key_compare_direction[i];

		if(new_cmp_ltp1_ie == -1 && (new_cmp_ie_ftp2 == -1 || new_cmp_ie_ftp2 == 0))
		{
			cmp_ltp1_ie = new_cmp_ltp1_ie;
			cmp_ie_ftp2 = new_cmp_ie_ftp2;
			i++;
			break;
		}

		const element_def* ele_d = get_element_def_by_id(bpttd_p->index_def, i);

		// raw compare result (without compare direction) of the current elements of ith keys in last_tuple_page1 and first_tuple_page2
		int cmp = compare_elements_of_tuple(last_tuple_page1, bpttd_p->record_def, bpttd_p->key_element_ids[i], first_tuple_page2, bpttd_p->record_def, bpttd_p->key_element_ids[i]);

		// if the corresponding elements in the record tuples are equal OR
		// if the elements are not VAR_STRING or VAR_BLOB, then proceed as usual
		// Even in case of STRING and BLOB, we can reduce the compare lengths using suffix-truncation, hence try that
		if(0 == cmp || (ele_d->type != VAR_STRING && ele_d->type != VAR_BLOB
				&& ele_d->type != STRING && ele_d->type != BLOB))
		{
			// if not set, fail
			if(!set_element_in_tuple_from_tuple(bpttd_p->index_def, i, index_entry, bpttd_p->record_def, bpttd_p->key_element_ids[i], first_tuple_page2))
				return 0;
		}
		else // we can only suffix truncate VAR_STRING or VAR_BLOB
		{
			const user_value last_tuple_page1_element = get_value_from_element_from_tuple(bpttd_p->record_def, bpttd_p->key_element_ids[i], last_tuple_page1);
			const user_value first_tuple_page2_element = get_value_from_element_from_tuple(bpttd_p->record_def, bpttd_p->key_element_ids[i], first_tuple_page2);

			switch(bpttd_p->key_compare_direction[i])
			{
				case ASC:
				{
					// last_tuple_page1_element != first_tuple_page2_element, for sure
					// and since the ordering is ASC, only last_tuple_page1_element may be NULL
					if(is_user_value_NULL(&last_tuple_page1_element))
					{
						// if last_tuple_page1_element is NULL, then set the corresponding index_entry_element to EMPTY_USER_VALUE
						// first_tuple_page2 is bound to be greater than or equal to EMPTY_USER_VALUE
						if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, EMPTY_USER_VALUE))
							return 0;
						break;
					}

					// case when none of them is NULL

					// compute the number of characters matched
					uint32_t match_lengths = 0;
					for(; match_lengths < last_tuple_page1_element.data_size && match_lengths < first_tuple_page2_element.data_size && ((const char*)(last_tuple_page1_element.data))[match_lengths] == ((const char*)(first_tuple_page2_element.data))[match_lengths]; match_lengths++);

					user_value first_tuple_page2_element_copy = first_tuple_page2_element;
					first_tuple_page2_element_copy.data_size = match_lengths + 1;
					// if not set, fail
					if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, &first_tuple_page2_element_copy))
						return 0;
					break;
				}
				case DESC :
				{
					// last_tuple_page1_element != first_tuple_page2_element, for sure
					// and since the ordering is DESC, only first_tuple_page2_element may be NULL
					if(is_user_value_NULL(&first_tuple_page2_element))
					{
						// if first_tuple_page2_element is NULL, then set the corresponding index_entry_element to NULL_USER_VALUE
						if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, NULL_USER_VALUE))
							return 0;
						break;
					}

					// case when none of them is NULL

					// compute the number of characters matched
					uint32_t match_lengths = 0;
					for(; match_lengths < last_tuple_page1_element.data_size && match_lengths < first_tuple_page2_element.data_size && ((const char*)(last_tuple_page1_element.data))[match_lengths] == ((const char*)(first_tuple_page2_element.data))[match_lengths]; match_lengths++);

					if(match_lengths + 1 < last_tuple_page1_element.data_size) // to ensure that index_entry does not become equal to last_tuple_page1's index representation
					{
						user_value last_tuple_page1_element_copy = last_tuple_page1_element;
						last_tuple_page1_element_copy.data_size = match_lengths + 1;
						// if not set, fail
						if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, &last_tuple_page1_element_copy))
							return 0;
					}
					else // bad edge case, no truncation can be done now
					{
						// if not set, fail
						if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, &first_tuple_page2_element))
							return 0;
					}
					break;
				}
			}
		}

		if(cmp_ltp1_ie == 0)
			cmp_ltp1_ie = compare_elements_of_tuple(last_tuple_page1, bpttd_p->record_def, bpttd_p->key_element_ids[i], index_entry, bpttd_p->index_def, i) * bpttd_p->key_compare_direction[i];
		if(cmp_ie_ftp2 == 0)
			cmp_ie_ftp2 = compare_elements_of_tuple(index_entry, bpttd_p->index_def, i, first_tuple_page2, bpttd_p->record_def, bpttd_p->key_element_ids[i]) * bpttd_p->key_compare_direction[i];

		i++;
	}

	// now at this point, last_tuple_page1 and index_entry are not equal for the first i elements

	while(i < bpttd_p->key_element_count && cmp_ie_ftp2 == 0)
	{
		int new_cmp_ie_ftp2 = cmp_ie_ftp2;
		if(new_cmp_ie_ftp2 == 0)
			new_cmp_ie_ftp2 = compare_elements_of_tuple(index_entry, bpttd_p->index_def, i, first_tuple_page2, bpttd_p->record_def, bpttd_p->key_element_ids[i]) * bpttd_p->key_compare_direction[i];

		if(new_cmp_ie_ftp2 == -1)
		{
			cmp_ie_ftp2 = new_cmp_ie_ftp2;
			i++;
			break;
		}

		const element_def* ele_d = get_element_def_by_id(bpttd_p->index_def, i);

		switch(bpttd_p->key_compare_direction[i])
		{
			// min value of element always exists, set it
			case ASC:
			{
				// NULL is the least value of any type
				// check if index entry element is NULL
				if(is_NULL_in_tuple(bpttd_p->index_def, i, index_entry))
					break;

				// if not NULL, then set it to NULL
				// this may fail if the element is non-nullable
				if(set_element_in_tuple(bpttd_p->index_def, i, index_entry, NULL_USER_VALUE))
					break;

				// else set it to min_val for that element
				const user_value min_val = get_MIN_user_value(ele_d);
				// if not set, fail
				if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, &min_val))
					return 0;
				break;
			}
			// if max value exists then set it, else set the last_tuple_page2's ith element
			case DESC :
			{
				// max values of *STRING and *BLOB types are difficult to compute
				// handling the numeral element case first
				if(ele_d->type != STRING && ele_d->type != BLOB
				&& ele_d->type != VAR_STRING && ele_d->type != VAR_BLOB)
				{
					// if not set, fail
					const user_value max_val = get_MAX_user_value_for_numeral_element_def(ele_d);
					if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, &max_val))
						return 0;
				}
				else
				{
					const user_value first_tuple_page2_element = get_value_from_element_from_tuple(bpttd_p->record_def, bpttd_p->key_element_ids[i], first_tuple_page2);

					if(is_user_value_NULL(&first_tuple_page2_element))
					{
						// the shortest string greater than NULL, is an empty string
						if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, EMPTY_USER_VALUE))
							return 0;
						break;
					}

					// count the number of CHAR_MAX characters in the prefix of the first_tuple_page2_element
					uint32_t maximum_char_value_prefix_count = 0;
					while(maximum_char_value_prefix_count < first_tuple_page2_element.data_size
						&& ((const char*)(first_tuple_page2_element.data))[maximum_char_value_prefix_count] == SIGNED_MAX_VALUE_OF(char))
						maximum_char_value_prefix_count++;

					// build max value element, and if memory allocation fails then quit
					int memory_allocation_error = 0;
					user_value max_val = get_MAX_user_value_for_string_OR_blob_element_def(ele_d, min(maximum_char_value_prefix_count + 1, first_tuple_page2_element.data_size), &memory_allocation_error);
					if(memory_allocation_error)
						exit(-1);

					// if not set, fail
					if(!set_element_in_tuple(bpttd_p->index_def, i, index_entry, &max_val))
					{
						free((void*)max_val.data);
						return 0;
					}
					free((void*)max_val.data);
				}
				break;
			}
		}

		if(cmp_ie_ftp2 == 0)
			cmp_ie_ftp2 = compare_elements_of_tuple(index_entry, bpttd_p->index_def, i, first_tuple_page2, bpttd_p->record_def, bpttd_p->key_element_ids[i]) * bpttd_p->key_compare_direction[i];

		i++;
	}

	// copy the child_page_id to the last element in the index entry
	if(!set_element_in_tuple(bpttd_p->index_def, get_element_def_count_tuple_def(bpttd_p->index_def) - 1, index_entry, &((const user_value){.uint_value = child_page_id})))
		return 0;

	// success
	return 1;
}

int split_insert_bplus_tree_leaf_page(persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error, void* output_parent_insert)
{
	// check if a page must split to accomodate the new tuple
	if(!must_split_for_insert_bplus_tree_leaf_page(page1, tuple_to_insert, bpttd_p))
		return 0;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page1, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));

	// total number of tuples we would be dealing with
	//uint32_t total_tuple_count = page1_tuple_count + 1;

	// lingo for variables page1 => page to be split, page2 => page that will be allocated to handle the split

	// final tuple count of the page that will be split
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page1, tuple_to_insert, tuple_to_insert_at, bpttd_p);

	// final tuple count of the page that will be newly allocated
	//uint32_t final_tuple_count_page2 = total_tuple_count - final_tuple_count_page1;

	// figure out if the new tuple (tuple_to_insert) will go to new page OR should be inserted to the existing page
	int new_tuple_goes_to_page1 = (tuple_to_insert_at < final_tuple_count_page1);

	// number of the tuples of the page1 that will stay in the same page
	uint32_t tuples_stay_in_page1 = final_tuple_count_page1;
	if(new_tuple_goes_to_page1)
		tuples_stay_in_page1--;

	// number of tuples of the page1 that will leave page1 and be moved to page2
	//uint32_t tuples_leave_page1 = page1_tuple_count - tuples_stay_in_page1;

	// lock the next page of page1, we call it page3
	persistent_page page3 = get_NULL_persistent_page(pam_p);
	{
		uint64_t page3_id = get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p);
		if(page3_id != bpttd_p->pas_p->NULL_PAGE_ID)
		{
			page3 = acquire_persistent_page_with_lock(pam_p, transaction_id, page3_id, WRITE_LOCK, abort_error);

			// if we could not acquire lock on page3, then fail split_insert
			if(*abort_error)
				return 0;
		}
	}

	// allocate a new page, to place in between page1 and page3, it will accomodate half of the tuples of page1
	persistent_page page2 = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);

	// return with a split failure if the page2 could not be allocated
	if(*abort_error)
	{
		// on failure, do not forget to release writer lock on page3, if you had it
		if(!is_persistent_page_NULL(&page3, pam_p))
			release_lock_on_persistent_page(pam_p, transaction_id, &page3, NONE_OPTION, abort_error);
		return 0;
	}

	// initialize page2 (as a leaf page)
	init_bplus_tree_leaf_page(&page2, bpttd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
	{
		// on failure, do not forget to release writer lock on page3 (if you had it) and page2
		if(!is_persistent_page_NULL(&page3, pam_p))
			release_lock_on_persistent_page(pam_p, transaction_id, &page3, NONE_OPTION, abort_error);
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		return 0;
	}

	// link page2 in between page1 and the next page of page1
	{
		// read headers of page1 and page2 inorder to link page2 in between, page1 and page3
		bplus_tree_leaf_page_header page1_hdr = get_bplus_tree_leaf_page_header(page1, bpttd_p);
		bplus_tree_leaf_page_header page2_hdr = get_bplus_tree_leaf_page_header(&page2, bpttd_p);

		// perform pointer manipulations for the linkedlist of leaf pages
		if(page3.page_id != bpttd_p->pas_p->NULL_PAGE_ID)
		{
			// if page3 exists, then we already have lock on it

			// read page3 header
			bplus_tree_leaf_page_header page3_hdr = get_bplus_tree_leaf_page_header(&page3, bpttd_p);

			// perform pointer manipulations to insert page2 between page1 and page3
			page1_hdr.next_page_id = page2.page_id;
			page3_hdr.prev_page_id = page2.page_id;
			page2_hdr.prev_page_id = page1->page_id;
			page2_hdr.next_page_id = page3.page_id;

			// set the page3_hdr back onto the page
			set_bplus_tree_leaf_page_header(&page3, &page3_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
			if(*abort_error) // if aborted here, release locks on page2 and page3 and return failure
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
				release_lock_on_persistent_page(pam_p, transaction_id, &page3, NONE_OPTION, abort_error);
				return 0;
			}

			// release writer lock on page3
			release_lock_on_persistent_page(pam_p, transaction_id, &page3, NONE_OPTION, abort_error);
			if(*abort_error) // on abort we need to only release lock on page2
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
				return 0;
			}
		}
		else
		{
			// perform pointer manipulations to put page2 at the end of this linkedlist after page1
			page1_hdr.next_page_id = page2.page_id;
			page2_hdr.prev_page_id = page1->page_id;
			page2_hdr.next_page_id = bpttd_p->pas_p->NULL_PAGE_ID;
		}

		// page3 not must not be accessed beyond this point, even if it is in your scope

		// set the page1_hdr and page2_hdr back onto the page
		set_bplus_tree_leaf_page_header(page1, &page1_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error) // if aborted here, release lock on page2 and return failure
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
		set_bplus_tree_leaf_page_header(&page2, &page2_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error) // if aborted here, release lock on page2 and return failure
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
	}

	// while moving tuples, we assume that there will be atleast 1 tuple that will get moved from page1 to page2
	// we made this sure by all the above conditions
	// hence no need to check bounds of start_index and last_index

	// copy all required tuples from the page1 to page2
	insert_all_from_sorted_packed_page(
									&page2, page1, bpttd_p->pas_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p,
									transaction_id,
									abort_error
								);

	if(*abort_error) // if aborted here, release lock on page2 and return failure
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		return 0;
	}

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page1, bpttd_p->pas_p->page_size,
									bpttd_p->record_def,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p,
									transaction_id,
									abort_error
								);

	if(*abort_error) // if aborted here, release lock on page2 and return failure
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		return 0;
	}

	// insert the new tuple (tuple_to_insert) to page1 or page2 based on "new_tuple_goes_to_page1", as calculated earlier
	if(new_tuple_goes_to_page1)
	{
		// insert the tuple_to_insert (the new tuple) at the desired index in the page1
		insert_at_in_sorted_packed_page(
									page1, bpttd_p->pas_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuple_to_insert, 
									tuple_to_insert_at,
									pmm_p,
									transaction_id,
									abort_error
								);

		if(*abort_error) // if aborted here, release lock on page2 and return failure
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
	}
	else
	{
		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									&page2, bpttd_p->pas_p->page_size,
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at - tuples_stay_in_page1,
									pmm_p,
									transaction_id,
									abort_error
								);

		if(*abort_error) // if aborted here, release lock on page2 and return failure
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
	}

	// create tuple to be returned, this tuple needs to be inserted into the parent page, after the child_index
	const void* last_tuple_page1 = get_nth_tuple_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def), get_tuple_count_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def)) - 1);
	const void* first_tuple_page2 = get_nth_tuple_on_persistent_page(&page2, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def), 0);

	#ifndef USE_SUFFIX_TRUNCATION
		// build_index_entry call when suffix_truncation is disabled
		build_index_entry_from_record_tuples_for_split(bpttd_p, last_tuple_page1, first_tuple_page2, page2.page_id, output_parent_insert);
	#else
		// build_index_entry call when suffix_truncation is enabled
		build_suffix_truncated_index_entry_from_record_tuples_for_split(bpttd_p, last_tuple_page1, first_tuple_page2, page2.page_id, output_parent_insert);
	#endif

	// release lock on the page2
	release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
	if(*abort_error) // no locks, required to be released here
		return 0;

	// return success
	return 1;
}

int can_merge_bplus_tree_leaf_pages(const persistent_page* page1, const persistent_page* page2, const bplus_tree_tuple_defs* bpttd_p)
{
	// read headers of page1 and page2
	bplus_tree_leaf_page_header page1_hdr = get_bplus_tree_leaf_page_header(page1, bpttd_p);
	bplus_tree_leaf_page_header page2_hdr = get_bplus_tree_leaf_page_header(page2, bpttd_p);

	// and then make sure that the next of page1 is page2 and the prev of page2 is page1
	if(page1_hdr.next_page_id != page2->page_id
	|| page2_hdr.prev_page_id != page1->page_id)
		return 0;

	// check if a merge can be performed, by comparing the used space in both the pages
	uint32_t total_space_page1 = get_space_allotted_to_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples_on_persistent_page(page2, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));

	if(total_space_page1 < space_in_use_page1 + space_in_use_page2)
		return 0;

	return 1;
}

int merge_bplus_tree_leaf_pages(persistent_page* page1, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// get the next adjacent page of this page
	persistent_page page2 = get_NULL_persistent_page(pam_p);
	{
		uint64_t page2_id = get_next_page_id_of_bplus_tree_leaf_page(page1, bpttd_p);

		if(page2_id != bpttd_p->pas_p->NULL_PAGE_ID)
		{
			// acquire lock on the next page, i.e. page2
			page2 = acquire_persistent_page_with_lock(pam_p, transaction_id, page2_id, WRITE_LOCK, abort_error);

			// failed acquiring lock on this page, return 0 (failure)
			if(*abort_error)
				return 0;
		}
		else // no page to merge page1 with
			return 0;
	}

	// check if a merge can be performed, and on failure release writer lock on page2, that we just acquired
	if(!can_merge_bplus_tree_leaf_pages(page1, &page2, bpttd_p))
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		if(*abort_error)
			return 0;
		return 0;
	}

	// now we can be sure that a merge can be performed on page1 and page2

	// we start by perfoming the pointer manipulation
	// but we need lock on the next page of the page2 to change its previous page pointer
	// we are calling the page next to page2 as page 3
	{
		uint64_t page3_id = get_next_page_id_of_bplus_tree_leaf_page(&page2, bpttd_p);

		// read headers of page1 and page2 inorder to unlink page2 from the between of page1 and page3
		bplus_tree_leaf_page_header page1_hdr = get_bplus_tree_leaf_page_header(page1, bpttd_p);
		bplus_tree_leaf_page_header page2_hdr = get_bplus_tree_leaf_page_header(&page2, bpttd_p);

		// page3 does exist and page2 is not the last page
		if(page3_id != bpttd_p->pas_p->NULL_PAGE_ID)
		{
			// acquire lock on the page3
			persistent_page page3 = acquire_persistent_page_with_lock(pam_p, transaction_id, page3_id, WRITE_LOCK, abort_error);

			// could not acquire lock on page3, so can not perform a merge
			if(*abort_error)
			{
				// on error, we release writer lock on page2
				release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
				return 0;
			}

			// read page3 header
			bplus_tree_leaf_page_header page3_hdr = get_bplus_tree_leaf_page_header(&page3, bpttd_p);

			// perform pointer manipulations to remove page2 from the between of page1 and page3
			page1_hdr.next_page_id = page3.page_id;
			page3_hdr.prev_page_id = page1->page_id;

			// set the page3_hdr back onto the page
			set_bplus_tree_leaf_page_header(&page3, &page3_hdr, bpttd_p, pmm_p, transaction_id, abort_error);

			if(*abort_error) // if aborted here, release lock on page2 and page3
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
				release_lock_on_persistent_page(pam_p, transaction_id, &page3, NONE_OPTION, abort_error);
				return 0;
			}

			// release lock on page3
			release_lock_on_persistent_page(pam_p, transaction_id, &page3, NONE_OPTION, abort_error);
			if(*abort_error) // only page2 needs to be unlocked here
			{
				release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
				return 0;
			}
		}
		else // page2 is indeed the last page
		{
			// perform pointer manipulations to make page1 as the last page
			page1_hdr.next_page_id = bpttd_p->pas_p->NULL_PAGE_ID;
		}

		// since page2 has been removed from the linkedlist of leaf pages
		// modify page2 pointers (next and prev) to point NULL_PAGE_ID
		// this step is not needed
		page2_hdr.prev_page_id = bpttd_p->pas_p->NULL_PAGE_ID;
		page2_hdr.next_page_id = bpttd_p->pas_p->NULL_PAGE_ID;

		// set the page1_hdr and page2_hdr back onto the page
		set_bplus_tree_leaf_page_header(page1, &page1_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
		set_bplus_tree_leaf_page_header(&page2, &page2_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
	}

	// now, we can safely transfer all tuples from page2 to page1

	// only if there are any tuples to move
	uint32_t tuple_count_page2 = get_tuple_count_on_persistent_page(&page2, bpttd_p->pas_p->page_size, &(bpttd_p->record_def->size_def));
	if(tuple_count_page2 > 0)
	{
		// only if there are any tuples to move
		insert_all_from_sorted_packed_page(
									page1, &page2, bpttd_p->pas_p->page_size, 
									bpttd_p->record_def, bpttd_p->key_element_ids, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									0, tuple_count_page2 - 1,
									pmm_p,
									transaction_id,
									abort_error
								);

		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
	}

	// free page2 and release its lock
	release_lock_on_persistent_page(pam_p, transaction_id, &page2, FREE_PAGE, abort_error);
	if(*abort_error) // release lock, the free_page call failed, so release lock with NONE_OPTION
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		return 0;
	}

	return 1;
}