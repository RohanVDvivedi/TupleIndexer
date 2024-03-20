#include<bplus_tree_interior_page_util.h>

#include<sorted_packed_page_util.h>
#include<bplus_tree_interior_page_header.h>
#include<bplus_tree_index_tuple_functions_util.h>

#include<persistent_page_functions.h>
#include<virtual_unsplitted_persistent_page.h>
#include<tuple.h>

#include<cutlery_stds.h>

int init_bplus_tree_interior_page(persistent_page* ppage, uint32_t level, int is_last_page_of_level, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	int inited = init_persistent_page(pmm_p, transaction_id, ppage, bpttd_p->pas_p->page_size, sizeof_BPLUS_TREE_INTERIOR_PAGE_HEADER(bpttd_p), &(bpttd_p->index_def->size_def), abort_error);
	if((*abort_error) || !inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	bplus_tree_interior_page_header hdr = get_bplus_tree_interior_page_header(ppage, bpttd_p);
	hdr.parent.type = BPLUS_TREE_INTERIOR_PAGE;
	hdr.level = level;
	hdr.least_keys_page_id = bpttd_p->pas_p->NULL_PAGE_ID;
	hdr.is_last_page_of_level = is_last_page_of_level;
	set_bplus_tree_interior_page_header(ppage, &hdr, bpttd_p, pmm_p, transaction_id, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}

void print_bplus_tree_interior_page(const persistent_page* ppage, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_interior_page_header(ppage, bpttd_p);
	print_persistent_page(ppage, bpttd_p->pas_p->page_size, bpttd_p->index_def);
}

uint32_t find_child_index_for_key(const persistent_page* ppage, const void* key, uint32_t key_element_count_concerned, const bplus_tree_tuple_defs* bpttd_p)
{
	// find preceding equals in the interior pages, by comparing against all index entries
	uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
										ppage, bpttd_p->pas_p->page_size,
										bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned,
										key, bpttd_p->key_def, NULL
									);

	return (child_index == NO_TUPLE_FOUND) ? ALL_LEAST_KEYS_CHILD_INDEX : child_index;
}

uint32_t find_child_index_for_record(const persistent_page* ppage, const void* record, uint32_t key_element_count_concerned, const bplus_tree_tuple_defs* bpttd_p)
{
	// find preceding equals in the interior pages, by comparing against all index entries
	uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
										ppage, bpttd_p->pas_p->page_size,
										bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, key_element_count_concerned,
										record, bpttd_p->record_def, bpttd_p->key_element_ids
									);

	return (child_index == NO_TUPLE_FOUND) ? ALL_LEAST_KEYS_CHILD_INDEX : child_index;
}

uint64_t get_child_page_id_by_child_index(const persistent_page* ppage, uint32_t index, const bplus_tree_tuple_defs* bpttd_p)
{
	// if the index is ALL_LEAST_KEYS_CHILD_INDEX, return the page_id stored in the header
	if(index == ALL_LEAST_KEYS_CHILD_INDEX)
		return get_least_keys_page_id_of_bplus_tree_interior_page(ppage, bpttd_p);

	// tuple of the interior page that is at index
	const void* index_tuple = get_nth_tuple_on_persistent_page(ppage, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def), index);

	// get child_page_id of the index tuple
	uint64_t child_page_id = get_child_page_id_from_index_tuple(index_tuple, bpttd_p);

	return child_page_id;
}

static uint32_t calculate_final_tuple_count_of_page_to_be_split(const persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p)
{
	// construct a virtual unsplitted persistent page to work on
	virtual_unsplitted_persistent_page vupp = get_virtual_unsplitted_persistent_page(page1, bpttd_p->pas_p->page_size, tuple_to_insert, tuple_to_insert_at, bpttd_p->index_def);

	// get total tuple count that we would be dealing with
	uint32_t total_tuple_count = get_tuple_count_on_virtual_unsplitted_persistent_page(&vupp);

	if(is_fixed_sized_tuple_def(bpttd_p->index_def))
	{
		// if the new index tuple is to be inserted after the last tuple in the last interior page of that level
		if(is_last_page_of_level_of_bplus_tree_interior_page(page1, bpttd_p) && tuple_to_insert_at == total_tuple_count-1)
			return total_tuple_count - 1;	// i.e. only 1 tuple goes to the new page
		else // else equal split
			return total_tuple_count / 2;
	}
	else
	{
		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

		// this is the result number of tuple that should stay on this page
		uint32_t result = 0;

		// if new tuple is to be inserted after the last tuple in the last interior page of the level => split it such that it is almost full
		if(is_last_page_of_level_of_bplus_tree_interior_page(page1, bpttd_p) && tuple_to_insert_at == total_tuple_count-1)
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

int must_split_for_insert_bplus_tree_interior_page(const persistent_page* page1, const void* tuple_to_insert, const bplus_tree_tuple_defs* bpttd_p)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_append_tuple_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def), tuple_to_insert))
		return 0;

	// we need to make sure that the new_tuple will not be fitting on the page even after a compaction
	// if tuple_to_insert can fit on the page after a compaction, then you should not be splitting the page
	uint32_t space_to_be_occupied_by_new_tuple = get_space_to_be_occupied_by_tuple_on_persistent_page(bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def), tuple_to_insert);
	uint32_t space_available_page1 = get_space_allotted_to_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def)) - get_space_occupied_by_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

	// we fail here because the new tuple can be accomodated in page1, if you had considered compacting the page
	if(space_available_page1 >= space_to_be_occupied_by_new_tuple)
		return 0;

	return 1;
}

int split_insert_bplus_tree_interior_page(persistent_page* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error, void* output_parent_insert)
{
	// check if a page must split to accomodate the new tuple
	if(!must_split_for_insert_bplus_tree_interior_page(page1, tuple_to_insert, bpttd_p))
		return 0;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page1, bpttd_p->pas_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

	// total number of tuples we would be dealing with
	//uint32_t total_tuple_count = page1_tuple_count + 1;

	// lingo for variables page1 => page to be split, page2 => page that will be allocated to handle the split

	// final tuple count of the page that will be split
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page1, tuple_to_insert, tuple_to_insert_at, bpttd_p);

	// final tuple count of the page that will be newly allocated
	// this will include the tuple that will be later modified to form the least_keys_page_id and the parent_insert (the tuple that will/should be inserted to the parent page)
	//uint32_t final_tuple_count_page2 = total_tuple_count - final_tuple_count_page1;

	// figure out if the new tuple (tuple_to_insert) will go to new page OR should be inserted to the existing page
	int new_tuple_goes_to_page1 = (tuple_to_insert_at < final_tuple_count_page1);

	// number of the tuples of the page1 that will stay in the same page
	uint32_t tuples_stay_in_page1 = final_tuple_count_page1;
	if(new_tuple_goes_to_page1)
		tuples_stay_in_page1--;

	// number of tuples of the page1 that will leave page1 and be moved to page2
	//uint32_t tuples_leave_page1 = page1_tuple_count - tuples_stay_in_page1;

	// allocate a new page
	persistent_page page2 = get_new_persistent_page_with_write_lock(pam_p, transaction_id, abort_error);

	// return with a split failure if the page2 could not be allocated
	if(*abort_error)
		return 0;

	// initialize page2 (as an interior page)
	uint32_t level = get_level_of_bplus_tree_interior_page(page1, bpttd_p);	// get the level of bplus_tree we are dealing with
	init_bplus_tree_interior_page(&page2, level, 0, bpttd_p, pmm_p, transaction_id, abort_error);

	// if the bplus_tree interior page could not be inited, release lock and fail
	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		return 0;
	}

	// check if page1 is last page of the level, if yes then page2 now becomes the last page of the level
	{
		// read page1 and page2 header
		bplus_tree_interior_page_header page1_hdr = get_bplus_tree_interior_page_header(page1, bpttd_p);
		bplus_tree_interior_page_header page2_hdr = get_bplus_tree_interior_page_header(&page2, bpttd_p);

		// page1 now can not be the last page of the level
		// page2 will be the last page of the level if page1 was the last page of the level
		page2_hdr.is_last_page_of_level = page1_hdr.is_last_page_of_level;
		page1_hdr.is_last_page_of_level = 0;

		// set the page headers back on to the page
		set_bplus_tree_interior_page_header(page1, &page1_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
		set_bplus_tree_interior_page_header(&page2, &page2_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
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
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p,
									transaction_id,
									abort_error
								);

	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		return 0;
	}

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page1, bpttd_p->pas_p->page_size,
									bpttd_p->index_def,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p,
									transaction_id,
									abort_error
								);

	if(*abort_error)
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
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at,
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
	else
	{
		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									&page2, bpttd_p->pas_p->page_size,
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at - tuples_stay_in_page1,
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

	// now the page2 contains all the other tuples discarded from the page1, (due to the split)
	// The first tuple of the page2 has to be disintegrated as follows
	// first_tuple_page2 =>> key : page_id
	// this page_id of the first tuple needs to become the least_keys_page_id of page2
	// and we then create a output_parent_insert tuple as =>> key : page2_id
	// we then can delete the first tuple of this page2
	// and then finally we return the output_parent_insert tuple that can be inserted into parent page

	const void* first_tuple_page2 = get_nth_tuple_on_persistent_page(&page2, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def), 0);
	uint32_t size_of_first_tuple_page2 = get_tuple_size(bpttd_p->index_def, first_tuple_page2);

	// get least_keys_page_id for page2 from its first tuple
	uint64_t page2_least_keys_page_id = get_child_page_id_from_index_tuple(first_tuple_page2, bpttd_p);

	// set the least_keys_page_id for page2
	{
		// read page2 header
		bplus_tree_interior_page_header page2_hdr = get_bplus_tree_interior_page_header(&page2, bpttd_p);

		// update it's to least_keys_page_id
		page2_hdr.least_keys_page_id = page2_least_keys_page_id;

		// set page2_hdr back onto the page
		set_bplus_tree_interior_page_header(&page2, &page2_hdr, bpttd_p, pmm_p, transaction_id, abort_error);

		if(*abort_error)
		{
			release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
			return 0;
		}
	}

	// copy all the contents of the first_tuple_page2 to output_parent_insert
	memory_move(output_parent_insert, first_tuple_page2, size_of_first_tuple_page2);

	// now insert the pointer to the page2 in this parent tuple
	set_child_page_id_in_index_tuple(output_parent_insert, page2.page_id, bpttd_p);

	// now you may, delete the first tuple of page2
	delete_in_sorted_packed_page(
									&page2, bpttd_p->pas_p->page_size, 
									bpttd_p->index_def,
									0,
									pmm_p,
									transaction_id,
									abort_error
								);

	if(*abort_error)
	{
		release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);
		return 0;
	}

	// release lock on the page2
	release_lock_on_persistent_page(pam_p, transaction_id, &page2, NONE_OPTION, abort_error);

	// on abort, return 0
	if(*abort_error)
		return 0;

	// return success
	return 1;
}

int can_merge_bplus_tree_interior_pages(const persistent_page* page1, const void* separator_parent_tuple, const persistent_page* page2, const bplus_tree_tuple_defs* bpttd_p)
{
	// ensure that the separator_parent_tuple child_page_id is equal to the page2_id
	if(get_child_page_id_from_index_tuple(separator_parent_tuple, bpttd_p) != page2->page_id)
		return 0;

	// check if a merge is possible, by comparing the total space requirement
	uint32_t total_space_page1 = get_space_allotted_to_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));
	uint32_t space_to_be_occupied_by_separator_tuple = get_space_to_be_occupied_by_tuple_on_persistent_page(bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def), separator_parent_tuple);
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples_on_persistent_page(page2, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));

	// the page1 must be able to accomodate all its current tuples, the separator tuple and the tuples of page2
	if(total_space_page1 < space_in_use_page1 + space_to_be_occupied_by_separator_tuple + space_in_use_page2)
		return 0;

	return 1;
}

int merge_bplus_tree_interior_pages(persistent_page* page1, const void* separator_parent_tuple, persistent_page* page2, const bplus_tree_tuple_defs* bpttd_p, const page_access_methods* pam_p, const page_modification_methods* pmm_p, const void* transaction_id, int* abort_error)
{
	// ensure that we can merge
	if(!can_merge_bplus_tree_interior_pages(page1, separator_parent_tuple, page2, bpttd_p))
		return 0;

	// now we can be sure that a merge can be performed on page1 and page2

	// check if page2 was the last page of the level, if so page1 is now the last page of the level
	{
		// read page1 and page2 header
		bplus_tree_interior_page_header page1_hdr = get_bplus_tree_interior_page_header(page1, bpttd_p);
		bplus_tree_interior_page_header page2_hdr = get_bplus_tree_interior_page_header(page2, bpttd_p);

		// if page2 was the last page of the level, then page1 will now inherit that
		page1_hdr.is_last_page_of_level = page2_hdr.is_last_page_of_level;
		// below operation is not needed, as page2 is already going to be freed
		page2_hdr.is_last_page_of_level = 0;

		// set the page headers back on to the page
		set_bplus_tree_interior_page_header(page1, &page1_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		set_bplus_tree_interior_page_header(page2, &page2_hdr, bpttd_p, pmm_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	// insert separator_parent_tuple in the page1, at the end, we will call this separator_tuple
	insert_at_in_sorted_packed_page(
									page1, bpttd_p->pas_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									separator_parent_tuple, 
									get_tuple_count_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def)),
									pmm_p,
									transaction_id,
									abort_error
								);
	if(*abort_error)
		return 0;

	// update child_page_id of separator_tuple with the value from least_keys_page_id
	uint64_t separator_tuple_child_page_id = get_least_keys_page_id_of_bplus_tree_interior_page(page2, bpttd_p);

	// since this update is to a fixed_length UINT type, it must either end in success OR in abort_error
	set_element_in_tuple_in_place_on_persistent_page(pmm_p, transaction_id, page1, bpttd_p->pas_p->page_size, bpttd_p->index_def,
												get_tuple_count_on_persistent_page(page1, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def)) - 1,
												get_element_def_count_tuple_def(bpttd_p->index_def) - 1,
												&((const user_value){.uint_value = separator_tuple_child_page_id}),
												abort_error);
	if(*abort_error)
		return 0;

	// now, we can safely transfer all tuples from page2 to page1

	// only if there are any tuples to move from page2
	uint32_t tuple_count_page2 = get_tuple_count_on_persistent_page(page2, bpttd_p->pas_p->page_size, &(bpttd_p->index_def->size_def));
	if(tuple_count_page2 > 0)
	{
		// only if there are any tuples to move
		insert_all_from_sorted_packed_page(
									page1, page2, bpttd_p->pas_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_compare_direction, bpttd_p->key_element_count,
									0, tuple_count_page2 - 1,
									pmm_p,
									transaction_id,
									abort_error
								);

		if(*abort_error)
			return 0;
	}

	return 1;
}