#include<bplus_tree_interior_page_util.h>

#include<sorted_packed_page_util.h>
#include<bplus_tree_interior_page_header.h>
#include<bplus_tree_index_tuple_functions_util.h>

#include<page_layout_unaltered.h>
#include<tuple.h>

#include<cutlery_stds.h>

#include<stdlib.h>

int init_bplus_tree_interior_page(persistent_page ppage, uint32_t level, int is_last_page_of_level, const bplus_tree_tuple_defs* bpttd_p, const page_modification_methods* pmm_p)
{
	int inited = pmm_p->init_page(pmm_p->context, ppage, bpttd_p->page_size, sizeof_INTERIOR_PAGE_HEADER(bpttd_p), &(bpttd_p->index_def->size_def));
	if(!inited)
		return 0;

	// get the header, initialize it and set it back on to the page
	bplus_tree_interior_page_header hdr = get_bplus_tree_interior_page_header(ppage.page, bpttd_p);
	hdr.parent.parent.type = BPLUS_TREE_INTERIOR_PAGE;
	hdr.parent.level = level;
	hdr.least_keys_page_id = bpttd_p->NULL_PAGE_ID;
	hdr.is_last_page_of_level = is_last_page_of_level;
	set_bplus_tree_interior_page_header(ppage, &hdr, bpttd_p, pmm_p);

	return 1;
}

void print_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_interior_page_header(page, bpttd_p);
	print_page(page, bpttd_p->page_size, bpttd_p->index_def);
}

uint32_t find_child_index_for_key(const void* page, const void* key, uint32_t key_element_count_concerned, find_child_index_type type, const bplus_tree_tuple_defs* bpttd_p)
{
	switch(type)
	{
		case TOWARDS_FIRST_WITH_KEY :
		{
			// find preceding in the interior pages, by comparing against all index entries
			uint32_t child_index = find_preceding_in_sorted_packed_page(
										page, bpttd_p->page_size,
										bpttd_p->index_def, NULL, key_element_count_concerned,
										key, bpttd_p->key_def, NULL
									);

			return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
		}
		case TOWARDS_LAST_WITH_KEY :
		default :
		{
			// find preceding equals in the interior pages, by comparing against all index entries
			uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
										page, bpttd_p->page_size,
										bpttd_p->index_def, NULL, key_element_count_concerned,
										key, bpttd_p->key_def, NULL
									);

			return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
		}
	}
}

uint32_t find_child_index_for_record(const void* page, const void* record, uint32_t key_element_count_concerned, find_child_index_type type, const bplus_tree_tuple_defs* bpttd_p)
{
	switch(type)
	{
		case TOWARDS_FIRST_WITH_KEY :
		{
			// find preceding in the interior pages, by comparing against all index entries
			uint32_t child_index = find_preceding_in_sorted_packed_page(
										page, bpttd_p->page_size,
										bpttd_p->index_def, NULL, key_element_count_concerned,
										record, bpttd_p->record_def, bpttd_p->key_element_ids
									);

			return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
		}
		case TOWARDS_LAST_WITH_KEY :
		default :
		{
			// find preceding equals in the interior pages, by comparing against all index entries
			uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
										page, bpttd_p->page_size,
										bpttd_p->index_def, NULL, key_element_count_concerned,
										record, bpttd_p->record_def, bpttd_p->key_element_ids
									);

			return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
		}
	}
}

uint64_t find_child_page_id_by_child_index(const void* page, uint32_t index, const bplus_tree_tuple_defs* bpttd_p)
{
	// if the index is -1, return the page_id stored in the header
	if(index == -1)
		return get_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p);

	// tuple of the interior page that is at index
	const void* index_tuple = get_nth_tuple_on_page(page, bpttd_p->page_size, &(bpttd_p->index_def->size_def), index);

	// get child_page_id of the index tuple
	uint64_t child_page_id = get_child_page_id_from_index_tuple(index_tuple, bpttd_p);

	return child_page_id;
}

static uint32_t calculate_final_tuple_count_of_page_to_be_split(const void* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p)
{
	uint32_t tuple_count = get_tuple_count_on_page(page1, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

	if(is_fixed_sized_tuple_def(bpttd_p->index_def))
	{
		// if the new index tuple is to be inserted after the last tuple in the last interior page of that level
		if(is_last_page_of_level_of_bplus_tree_interior_page(page1, bpttd_p) && tuple_to_insert_at == tuple_count)
			return tuple_count;	// i.e. only 1 tuple goes to the new page
		else // else
			return (tuple_count + 1) / 2;	// equal split
	}
	else
	{
		// pre calculate the space that will be occupied by the new tuple
		uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->index_def, tuple_to_insert) + get_additional_space_overhead_per_tuple_on_page(bpttd_p->page_size, &(bpttd_p->index_def->size_def));

		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples_on_page(page1, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

		// this is the result number of tuple that should stay on this page
		uint32_t result = 0;

		// if new tuple is to be inserted after the last tuple in the last interior page of the level => split it such that it is almost full
		if(is_last_page_of_level_of_bplus_tree_interior_page(page1, bpttd_p) && tuple_to_insert_at == tuple_count)
		{
			uint32_t limit = space_allotted_to_tuples;

			uint32_t space_occupied_until = 0;

			for(uint32_t i = 0; i < tuple_count; i++)
			{
				// if the new tuple is suppossed to be inserted at i then process it first
				if(i == tuple_to_insert_at)
				{
					space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
					if(space_occupied_until <= limit)
						result++;
					if(space_occupied_until >= limit)
						break;
				}

				// process the ith tuple
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples_on_page(page1, bpttd_p->page_size, &(bpttd_p->index_def->size_def), i, i);
				space_occupied_until = space_occupied_by_ith_tuple + space_occupied_until;
				if(space_occupied_until <= limit)
					result++;
				if(space_occupied_until >= limit)
					break;
			}

			if(space_occupied_until < limit && tuple_count == tuple_to_insert_at && result == tuple_count)
			{
				// if the new tuple is suppossed to be inserted at the end then append its occupied size to the end
				space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
				if(space_occupied_until <= limit)
					result++;
			}
		}
		else // else => result is the number of tuples that will take the page occupancy just above or equal to 50%
		{
			uint32_t limit = space_allotted_to_tuples / 2;

			uint32_t space_occupied_until = 0;

			for(uint32_t i = 0; i < tuple_count; i++)
			{
				// if the new tuple is suppossed to be inserted at i then process it first
				if(i == tuple_to_insert_at)
				{
					space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
					result++;
					if(space_occupied_until > limit)
						break;
				}

				// process the ith tuple
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples_on_page(page1, bpttd_p->page_size, &(bpttd_p->index_def->size_def), i, i);
				space_occupied_until = space_occupied_by_ith_tuple + space_occupied_until;
				result++;
				if(space_occupied_until > limit)
					break;
			}

			if(space_occupied_until <= limit && tuple_count == tuple_to_insert_at && result == tuple_count)
			{
				// if the new tuple is suppossed to be inserted at the end then append its occupied size to the end
				space_occupied_until = space_occupied_by_new_tuple + space_occupied_until;
				result++;
			}
		}

		return result;
	}
}

int must_split_for_insert_bplus_tree_interior_page(persistent_page page1, const void* tuple_to_insert, const bplus_tree_tuple_defs* bpttd_p)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_append_tuple_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def), tuple_to_insert))
		return 0;

	// we need to make sure that the new_tuple will not be fitting on the page even after a compaction
	// if tuple_to_insert can fit on the page after a compaction, then you should not be splitting the page
	uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->index_def, tuple_to_insert) + get_additional_space_overhead_per_tuple_on_page(bpttd_p->page_size, &(bpttd_p->index_def->size_def));
	uint32_t space_available_page1 = get_space_allotted_to_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def)) - get_space_occupied_by_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

	// we fail here because the new tuple can be accomodated in page1, if you had considered compacting the page
	if(space_available_page1 >= space_occupied_by_new_tuple)
		return 0;

	return 1;
}

int split_insert_bplus_tree_interior_page(persistent_page page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p, void* output_parent_insert)
{
	// check if a page must split to accomodate the new tuple
	if(!must_split_for_insert_bplus_tree_interior_page(page1, tuple_to_insert, bpttd_p))
		return 0;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page1.page, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

	// total number of tuples we would be dealing with
	//uint32_t total_tuple_count = page1_tuple_count + 1;

	// lingo for variables page1 => page to be split, page2 => page that will be allocated to handle the split

	// final tuple count of the page that will be split
	uint32_t final_tuple_count_page1 = calculate_final_tuple_count_of_page_to_be_split(page1.page, tuple_to_insert, tuple_to_insert_at, bpttd_p);

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
	persistent_page page2;
	page2.page = dam_p->get_new_page_with_write_lock(dam_p->context, &(page2.page_id));

	// return with a split failure if the page2 could not be allocated
	if(page2.page == NULL)
		return 0;

	// initialize page2 (as an interior page)
	uint32_t level = get_level_of_bplus_tree_page(page1.page, bpttd_p);	// get the level of bplus_tree we are dealing with
	init_bplus_tree_interior_page(page2, level, 0, bpttd_p, pmm_p);

	// check if page1 is last page of the level, if yes then page2 now becomes the last page of the level
	{
		// read page1 and page2 header
		bplus_tree_interior_page_header page1_hdr = get_bplus_tree_interior_page_header(page1.page, bpttd_p);
		bplus_tree_interior_page_header page2_hdr = get_bplus_tree_interior_page_header(page2.page, bpttd_p);

		// page1 now can not be the last page of the level
		// page2 will be the last page of the level if page1 was the last page of the level
		page2_hdr.is_last_page_of_level = page1_hdr.is_last_page_of_level;
		page1_hdr.is_last_page_of_level = 0;

		// set the page headers back on to the page
		set_bplus_tree_interior_page_header(page1, &page1_hdr, bpttd_p, pmm_p);
		set_bplus_tree_interior_page_header(page2, &page2_hdr, bpttd_p, pmm_p);
	}

	// while moving tuples, we assume that there will be atleast 1 tuple that will get moved from page1 to page2
	// we made this sure by all the above conditions
	// hence no need to check bounds of start_index and last_index

	// copy all required tuples from the page1 to page2
	insert_all_from_sorted_packed_page(
									page2, page1, bpttd_p->page_size,
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p
								);

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->index_def,
									tuples_stay_in_page1, page1_tuple_count - 1,
									pmm_p
								);

	// insert the new tuple (tuple_to_insert) to page1 or page2 based on "new_tuple_goes_to_page1", as calculated earlier
	if(new_tuple_goes_to_page1)
	{
		// insert the tuple_to_insert (the new tuple) at the desired index in the page1
		insert_at_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at,
									pmm_p
								);
	}
	else
	{
		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									page2, bpttd_p->page_size,
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at - tuples_stay_in_page1,
									pmm_p
								);
	}

	// now the page2 contains all the other tuples discarded from the page1, (due to the split)
	// The first tuple of the page2 has to be disintegrated as follows
	// first_tuple_page2 =>> key : page_id
	// this page_id of the first tuple needs to become the least_keys_page_id of page2
	// and we then create a output_parent_insert tuple as =>> key : page2_id
	// we then can delete the first tuple of this page2
	// and then finally we return the output_parent_insert tuple that can be inserted into parent page

	const void* first_tuple_page2 = get_nth_tuple_on_page(page2.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def), 0);
	uint32_t size_of_first_tuple_page2 = get_tuple_size(bpttd_p->index_def, first_tuple_page2);

	// get least_keys_page_id for page2 from its first tuple
	uint64_t page2_least_keys_page_id = get_child_page_id_from_index_tuple(first_tuple_page2, bpttd_p);

	// set the least_keys_page_id for page2
	{
		// read page2 header
		bplus_tree_interior_page_header page2_hdr = get_bplus_tree_interior_page_header(page2.page, bpttd_p);

		// update it's to least_keys_page_id
		page2_hdr.least_keys_page_id = page2_least_keys_page_id;

		// set page2_hdr back onto the page
		set_bplus_tree_interior_page_header(page2, &page2_hdr, bpttd_p, pmm_p);
	}

	// copy all the contents of the first_tuple_page2 to output_parent_insert
	memory_move(output_parent_insert, first_tuple_page2, size_of_first_tuple_page2);

	// now insert the pointer to the page2 in this parent tuple
	set_child_page_id_in_index_tuple(output_parent_insert, page2.page_id, bpttd_p);

	// now you may, delete the first tuple of page2
	delete_in_sorted_packed_page(
									page2, bpttd_p->page_size, 
									bpttd_p->index_def,
									0,
									pmm_p
								);

	// release lock on the page2, and mark it as modified
	dam_p->release_writer_lock_on_page(dam_p->context, page2.page, WAS_MODIFIED);

	// return success
	return 1;
}

int can_merge_bplus_tree_interior_pages(persistent_page page1, const void* separator_parent_tuple, persistent_page page2, const bplus_tree_tuple_defs* bpttd_p)
{
	// ensure that the separator_parent_tuple child_page_id is equal to the page2_id
	if(get_child_page_id_from_index_tuple(separator_parent_tuple, bpttd_p) != page2.page_id)
		return 0;

	uint32_t separator_tuple_size = get_tuple_size(bpttd_p->index_def, separator_parent_tuple);

	// check if a merge is possible, by comparing the total space requirement
	uint32_t total_space_page1 = get_space_allotted_to_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));
	uint32_t space_to_be_occupied_by_separator_tuple = separator_tuple_size + get_additional_space_overhead_per_tuple_on_page(bpttd_p->page_size, &(bpttd_p->index_def->size_def));
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples_on_page(page2.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));

	// the page1 must be able to accomodate all its current tuples, the separator tuple and the tuples of page2
	if(total_space_page1 < space_in_use_page1 + space_to_be_occupied_by_separator_tuple + space_in_use_page2)
		return 0;

	return 1;
}

int merge_bplus_tree_interior_pages(persistent_page page1, const void* separator_parent_tuple, persistent_page page2, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, const page_modification_methods* pmm_p)
{
	// ensure that we can merge
	if(!can_merge_bplus_tree_interior_pages(page1, separator_parent_tuple, page2, bpttd_p))
		return 0;

	// now we can be sure that a merge can be performed on page1 and page2

	// get the size of the separator tuple
	uint32_t separator_tuple_size = get_tuple_size(bpttd_p->index_def, separator_parent_tuple);

	// check if page2 was the last page of the level, if so page1 is now the last page of the level
	{
		// read page1 and page2 header
		bplus_tree_interior_page_header page1_hdr = get_bplus_tree_interior_page_header(page1.page, bpttd_p);
		bplus_tree_interior_page_header page2_hdr = get_bplus_tree_interior_page_header(page2.page, bpttd_p);

		// if page2 was the last page of the level, then page1 will now inherit that
		page1_hdr.is_last_page_of_level = page2_hdr.is_last_page_of_level;
		// below operation is not needed, as page2 is already going to be freed
		page2_hdr.is_last_page_of_level = 0;

		// set the page headers back on to the page
		set_bplus_tree_interior_page_header(page1, &page1_hdr, bpttd_p, pmm_p);
		set_bplus_tree_interior_page_header(page2, &page2_hdr, bpttd_p, pmm_p);
	}

	// now we construct a separator tuple and insert it into the page 1
	void* separator_tuple = malloc(sizeof(char) * separator_tuple_size);
	memory_move(separator_tuple, separator_parent_tuple, sizeof(char) * separator_tuple_size);

	// update child_page_id of separator_tuple with the value from least_keys_page_id
	uint64_t separator_tuple_child_page_id = get_least_keys_page_id_of_bplus_tree_interior_page(page2.page, bpttd_p);
	set_child_page_id_in_index_tuple(separator_tuple, separator_tuple_child_page_id, bpttd_p);

	// insert separator tuple in the page1, at the end
	insert_at_in_sorted_packed_page(
									page1, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									separator_tuple, 
									get_tuple_count_on_page(page1.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def)),
									pmm_p
								);

	// free memory allocated for separator_tuple
	free(separator_tuple);

	// now, we can safely transfer all tuples from page2 to page1

	// only if there are any tuples to move from page2
	uint32_t tuple_count_page2 = get_tuple_count_on_page(page2.page, bpttd_p->page_size, &(bpttd_p->index_def->size_def));
	if(tuple_count_page2 > 0)
		// only if there are any tuples to move
		insert_all_from_sorted_packed_page(
									page1, page2, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									0, tuple_count_page2 - 1,
									pmm_p
								);

	return 1;
}