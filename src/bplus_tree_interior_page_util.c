#include<bplus_tree_interior_page_util.h>

#include<sorted_packed_page_util.h>
#include<bplus_tree_interior_page_header.h>
#include<bplus_tree_index_tuple_functions_util.h>

#include<page_layout.h>
#include<tuple.h>

#include<string.h>
#include<stdlib.h>

int init_bplus_tree_interior_page(void* page, uint32_t level, int is_last_page_of_level, const bplus_tree_tuple_defs* bpttd_p)
{
	int inited = init_page(page, bpttd_p->page_size, sizeof_INTERIOR_PAGE_HEADER(bpttd_p), bpttd_p->index_def);
	if(!inited)
		return 0;
	set_type_of_page(page, bpttd_p->page_size, BPLUS_TREE_INTERIOR_PAGE);
	set_level_of_bplus_tree_page(page, bpttd_p->page_size, level);
	set_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p->NULL_PAGE_ID, bpttd_p);
	set_is_last_page_of_level_of_bplus_tree_interior_page(page, is_last_page_of_level, bpttd_p);
	return 1;
}

void print_bplus_tree_interior_page(const void* page, const bplus_tree_tuple_defs* bpttd_p)
{
	print_bplus_tree_interior_page_header(page, bpttd_p);
	print_page(page, bpttd_p->page_size, bpttd_p->index_def);
}

uint32_t find_child_index_for_key(const void* page, const void* key, const bplus_tree_tuple_defs* bpttd_p)
{
	// find preceding equals in the interior pages, by comparing against all index entries
	uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									key, bpttd_p->key_def, NULL
								);

	return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
}

uint32_t find_child_index_for_record(const void* page, const void* record, const bplus_tree_tuple_defs* bpttd_p)
{
	// find preceding equals in the interior pages, by comparing against all index entries
	uint32_t child_index = find_preceding_equals_in_sorted_packed_page(
									page, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									record, bpttd_p->record_def, bpttd_p->key_element_ids
								);

	return (child_index == NO_TUPLE_FOUND) ? -1 : child_index;
}

uint64_t find_child_page_id_by_child_index(const void* page, uint32_t index, const bplus_tree_tuple_defs* bpttd_p)
{
	// if the index is -1, return the page_id stored in the header
	if(index == -1)
		return get_least_keys_page_id_of_bplus_tree_interior_page(page, bpttd_p);

	// tuple of the interior page that is at index
	const void* index_tuple = get_nth_tuple(page, bpttd_p->page_size, bpttd_p->index_def, index);

	// get child_page_id of the index tuple
	uint64_t child_page_id = get_child_page_id_from_index_tuple(index_tuple, bpttd_p);

	return child_page_id;
}

static uint32_t calculate_final_tuple_count_of_page_to_be_split(void* page1, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p)
{
	uint32_t tuple_count = get_tuple_count(page1, bpttd_p->page_size, bpttd_p->index_def);

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
		uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->index_def, tuple_to_insert) + get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->index_def);

		// this is the total space available to you to store the tuples
		uint32_t space_allotted_to_tuples = get_space_allotted_to_all_tuples(page1, bpttd_p->page_size, bpttd_p->index_def);

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
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples(page1, bpttd_p->page_size, bpttd_p->index_def, i, i);
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
				uint32_t space_occupied_by_ith_tuple = get_space_occupied_by_tuples(page1, bpttd_p->page_size, bpttd_p->index_def, i, i);
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

int split_insert_bplus_tree_interior_page(void* page1, uint64_t page1_id, const void* tuple_to_insert, uint32_t tuple_to_insert_at, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p, void* output_parent_insert)
{
	// do not perform a split if the page can accomodate the new tuple
	if(can_insert_tuple(page1, bpttd_p->page_size, bpttd_p->index_def, tuple_to_insert))
		return 0;

	// we need to make sure that the new_tuple will not be fitting on the page even after a compaction
	// if it does then you should not be calling this function
	uint32_t space_occupied_by_new_tuple = get_tuple_size(bpttd_p->index_def, tuple_to_insert) + get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->index_def);
	uint32_t space_available_page1 = get_space_allotted_to_all_tuples(page1, bpttd_p->page_size, bpttd_p->index_def) - get_space_occupied_by_all_tuples(page1, bpttd_p->page_size, bpttd_p->index_def);

	// we fail here because the new tuple can be accomodated in page1, if you had considered compacting the page
	if(space_available_page1 >= space_occupied_by_new_tuple)
		return 0;

	// if the index of the new tuple was not provided then calculate it
	if(tuple_to_insert_at == NO_TUPLE_FOUND)
		tuple_to_insert_at = find_insertion_point_in_sorted_packed_page(
									page1, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuple_to_insert
								);

	// current tuple count of the page to be split
	uint32_t page1_tuple_count = get_tuple_count(page1, bpttd_p->page_size, bpttd_p->index_def);

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
	uint64_t page2_id;
	void* page2 = dam_p->get_new_page_with_write_lock(dam_p->context, &page2_id);

	// return with a split failure if the page2 could not be allocated
	if(page2 == NULL)
		return 0;

	// initialize page2 (as an interior page)
	uint32_t level = get_level_of_bplus_tree_page(page1, bpttd_p->page_size);	// get the level of bplus_tree we are dealing with
	init_bplus_tree_interior_page(page2, level, 0, bpttd_p);

	// check if page1 is last page of the level
	int page1_is_last_page_of_level = is_last_page_of_level_of_bplus_tree_interior_page(page1, bpttd_p);

	// page1 now can not be the last page of the level
	// page2 will be the last page of the level if page1 was the last page of the level
	set_is_last_page_of_level_of_bplus_tree_interior_page(page1, 0, bpttd_p);
	set_is_last_page_of_level_of_bplus_tree_interior_page(page2, page1_is_last_page_of_level, bpttd_p);
	page1_is_last_page_of_level = 0;

	// while moving tuples, we assume that there will be atleast 1 tuple that will get moved from page1 to page2
	// we made this sure by all the above conditions
	// hence no need to check bounds of start_index and last_index

	// copy all required tuples from the page1 to page2
	insert_all_from_sorted_packed_page(
									page2, page1, bpttd_p->page_size,
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// delete the corresponding (copied) tuples in the page1
	delete_all_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->index_def,
									tuples_stay_in_page1, page1_tuple_count - 1
								);

	// insert the new tuple (tuple_to_insert) to page1 or page2 based on "new_tuple_goes_to_page1", as calculated earlier
	if(new_tuple_goes_to_page1)
	{
		// if can not insert then compact the page first
		if(!can_insert_tuple(page1, bpttd_p->page_size, bpttd_p->index_def, tuple_to_insert))
			run_page_compaction(page1, bpttd_p->page_size, bpttd_p->index_def, 0, 1);

		// insert the tuple_to_insert (the new tuple) at the desired index in the page1
		insert_at_in_sorted_packed_page(
									page1, bpttd_p->page_size,
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at
								);
	}
	else
	{
		// if can not insert then compact the page first
		if(!can_insert_tuple(page2, bpttd_p->page_size, bpttd_p->index_def, tuple_to_insert))
			run_page_compaction(page2, bpttd_p->page_size, bpttd_p->index_def, 0, 1);

		// insert the tuple_to_insert (the new tuple) at the desired index in the page2
		insert_at_in_sorted_packed_page(
									page2, bpttd_p->page_size,
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									tuple_to_insert,
									tuple_to_insert_at - tuples_stay_in_page1
								);
	}

	// now the page2 contains all the other tuples discarded from the page1, (due to the split)
	// The first tuple of the page2 has to be disintegrated as follows
	// first_tuple_page2 =>> key : page_id
	// this page_id of the first tuple needs to become the least_keys_page_id of page2
	// and we then create a output_parent_insert tuple as =>> key : page2_id
	// we then can delete the first tuple of this page2
	// and then finally we return the output_parent_insert tuple that can be inserted into parent page

	const void* first_tuple_page2 = get_nth_tuple(page2, bpttd_p->page_size, bpttd_p->index_def, 0);
	uint32_t size_of_first_tuple_page2 = get_tuple_size(bpttd_p->index_def, first_tuple_page2);

	// get least_keys_page_id for page2 from its first tuple
	uint64_t page2_least_keys_page_id = get_child_page_id_from_index_tuple(first_tuple_page2, bpttd_p);

	// set the least_keys_page_id for page2
	set_least_keys_page_id_of_bplus_tree_interior_page(page2, page2_least_keys_page_id, bpttd_p);

	// copy all the contents of the first_tuple_page2 to output_parent_insert
	memmove(output_parent_insert, first_tuple_page2, sizeof(char) * size_of_first_tuple_page2);

	// now insert the pointer to the page2 in this parent tuple
	set_child_page_id_in_index_tuple(output_parent_insert, page2_id, bpttd_p);

	// now you may, delete the first tuple of page2
	delete_in_sorted_packed_page(
									page2, bpttd_p->page_size, 
									bpttd_p->index_def,
									0
								);

	// release lock on the page2, and mark it as modified
	dam_p->release_writer_lock_on_page(dam_p->context, page2, 1);

	// return success
	return 1;
}

int merge_bplus_tree_interior_pages(void* page1, uint64_t page1_id, const void* separator_parent_tuple, void* page2, uint64_t page2_id, const bplus_tree_tuple_defs* bpttd_p, const data_access_methods* dam_p)
{
	// ensure that the separator_parent_tuple child_page_id is equal to the page2_id
	if(get_child_page_id_from_index_tuple(separator_parent_tuple, bpttd_p) != page2_id)
		return 0;

	uint32_t separator_tuple_size = get_tuple_size(bpttd_p->index_def, separator_parent_tuple);

	// check if a merge can be performed
	uint32_t total_space_page1 = get_space_allotted_to_all_tuples(page1, bpttd_p->page_size, bpttd_p->index_def);
	uint32_t space_in_use_page1 = get_space_occupied_by_all_tuples(page1, bpttd_p->page_size, bpttd_p->index_def);
	uint32_t space_to_be_occupied_by_separator_tuple = separator_tuple_size + get_additional_space_overhead_per_tuple(bpttd_p->page_size, bpttd_p->index_def);
	uint32_t space_in_use_page2 = get_space_occupied_by_all_tuples(page2, bpttd_p->page_size, bpttd_p->index_def);
	uint32_t space_in_tomb_stones_page2 = get_space_occupied_by_all_tomb_stones(page2, bpttd_p->page_size, bpttd_p->index_def);

	// the page1 must be able to accomodate all its current tuples, the separator tuple and the tuples of page2
	// inserting tuples from one page to the another is done while discarding tomb_stones during the insertion
	if(total_space_page1 < space_in_use_page1 + space_to_be_occupied_by_separator_tuple + space_in_use_page2 - space_in_tomb_stones_page2)
		return 0;

	// now we can be sure that a merge can be performed on page1 and page2

	// check if page2 was the last page of the level
	int page2_is_last_page_of_level = is_last_page_of_level_of_bplus_tree_interior_page(page2, bpttd_p);
	// if page2 was the last page of the level, then page1 will now inherit that
	set_is_last_page_of_level_of_bplus_tree_interior_page(page1, page2_is_last_page_of_level, bpttd_p);
	// the 2 steps below are not needed, but it is performed to keep things clean
	page2_is_last_page_of_level = 0;
	set_is_last_page_of_level_of_bplus_tree_interior_page(page2, page2_is_last_page_of_level, bpttd_p);

	// make sure that there is enough free space on page1 else defragment the page first
	uint32_t free_space_page1 = get_free_space(page1, bpttd_p->page_size, bpttd_p->index_def);
	if(free_space_page1 < space_to_be_occupied_by_separator_tuple + space_in_use_page2)
		run_page_compaction(page1, bpttd_p->page_size, bpttd_p->index_def, 0, 1);

	// now we construct a separator tuple and insert it into the page 1
	void* separator_tuple = malloc(sizeof(char) * separator_tuple_size);
	memmove(separator_tuple, separator_parent_tuple, sizeof(char) * separator_tuple_size);

	// update child_page_id of separator_tuple with the value from least_keys_page_id
	uint64_t separator_tuple_child_page_id = get_least_keys_page_id_of_bplus_tree_interior_page(page2, bpttd_p);
	set_child_page_id_in_index_tuple(separator_tuple, separator_tuple_child_page_id, bpttd_p);

	// insert separator tuple in the page1, at the end
	insert_at_in_sorted_packed_page(
									page1, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									separator_tuple, 
									get_tuple_count(page1, bpttd_p->page_size, bpttd_p->index_def)
								);

	// free memory allocated for separator_tuple
	free(separator_tuple);

	// now, we can safely transfer all tuples from page2 to page1

	// only if there are any tuples to move from page2
	uint32_t tuple_count_page2 = get_tuple_count(page2, bpttd_p->page_size, bpttd_p->index_def);
	if(tuple_count_page2 > 0)
		// only if there are any tuples to move
		insert_all_from_sorted_packed_page(
									page1, page2, bpttd_p->page_size, 
									bpttd_p->index_def, NULL, bpttd_p->key_element_count,
									0, tuple_count_page2 - 1
								);

	return 1;
}